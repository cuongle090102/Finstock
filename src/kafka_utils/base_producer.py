"""
Base Kafka Producer Framework for Vietnamese Trading System
Phase 2: Messaging Layer Implementation - Task 2.2

This module provides a comprehensive base producer with:
- Connection management and error handling
- Message serialization with compression
- Rate limiting and batching optimization
- Circuit breaker pattern for resilience
- Dead letter queue for failed messages
- Performance monitoring and logging
- Vietnamese market specific optimizations
"""

import asyncio
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import defaultdict, deque
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from kafka import KafkaProducer
    from kafka.errors import (
        KafkaError, KafkaTimeoutError, MessageSizeTooLargeError,
        TopicAuthorizationFailedError, BrokerNotAvailableError
    )
    KAFKA_AVAILABLE = True
except ImportError:
    logging.getLogger(__name__).warning("kafka-python not installed. Install with: pip install kafka-python")
    KafkaProducer = None
    KafkaError = Exception
    KAFKA_AVAILABLE = False

from src.utils.logging import StructuredLogger


@dataclass
class ProducerConfig:
    """Configuration for Kafka producers"""
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "finstock-producer"
    
    # Performance settings
    batch_size: int = 16384  # 16KB
    linger_ms: int = 10
    buffer_memory: int = 33554432  # 32MB
    compression_type: str = "snappy"
    max_request_size: int = 1048576  # 1MB
    
    # Reliability settings  
    acks: str = "all"  # Required for idempotent producer
    retries: int = 3
    retry_backoff_ms: int = 100
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 1
    
    # Timeout settings
    request_timeout_ms: int = 30000  # 30 seconds
    delivery_timeout_ms: int = 120000  # 2 minutes
    
    # Circuit breaker settings
    circuit_breaker_enabled: bool = True
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_timeout: int = 60  # seconds
    
    # Dead letter queue settings
    dlq_enabled: bool = True
    dlq_topic_suffix: str = "_dlq"
    
    # Rate limiting settings
    rate_limit_enabled: bool = True
    max_messages_per_second: Optional[int] = None
    
    # Monitoring settings
    metrics_enabled: bool = True
    log_level: str = "INFO"


@dataclass
class MessageMetadata:
    """Metadata for Kafka messages"""
    correlation_id: str
    timestamp: datetime
    source: str
    version: str = "1.0"
    retry_count: int = 0
    original_topic: Optional[str] = None


class CircuitBreakerState:
    """Circuit breaker implementation for producer resilience"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def can_execute(self) -> bool:
        """Check if operation can be executed based on circuit breaker state"""
        with self._lock:
            if self.state == "CLOSED":
                return True
            elif self.state == "OPEN":
                if self.last_failure_time and (time.time() - self.last_failure_time) > self.timeout:
                    self.state = "HALF_OPEN"
                    return True
                return False
            elif self.state == "HALF_OPEN":
                return True
            return False
    
    def on_success(self):
        """Record successful operation"""
        with self._lock:
            self.failure_count = 0
            self.state = "CLOSED"
    
    def on_failure(self):
        """Record failed operation"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"


class MessageBuffer:
    """Thread-safe message buffer for batching"""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.messages = deque()
        self._lock = threading.Lock()
    
    def add_message(self, topic: str, key: str, value: dict, metadata: MessageMetadata) -> bool:
        """Add message to buffer. Returns True if buffer is full"""
        with self._lock:
            self.messages.append((topic, key, value, metadata))
            return len(self.messages) >= self.max_size
    
    def get_batch(self, batch_size: Optional[int] = None) -> List:
        """Get a batch of messages and clear the buffer"""
        with self._lock:
            if not self.messages:
                return []
            
            if batch_size is None:
                batch = list(self.messages)
                self.messages.clear()
            else:
                batch = []
                for _ in range(min(batch_size, len(self.messages))):
                    batch.append(self.messages.popleft())
            
            return batch
    
    def size(self) -> int:
        """Get current buffer size"""
        with self._lock:
            return len(self.messages)


class RateLimiter:
    """Token bucket rate limiter for message production"""
    
    def __init__(self, max_messages_per_second: Optional[int] = None):
        if max_messages_per_second is None:
            self.enabled = False
            return
            
        self.enabled = True
        self.max_messages = max_messages_per_second
        self.tokens = max_messages_per_second
        self.last_refill = time.time()
        self._lock = threading.Lock()
    
    def can_send(self) -> bool:
        """Check if a message can be sent based on rate limit"""
        if not self.enabled:
            return True
            
        with self._lock:
            now = time.time()
            
            # Refill tokens
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * self.max_messages
            self.tokens = min(self.max_messages, self.tokens + tokens_to_add)
            self.last_refill = now
            
            # Check if we can send
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False
    
    def wait_time(self) -> float:
        """Get time to wait before next message can be sent"""
        if not self.enabled or self.tokens >= 1:
            return 0.0
        
        with self._lock:
            return (1 - self.tokens) / self.max_messages


class BaseKafkaProducer(ABC):
    """Base class for all Kafka producers with comprehensive functionality"""
    
    def __init__(self, config: ProducerConfig, topic: str):
        self.config = config
        self.topic = topic
        self.logger = StructuredLogger(f"KafkaProducer-{topic}")
        
        # Initialize components
        self.circuit_breaker = CircuitBreakerState(
            config.circuit_breaker_failure_threshold,
            config.circuit_breaker_timeout
        ) if config.circuit_breaker_enabled else None
        
        self.rate_limiter = RateLimiter(config.max_messages_per_second)
        self.message_buffer = MessageBuffer()
        
        # Metrics
        self.metrics = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
            "send_time_total": 0.0,
            "circuit_breaker_trips": 0,
            "rate_limit_blocks": 0
        }
        
        # Producer instance (initialized lazily)
        self._producer = None
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"producer-{topic}")
        
        self.logger.info("BaseKafkaProducer initialized",
            topic=topic,
            bootstrap_servers=config.bootstrap_servers,
            batch_size=config.batch_size,
            compression_type=config.compression_type,
            circuit_breaker_enabled=config.circuit_breaker_enabled,
            rate_limit_enabled=config.rate_limit_enabled
        )
    
    @property
    def producer(self) -> KafkaProducer:
        """Lazy initialization of Kafka producer"""
        if self._producer is None:
            if not KAFKA_AVAILABLE:
                raise RuntimeError("kafka-python library not available")
            
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=f"{self.config.client_id}-{self.topic}",
                
                # Serialization
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                value_serializer=self._serialize_message,
                
                # Performance settings
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                buffer_memory=self.config.buffer_memory,
                compression_type=self.config.compression_type,
                max_request_size=self.config.max_request_size,
                
                # Reliability settings
                acks=self.config.acks,
                retries=self.config.retries,
                retry_backoff_ms=self.config.retry_backoff_ms,
                enable_idempotence=self.config.enable_idempotence,
                max_in_flight_requests_per_connection=self.config.max_in_flight_requests_per_connection,
                
                # Timeout settings
                request_timeout_ms=self.config.request_timeout_ms,
                delivery_timeout_ms=self.config.delivery_timeout_ms
            )
        
        return self._producer
    
    def _serialize_message(self, message: dict) -> bytes:
        """Serialize message with metadata"""
        try:
            def json_serializer(obj):
                """Custom JSON serializer for datetime and other objects"""
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return str(obj)
            
            return json.dumps(message, default=json_serializer, ensure_ascii=False).encode('utf-8')
        except Exception as e:
            self.logger.error("Message serialization failed", error=str(e), message_type=type(message))
            raise
    
    def _create_metadata(self, source: str, correlation_id: Optional[str] = None) -> MessageMetadata:
        """Create message metadata"""
        return MessageMetadata(
            correlation_id=correlation_id or str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc),
            source=source,
            version="1.0"
        )
    
    async def send_message_async(
        self,
        key: str,
        value: dict,
        source: str,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """Send a message asynchronously with full error handling and monitoring"""
        
        # Check circuit breaker
        if self.circuit_breaker and not self.circuit_breaker.can_execute():
            self.metrics["circuit_breaker_trips"] += 1
            self.logger.warning("Circuit breaker is OPEN, message dropped", key=key)
            return False
        
        # Check rate limiting
        if not self.rate_limiter.can_send():
            wait_time = self.rate_limiter.wait_time()
            if wait_time > 0:
                self.metrics["rate_limit_blocks"] += 1
                await asyncio.sleep(wait_time)
        
        # Create message with metadata
        metadata = self._create_metadata(source, correlation_id)
        
        # Validate and process message
        try:
            processed_value = await self.process_message(value, metadata)
        except Exception as e:
            self.logger.error("Message processing failed", 
                key=key,
                error=str(e),
                correlation_id=metadata.correlation_id
            )
            return False
        
        # Create final message structure with serialized metadata
        metadata_dict = asdict(metadata)
        # Convert datetime to string
        if 'timestamp' in metadata_dict:
            metadata_dict['timestamp'] = metadata_dict['timestamp'].isoformat()
            
        final_message = {
            "data": processed_value,
            "metadata": metadata_dict
        }
        
        # Add headers
        kafka_headers = []
        if headers:
            kafka_headers.extend([(k, v.encode('utf-8')) for k, v in headers.items()])
        kafka_headers.append(("correlation_id", metadata.correlation_id.encode('utf-8')))
        
        # Send message
        start_time = time.time()
        try:
            future = self.producer.send(
                self.topic,
                key=key,
                value=final_message,
                headers=kafka_headers
            )
            
            # Wait for result with timeout
            record_metadata = future.get(timeout=self.config.request_timeout_ms / 1000)
            
            # Update metrics
            send_time = time.time() - start_time
            self.metrics["messages_sent"] += 1
            self.metrics["send_time_total"] += send_time
            message_size = len(json.dumps(final_message).encode('utf-8'))
            self.metrics["bytes_sent"] += message_size
            
            # Circuit breaker success
            if self.circuit_breaker:
                self.circuit_breaker.on_success()
            
            self.logger.debug("Message sent successfully",
                key=key,
                topic=self.topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                send_time_ms=send_time * 1000,
                message_size=message_size,
                correlation_id=metadata.correlation_id
            )
            
            return True
            
        except Exception as e:
            # Update failure metrics
            self.metrics["messages_failed"] += 1
            
            # Circuit breaker failure
            if self.circuit_breaker:
                self.circuit_breaker.on_failure()
            
            self.logger.error("Message send failed",
                key=key,
                topic=self.topic,
                error=str(e),
                error_type=type(e).__name__,
                correlation_id=metadata.correlation_id,
                retry_count=metadata.retry_count
            )
            
            # Send to dead letter queue if enabled
            if self.config.dlq_enabled:
                await self._send_to_dlq(key, final_message, metadata, str(e))
            
            return False
    
    async def _send_to_dlq(self, key: str, message: dict, metadata: MessageMetadata, error: str):
        """Send failed message to dead letter queue"""
        dlq_topic = f"{self.topic}{self.config.dlq_topic_suffix}"
        dlq_message = {
            "original_topic": self.topic,
            "original_key": key,
            "original_message": message,
            "failure_reason": error,
            "failure_timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": asdict(metadata)
        }
        
        try:
            future = self.producer.send(dlq_topic, key=key, value=dlq_message)
            future.get(timeout=10)  # Shorter timeout for DLQ
            
            self.logger.info("Message sent to DLQ",
                original_key=key,
                dlq_topic=dlq_topic,
                failure_reason=error
            )
        except Exception as dlq_error:
            self.logger.error("Failed to send to DLQ",
                original_key=key,
                dlq_topic=dlq_topic,
                dlq_error=str(dlq_error)
            )
    
    def send_message_sync(
        self,
        key: str,
        value: dict,
        source: str,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """Synchronous wrapper for send_message_async"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.send_message_async(key, value, source, correlation_id, headers)
            )
        finally:
            loop.close()
    
    async def send_batch_async(self, messages: List[Dict[str, Any]]) -> int:
        """Send multiple messages in batch"""
        tasks = []
        for msg in messages:
            task = self.send_message_async(
                key=msg.get("key", ""),
                value=msg.get("value", {}),
                source=msg.get("source", "batch"),
                correlation_id=msg.get("correlation_id"),
                headers=msg.get("headers")
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        
        self.logger.info("Batch send completed",
            total_messages=len(messages),
            successful=success_count,
            failed=len(messages) - success_count
        )
        
        return success_count
    
    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages"""
        if self._producer:
            self._producer.flush(timeout)
    
    def close(self):
        """Close producer and cleanup resources"""
        if self._producer:
            self._producer.close()
            self._producer = None
        
        if self._executor:
            self._executor.shutdown(wait=True)
        
        self.logger.info("Producer closed", topic=self.topic, metrics=self.metrics)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics"""
        metrics = self.metrics.copy()
        metrics.update({
            "avg_send_time_ms": (
                (metrics["send_time_total"] / metrics["messages_sent"] * 1000) 
                if metrics["messages_sent"] > 0 else 0
            ),
            "success_rate": (
                metrics["messages_sent"] / (metrics["messages_sent"] + metrics["messages_failed"])
                if (metrics["messages_sent"] + metrics["messages_failed"]) > 0 else 0
            ),
            "circuit_breaker_state": self.circuit_breaker.state if self.circuit_breaker else "DISABLED",
            "buffer_size": self.message_buffer.size()
        })
        return metrics
    
    @abstractmethod
    async def process_message(self, value: dict, metadata: MessageMetadata) -> dict:
        """Process message before sending. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def validate_message(self, value: dict) -> bool:
        """Validate message format. Must be implemented by subclasses."""
        pass


class HealthCheckMixin:
    """Health check functionality for producers"""
    
    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {}
        }
        
        # Check circuit breaker state
        if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
            cb_healthy = self.circuit_breaker.state == "CLOSED"
            health_status["checks"]["circuit_breaker"] = {
                "healthy": cb_healthy,
                "state": self.circuit_breaker.state,
                "failure_count": self.circuit_breaker.failure_count
            }
            if not cb_healthy:
                health_status["status"] = "degraded"
        
        # Check producer connection
        try:
            if hasattr(self, '_producer') and self._producer:
                # This will raise an exception if Kafka is not accessible
                bootstrap_connected = True
                health_status["checks"]["kafka_connection"] = {
                    "healthy": bootstrap_connected,
                    "bootstrap_servers": self.config.bootstrap_servers
                }
            else:
                health_status["checks"]["kafka_connection"] = {
                    "healthy": False,
                    "reason": "Producer not initialized"
                }
                health_status["status"] = "unhealthy"
        except Exception as e:
            health_status["checks"]["kafka_connection"] = {
                "healthy": False,
                "error": str(e)
            }
            health_status["status"] = "unhealthy"
        
        # Check metrics
        metrics = self.get_metrics() if hasattr(self, 'get_metrics') else {}
        success_rate = metrics.get("success_rate", 0)
        health_status["checks"]["success_rate"] = {
            "healthy": success_rate >= 0.95,  # 95% success rate threshold
            "value": success_rate,
            "threshold": 0.95
        }
        
        if success_rate < 0.95:
            health_status["status"] = "degraded"
        
        return health_status


# Export classes
__all__ = [
    'BaseKafkaProducer',
    'ProducerConfig', 
    'MessageMetadata',
    'CircuitBreakerState',
    'MessageBuffer',
    'RateLimiter',
    'HealthCheckMixin'
]