"""
Kafka Producers for Vietnamese Trading System
Phase 2: Messaging Layer Implementation - Task 2.2

This module provides comprehensive Kafka producer implementations including:
- MarketDataProducer: High-throughput OHLCV data streaming
- IndicatorProducer: Technical indicator calculations and streaming
- Base producer framework with reliability features
- Vietnamese market specific optimizations
"""

from .base_producer import (
    BaseKafkaProducer, ProducerConfig, MessageMetadata, 
    CircuitBreakerState, MessageBuffer, RateLimiter, HealthCheckMixin
)
from .market_data_producer import MarketDataProducer, MarketDataConfig
from .indicator_producer import IndicatorProducer, IndicatorConfig

# Re-export for backward compatibility and convenience
import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logging import StructuredLogger


class ProducerManager:
    """Manager for all Kafka producers with coordinated lifecycle management"""
    
    def __init__(self, producer_config: ProducerConfig):
        self.producer_config = producer_config
        self.logger = StructuredLogger("ProducerManager")
        
        # Producer instances
        self.market_data_producer: Optional[MarketDataProducer] = None
        self.indicator_producer: Optional[IndicatorProducer] = None
        
        # Manager state
        self.is_running = False
        self.producers = {}
        
        self.logger.info("ProducerManager initialized",
            bootstrap_servers=producer_config.bootstrap_servers,
            client_id=producer_config.client_id
        )
    
    async def initialize_producers(
        self, 
        market_config: Optional[MarketDataConfig] = None,
        indicator_config: Optional[IndicatorConfig] = None,
        vnstock_collector=None,
        vnquant_collector=None
    ):
        """Initialize all producers with their configurations"""
        try:
            # Initialize MarketDataProducer if config provided
            if market_config:
                self.market_data_producer = MarketDataProducer(
                    self.producer_config, market_config, vnstock_collector, vnquant_collector
                )
                self.producers["market_data"] = self.market_data_producer
                self.logger.info("MarketDataProducer initialized")
            
            # Initialize IndicatorProducer if config provided
            if indicator_config:
                self.indicator_producer = IndicatorProducer(
                    self.producer_config, indicator_config
                )
                self.producers["indicators"] = self.indicator_producer
                self.logger.info("IndicatorProducer initialized")
            
            if not self.producers:
                raise ValueError("No producers configured")
            
            self.logger.info("All producers initialized",
                producers=list(self.producers.keys())
            )
            
        except Exception as e:
            self.logger.error("Producer initialization failed", error=str(e))
            raise
    
    async def start_all_producers(self):
        """Start all producers and their streaming/calculation loops"""
        if self.is_running:
            self.logger.warning("Producers already running")
            return
        
        try:
            # Start market data streaming
            if self.market_data_producer:
                await self.market_data_producer.start_streaming()
                self.logger.info("Market data streaming started")
            
            # Start indicator calculations
            if self.indicator_producer:
                await self.indicator_producer.start_calculation_loop()
                self.logger.info("Indicator calculations started")
            
            self.is_running = True
            self.logger.info("All producers started successfully")
            
        except Exception as e:
            self.logger.error("Failed to start producers", error=str(e))
            await self.stop_all_producers()  # Cleanup on failure
            raise
    
    async def stop_all_producers(self):
        """Stop all producers and cleanup resources"""
        if not self.is_running:
            return
        
        try:
            # Stop market data streaming
            if self.market_data_producer:
                await self.market_data_producer.stop_streaming()
                self.logger.info("Market data streaming stopped")
            
            # Stop indicator calculations
            if self.indicator_producer:
                await self.indicator_producer.stop_calculation_loop()
                self.logger.info("Indicator calculations stopped")
            
            # Close all producers
            for name, producer in self.producers.items():
                producer.close()
                self.logger.info(f"Producer {name} closed")
            
            self.is_running = False
            self.logger.info("All producers stopped")
            
        except Exception as e:
            self.logger.error("Error stopping producers", error=str(e))
    
    async def feed_market_data(self, symbol: str, timestamp: datetime, ohlcv: Dict[str, float]):
        """Feed market data to both market data and indicator producers"""
        try:
            # Send to market data stream
            if self.market_data_producer:
                # This would be called from external data feed
                pass
            
            # Add to indicator calculations
            if self.indicator_producer:
                await self.indicator_producer.add_market_data(symbol, timestamp, ohlcv)
            
        except Exception as e:
            self.logger.error("Failed to feed market data",
                symbol=symbol,
                error=str(e)
            )
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all producers"""
        health_status = {
            "overall_status": "healthy",
            "is_running": self.is_running,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "producers": {}
        }
        
        overall_healthy = True
        
        for name, producer in self.producers.items():
            if hasattr(producer, 'health_check'):
                producer_health = producer.health_check()
                health_status["producers"][name] = producer_health
                
                if producer_health.get("status") != "healthy":
                    overall_healthy = False
            else:
                health_status["producers"][name] = {"status": "unknown"}
        
        if not overall_healthy:
            health_status["overall_status"] = "degraded"
        
        return health_status
    
    def get_comprehensive_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics from all producers"""
        metrics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "manager": {
                "is_running": self.is_running,
                "producers_count": len(self.producers)
            },
            "producers": {}
        }
        
        for name, producer in self.producers.items():
            if hasattr(producer, 'get_metrics'):
                producer_metrics = producer.get_metrics()
                if hasattr(producer, 'get_streaming_stats'):
                    # Market data producer
                    streaming_stats = producer.get_streaming_stats()
                    metrics["producers"][name] = {**producer_metrics, **streaming_stats}
                elif hasattr(producer, 'get_calculation_stats'):
                    # Indicator producer  
                    calc_stats = producer.get_calculation_stats()
                    metrics["producers"][name] = {**producer_metrics, **calc_stats}
                else:
                    metrics["producers"][name] = producer_metrics
        
        return metrics
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup"""
        await self.stop_all_producers()


# Factory functions for easy producer creation
def create_market_data_producer(
    bootstrap_servers: str = "localhost:9092",
    market_config: Optional[MarketDataConfig] = None
) -> MarketDataProducer:
    """Create a configured MarketDataProducer"""
    producer_config = ProducerConfig(bootstrap_servers=bootstrap_servers)
    if market_config is None:
        market_config = MarketDataConfig()
    
    return MarketDataProducer(producer_config, market_config)


def create_indicator_producer(
    bootstrap_servers: str = "localhost:9092", 
    indicator_config: Optional[IndicatorConfig] = None
) -> IndicatorProducer:
    """Create a configured IndicatorProducer"""
    producer_config = ProducerConfig(bootstrap_servers=bootstrap_servers)
    if indicator_config is None:
        indicator_config = IndicatorConfig()
    
    return IndicatorProducer(producer_config, indicator_config)


def create_producer_manager(
    bootstrap_servers: str = "localhost:9092",
    market_config: Optional[MarketDataConfig] = None,
    indicator_config: Optional[IndicatorConfig] = None
) -> ProducerManager:
    """Create a configured ProducerManager with all producers"""
    producer_config = ProducerConfig(bootstrap_servers=bootstrap_servers)
    manager = ProducerManager(producer_config)
    
    # Initialize with provided configs
    asyncio.create_task(manager.initialize_producers(market_config, indicator_config))
    
    return manager


# Export all classes and functions
__all__ = [
    # Base framework
    "BaseKafkaProducer", "ProducerConfig", "MessageMetadata", 
    "CircuitBreakerState", "MessageBuffer", "RateLimiter", "HealthCheckMixin",
    
    # Specific producers
    "MarketDataProducer", "MarketDataConfig",
    "IndicatorProducer", "IndicatorConfig", 
    
    # Management
    "ProducerManager",
    
    # Factory functions
    "create_market_data_producer",
    "create_indicator_producer", 
    "create_producer_manager"
]
