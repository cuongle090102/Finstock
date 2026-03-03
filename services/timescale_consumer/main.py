#!/usr/bin/env python3
"""
TimescaleDB Consumer Service
Task 1.3: Consume market data from Kafka and write to TimescaleDB

This service:
- Consumes messages from Kafka topic 'market_data'
- Batches inserts for performance (100 records or 1 second)
- Writes to TimescaleDB table 'market_data_hot'
- Handles duplicates and data validation
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import threading

# Add src to path
sys.path.insert(0, '/app')

import yaml
import psycopg2
from psycopg2 import pool, extras
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer

from src.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TimescaleDBConsumerService:
    """TimescaleDB Consumer Service - Consume Kafka and write to database"""

    _vn30_cache = None

    @classmethod
    def _get_vn30_symbols(cls):
        if cls._vn30_cache is None:
            from src.utils.market_config import VN30_SYMBOLS
            cls._vn30_cache = VN30_SYMBOLS
        return cls._vn30_cache

    def __init__(self, config_path: str = "config.yaml"):
        """Initialize TimescaleDB Consumer Service"""
        self.config = self._load_config(config_path)
        self.running = False
        self.shutdown_event = threading.Event()

        # Statistics
        self.stats = {
            'messages_consumed': 0,
            'messages_written': 0,
            'messages_failed': 0,
            'batches_written': 0,
            'consecutive_errors': 0,
            'last_write_time': None,
            'start_time': datetime.now()
        }

        # Clients
        self.kafka_consumer = None
        self.db_pool = None

        # Message buffer for batching
        self.message_buffer = []
        self.last_flush_time = time.time()

        # Circuit breakers for external dependencies
        self.db_breaker = CircuitBreaker(
            name="timescaledb",
            failure_threshold=5,
            recovery_timeout=30.0,
            success_threshold=2,
        )
        self.kafka_breaker = CircuitBreaker(
            name="kafka",
            failure_threshold=5,
            recovery_timeout=30.0,
            success_threshold=2,
        )

        logger.info("TimescaleDB Consumer Service initialized")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    def _initialize_database(self) -> bool:
        """Initialize database connection pool"""
        try:
            db_config = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': int(os.getenv('POSTGRES_PORT', '5432')),
                'database': os.getenv('POSTGRES_DB', 'finstock_market_data'),
                'user': os.getenv('POSTGRES_USER', 'finstock_user'),
                'password': os.getenv('POSTGRES_PASSWORD', 'finstock_password')
            }

            # Create connection pool
            self.db_pool = psycopg2.pool.SimpleConnectionPool(
                1,
                self.config['database']['connection_pool_size'],
                **db_config
            )

            logger.info(
                f"Database pool created: {db_config['host']}:{db_config['port']}/{db_config['database']}"
            )

            # Test connection
            conn = self.db_pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    logger.info(f"Connected to: {version}")

                    # Check if table exists
                    cur.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = %s);",
                        (self.config['database']['table'],)
                    )
                    exists = cur.fetchone()[0]
                    if exists:
                        logger.info(f"Table '{self.config['database']['table']}' exists")
                    else:
                        logger.error(f"Table '{self.config['database']['table']}' does not exist!")
                        return False
            finally:
                self.db_pool.putconn(conn)

            return True

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return False

    def _initialize_kafka(self) -> bool:
        """Initialize Kafka consumer"""
        try:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            kafka_config = self.config['kafka']

            self.kafka_consumer = KafkaConsumer(
                kafka_config['topic'],
                bootstrap_servers=bootstrap_servers,
                group_id=kafka_config['consumer_group'],
                auto_offset_reset=kafka_config['auto_offset_reset'],
                enable_auto_commit=kafka_config['enable_auto_commit'],
                max_poll_records=kafka_config['max_poll_records'],
                value_deserializer=lambda m: self._safe_deserialize(m),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                session_timeout_ms=60000,       # 60s session timeout
                heartbeat_interval_ms=10000,    # 10s heartbeat
                max_poll_interval_ms=600000,    # 10min max poll interval
            )

            logger.info(
                f"Kafka consumer connected to {bootstrap_servers}, "
                f"topic='{kafka_config['topic']}', group='{kafka_config['consumer_group']}'"
            )
            return True

        except Exception as e:
            logger.error(f"Error initializing Kafka consumer: {e}")
            return False

    @staticmethod
    def _safe_deserialize(raw: bytes) -> Optional[Dict]:
        """Safely deserialize Kafka message, returning None on failure."""
        try:
            return json.loads(raw.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to deserialize message: {e}")
            return None

    def _validate_message(self, message: Dict[str, Any]) -> bool:
        """Validate message has required fields and sane values."""
        if message is None:
            return False

        required_fields = ['symbol', 'timestamp', 'close_price']

        for field in required_fields:
            if field not in message or message[field] is None:
                logger.warning(f"Message missing required field '{field}': {message}")
                return False

        # Validate symbol format
        symbol = message['symbol']
        if not isinstance(symbol, str) or not symbol.isalpha() or not (2 <= len(symbol) <= 4):
            logger.warning(f"Invalid symbol format: {symbol!r}")
            return False

        # Validate price ranges
        close = message.get('close_price', 0)
        if not isinstance(close, (int, float)) or close <= 0:
            logger.warning(f"Invalid close_price: {close}")
            return False

        # Validate OHLC consistency if all fields present
        high = message.get('high_price')
        low = message.get('low_price')
        if high is not None and low is not None:
            if not isinstance(high, (int, float)) or not isinstance(low, (int, float)):
                logger.warning(f"Invalid high/low types: high={high!r}, low={low!r}")
                return False
            if high < low:
                logger.warning(f"OHLC inconsistency: high ({high}) < low ({low})")
                return False

        return True

    def _write_batch_to_db(self, messages: List[Dict[str, Any]]) -> int:
        """Write batch of messages to TimescaleDB

        Returns:
            Number of successfully written messages
        """
        if not messages:
            return 0

        conn = None
        try:
            conn = self.db_pool.getconn()

            # Prepare INSERT statement
            insert_sql = """
                INSERT INTO market_data_hot (
                    timestamp, symbol, open, high, low, close, volume, exchange, is_vn30
                ) VALUES (
                    %(timestamp)s, %(symbol)s, %(open)s, %(high)s, %(low)s,
                    %(close)s, %(volume)s, %(exchange)s, %(is_vn30)s
                )
                ON CONFLICT (timestamp, symbol) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    exchange = EXCLUDED.exchange,
                    is_vn30 = EXCLUDED.is_vn30;
            """

            # Prepare data
            data_to_insert = []
            for msg in messages:
                # Parse timestamp
                if isinstance(msg['timestamp'], str):
                    timestamp = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
                else:
                    timestamp = msg['timestamp']

                record = {
                    'timestamp': timestamp,
                    'symbol': msg['symbol'],
                    'open': msg.get('open_price'),
                    'high': msg.get('high_price'),
                    'low': msg.get('low_price'),
                    'close': msg.get('close_price'),
                    'volume': msg.get('volume', 0),
                    'exchange': msg.get('exchange', 'HOSE'),
                    'is_vn30': msg.get('symbol', '') in self._get_vn30_symbols()
                }
                data_to_insert.append(record)

            # Execute batch insert
            with conn.cursor() as cur:
                execute_batch(cur, insert_sql, data_to_insert, page_size=100)
                conn.commit()

            self.stats['messages_written'] += len(messages)
            self.stats['batches_written'] += 1
            self.stats['last_write_time'] = datetime.now()
            self.stats['consecutive_errors'] = 0

            logger.info(f"Wrote batch of {len(messages)} messages to database")
            return len(messages)

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error writing batch to database: {e}")
            self.stats['messages_failed'] += len(messages)
            self.stats['consecutive_errors'] += 1
            return 0

        finally:
            if conn:
                self.db_pool.putconn(conn)

    def _should_flush(self) -> bool:
        """Check if buffer should be flushed"""
        batch_size = self.config['database']['batch_size']
        batch_timeout = self.config['database']['batch_timeout']

        # Flush if batch size reached
        if len(self.message_buffer) >= batch_size:
            return True

        # Flush if timeout reached and buffer not empty
        if (time.time() - self.last_flush_time) >= batch_timeout and self.message_buffer:
            return True

        return False

    def _flush_buffer(self):
        """Flush message buffer to database (with circuit breaker)."""
        if not self.message_buffer:
            return

        logger.debug(f"Flushing {len(self.message_buffer)} messages to database")

        try:
            written = self.db_breaker.call(self._write_batch_to_db, self.message_buffer)
        except CircuitBreakerError:
            logger.warning(
                f"Database circuit breaker OPEN — skipping flush of {len(self.message_buffer)} messages"
            )
            return

        if written > 0:
            self.message_buffer = []
            self.last_flush_time = time.time()
        else:
            # Keep messages if write failed
            logger.warning("Write failed, keeping messages in buffer")

    def _log_statistics(self):
        """Log current statistics"""
        uptime = datetime.now() - self.stats['start_time']

        logger.info("=" * 60)
        logger.info("TimescaleDB Consumer Statistics")
        logger.info("=" * 60)
        logger.info(f"Uptime: {uptime}")
        logger.info(f"Messages consumed: {self.stats['messages_consumed']}")
        logger.info(f"Messages written: {self.stats['messages_written']}")
        logger.info(f"Messages failed: {self.stats['messages_failed']}")
        logger.info(f"Batches written: {self.stats['batches_written']}")
        logger.info(f"Buffer size: {len(self.message_buffer)}")
        logger.info(f"Consecutive errors: {self.stats['consecutive_errors']}")
        logger.info(f"Last write: {self.stats['last_write_time']}")
        logger.info("=" * 60)

    def start(self):
        """Start the TimescaleDB Consumer Service"""
        logger.info("=" * 60)
        logger.info("TIMESCALEDB CONSUMER SERVICE")
        logger.info("=" * 60)

        try:
            # Initialize database
            logger.info("Initializing database connection...")
            if not self._initialize_database():
                logger.error("Failed to initialize database")
                return

            # Initialize Kafka
            logger.info("Initializing Kafka consumer...")
            if not self._initialize_kafka():
                logger.error("Failed to initialize Kafka consumer")
                return

            logger.info("TimescaleDB Consumer Service started successfully")
            logger.info(f"Consuming from topic: {self.config['kafka']['topic']}")
            logger.info(f"Writing to table: {self.config['database']['table']}")
            logger.info(f"Batch size: {self.config['database']['batch_size']}")
            logger.info("=" * 60)

            self.running = True
            stats_interval = 60  # Log stats every 60 seconds
            last_stats_time = time.time()

            # Main consume loop — use poll() instead of blocking iterator
            # to maintain heartbeats during idle periods
            while self.running and not self.shutdown_event.is_set():
                try:
                    try:
                        records = self.kafka_consumer.poll(timeout_ms=1000, max_records=100)
                    except Exception as poll_err:
                        if 'snappy' in str(poll_err).lower() or 'codec' in str(poll_err).lower():
                            logger.warning("Skipping messages with unsupported codec — seeking to end")
                            self.kafka_consumer.seek_to_end()
                            time.sleep(1)
                            continue
                        raise

                    for tp, messages in records.items():
                        for message in messages:
                            data = message.value
                            self.stats['messages_consumed'] += 1

                            if not self._validate_message(data):
                                self.stats['messages_failed'] += 1
                                continue

                            self.message_buffer.append(data)

                    # Flush if needed (batch full or timeout)
                    if self._should_flush():
                        self._flush_buffer()

                    # Log statistics periodically
                    if time.time() - last_stats_time >= stats_interval:
                        self._log_statistics()
                        last_stats_time = time.time()

                    # Check for too many consecutive errors
                    max_errors = self.config['error_handling']['max_consecutive_errors']
                    if self.stats['consecutive_errors'] >= max_errors:
                        logger.error(f"Too many consecutive errors ({self.stats['consecutive_errors']})")
                        backoff_time = min(
                            self.config['error_handling']['max_backoff_seconds'],
                            self.config['error_handling']['backoff_multiplier'] ** self.stats['consecutive_errors']
                        )
                        logger.info(f"Backing off for {backoff_time}s")
                        time.sleep(backoff_time)
                        self.stats['consecutive_errors'] = 0

                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self.stats['messages_failed'] += 1
                    continue

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)

        finally:
            self.stop()

    def stop(self):
        """Stop the TimescaleDB Consumer Service"""
        logger.info("Stopping TimescaleDB Consumer Service...")

        self.running = False
        self.shutdown_event.set()

        # Flush remaining messages
        if self.message_buffer:
            logger.info(f"Flushing {len(self.message_buffer)} remaining messages...")
            self._flush_buffer()

        # Log final statistics
        self._log_statistics()

        # Close Kafka consumer
        if self.kafka_consumer:
            try:
                self.kafka_consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")

        # Close database pool
        if self.db_pool:
            try:
                self.db_pool.closeall()
                logger.info("Database connection pool closed")
            except Exception as e:
                logger.error(f"Error closing database pool: {e}")

        logger.info("TimescaleDB Consumer Service stopped")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}")
    if hasattr(signal_handler, 'service'):
        signal_handler.service.stop()
    sys.exit(0)


def main():
    """Main entry point"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start service
    service = TimescaleDBConsumerService()
    signal_handler.service = service

    service.start()


if __name__ == "__main__":
    main()
