#!/usr/bin/env python3
"""
VnStock Producer Service
Task 1.2: Fetch Vietnamese market data and publish to Kafka

This service:
- Fetches real-time market data from VnStock API every 60 seconds
- Publishes to Kafka topic 'market_data'
- Handles rate limiting and errors with retry logic
- Only operates during Vietnamese market hours (9:00-15:00)
"""

import os
import sys
import time
import json
import signal
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Add src to path
sys.path.insert(0, '/app')

import yaml
import pytz
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError

# Import existing VnStock client
from src.ingestion.vnstock_client import VNStockClient
from src.ingestion.base import MarketDataPoint

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class VnStockProducerService:
    """VnStock Producer Service - Fetches market data and publishes to Kafka"""

    def __init__(self, config_path: str = "config.yaml"):
        """Initialize VnStock Producer Service

        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.running = False
        self.shutdown_event = threading.Event()

        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'api_calls': 0,
            'api_errors': 0,
            'consecutive_errors': 0,
            'last_success_time': None,
            'start_time': datetime.now()
        }

        # Circuit breakers
        self.kafka_breaker = CircuitBreaker(name="vnstock_kafka", failure_threshold=5, recovery_timeout=30.0)
        self.api_breaker = CircuitBreaker(name="vnstock_api", failure_threshold=5, recovery_timeout=60.0)

        # Initialize clients
        self.vnstock_client = None
        self.kafka_producer = None
        self.vn_timezone = pytz.timezone(self.config['market_hours']['timezone'])

        # Symbols to track
        self.symbols = []

        logger.info("VnStock Producer Service initialized")

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

    def _initialize_vnstock(self) -> bool:
        """Initialize VnStock client using existing implementation"""
        try:
            vnstock_config = {
                'source': self.config['vnstock']['source'],
                'timeout': self.config['vnstock']['timeout']
            }

            self.vnstock_client = VNStockClient(vnstock_config)

            if self.vnstock_client.connect():
                logger.info(f"VnStock client connected (source: {vnstock_config['source']})")
                return True
            else:
                logger.error("Failed to connect to VnStock")
                return False

        except Exception as e:
            logger.error(f"Error initializing VnStock client: {e}")
            return False

    def _initialize_kafka(self) -> bool:
        """Initialize Kafka producer"""
        try:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

            kafka_config = self.config['kafka']

            self.kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                compression_type=kafka_config['compression_type'],
                batch_size=kafka_config['batch_size'],
                linger_ms=kafka_config['linger_ms'],
                acks=kafka_config['acks'],
                retries=kafka_config['retries'],
                max_in_flight_requests_per_connection=1
            )

            logger.info(f"Kafka producer connected to {bootstrap_servers}")
            return True

        except Exception as e:
            logger.error(f"Error initializing Kafka producer: {e}")
            return False

    def _load_symbols(self) -> bool:
        """Load symbols to track based on configuration"""
        try:
            symbols_source = self.config['data_fetching']['symbols_source']

            if symbols_source == 'vn30':
                # Fetch VN30 symbols from VnStock
                logger.info("Fetching VN30 symbols...")
                self.symbols = self.vnstock_client.get_vn30_symbols()

                if not self.symbols:
                    # Fallback to hardcoded VN30 symbols
                    logger.warning("Failed to fetch VN30 symbols, using fallback list")
                    self.symbols = [
                        'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB',
                        'HPG', 'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SSB', 'SSI',
                        'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB',
                        'VRE', 'HVN', 'KDH'
                    ]

            elif symbols_source == 'custom':
                self.symbols = self.config['data_fetching']['custom_symbols']

            elif symbols_source == 'all':
                logger.info("Fetching all symbols...")
                all_symbols_df = self.vnstock_client.get_all_symbols()
                if not all_symbols_df.empty and 'symbol' in all_symbols_df.columns:
                    self.symbols = all_symbols_df['symbol'].tolist()[:50]  # Limit to 50
                else:
                    logger.error("Failed to fetch all symbols")
                    return False

            logger.info(f"Loaded {len(self.symbols)} symbols to track: {self.symbols[:5]}...")
            return len(self.symbols) > 0

        except Exception as e:
            logger.error(f"Error loading symbols: {e}")
            return False

    def is_market_open(self) -> bool:
        """Check if Vietnamese market is currently open"""
        if not self.config['market_hours']['enabled']:
            logger.debug("Market hours check disabled, returning True")
            return True  # Market hours check disabled

        now = datetime.now(self.vn_timezone)
        current_time = now.strftime("%H:%M")

        # Check if weekend
        if now.weekday() >= 5:  # Saturday=5, Sunday=6
            logger.info(f"Weekend detected: {now.strftime('%A, %Y-%m-%d')} (weekday={now.weekday()})")
            return False

        # Check market hours
        morning_start = self.config['market_hours']['morning_start']
        morning_end = self.config['market_hours']['morning_end']
        afternoon_start = self.config['market_hours']['afternoon_start']
        afternoon_end = self.config['market_hours']['afternoon_end']

        is_morning = morning_start <= current_time <= morning_end
        is_afternoon = afternoon_start <= current_time <= afternoon_end

        logger.info(
            f"Market hours check: {now.strftime('%A, %Y-%m-%d %H:%M')} | "
            f"Weekday={now.weekday()} | Morning: {is_morning} ({morning_start}-{morning_end}) | "
            f"Afternoon: {is_afternoon} ({afternoon_start}-{afternoon_end}) | "
            f"Result: {is_morning or is_afternoon}"
        )

        return is_morning or is_afternoon

    def _fetch_symbol_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch data for a single symbol

        Args:
            symbol: Stock symbol to fetch

        Returns:
            Dictionary with market data or None if failed
        """
        try:
            self.stats['api_calls'] += 1

            # Use existing VNStockClient to fetch real-time data
            data_points = self.vnstock_client.get_real_time_data([symbol])

            if data_points and len(data_points) > 0:
                data_point = data_points[0]

                # Convert to Kafka message format
                message = {
                    'symbol': data_point.symbol,
                    'timestamp': data_point.timestamp.isoformat(),
                    'open_price': float(data_point.open_price) if data_point.open_price else None,
                    'high_price': float(data_point.high_price) if data_point.high_price else None,
                    'low_price': float(data_point.low_price) if data_point.low_price else None,
                    'close_price': float(data_point.close_price) if data_point.close_price else None,
                    'volume': int(data_point.volume) if data_point.volume else 0,
                    'value': float(data_point.value) if data_point.value else None,
                    'data_source': self.config['vnstock']['source'],
                    'exchange': data_point.exchange if hasattr(data_point, 'exchange') else 'HOSE',
                    'collector': 'vnstock_producer',
                    'data_type': 'real_time'
                }

                return message
            else:
                logger.debug(f"No data returned for {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            self.stats['api_errors'] += 1
            return None

    def _fetch_all_symbols(self) -> List[Dict[str, Any]]:
        """Fetch data for all symbols in a single batch price_board call.

        Uses one API call for all symbols to stay within the 60 req/min rate limit.

        Returns:
            List of market data dictionaries
        """
        messages = []
        try:
            self.stats['api_calls'] += 1
            data_points = self.vnstock_client.get_real_time_data(self.symbols)

            for data_point in data_points:
                try:
                    message = {
                        'symbol': data_point.symbol,
                        'timestamp': data_point.timestamp.isoformat(),
                        'open_price': float(data_point.open_price) if data_point.open_price else None,
                        'high_price': float(data_point.high_price) if data_point.high_price else None,
                        'low_price': float(data_point.low_price) if data_point.low_price else None,
                        'close_price': float(data_point.close_price) if data_point.close_price else None,
                        'volume': int(data_point.volume) if data_point.volume else 0,
                        'value': float(data_point.value) if data_point.value else None,
                        'data_source': self.config['vnstock']['source'],
                        'exchange': data_point.exchange if hasattr(data_point, 'exchange') else 'HOSE',
                        'collector': 'vnstock_producer',
                        'data_type': 'real_time'
                    }
                    messages.append(message)
                except Exception as e:
                    logger.error(f"Error converting data point for {getattr(data_point, 'symbol', '?')}: {e}")

        except Exception as e:
            logger.error(f"Error in batch fetch: {e}")
            self.stats['api_errors'] += 1

        return messages

    def _publish_to_kafka(self, messages: List[Dict[str, Any]]) -> int:
        """Publish messages to Kafka (with circuit breaker).

        Args:
            messages: List of market data messages

        Returns:
            Number of successfully published messages
        """
        if not messages:
            return 0

        if self.kafka_breaker.state.value == "open":
            logger.warning(f"Kafka circuit breaker OPEN — dropping {len(messages)} messages")
            return 0

        topic = os.getenv('KAFKA_TOPIC_MARKET_DATA', self.config['kafka']['topic'])
        success_count = 0

        for message in messages:
            try:
                # Use symbol as key for partitioning
                key = message['symbol']

                # Send to Kafka
                future = self.kafka_producer.send(topic, key=key, value=message)

                # Wait for send to complete (with timeout)
                record_metadata = future.get(timeout=10)

                success_count += 1
                self.stats['messages_sent'] += 1
                self.kafka_breaker.record_success()

                logger.debug(
                    f"Published {message['symbol']} to {topic} "
                    f"(partition={record_metadata.partition}, offset={record_metadata.offset})"
                )

            except KafkaError as e:
                logger.error(f"Kafka error publishing {message['symbol']}: {e}")
                self.stats['messages_failed'] += 1
                self.kafka_breaker.record_failure()

            except Exception as e:
                logger.error(f"Error publishing {message['symbol']}: {e}")
                self.stats['messages_failed'] += 1
                self.kafka_breaker.record_failure()

        # Flush to ensure all messages are sent
        self.kafka_producer.flush()

        return success_count

    def _run_fetch_cycle(self):
        """Execute one fetch and publish cycle"""
        try:
            logger.info("Starting fetch cycle...")

            # Check if market is open
            if not self.is_market_open():
                logger.info("Market is closed, skipping fetch cycle")
                return

            # Fetch data for all symbols
            start_time = time.time()
            messages = self._fetch_all_symbols()
            fetch_duration = time.time() - start_time

            logger.info(f"Fetched {len(messages)} symbols in {fetch_duration:.2f}s")

            if not messages:
                logger.warning("No data fetched, skipping publish")
                self.stats['consecutive_errors'] += 1
                return

            # Publish to Kafka
            publish_start = time.time()
            published_count = self._publish_to_kafka(messages)
            publish_duration = time.time() - publish_start

            logger.info(
                f"Published {published_count}/{len(messages)} messages in {publish_duration:.2f}s"
            )

            # Reset error counter on success
            if published_count > 0:
                self.stats['consecutive_errors'] = 0
                self.stats['last_success_time'] = datetime.now()
            else:
                self.stats['consecutive_errors'] += 1

        except Exception as e:
            logger.error(f"Error in fetch cycle: {e}", exc_info=True)
            self.stats['consecutive_errors'] += 1

    def _log_statistics(self):
        """Log current statistics"""
        uptime = datetime.now() - self.stats['start_time']

        logger.info("=" * 60)
        logger.info("VnStock Producer Statistics")
        logger.info("=" * 60)
        logger.info(f"Uptime: {uptime}")
        logger.info(f"Messages sent: {self.stats['messages_sent']}")
        logger.info(f"Messages failed: {self.stats['messages_failed']}")
        logger.info(f"API calls: {self.stats['api_calls']}")
        logger.info(f"API errors: {self.stats['api_errors']}")
        logger.info(f"Consecutive errors: {self.stats['consecutive_errors']}")
        logger.info(f"Last success: {self.stats['last_success_time']}")
        logger.info("=" * 60)

    def start(self):
        """Start the VnStock Producer Service"""
        logger.info("=" * 60)
        logger.info("VNSTOCK PRODUCER SERVICE")
        logger.info("=" * 60)

        try:
            # Initialize VnStock client
            logger.info("Initializing VnStock client...")
            if not self._initialize_vnstock():
                logger.error("Failed to initialize VnStock client")
                return

            # Initialize Kafka producer
            logger.info("Initializing Kafka producer...")
            if not self._initialize_kafka():
                logger.error("Failed to initialize Kafka producer")
                return

            # Load symbols
            logger.info("Loading symbols...")
            if not self._load_symbols():
                logger.error("Failed to load symbols")
                return

            logger.info("VnStock Producer Service started successfully")
            logger.info(f"Tracking {len(self.symbols)} symbols")
            logger.info(f"Fetch interval: {self.config['data_fetching']['interval_seconds']}s")
            logger.info("=" * 60)

            self.running = True
            interval = self.config['data_fetching']['interval_seconds']
            stats_interval = 300  # Log stats every 5 minutes
            last_stats_time = time.time()

            # Main loop
            while self.running and not self.shutdown_event.is_set():
                try:
                    # Run fetch cycle
                    self._run_fetch_cycle()

                    # Log statistics periodically
                    if time.time() - last_stats_time >= stats_interval:
                        self._log_statistics()
                        last_stats_time = time.time()

                    # Check if too many consecutive errors
                    max_errors = self.config['error_handling']['max_consecutive_errors']
                    if self.stats['consecutive_errors'] >= max_errors:
                        logger.error(
                            f"Too many consecutive errors ({self.stats['consecutive_errors']}), "
                            "backing off..."
                        )
                        backoff_time = min(
                            self.config['error_handling']['max_backoff_seconds'],
                            interval * (2 ** min(self.stats['consecutive_errors'] - max_errors, 5))
                        )
                        logger.info(f"Backing off for {backoff_time}s")
                        time.sleep(backoff_time)
                        self.stats['consecutive_errors'] = 0  # Reset after backoff

                    # Wait for next cycle
                    logger.debug(f"Waiting {interval}s until next fetch cycle...")
                    self.shutdown_event.wait(timeout=interval)

                except KeyboardInterrupt:
                    logger.info("Received keyboard interrupt")
                    break

                except Exception as e:
                    logger.error(f"Error in main loop: {e}", exc_info=True)
                    time.sleep(interval)

        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)

        finally:
            self.stop()

    def stop(self):
        """Stop the VnStock Producer Service"""
        logger.info("Stopping VnStock Producer Service...")

        self.running = False
        self.shutdown_event.set()

        # Log final statistics
        self._log_statistics()

        # Close connections
        if self.kafka_producer:
            try:
                self.kafka_producer.flush()
                self.kafka_producer.close(timeout=10)
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")

        if self.vnstock_client:
            try:
                self.vnstock_client.disconnect()
                logger.info("VnStock client disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting VnStock client: {e}")

        logger.info("VnStock Producer Service stopped")


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
    service = VnStockProducerService()
    signal_handler.service = service  # Store reference for signal handler

    service.start()


if __name__ == "__main__":
    main()
