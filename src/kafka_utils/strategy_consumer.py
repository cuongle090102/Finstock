"""Strategy-focused Kafka consumers for real-time market data processing.

This module provides specialized Kafka consumers that feed market data to trading
strategies with Vietnamese market session awareness and data quality validation.
"""

import json
import logging
import threading

logger = logging.getLogger(__name__)
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timezone, timedelta
import pandas as pd
import pytz
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from src.strategies.base_strategy import BaseStrategy
from src.utils.market_config import MarketSession, VN_TIMEZONE, TRADING_HOURS, get_current_market_session, is_market_open as _is_market_open
from src.technical_analysis import IndicatorCalculator

class StrategyKafkaConsumer:
    """Kafka consumer optimized for feeding data to trading strategies."""
    
    def __init__(self, consumer_config: Dict[str, Any], vietnamese_market: bool = True):
        self.consumer_config = consumer_config
        self.vietnamese_market = vietnamese_market
        self.vn_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        
        # Kafka consumers
        self.market_data_consumer = None
        self.indicators_consumer = None
        
        # Strategy registry
        self.registered_strategies: Dict[str, BaseStrategy] = {}
        
        # Data buffers and processors
        self.market_data_buffer = pd.DataFrame()
        self.indicators_buffer = {}
        self.data_quality_stats = {
            'messages_processed': 0,
            'invalid_messages': 0,
            'strategies_updated': 0,
            'last_update': None
        }
        
        # Threading controls
        self.running = False
        self.consumer_threads = {}
        
        # Vietnamese market session tracking (centralized)
        self.trading_hours = TRADING_HOURS
        
        # Logging
        self.logger = logging.getLogger(f'kafka.strategy_consumer')
        
    def initialize(self) -> bool:
        """Initialize Kafka consumers and connections."""
        try:
            # Market data consumer
            self.market_data_consumer = KafkaConsumer(
                'market_data',
                bootstrap_servers=self.consumer_config.get('bootstrap_servers', 'localhost:9092'),
                group_id='strategy_consumers',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                max_poll_records=100,
                fetch_min_bytes=1024,
                fetch_max_wait_ms=500
            )
            
            # Indicators consumer
            self.indicators_consumer = KafkaConsumer(
                'indicators',
                bootstrap_servers=self.consumer_config.get('bootstrap_servers', 'localhost:9092'),
                group_id='strategy_indicators',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None,
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            
            self.logger.info("Strategy Kafka consumers initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumers: {e}")
            return False
    
    def register_strategy(self, strategy: BaseStrategy) -> bool:
        """Register a strategy to receive market data updates."""
        try:
            if not isinstance(strategy, BaseStrategy):
                raise ValueError("Strategy must inherit from BaseStrategy")
                
            self.registered_strategies[strategy.name] = strategy
            strategy.initialize()  # Initialize strategy if not already done
            
            self.logger.info(f"Registered strategy: {strategy.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register strategy {strategy.name}: {e}")
            return False
    
    def get_current_market_session(self) -> MarketSession:
        """Determine current Vietnamese market trading session."""
        return get_current_market_session()

    def is_market_open(self) -> bool:
        """Check if Vietnamese market is currently open for trading."""
        return _is_market_open()
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive status information."""
        return {
            'running': self.running,
            'registered_strategies': list(self.registered_strategies.keys()),
            'current_market_session': self.get_current_market_session().value,
            'market_open': self.is_market_open(),
            'data_quality_stats': self.data_quality_stats.copy(),
            'market_data_buffer_size': len(self.market_data_buffer),
            'indicators_symbols': len(self.indicators_buffer),
            'vietnamese_market': self.vietnamese_market
        }

    def _consume_market_data(self):
        """Consume market data messages and feed to strategies (Task 1.3)."""
        self.logger.info("Starting market data consumer thread")

        try:
            while self.running:
                # Poll for messages
                messages = self.market_data_consumer.poll(timeout_ms=1000, max_records=100)

                if not messages:
                    continue

                # Process messages
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            data = record.value
                            self._process_market_data(data)
                            self.data_quality_stats['messages_processed'] += 1

                        except Exception as e:
                            self.logger.error(f"Error processing market data: {e}")
                            self.data_quality_stats['invalid_messages'] += 1

                # Update stats
                self.data_quality_stats['last_update'] = datetime.now(self.vn_timezone).isoformat()

        except Exception as e:
            self.logger.error(f"Market data consumer error: {e}")
        finally:
            self.logger.info("Market data consumer thread stopped")

    def _process_market_data(self, data: Dict[str, Any]):
        """Process market data and feed to registered strategies."""
        try:
            # Extract key information
            symbol = data.get('symbol')
            data_type = data.get('data_type', 'unknown')

            if not symbol:
                return

            # Feed data to all registered strategies
            for strategy_name, strategy in self.registered_strategies.items():
                try:
                    # Call strategy's on_data method
                    strategy.on_data(data)

                    # Generate signals if market is open
                    if self.is_market_open():
                        signals = strategy.generate_signals()

                        if signals:
                            self.logger.info(f"Strategy {strategy_name} generated {len(signals)} signals for {symbol}")
                            self.data_quality_stats['strategies_updated'] += 1

                except Exception as e:
                    self.logger.error(f"Error feeding data to strategy {strategy_name}: {e}")

        except Exception as e:
            self.logger.error(f"Error in _process_market_data: {e}")

    def start(self):
        """Start consuming market data and feeding to strategies (Task 1.3)."""
        if self.running:
            self.logger.warning("Strategy consumer already running")
            return False

        if not self.market_data_consumer:
            self.logger.error("Consumers not initialized. Call initialize() first")
            return False

        if not self.registered_strategies:
            self.logger.warning("No strategies registered")

        self.running = True

        # Start consumer thread
        self.consumer_threads['market_data'] = threading.Thread(
            target=self._consume_market_data,
            daemon=True,
            name='MarketDataConsumer'
        )
        self.consumer_threads['market_data'].start()

        self.logger.info(f"Strategy consumer started with {len(self.registered_strategies)} strategies")
        return True

    def stop(self):
        """Stop consuming and cleanup resources."""
        self.logger.info("Stopping strategy consumer...")
        self.running = False

        # Wait for threads to finish
        for thread_name, thread in self.consumer_threads.items():
            if thread and thread.is_alive():
                thread.join(timeout=5)
                self.logger.info(f"Thread {thread_name} stopped")

        # Close Kafka consumers
        if self.market_data_consumer:
            self.market_data_consumer.close()
        if self.indicators_consumer:
            self.indicators_consumer.close()

        self.logger.info("Strategy consumer stopped")

    def __del__(self):
        """Cleanup on deletion."""
        if self.running:
            self.stop()

# Export classes
__all__ = [
    'StrategyKafkaConsumer'
]