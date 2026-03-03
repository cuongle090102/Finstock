"""
Paper Trading Service - Main Entry Point

Runs during market hours (9:00-15:00 Vietnam time)
Consumes Kafka market_data → Generate signals → Execute trades → Record to TimescaleDB
"""

import os
import sys
import time
import logging
import json
from datetime import datetime, time as dt_time
from typing import Optional, Dict, Any
import yaml
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
from psycopg2.pool import SimpleConnectionPool
import signal

# Add src to path
sys.path.insert(0, '/app/src')

from paper_trading.paper_broker import PaperBroker, OrderSide, OrderStatus
from strategies.adaptive_strategy import AdaptiveStrategy
from strategies.ma_crossover_strategy import MovingAverageCrossoverStrategy
from src.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError
# from risk.pre_trade_checker import PreTradeChecker
# from risk.position_tracker import PositionTracker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PaperTradingService:
    """Paper Trading Service - Market Hours Trading Loop"""

    def __init__(self):
        """Initialize the service"""
        logger.info("Initializing Paper Trading Service...")

        # Load configuration
        self.config = self._load_config()

        # Initialize components
        self.db_pool = self._init_database()
        self.kafka_consumer = self._init_kafka()
        self.paper_broker = self._init_broker()
        self.strategy = self._init_strategy()
        # self.risk_checker = self._init_risk_checker()
        # self.position_tracker = PositionTracker()

        # Circuit breakers for external dependencies
        self.db_breaker = CircuitBreaker(name="paper_trading_db", failure_threshold=5, recovery_timeout=30.0)
        self.kafka_breaker = CircuitBreaker(name="paper_trading_kafka", failure_threshold=5, recovery_timeout=30.0)

        # Service state
        self.running = True
        self.current_parameters = {}
        self.trading_enabled = False  # Start disabled until data + backtest ready
        self.data_collection_mode = True  # Start in data collection mode

        # Setup graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info("Paper Trading Service initialized successfully")

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from config.yaml"""
        config_path = '/app/config.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return {}

    def _init_database(self) -> SimpleConnectionPool:
        """Initialize PostgreSQL connection pool"""
        logger.info("Connecting to TimescaleDB...")

        db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'finstock_market_data'),
            'user': os.getenv('POSTGRES_USER', 'finstock_user'),
            'password': os.getenv('POSTGRES_PASSWORD', '')
        }

        try:
            pool = SimpleConnectionPool(
                minconn=1,
                maxconn=self.config.get('database', {}).get('connection_pool_size', 5),
                **db_config
            )
            logger.info("Database connection pool created")
            return pool
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _init_kafka(self) -> KafkaConsumer:
        """Initialize Kafka consumer"""
        logger.info("Connecting to Kafka...")

        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        consumer_group = self.config.get('kafka', {}).get('consumer_group', 'paper_trading_group')

        try:
            consumer = KafkaConsumer(
                'market_data',
                'backtest_results',
                bootstrap_servers=bootstrap_servers,
                group_id=consumer_group,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: self._safe_deserialize(x),
                session_timeout_ms=60000,       # 60s session timeout
                heartbeat_interval_ms=10000,    # 10s heartbeat
                max_poll_interval_ms=600000,    # 10min max poll interval
            )
            logger.info(f"Kafka consumer connected to {bootstrap_servers}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    @staticmethod
    def _safe_deserialize(raw: bytes) -> Optional[Dict]:
        """Safely deserialize Kafka message, returning None on failure."""
        try:
            return json.loads(raw.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to deserialize Kafka message: {e}")
            return None

    def _init_broker(self) -> PaperBroker:
        """Initialize paper broker"""
        logger.info("Initializing paper broker...")

        trading_config = self.config.get('trading', {})

        # PaperBroker expects a config dict
        broker_config = {
            'initial_capital': trading_config.get('initial_capital', 1000000000),
            'commission_rate': trading_config.get('commission_rate', 0.0015),
            'tax_rate': trading_config.get('tax_rate', 0.001),
            'slippage_rate': 0.001,  # 0.1% slippage
            'fill_probability': 1.0,  # 100% fill rate for paper trading
        }

        broker = PaperBroker(broker_config)
        logger.info(f"Paper broker initialized with capital: {broker.cash:,.0f} VND")
        return broker

    def _init_strategy(self) -> AdaptiveStrategy:
        """Initialize adaptive trading strategy"""
        logger.info("Initializing Adaptive Strategy...")

        trading_config = self.config.get('trading', {})
        kafka_config = self.config.get('kafka', {})

        # Adaptive strategy configuration
        strategy_config = {
            'portfolio_value': trading_config.get('initial_capital', 1000000000),
            'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'parameter_update_topic': 'backtest_results',
            'transition_delay_minutes': 10,  # Wait 10 minutes before switching
            'confidence_threshold': 0.6,  # Minimum confidence for regime detection
            'regime_detector_config': {
                'adx_period': 14,
                'adx_trending_threshold': 25.0,
                'adx_ranging_threshold': 20.0,
                'volatility_short_period': 20,
                'volatility_long_period': 60
            }
        }

        strategy = AdaptiveStrategy(name='adaptive_strategy', params=strategy_config)
        strategy.initialize()
        logger.info("Adaptive Strategy initialized successfully")
        return strategy

    # def _init_risk_checker(self) -> PreTradeChecker:
    #     """Initialize risk checker"""
    #     logger.info("Initializing risk checker...")
    #     # Simplified for initial implementation
    #     return None

    def _is_market_hours(self) -> bool:
        """Check if currently in market hours (9:00-11:30, 13:00-15:00 Vietnam time)"""
        from utils.market_config import is_market_open
        return is_market_open()

    def _handle_market_data(self, message: Dict[str, Any]):
        """Handle incoming market data message"""
        try:
            data = message
            symbol = data.get('symbol')
            # Producer sends 'close_price'; fall back to 'close' for compatibility
            close = float(data.get('close_price', data.get('close', 0)))
            timestamp = data.get('timestamp')

            # Check trading readiness before processing signals
            if not self.trading_enabled:
                # Silently skip - we're in data collection mode
                return

            # Skip messages with invalid prices
            if close <= 0:
                return

            # Convert to DataFrame for strategy processing
            import pandas as pd
            df = pd.DataFrame([{
                'timestamp': pd.to_datetime(timestamp),  # Ensure Timestamp, not raw string
                'symbol': symbol,
                'open': data.get('open_price', data.get('open', close)),
                'high': data.get('high_price', data.get('high', close)),
                'low': data.get('low_price', data.get('low', close)),
                'close': close,
                'volume': data.get('volume', 0)
            }])

            # Update strategy with new data
            self.strategy.update_market_data(df)

            # Generate signal using adaptive strategy
            signal = self.strategy.generate_signal(df)

            if signal and signal.get('signal_type') != 'HOLD':
                signal_type = signal.get('signal_type')
                confidence = signal.get('confidence', 0.5)
                regime = signal.get('regime', 'unknown')
                active_strategy = signal.get('active_strategy', 'unknown')

                logger.info(
                    f"Signal: {signal_type} for {symbol} at {close:,.0f} "
                    f"(regime: {regime}, strategy: {active_strategy}, confidence: {confidence:.2f})"
                )

                # Only execute if confidence is above threshold
                if confidence >= 0.5:
                    # Fixed 1 lot (100 shares) per signal — keeps capital usage predictable.
                    # At avg 60,000 VND × 100 shares = 6M VND per trade; 1B capital = ~160 trades.
                    quantity = 100

                    # Execute trade via place_order()
                    from decimal import Decimal
                    side = OrderSide.BUY if signal_type == 'BUY' else OrderSide.SELL
                    order = self.paper_broker.place_order(
                        symbol=symbol,
                        side=side,
                        quantity=int(quantity),
                        price=Decimal(str(close))
                    )

                    if order and order.status == OrderStatus.FILLED:
                        logger.info(f"Order executed: {order.order_id} {order.side.value} {order.symbol}")
                        self._record_trade(order, timestamp, regime, active_strategy)

        except Exception as e:
            logger.error(f"Error handling market data: {e}", exc_info=True)

    def _handle_backtest_results(self, message: Dict[str, Any]):
        """Handle optimized parameters from Airflow DAG"""
        try:
            # Note: Adaptive strategy has its own Kafka listener that handles
            # parameter updates automatically. This handler is here for logging.

            regime = message.get('regime')
            strategies = message.get('strategies', {})

            logger.info(f"Parameter update broadcast received for regime: {regime}")
            logger.info(f"Strategies updated: {list(strategies.keys())}")

            # Log strategy status
            if hasattr(self.strategy, 'get_status'):
                status = self.strategy.get_status()
                logger.info(f"Adaptive strategy status: {status}")

            # Store for trade recording
            self.current_parameters = {
                'regime': regime,
                'strategies': strategies
            }

        except Exception as e:
            logger.error(f"Error handling backtest results: {e}", exc_info=True)

    def _record_trade(self, order, timestamp: str, regime: str = 'unknown', active_strategy: str = 'adaptive'):
        """Record trade to TimescaleDB paper_trades table (with circuit breaker)."""
        try:
            if self.db_breaker.state.value == "open":
                logger.warning(f"DB circuit breaker OPEN — trade {order.order_id} not recorded to DB")
                return
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO paper_trades (
                    timestamp, trade_id, order_id, symbol, side,
                    quantity, price, commission, tax, total_cost,
                    pnl, strategy, regime, exchange, is_vn30, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """

            cursor.execute(insert_query, (
                timestamp or datetime.now().isoformat(),
                order.order_id,
                order.order_id,
                order.symbol,
                order.side.value,
                order.quantity,
                float(order.price),
                float(order.commission),
                float(order.tax),
                float(order.total_cost),
                float(order.realized_pnl),  # Realized P&L for SELL; 0 for BUY
                active_strategy,
                regime,
                order.exchange,
                order.is_vn30,
                json.dumps({
                    'regime': regime,
                    'strategy': active_strategy,
                    'parameters': self.current_parameters
                })
            ))

            conn.commit()
            cursor.close()
            self.db_pool.putconn(conn)

            self.db_breaker.record_success()
            logger.info(f"Trade recorded to database: {order.order_id}")

        except Exception as e:
            self.db_breaker.record_failure()
            logger.error(f"Error recording trade to database: {e}", exc_info=True)

    def _check_trading_readiness(self) -> bool:
        """
        Check if paper trading should be enabled.

        Requirements:
        1. At least 7 days of data available
        2. Backtest has run and published parameters

        Returns:
            True if ready to trade
        """
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            # Check 1: Data availability (7+ days)
            cursor.execute("""
                SELECT COUNT(DISTINCT DATE(timestamp)) as unique_days
                FROM market_data_hot;
            """)
            result = cursor.fetchone()
            unique_days = result[0] if result else 0

            # Check 2: Backtest results exist
            cursor.execute("""
                SELECT COUNT(*) as result_count
                FROM backtest_results
                WHERE timestamp >= NOW() - INTERVAL '2 days';
            """)
            result = cursor.fetchone()
            backtest_count = result[0] if result else 0

            cursor.close()
            self.db_pool.putconn(conn)

            # Evaluate readiness
            if unique_days < 7:
                if self.data_collection_mode:
                    # Only log every 10 checks to avoid spam
                    if not hasattr(self, '_readiness_check_count'):
                        self._readiness_check_count = 0
                    self._readiness_check_count += 1

                    if self._readiness_check_count % 10 == 0:
                        logger.info(
                            f"[DATA COLLECTION MODE] Currently have {unique_days}/7 days of data. "
                            f"Backtest will start on Day 7."
                        )
                return False

            if backtest_count == 0:
                if self.data_collection_mode:
                    logger.info(
                        f"[WAITING FOR BACKTEST] Have {unique_days} days of data. "
                        f"Waiting for first backtest to complete (runs at 06:00 daily)."
                    )
                    self.data_collection_mode = False  # Transition state
                return False

            # All checks passed!
            if not self.trading_enabled:
                logger.info("=" * 60)
                logger.info("PAPER TRADING ENABLED")
                logger.info(f"Data available: {unique_days} days")
                logger.info(f"Backtest results: {backtest_count} recent runs")
                logger.info("System ready for live trading")
                logger.info("=" * 60)
                self.trading_enabled = True
                self.data_collection_mode = False

            return True

        except Exception as e:
            logger.error(f"Error checking trading readiness: {e}")
            return False

    def _warmup_strategy_buffers(self):
        """Pre-load recent price history from TimescaleDB to warm up strategy buffers.

        Loads the last 100 ticks per symbol so the MA crossover (needs 20) and
        other strategies can generate signals immediately on startup.
        """
        try:
            import pandas as pd

            # Suppress Kafka signal publishing while replaying historical ticks
            self.strategy.set_warmup_mode(True)

            conn = self.db_pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT symbol, close, open, high, low, volume, timestamp
                FROM (
                    SELECT symbol, close, open, high, low, volume, timestamp,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
                    FROM market_data_hot
                    WHERE timestamp >= NOW() - INTERVAL '3 days'
                      AND close > 0
                ) ranked
                WHERE rn <= 100
                ORDER BY symbol, timestamp ASC
            """)
            rows = cursor.fetchall()
            cursor.close()
            self.db_pool.putconn(conn)

            if not rows:
                logger.info("No historical data available for strategy warmup")
                return

            df = pd.DataFrame(rows, columns=['symbol', 'close', 'open', 'high', 'low', 'volume', 'timestamp'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            symbol_count = df['symbol'].nunique()
            total_rows = len(df)
            logger.info(f"Warming up strategy buffers with {total_rows} rows across {symbol_count} symbols...")

            # Feed historical data row-by-row into strategy buffers
            for _, row in df.iterrows():
                tick = pd.DataFrame([{
                    'timestamp': row['timestamp'],
                    'symbol': row['symbol'],
                    'open': float(row['open'] or row['close']),
                    'high': float(row['high'] or row['close']),
                    'low': float(row['low'] or row['close']),
                    'close': float(row['close']),
                    'volume': int(row['volume'] or 0),
                }])
                self.strategy.update_market_data(tick)

            # Re-enable signal publishing now that buffers are loaded
            self.strategy.set_warmup_mode(False)
            logger.info("Strategy buffer warmup complete — signals can fire immediately")

        except Exception as e:
            self.strategy.set_warmup_mode(False)  # Always re-enable even on error
            logger.error(f"Strategy warmup failed (non-critical): {e}")

    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info("Shutdown signal received. Closing service...")
        self.running = False

    def run(self):
        """Main service loop"""
        logger.info("Paper Trading Service started")

        # Initial readiness check
        logger.info("Performing startup readiness check...")
        is_ready = self._check_trading_readiness()

        if not is_ready:
            if self.data_collection_mode:
                logger.info("Starting in DATA COLLECTION MODE")
                logger.info("System will enable trading once requirements are met:")
                logger.info("  1. At least 7 days of data available")
                logger.info("  2. Backtest has run and published parameters")
            else:
                logger.info("Waiting for backtest to complete...")

        # Pre-load historical prices so strategies can signal immediately
        self._warmup_strategy_buffers()

        logger.info("Waiting for market hours...")

        # Counter for periodic readiness checks
        readiness_check_counter = 0
        READINESS_CHECK_INTERVAL = 100  # Check every 100 iterations (~10 seconds)

        try:
            while self.running:
                # Periodic readiness check (if not already enabled)
                if not self.trading_enabled:
                    readiness_check_counter += 1
                    if readiness_check_counter >= READINESS_CHECK_INTERVAL:
                        self._check_trading_readiness()
                        readiness_check_counter = 0

                # Check if in market hours
                if not self._is_market_hours():
                    # Outside market hours - sleep for 1 minute
                    time.sleep(60)
                    continue

                # Only log "Market is open" if trading is enabled
                if self.trading_enabled and not hasattr(self, '_market_open_logged'):
                    logger.info("Market is open - processing messages and trading...")
                    self._market_open_logged = True

                # Poll Kafka for messages with timeout
                try:
                    messages = self.kafka_consumer.poll(timeout_ms=1000, max_records=10)
                except Exception as poll_err:
                    if 'snappy' in str(poll_err).lower() or 'codec' in str(poll_err).lower():
                        logger.warning("Skipping Kafka messages with unsupported codec — "
                                       "seeking to end of partitions")
                        self.kafka_consumer.seek_to_end()
                        time.sleep(1)
                        continue
                    raise

                for topic_partition, records in messages.items():
                    for record in records:
                        topic = record.topic

                        if topic == 'market_data':
                            self._handle_market_data(record.value)
                        elif topic == 'backtest_results':
                            self._handle_backtest_results(record.value)

                # Small delay to prevent CPU spinning
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
        finally:
            self._cleanup()

    def _cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")

        if hasattr(self, 'kafka_consumer'):
            self.kafka_consumer.close()
            logger.info("Kafka consumer closed")

        if hasattr(self, 'db_pool'):
            self.db_pool.closeall()
            logger.info("Database connections closed")

        logger.info("Paper Trading Service stopped")


def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("FINSTOCK PAPER TRADING SERVICE")
    logger.info("=" * 60)

    try:
        service = PaperTradingService()
        service.run()
    except Exception as e:
        logger.error(f"Failed to start service: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
