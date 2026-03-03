"""
Adaptive Strategy Module
Switches between sub-strategies based on detected market regime
Dynamically loads optimized parameters from Kafka
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Literal
from datetime import datetime, timedelta
import logging
import json
from kafka import KafkaConsumer
import threading
import time

from .base_strategy import BaseStrategy, SignalType
from .ma_crossover_strategy import MovingAverageCrossoverStrategy
from .breakout_strategy import BreakoutStrategy
from .mean_reversion_strategy import MeanReversionStrategy
from .momentum_strategy import MomentumStrategy
from src.regime.regime_detector import RegimeDetector, MarketRegime

logger = logging.getLogger(__name__)


class AdaptiveStrategy(BaseStrategy):
    """
    Adaptive strategy that switches between sub-strategies based on market regime.

    Strategy Mapping:
    - Trending Regime: Momentum or Breakout Strategy
    - Ranging Regime: Mean Reversion Strategy
    - Neutral Regime: MA Crossover Strategy (conservative)

    Features:
    - Dynamic parameter loading from Kafka backtest_results topic
    - Regime detection using RegimeDetector
    - Signal confidence based on regime strength
    - Smooth transitions (10-minute delay to prevent rapid switching)
    """

    def __init__(self, name: str = "adaptive_strategy", params: Dict[str, Any] = None):
        """
        Initialize Adaptive Strategy.

        Args:
            name: Strategy name
            params: Strategy parameters including:
                - regime_detector_config: Config for RegimeDetector
                - kafka_servers: Kafka bootstrap servers
                - parameter_update_topic: Kafka topic for parameter updates
                - transition_delay_minutes: Delay before switching strategies
                - confidence_threshold: Minimum confidence to trust regime
        """
        if params is None:
            params = {}

        super().__init__(name, params)

        # Regime detection
        regime_config = params.get('regime_detector_config', {})
        self.regime_detector = RegimeDetector(**regime_config)

        # Sub-strategies
        self.sub_strategies = {}
        self.active_strategy = None
        self.current_regime = MarketRegime.NEUTRAL

        # Parameter management
        self.parameter_update_topic = params.get('parameter_update_topic', 'backtest_results')
        self.kafka_consumer = None
        self.parameter_listener_thread = None
        self.latest_parameters = {}
        self.parameter_lock = threading.Lock()

        # Transition management
        self.transition_delay = timedelta(minutes=params.get('transition_delay_minutes', 10))
        self.last_regime_change = datetime.now()
        self.pending_regime = None

        # Fallback deduplication: track the last signal_history entry returned per symbol
        # so the same crossover event is not returned on every tick for 30 seconds
        self._last_fallback_signal_key: Dict[str, str] = {}  # symbol → timestamp string

        # Confidence threshold
        self.confidence_threshold = params.get('confidence_threshold', 0.6)

        # Strategy performance tracking
        self.strategy_performance = {
            'ma_crossover': {'signals': 0, 'wins': 0, 'total_pnl': 0.0},
            'breakout': {'signals': 0, 'wins': 0, 'total_pnl': 0.0},
            'mean_reversion': {'signals': 0, 'wins': 0, 'total_pnl': 0.0},
            'momentum': {'signals': 0, 'wins': 0, 'total_pnl': 0.0}
        }

    def _initialize_strategy(self):
        """Initialize sub-strategies and Kafka parameter listener."""
        logger.info("Initializing Adaptive Strategy")

        # Initialize sub-strategies with default parameters
        self._initialize_sub_strategies()

        # Start Kafka parameter listener
        self._start_parameter_listener()

        # Set initial active strategy
        self.active_strategy = self.sub_strategies['ma_crossover']
        logger.info(f"Active strategy initialized: {self.active_strategy.name}")

    def _initialize_sub_strategies(self):
        """Initialize all sub-strategies with default parameters."""
        # MA Crossover (conservative, for neutral regime)
        self.sub_strategies['ma_crossover'] = MovingAverageCrossoverStrategy(
            params={
                'short_period': 10,
                'long_period': 30,
                'portfolio_value': self.portfolio_value,
                'kafka_servers': self.kafka_bootstrap_servers
            }
        )

        # Breakout (for trending regime)
        self.sub_strategies['breakout'] = BreakoutStrategy(
            params={
                'lookback_period': 20,
                'breakout_threshold': 1.5,
                'portfolio_value': self.portfolio_value,
                'kafka_servers': self.kafka_bootstrap_servers
            }
        )

        # Mean Reversion (for ranging regime)
        self.sub_strategies['mean_reversion'] = MeanReversionStrategy(
            params={
                'bb_period': 20,
                'bb_std': 2.0,
                'rsi_period': 14,
                'rsi_oversold': 30,
                'rsi_overbought': 70,
                'portfolio_value': self.portfolio_value,
                'kafka_servers': self.kafka_bootstrap_servers
            }
        )

        # Momentum (alternative for trending regime)
        self.sub_strategies['momentum'] = MomentumStrategy(
            params={
                'adx_period': 14,
                'adx_threshold': 25,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9,
                'portfolio_value': self.portfolio_value,
                'kafka_servers': self.kafka_bootstrap_servers
            }
        )

        logger.info(f"Initialized {len(self.sub_strategies)} sub-strategies")

    def _start_parameter_listener(self):
        """Start background thread to listen for parameter updates from Kafka."""
        try:
            self.kafka_consumer = KafkaConsumer(
                self.parameter_update_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='adaptive_strategy_consumer'
            )

            # Start listener thread
            self.parameter_listener_thread = threading.Thread(
                target=self._parameter_listener_loop,
                daemon=True
            )
            self.parameter_listener_thread.start()

            logger.info(f"Started parameter listener for topic: {self.parameter_update_topic}")

        except Exception as e:
            logger.error(f"Failed to start parameter listener: {e}")

    def _parameter_listener_loop(self):
        """Background loop to listen for parameter updates."""
        logger.info("Parameter listener loop started")

        while True:
            try:
                # Poll for new messages (1 second timeout)
                messages = self.kafka_consumer.poll(timeout_ms=1000)

                for topic_partition, records in messages.items():
                    for record in records:
                        self._process_parameter_update(record.value)

            except Exception as e:
                err_msg = str(e)
                if 'snappy' in err_msg.lower() or 'UnsupportedCodecError' in type(e).__name__:
                    logger.warning("Snappy codec not available for parameter listener — "
                                   "parameter updates via Kafka disabled")
                    return  # Exit thread; parameters still update via DB
                logger.error(f"Error in parameter listener loop: {e}")
                time.sleep(5)  # Wait before retrying

    def _process_parameter_update(self, message: Dict[str, Any]):
        """
        Process parameter update message from Kafka.

        Expected message format:
        {
            'timestamp': '2025-10-09T06:45:00',
            'regime': 'trending',
            'strategies': {
                'ma_crossover': {
                    'parameters': {'short_window': 10, 'long_window': 30},
                    'expected_sharpe': 1.5
                },
                ...
            }
        }
        """
        try:
            logger.info(f"Received parameter update: {message.get('timestamp')}")

            regime = message.get('regime')
            strategies = message.get('strategies', {})

            with self.parameter_lock:
                self.latest_parameters = {
                    'regime': regime,
                    'strategies': strategies,
                    'received_at': datetime.now()
                }

            # Update sub-strategy parameters
            for strategy_name, strategy_data in strategies.items():
                if strategy_name in self.sub_strategies:
                    new_params = strategy_data.get('parameters', {})
                    self._update_strategy_parameters(strategy_name, new_params)

            logger.info(f"Updated parameters for {len(strategies)} strategies")

        except Exception as e:
            logger.error(f"Error processing parameter update: {e}")

    def _update_strategy_parameters(self, strategy_name: str, new_params: Dict[str, Any]):
        """Update parameters for a specific sub-strategy."""
        try:
            strategy = self.sub_strategies.get(strategy_name)
            if strategy:
                # Validate parameters before applying
                if self._validate_parameters(strategy_name, new_params):
                    strategy.params.update(new_params)
                    logger.info(f"Updated {strategy_name} parameters: {new_params}")
                else:
                    logger.warning(f"Invalid parameters for {strategy_name}, skipping update")

        except Exception as e:
            logger.error(f"Error updating {strategy_name} parameters: {e}")

    def _validate_parameters(self, strategy_name: str, params: Dict[str, Any]) -> bool:
        """Validate strategy parameters before applying."""
        # Add sanity checks for parameters
        if strategy_name == 'ma_crossover':
            short = params.get('short_window', 0)
            long = params.get('long_window', 0)
            if short >= long or short < 2 or long > 200:
                return False

        elif strategy_name == 'breakout':
            lookback = params.get('lookback_period', 0)
            threshold = params.get('breakout_threshold', 0)
            if lookback < 5 or lookback > 100 or threshold < 0.5 or threshold > 5.0:
                return False

        elif strategy_name == 'mean_reversion':
            window = params.get('window', 0)
            std = params.get('num_std', 0)
            if window < 10 or window > 100 or std < 1.0 or std > 4.0:
                return False

        elif strategy_name == 'momentum':
            lookback = params.get('lookback_period', 0)
            rsi = params.get('rsi_period', 0)
            if lookback < 5 or lookback > 50 or rsi < 5 or rsi > 30:
                return False

        return True

    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data and generate signals."""
        try:
            # Update all sub-strategies with new data
            for strategy in self.sub_strategies.values():
                strategy.update_market_data(market_data)

            # Detect current regime
            if len(self.market_data_buffer) >= 60:
                self._update_regime()

            # Check for pending regime transitions
            self._check_regime_transition()

            # Generate signal using active strategy
            signal = self.active_strategy.generate_signal(market_data)

            # Adjust confidence based on regime strength
            if signal and hasattr(self, 'regime_confidence'):
                signal['confidence'] *= self.regime_confidence

            return signal

        except Exception as e:
            logger.error(f"Error processing market data: {e}")
            return None

    def _update_regime(self):
        """Detect current market regime and initiate strategy switch if needed."""
        try:
            regime_result = self.regime_detector.detect_regime(self.market_data_buffer)

            # Check if regime changed with sufficient confidence
            if (regime_result.regime != self.current_regime and
                regime_result.confidence >= self.confidence_threshold):

                logger.info(
                    f"Regime change detected: {self.current_regime.value} → {regime_result.regime.value}"
                    f" (confidence: {regime_result.confidence:.2f})"
                )

                # Set pending regime change; only reset the transition timer when
                # the pending regime itself changes (not on every repeated detection)
                if self.pending_regime != regime_result.regime:
                    self.last_regime_change = datetime.now()
                self.pending_regime = regime_result.regime
                self.regime_confidence = regime_result.confidence

        except Exception as e:
            logger.error(f"Error updating regime: {e}")

    def _check_regime_transition(self):
        """Check if pending regime transition should be executed."""
        if self.pending_regime is None:
            return

        # Check if transition delay has passed
        time_since_change = datetime.now() - self.last_regime_change
        if time_since_change >= self.transition_delay:
            self._switch_strategy(self.pending_regime)
            self.pending_regime = None

    def _switch_strategy(self, new_regime: MarketRegime):
        """Switch to appropriate strategy for the new regime."""
        try:
            # Map regime to strategy
            if new_regime == MarketRegime.TRENDING:
                # Choose between momentum and breakout based on recent performance
                if self.strategy_performance['momentum']['wins'] > self.strategy_performance['breakout']['wins']:
                    new_strategy_name = 'momentum'
                else:
                    new_strategy_name = 'breakout'

            elif new_regime == MarketRegime.RANGING:
                new_strategy_name = 'mean_reversion'

            else:  # NEUTRAL
                new_strategy_name = 'ma_crossover'

            # Switch strategy
            old_strategy = self.active_strategy.name if self.active_strategy else "None"
            self.active_strategy = self.sub_strategies[new_strategy_name]
            self.current_regime = new_regime

            logger.info(
                f"Strategy switched: {old_strategy} → {new_strategy_name}"
                f" (regime: {new_regime.value})"
            )

        except Exception as e:
            logger.error(f"Error switching strategy: {e}")

    def generate_signal(self, market_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """
        Generate trading signal using active strategy.

        Returns:
            Signal dict with regime information, or None
        """
        if not self.active_strategy:
            return None

        try:
            # Generate signal from active strategy
            signal = self.active_strategy.generate_signal(market_data)

            if signal:
                signal['regime'] = self.current_regime.value
                signal['active_strategy'] = self.active_strategy.name
                signal['regime_confidence'] = getattr(self, 'regime_confidence', 0.5)
                return signal

            # Fallback: if active strategy (e.g. MeanReversion) has no signal,
            # check MA Crossover's signal_history for a freshly-published crossover.
            # On tick data, ADX is always low → regime always "ranging" →
            # MeanReversion rarely fires. MA Crossover's on_data() already ran
            # (via update_market_data) and stored any crossover in signal_history.
            # IMPORTANT: search by symbol — signal_history[-1] may belong to a
            # different symbol processed moments earlier.
            current_symbol = None
            if market_data is not None and not market_data.empty and 'symbol' in market_data.columns:
                current_symbol = market_data['symbol'].iloc[0]

            ma_crossover = self.sub_strategies.get('ma_crossover')
            if ma_crossover and ma_crossover.signal_history and current_symbol:
                import time as _time
                now = _time.time()
                # Iterate most-recent-first; stop at the first signal for this symbol
                for pub in reversed(ma_crossover.signal_history[-50:]):
                    if pub.get('symbol') != current_symbol:
                        continue
                    # Found the most-recent signal for this symbol — check freshness
                    # and dedup (don't return the same crossover event twice)
                    sig_key = str(pub.get('timestamp', ''))
                    if self._last_fallback_signal_key.get(current_symbol) == sig_key:
                        break  # Already returned this exact signal
                    try:
                        sig_epoch = pd.to_datetime(pub['timestamp']).timestamp()
                        sig_age = now - sig_epoch
                        if 0 <= sig_age <= 30:  # Fresh: published within last 30s
                            signal = dict(pub)
                            signal['regime'] = self.current_regime.value
                            signal['active_strategy'] = 'MA_Crossover'
                            signal['regime_confidence'] = getattr(self, 'regime_confidence', 0.5)
                            self._last_fallback_signal_key[current_symbol] = sig_key
                            return signal
                    except Exception as e:
                        logger.warning(f"MA Crossover fallback timestamp error: {e}")
                    break  # Only consider the single most-recent signal for this symbol

            return None

        except Exception as e:
            logger.error(f"Error generating signal: {e}")
            return None

    def generate_signals(self) -> list:
        """
        Generate trading signals based on current market data.
        Required by BaseStrategy abstract method.

        Returns:
            List of signal dictionaries
        """
        # For adaptive strategy, we generate signals through generate_signal()
        # This method is required by the base class but we use the singular version
        return []

    def set_warmup_mode(self, enabled: bool):
        """Propagate warmup mode to all sub-strategies so none publishes during warmup."""
        super().set_warmup_mode(enabled)
        for strategy in self.sub_strategies.values():
            strategy.set_warmup_mode(enabled)

    def get_status(self) -> Dict[str, Any]:
        """Get current status of adaptive strategy."""
        return {
            'active_strategy': self.active_strategy.name if self.active_strategy else None,
            'current_regime': self.current_regime.value,
            'regime_confidence': getattr(self, 'regime_confidence', 0.0),
            'pending_regime': self.pending_regime.value if self.pending_regime else None,
            'time_since_last_change': (datetime.now() - self.last_regime_change).total_seconds(),
            'strategy_performance': self.strategy_performance,
            'latest_parameters_received': self.latest_parameters.get('received_at')
        }

    def cleanup(self):
        """Cleanup resources."""
        if self.kafka_consumer:
            self.kafka_consumer.close()
        logger.info("Adaptive strategy cleaned up")
