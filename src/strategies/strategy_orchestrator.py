"""Strategy Orchestration Framework for managing multiple trading strategies.

This module provides coordination and management for multiple trading strategies,
handling signal aggregation, conflict resolution, and portfolio-level risk management
for Vietnamese market trading.
"""

from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
import threading
import time
from collections import defaultdict
import json

from src.strategies.base_strategy import BaseStrategy, SignalType, MarketSession
from src.strategies.ma_crossover_strategy import MovingAverageCrossoverStrategy
from src.strategies.breakout_strategy import BreakoutStrategy
from src.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.strategies.momentum_strategy import MomentumStrategy
from kafka import KafkaProducer

class SignalPriority(Enum):
    """Signal priority levels for conflict resolution."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

class StrategyType(Enum):
    """Available strategy types."""
    MA_CROSSOVER = "ma_crossover"
    BREAKOUT = "breakout"
    MEAN_REVERSION = "mean_reversion"
    MOMENTUM = "momentum"

class StrategyOrchestrator:
    """Orchestrates multiple trading strategies with signal aggregation and risk management."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Strategy management
        self.strategies: Dict[str, BaseStrategy] = {}
        self.strategy_weights: Dict[str, float] = {}
        self.strategy_enabled: Dict[str, bool] = {}
        
        # Signal aggregation settings
        self.signal_timeout_minutes = config.get('signal_timeout_minutes', 10)
        self.min_signal_confidence = config.get('min_signal_confidence', 0.6)
        self.max_signals_per_symbol = config.get('max_signals_per_symbol', 3)
        self.conflict_resolution_method = config.get('conflict_resolution_method', 'weighted_average')
        
        # Portfolio risk management
        self.max_portfolio_exposure = config.get('max_portfolio_exposure', 0.8)
        self.max_strategy_correlation = config.get('max_strategy_correlation', 0.7)
        self.risk_budget_per_strategy = config.get('risk_budget_per_strategy', 0.2)
        
        # Vietnamese market specific
        self.vn30_max_concentration = config.get('vn30_max_concentration', 0.6)
        self.session_based_allocation = config.get('session_based_allocation', True)
        
        # Signal tracking
        self.active_signals: Dict[str, List[Dict]] = defaultdict(list)  # symbol -> signals
        self.signal_history: List[Dict] = []
        self.aggregated_signals: List[Dict] = []
        
        # Performance tracking
        self.strategy_performance: Dict[str, Dict] = {}
        self.portfolio_metrics: Dict[str, float] = {}
        
        # Threading and synchronization
        self.running = False
        self.orchestrator_thread = None
        self.lock = threading.Lock()
        
        # Kafka integration
        kafka_config = config.get('kafka', {})
        self.kafka_producer = None
        if kafka_config.get('enabled', True):
            self._initialize_kafka(kafka_config)
        
        # Logging
        self.logger = logging.getLogger('strategy_orchestrator')
        
    def _initialize_kafka(self, kafka_config: Dict[str, Any]):
        """Initialize Kafka producer for aggregated signals."""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_config.get('bootstrap_servers', 'localhost:9092'),
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )
            self.logger.info("Strategy Orchestrator Kafka producer initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            self.kafka_producer = None
    
    def add_strategy(self, strategy_type: StrategyType, strategy_params: Dict[str, Any], 
                    weight: float = 1.0, enabled: bool = True) -> str:
        """Add a strategy to the orchestrator."""
        try:
            # Create strategy instance
            strategy_id = f"{strategy_type.value}_{len(self.strategies)}"
            
            if strategy_type == StrategyType.MA_CROSSOVER:
                strategy = MovingAverageCrossoverStrategy(strategy_params)
            elif strategy_type == StrategyType.BREAKOUT:
                strategy = BreakoutStrategy(strategy_params)
            elif strategy_type == StrategyType.MEAN_REVERSION:
                strategy = MeanReversionStrategy(strategy_params)
            elif strategy_type == StrategyType.MOMENTUM:
                strategy = MomentumStrategy(strategy_params)
            else:
                raise ValueError(f"Unknown strategy type: {strategy_type}")
            
            # Register strategy
            self.strategies[strategy_id] = strategy
            self.strategy_weights[strategy_id] = weight
            self.strategy_enabled[strategy_id] = enabled
            self.strategy_performance[strategy_id] = {
                'total_signals': 0,
                'successful_signals': 0,
                'win_rate': 0.0,
                'avg_confidence': 0.0,
                'last_signal_time': None
            }
            
            self.logger.info(f"Added strategy {strategy_id} with weight {weight}")
            return strategy_id
            
        except Exception as e:
            self.logger.error(f"Error adding strategy {strategy_type}: {e}")
            return None
    
    def remove_strategy(self, strategy_id: str) -> bool:
        """Remove a strategy from the orchestrator."""
        try:
            if strategy_id in self.strategies:
                # Cleanup strategy
                self.strategies[strategy_id].cleanup()
                
                # Remove from tracking
                del self.strategies[strategy_id]
                del self.strategy_weights[strategy_id]
                del self.strategy_enabled[strategy_id]
                del self.strategy_performance[strategy_id]
                
                self.logger.info(f"Removed strategy {strategy_id}")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error removing strategy {strategy_id}: {e}")
            return False
    
    def update_strategy_weight(self, strategy_id: str, new_weight: float):
        """Update the weight of a specific strategy."""
        if strategy_id in self.strategy_weights:
            old_weight = self.strategy_weights[strategy_id]
            self.strategy_weights[strategy_id] = new_weight
            self.logger.info(f"Updated strategy {strategy_id} weight: {old_weight} → {new_weight}")
    
    def enable_strategy(self, strategy_id: str, enabled: bool = True):
        """Enable or disable a specific strategy."""
        if strategy_id in self.strategy_enabled:
            self.strategy_enabled[strategy_id] = enabled
            status = "enabled" if enabled else "disabled"
            self.logger.info(f"Strategy {strategy_id} {status}")
    
    def process_market_data(self, market_data: pd.DataFrame):
        """Process market data through all enabled strategies."""
        try:
            with self.lock:
                for strategy_id, strategy in self.strategies.items():
                    if self.strategy_enabled.get(strategy_id, False):
                        try:
                            # Process data in strategy
                            strategy.on_data(market_data)
                        except Exception as e:
                            self.logger.error(f"Error processing data in strategy {strategy_id}: {e}")
                            
        except Exception as e:
            self.logger.error(f"Error processing market data: {e}")
    
    def collect_strategy_signals(self) -> Dict[str, List[Dict]]:
        """Collect signals from all enabled strategies."""
        all_signals = {}
        
        try:
            for strategy_id, strategy in self.strategies.items():
                if not self.strategy_enabled.get(strategy_id, False):
                    continue
                
                try:
                    # Generate signals from strategy
                    signals = strategy.generate_signals()
                    
                    if signals:
                        # Add strategy metadata to each signal
                        for signal in signals:
                            signal['strategy_id'] = strategy_id
                            signal['strategy_weight'] = self.strategy_weights[strategy_id]
                            signal['timestamp'] = datetime.now()
                        
                        all_signals[strategy_id] = signals
                        
                        # Update performance tracking
                        self.strategy_performance[strategy_id]['total_signals'] += len(signals)
                        if signals:
                            avg_conf = np.mean([s.get('confidence', 0) for s in signals])
                            self.strategy_performance[strategy_id]['avg_confidence'] = avg_conf
                            self.strategy_performance[strategy_id]['last_signal_time'] = datetime.now()
                        
                except Exception as e:
                    self.logger.error(f"Error collecting signals from strategy {strategy_id}: {e}")
            
            return all_signals
            
        except Exception as e:
            self.logger.error(f"Error collecting strategy signals: {e}")
            return {}
    
    def aggregate_signals_by_symbol(self, strategy_signals: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """Aggregate signals by symbol across all strategies."""
        symbol_signals = defaultdict(list)
        
        try:
            # Group signals by symbol
            for strategy_id, signals in strategy_signals.items():
                for signal in signals:
                    symbol = signal.get('symbol')
                    if symbol:
                        symbol_signals[symbol].append(signal)
            
            # Filter and sort signals for each symbol
            filtered_signals = {}
            for symbol, signals in symbol_signals.items():
                # Filter by confidence threshold
                valid_signals = [s for s in signals if s.get('confidence', 0) >= self.min_signal_confidence]
                
                # Sort by confidence and limit count
                valid_signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)
                valid_signals = valid_signals[:self.max_signals_per_symbol]
                
                if valid_signals:
                    filtered_signals[symbol] = valid_signals
            
            return filtered_signals
            
        except Exception as e:
            self.logger.error(f"Error aggregating signals by symbol: {e}")
            return {}
    
    def resolve_signal_conflicts(self, symbol_signals: List[Dict]) -> Optional[Dict]:
        """Resolve conflicts when multiple strategies provide opposing signals."""
        if not symbol_signals:
            return None
        
        try:
            # Separate signals by type
            buy_signals = [s for s in symbol_signals if s.get('signal_type') == 'BUY']
            sell_signals = [s for s in symbol_signals if s.get('signal_type') == 'SELL']
            hold_signals = [s for s in symbol_signals if s.get('signal_type') == 'HOLD']
            
            # If all signals agree, return aggregated signal
            if len(buy_signals) > 0 and len(sell_signals) == 0:
                return self._aggregate_same_direction_signals(buy_signals)
            elif len(sell_signals) > 0 and len(buy_signals) == 0:
                return self._aggregate_same_direction_signals(sell_signals)
            elif len(hold_signals) == len(symbol_signals):
                return self._aggregate_same_direction_signals(hold_signals)
            
            # Handle conflicts based on method
            if self.conflict_resolution_method == 'weighted_average':
                return self._resolve_by_weighted_average(symbol_signals)
            elif self.conflict_resolution_method == 'highest_confidence':
                return max(symbol_signals, key=lambda x: x.get('confidence', 0))
            elif self.conflict_resolution_method == 'majority_vote':
                return self._resolve_by_majority_vote(symbol_signals)
            else:
                # Default: highest confidence
                return max(symbol_signals, key=lambda x: x.get('confidence', 0))
                
        except Exception as e:
            self.logger.error(f"Error resolving signal conflicts: {e}")
            return None
    
    def _aggregate_same_direction_signals(self, signals: List[Dict]) -> Dict:
        """Aggregate signals that are in the same direction."""
        if not signals:
            return {}
        
        # Weighted average of key metrics
        total_weight = sum(s.get('strategy_weight', 1.0) for s in signals)
        
        aggregated = {
            'symbol': signals[0]['symbol'],
            'signal_type': signals[0]['signal_type'],
            'timestamp': datetime.now(),
            'strategy_id': 'orchestrator',
            'source_strategies': [s.get('strategy_id') for s in signals],
            'signal_count': len(signals),
            'aggregation_method': 'same_direction'
        }
        
        # Weighted averages
        if total_weight > 0:
            aggregated['confidence'] = sum(
                s.get('confidence', 0) * s.get('strategy_weight', 1.0) for s in signals
            ) / total_weight
            
            aggregated['price'] = sum(
                s.get('price', 0) * s.get('strategy_weight', 1.0) for s in signals
            ) / total_weight
            
            # Take weighted average of position sizes
            if all('position_size' in s for s in signals):
                aggregated['position_size'] = sum(
                    s.get('position_size', 0) * s.get('strategy_weight', 1.0) for s in signals
                ) / total_weight
        
        return aggregated
    
    def _resolve_by_weighted_average(self, signals: List[Dict]) -> Dict:
        """Resolve conflicts using weighted average approach."""
        buy_weight = sum(s.get('strategy_weight', 1.0) * s.get('confidence', 0) 
                        for s in signals if s.get('signal_type') == 'BUY')
        sell_weight = sum(s.get('strategy_weight', 1.0) * s.get('confidence', 0)
                         for s in signals if s.get('signal_type') == 'SELL')
        
        # Determine final signal based on weighted confidence
        if buy_weight > sell_weight * 1.1:  # 10% bias for confirmation
            final_signal_type = 'BUY'
            final_confidence = buy_weight / (buy_weight + sell_weight)
        elif sell_weight > buy_weight * 1.1:
            final_signal_type = 'SELL' 
            final_confidence = sell_weight / (buy_weight + sell_weight)
        else:
            final_signal_type = 'HOLD'
            final_confidence = 0.5  # Neutral
        
        return {
            'symbol': signals[0]['symbol'],
            'signal_type': final_signal_type,
            'confidence': final_confidence,
            'timestamp': datetime.now(),
            'strategy_id': 'orchestrator',
            'source_strategies': [s.get('strategy_id') for s in signals],
            'aggregation_method': 'weighted_average',
            'buy_weight': buy_weight,
            'sell_weight': sell_weight,
            'signal_count': len(signals)
        }
    
    def _resolve_by_majority_vote(self, signals: List[Dict]) -> Dict:
        """Resolve conflicts using majority vote."""
        signal_counts = defaultdict(int)
        signal_groups = defaultdict(list)
        
        for signal in signals:
            signal_type = signal.get('signal_type')
            signal_counts[signal_type] += 1
            signal_groups[signal_type].append(signal)
        
        # Get majority signal
        majority_signal = max(signal_counts.items(), key=lambda x: x[1])
        
        if majority_signal[1] > len(signals) / 2:  # True majority
            return self._aggregate_same_direction_signals(signal_groups[majority_signal[0]])
        else:
            # No clear majority, fall back to highest confidence
            return max(signals, key=lambda x: x.get('confidence', 0))
    
    def publish_aggregated_signal(self, signal: Dict):
        """Publish aggregated signal to Kafka."""
        try:
            if self.kafka_producer and signal:
                self.kafka_producer.send(
                    topic='aggregated_signals',
                    key=signal.get('symbol'),
                    value=signal
                )
                self.logger.debug(f"Published aggregated signal: {signal['symbol']} {signal['signal_type']}")
        except Exception as e:
            self.logger.error(f"Error publishing aggregated signal: {e}")
    
    def run_orchestration_cycle(self):
        """Run one cycle of strategy orchestration."""
        try:
            # Collect signals from all strategies
            strategy_signals = self.collect_strategy_signals()
            
            if not strategy_signals:
                return
            
            # Aggregate signals by symbol
            symbol_signals = self.aggregate_signals_by_symbol(strategy_signals)
            
            # Process each symbol's signals
            aggregated_signals = []
            for symbol, signals in symbol_signals.items():
                resolved_signal = self.resolve_signal_conflicts(signals)
                if resolved_signal:
                    aggregated_signals.append(resolved_signal)
                    self.publish_aggregated_signal(resolved_signal)
            
            # Update tracking
            with self.lock:
                self.aggregated_signals.extend(aggregated_signals)
                # Keep only recent signals (last 1000)
                self.aggregated_signals = self.aggregated_signals[-1000:]
            
            self.logger.debug(f"Orchestration cycle completed: {len(aggregated_signals)} aggregated signals")
            
        except Exception as e:
            self.logger.error(f"Error in orchestration cycle: {e}")
    
    def start_orchestration(self, cycle_interval_seconds: float = 5.0):
        """Start the orchestration thread."""
        if self.running:
            self.logger.warning("Orchestrator already running")
            return
        
        self.running = True
        self.orchestrator_thread = threading.Thread(
            target=self._orchestration_loop,
            args=(cycle_interval_seconds,),
            daemon=True
        )
        self.orchestrator_thread.start()
        self.logger.info("Strategy orchestrator started")
    
    def stop_orchestration(self):
        """Stop the orchestration thread."""
        self.running = False
        if self.orchestrator_thread:
            self.orchestrator_thread.join(timeout=10)
        self.logger.info("Strategy orchestrator stopped")
    
    def _orchestration_loop(self, cycle_interval: float):
        """Main orchestration loop."""
        while self.running:
            try:
                self.run_orchestration_cycle()
                time.sleep(cycle_interval)
            except Exception as e:
                self.logger.error(f"Error in orchestration loop: {e}")
                time.sleep(cycle_interval)
    
    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get comprehensive orchestrator status."""
        with self.lock:
            status = {
                'running': self.running,
                'total_strategies': len(self.strategies),
                'enabled_strategies': sum(self.strategy_enabled.values()),
                'strategy_details': {},
                'recent_signals': len(self.aggregated_signals),
                'performance_summary': {},
                'configuration': {
                    'signal_timeout_minutes': self.signal_timeout_minutes,
                    'min_signal_confidence': self.min_signal_confidence,
                    'max_signals_per_symbol': self.max_signals_per_symbol,
                    'conflict_resolution_method': self.conflict_resolution_method
                }
            }
            
            # Strategy details
            for strategy_id, strategy in self.strategies.items():
                status['strategy_details'][strategy_id] = {
                    'type': strategy.name,
                    'weight': self.strategy_weights.get(strategy_id, 1.0),
                    'enabled': self.strategy_enabled.get(strategy_id, False),
                    'performance': self.strategy_performance.get(strategy_id, {})
                }
            
            # Performance summary
            total_signals = sum(perf.get('total_signals', 0) for perf in self.strategy_performance.values())
            if total_signals > 0:
                avg_confidence = np.mean([
                    perf.get('avg_confidence', 0) for perf in self.strategy_performance.values()
                    if perf.get('avg_confidence', 0) > 0
                ])
                status['performance_summary'] = {
                    'total_signals_generated': total_signals,
                    'average_confidence': avg_confidence,
                    'aggregated_signals': len(self.aggregated_signals)
                }
        
        return status
    
    def cleanup(self):
        """Clean up orchestrator resources."""
        try:
            self.stop_orchestration()
            
            # Cleanup all strategies
            for strategy in self.strategies.values():
                strategy.cleanup()
            
            # Close Kafka producer
            if self.kafka_producer:
                self.kafka_producer.flush()
                self.kafka_producer.close()
            
            self.logger.info("Strategy orchestrator cleaned up")
            
        except Exception as e:
            self.logger.error(f"Error during orchestrator cleanup: {e}")

# Export main classes
__all__ = [
    'StrategyOrchestrator',
    'StrategyType',
    'SignalPriority'
]