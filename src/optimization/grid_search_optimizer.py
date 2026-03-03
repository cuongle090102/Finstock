"""Grid Search Hyperparameter Optimizer for Vietnamese Trading Strategies.

This module provides exhaustive grid search optimization for strategy parameters,
designed specifically for Vietnamese market characteristics and backtesting requirements.
"""

from typing import Dict, List, Any, Optional, Tuple, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from itertools import product

logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

from src.strategies.base_strategy import BaseStrategy
from src.strategies.ma_crossover_strategy import MovingAverageCrossoverStrategy
from src.strategies.breakout_strategy import BreakoutStrategy
from src.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.strategies.momentum_strategy import MomentumStrategy

class PerformanceMetrics:
    """Calculate and store strategy performance metrics."""
    
    @staticmethod
    def calculate_returns(signals: List[Dict], prices: pd.DataFrame) -> Dict[str, float]:
        """Calculate strategy returns from signals and price data."""
        if not signals or prices.empty:
            return {
                'total_return': 0.0,
                'annualized_return': 0.0,
                'volatility': 0.0,
                'sharpe_ratio': 0.0,
                'max_drawdown': 0.0,
                'win_rate': 0.0,
                'profit_factor': 1.0,
                'total_trades': 0
            }
        
        # Convert signals to trades
        trades = []
        current_position = None
        
        for signal in signals:
            symbol = signal.get('symbol')
            signal_type = signal.get('signal_type')
            price = signal.get('price', 0)
            timestamp = signal.get('timestamp')
            confidence = signal.get('confidence', 0)
            
            if not symbol or not price:
                continue
            
            if signal_type == 'BUY' and current_position is None:
                current_position = {
                    'entry_time': timestamp,
                    'entry_price': price,
                    'symbol': symbol,
                    'confidence': confidence
                }
            elif signal_type == 'SELL' and current_position:
                exit_return = (price - current_position['entry_price']) / current_position['entry_price']
                trades.append({
                    'symbol': symbol,
                    'entry_time': current_position['entry_time'],
                    'exit_time': timestamp,
                    'entry_price': current_position['entry_price'],
                    'exit_price': price,
                    'return': exit_return,
                    'confidence': current_position['confidence'],
                    'duration': (timestamp - current_position['entry_time']).total_seconds() / 3600  # hours
                })
                current_position = None
        
        if not trades:
            return {
                'total_return': 0.0,
                'annualized_return': 0.0,
                'volatility': 0.0,
                'sharpe_ratio': 0.0,
                'max_drawdown': 0.0,
                'win_rate': 0.0,
                'profit_factor': 1.0,
                'total_trades': 0
            }
        
        # Calculate metrics
        returns = [trade['return'] for trade in trades]
        total_return = sum(returns)
        win_trades = [r for r in returns if r > 0]
        loss_trades = [r for r in returns if r < 0]
        
        # Performance metrics
        win_rate = len(win_trades) / len(returns) if returns else 0
        avg_win = np.mean(win_trades) if win_trades else 0
        avg_loss = abs(np.mean(loss_trades)) if loss_trades else 0.001  # Avoid division by zero
        profit_factor = (avg_win * len(win_trades)) / (avg_loss * len(loss_trades)) if loss_trades else float('inf')
        
        # Risk metrics
        volatility = np.std(returns) if len(returns) > 1 else 0
        sharpe_ratio = (np.mean(returns) / volatility) if volatility > 0 else 0
        
        # Drawdown calculation
        cumulative_returns = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdown = running_max - cumulative_returns
        max_drawdown = np.max(drawdown) if len(drawdown) > 0 else 0
        
        # Annualized return (assuming daily data)
        if len(trades) > 0:
            first_trade = min(trade['entry_time'] for trade in trades)
            last_trade = max(trade['exit_time'] for trade in trades)
            period_days = (last_trade - first_trade).days or 1
            annualized_return = (1 + total_return) ** (365.25 / period_days) - 1
        else:
            annualized_return = 0
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'total_trades': len(trades),
            'avg_trade_duration': np.mean([trade['duration'] for trade in trades]) if trades else 0,
            'avg_confidence': np.mean([trade['confidence'] for trade in trades]) if trades else 0
        }

class GridSearchOptimizer:
    """Grid search hyperparameter optimizer for trading strategies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Optimization settings
        self.max_workers = config.get('max_workers', 4)
        self.timeout_seconds = config.get('timeout_seconds', 300)  # 5 minutes per test
        self.min_trades = config.get('min_trades', 10)  # Minimum trades for valid backtest
        
        # Vietnamese market settings
        self.vietnamese_market = config.get('vietnamese_market', True)
        self.vn30_focus = config.get('vn30_focus', True)
        
        # Optimization criteria
        self.primary_metric = config.get('primary_metric', 'sharpe_ratio')
        self.secondary_metrics = config.get('secondary_metrics', ['total_return', 'win_rate', 'max_drawdown'])
        
        # Results storage
        self.optimization_results = []
        self.best_parameters = {}
        self.convergence_history = []
        
        # Logging
        self.logger = logging.getLogger('grid_search_optimizer')
        
    def define_parameter_grid(self, strategy_type: str) -> Dict[str, List]:
        """Define parameter search space for different strategy types."""
        
        if strategy_type == 'ma_crossover':
            return {
                'short_period': [5, 8, 10, 12, 15],
                'long_period': [20, 25, 30, 40, 50],
                'ema_alpha': [0.1, 0.15, 0.2],
                'min_volume_ratio': [1.0, 1.2, 1.5, 2.0],
                'price_change_threshold': [0.002, 0.005, 0.01],
                'confidence_base': [0.6, 0.7, 0.8]
            }
        
        elif strategy_type == 'breakout':
            return {
                'lookback_period': [15, 20, 25, 30],
                'breakout_threshold': [0.01, 0.015, 0.02, 0.025],
                'volume_multiplier': [1.2, 1.5, 2.0, 2.5],
                'consolidation_threshold': [0.015, 0.02, 0.025, 0.03],
                'min_consolidation_periods': [8, 10, 12, 15],
                'breakout_confirmation_periods': [2, 3, 4, 5]
            }
        
        elif strategy_type == 'mean_reversion':
            return {
                'bb_period': [15, 20, 25],
                'bb_std_dev': [1.5, 2.0, 2.5],
                'rsi_period': [10, 14, 18],
                'rsi_oversold': [25, 30, 35],
                'rsi_overbought': [65, 70, 75],
                'volume_confirmation_multiplier': [1.2, 1.5, 2.0],
                'trend_filter_period': [40, 50, 60]
            }
        
        elif strategy_type == 'momentum':
            return {
                'roc_short_period': [5, 8, 10, 12],
                'roc_long_period': [15, 20, 25],
                'roc_threshold': [3.0, 5.0, 7.0, 10.0],
                'macd_fast': [8, 12, 16],
                'macd_slow': [21, 26, 31],
                'macd_signal': [6, 9, 12],
                'volume_surge_multiplier': [1.5, 2.0, 2.5, 3.0],
                'min_momentum_strength': [0.5, 0.6, 0.7, 0.8]
            }
        
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
    
    def create_strategy_instance(self, strategy_type: str, parameters: Dict[str, Any]) -> BaseStrategy:
        """Create strategy instance with given parameters."""
        
        # Add common parameters
        base_params = {
            'portfolio_value': 1000000,
            'max_position_size': 0.05,
            'stop_loss_pct': 0.02,
            'take_profit_pct': 0.04,
            'kafka_servers': None  # Disable Kafka for optimization
        }
        base_params.update(parameters)
        
        if strategy_type == 'ma_crossover':
            return MovingAverageCrossoverStrategy(base_params)
        elif strategy_type == 'breakout':
            return BreakoutStrategy(base_params)
        elif strategy_type == 'mean_reversion':
            return MeanReversionStrategy(base_params)
        elif strategy_type == 'momentum':
            return MomentumStrategy(base_params)
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")
    
    def backtest_strategy(self, strategy: BaseStrategy, market_data: pd.DataFrame, 
                         parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Backtest strategy with given parameters."""
        try:
            start_time = time.time()
            
            # Sort data by timestamp
            if 'timestamp' in market_data.columns:
                market_data = market_data.sort_values('timestamp')
            
            # Initialize strategy
            strategy.initialize()
            
            # Process market data in chunks to simulate real-time
            chunk_size = 50  # Process 50 data points at a time
            all_signals = []
            
            for i in range(0, len(market_data), chunk_size):
                chunk = market_data.iloc[i:i+chunk_size]
                
                # Process chunk
                strategy.on_data(chunk)
                
                # Generate signals
                signals = strategy.generate_signals()
                if signals:
                    all_signals.extend(signals)
            
            # Calculate performance metrics
            performance = PerformanceMetrics.calculate_returns(all_signals, market_data)
            
            # Add parameter info
            result = {
                'parameters': parameters.copy(),
                'performance': performance,
                'signals_generated': len(all_signals),
                'backtest_duration': time.time() - start_time,
                'valid': performance['total_trades'] >= self.min_trades
            }
            
            # Cleanup strategy
            strategy.cleanup()
            
            return result
            
        except Exception as e:
            self.logger.error(f"Backtest failed for parameters {parameters}: {e}")
            return {
                'parameters': parameters.copy(),
                'performance': {metric: 0.0 for metric in ['total_return', 'sharpe_ratio', 'win_rate']},
                'error': str(e),
                'valid': False
            }
    
    def optimize_strategy(self, strategy_type: str, market_data: pd.DataFrame, 
                         custom_grid: Optional[Dict[str, List]] = None) -> Dict[str, Any]:
        """Run grid search optimization for a strategy."""
        
        self.logger.info(f"Starting grid search optimization for {strategy_type}")
        
        # Get parameter grid
        parameter_grid = custom_grid or self.define_parameter_grid(strategy_type)
        
        # Generate all parameter combinations
        param_names = list(parameter_grid.keys())
        param_values = list(parameter_grid.values())
        param_combinations = list(product(*param_values))
        
        total_combinations = len(param_combinations)
        self.logger.info(f"Testing {total_combinations} parameter combinations")
        
        # Initialize results storage
        self.optimization_results = []
        completed = 0
        
        def test_single_combination(param_combo):
            """Test a single parameter combination."""
            try:
                # Create parameter dict
                parameters = dict(zip(param_names, param_combo))
                
                # Create strategy instance
                strategy = self.create_strategy_instance(strategy_type, parameters)
                
                # Run backtest
                result = self.backtest_strategy(strategy, market_data, parameters)
                
                return result
                
            except Exception as e:
                self.logger.error(f"Error testing combination {param_combo}: {e}")
                return {
                    'parameters': dict(zip(param_names, param_combo)),
                    'performance': {},
                    'error': str(e),
                    'valid': False
                }
        
        # Run optimization with parallel processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_params = {
                executor.submit(test_single_combination, combo): combo 
                for combo in param_combinations
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_params, timeout=self.timeout_seconds * total_combinations):
                try:
                    result = future.result(timeout=self.timeout_seconds)
                    self.optimization_results.append(result)
                    completed += 1
                    
                    if completed % 10 == 0 or completed == total_combinations:
                        progress = (completed / total_combinations) * 100
                        self.logger.info(f"Progress: {completed}/{total_combinations} ({progress:.1f}%)")
                        
                except Exception as e:
                    self.logger.error(f"Task failed: {e}")
                    completed += 1
        
        # Analyze results
        return self.analyze_optimization_results(strategy_type)
    
    def analyze_optimization_results(self, strategy_type: str) -> Dict[str, Any]:
        """Analyze optimization results and find best parameters."""
        
        if not self.optimization_results:
            return {'error': 'No valid results found'}
        
        # Filter valid results
        valid_results = [r for r in self.optimization_results if r.get('valid', False)]
        
        if not valid_results:
            return {'error': f'No valid backtests found (minimum {self.min_trades} trades required)'}
        
        self.logger.info(f"Analyzing {len(valid_results)} valid results out of {len(self.optimization_results)} total")
        
        # Sort by primary metric
        if self.primary_metric == 'max_drawdown':
            # For drawdown, lower is better
            valid_results.sort(key=lambda x: x['performance'].get(self.primary_metric, float('inf')))
        else:
            # For other metrics, higher is better
            valid_results.sort(key=lambda x: x['performance'].get(self.primary_metric, -float('inf')), reverse=True)
        
        best_result = valid_results[0]
        self.best_parameters = best_result['parameters']
        
        # Calculate statistics
        primary_values = [r['performance'].get(self.primary_metric, 0) for r in valid_results]
        
        analysis = {
            'strategy_type': strategy_type,
            'optimization_summary': {
                'total_combinations_tested': len(self.optimization_results),
                'valid_results': len(valid_results),
                'success_rate': len(valid_results) / len(self.optimization_results),
                'primary_metric': self.primary_metric
            },
            'best_parameters': self.best_parameters.copy(),
            'best_performance': best_result['performance'].copy(),
            'metric_statistics': {
                self.primary_metric: {
                    'best': primary_values[0] if primary_values else 0,
                    'worst': primary_values[-1] if primary_values else 0,
                    'mean': np.mean(primary_values) if primary_values else 0,
                    'std': np.std(primary_values) if len(primary_values) > 1 else 0,
                    'percentiles': {
                        '25th': np.percentile(primary_values, 25) if primary_values else 0,
                        '50th': np.percentile(primary_values, 50) if primary_values else 0,
                        '75th': np.percentile(primary_values, 75) if primary_values else 0,
                        '90th': np.percentile(primary_values, 90) if primary_values else 0
                    }
                }
            },
            'top_10_results': valid_results[:10]  # Top 10 parameter combinations
        }
        
        # Parameter sensitivity analysis
        analysis['parameter_sensitivity'] = self.analyze_parameter_sensitivity(valid_results)
        
        return analysis
    
    def analyze_parameter_sensitivity(self, results: List[Dict]) -> Dict[str, Any]:
        """Analyze which parameters have the most impact on performance."""
        
        if len(results) < 10:
            return {'error': 'Not enough results for sensitivity analysis'}
        
        sensitivity = {}
        
        try:
            # Get all parameter names
            param_names = list(results[0]['parameters'].keys())
            
            for param_name in param_names:
                param_values = []
                metric_values = []
                
                for result in results:
                    param_val = result['parameters'].get(param_name)
                    metric_val = result['performance'].get(self.primary_metric, 0)
                    
                    if param_val is not None:
                        param_values.append(param_val)
                        metric_values.append(metric_val)
                
                # Calculate correlation
                if len(param_values) > 5:
                    correlation = np.corrcoef(param_values, metric_values)[0, 1]
                    if not np.isnan(correlation):
                        sensitivity[param_name] = {
                            'correlation': correlation,
                            'importance': abs(correlation),
                            'direction': 'positive' if correlation > 0 else 'negative',
                            'optimal_value': results[0]['parameters'][param_name]  # Best result's value
                        }
            
            # Sort by importance
            sorted_sensitivity = dict(sorted(sensitivity.items(), 
                                           key=lambda x: x[1]['importance'], reverse=True))
            
            return sorted_sensitivity
            
        except Exception as e:
            self.logger.error(f"Error in sensitivity analysis: {e}")
            return {'error': str(e)}
    
    def save_results(self, filepath: str):
        """Save optimization results to JSON file."""
        try:
            results_data = {
                'timestamp': datetime.now().isoformat(),
                'config': self.config,
                'best_parameters': self.best_parameters,
                'optimization_results': self.optimization_results[-100:],  # Last 100 results
                'convergence_history': self.convergence_history
            }
            
            with open(filepath, 'w') as f:
                json.dump(results_data, f, indent=2, default=str)
                
            self.logger.info(f"Results saved to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
    
    def load_results(self, filepath: str) -> bool:
        """Load optimization results from JSON file."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            self.best_parameters = data.get('best_parameters', {})
            self.optimization_results = data.get('optimization_results', [])
            self.convergence_history = data.get('convergence_history', [])
            
            self.logger.info(f"Results loaded from {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading results: {e}")
            return False
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get summary of optimization results."""
        
        if not self.optimization_results:
            return {'status': 'No optimization results available'}
        
        valid_results = [r for r in self.optimization_results if r.get('valid', False)]
        
        summary = {
            'total_tests': len(self.optimization_results),
            'valid_tests': len(valid_results),
            'success_rate': len(valid_results) / len(self.optimization_results) if self.optimization_results else 0,
            'best_parameters': self.best_parameters.copy(),
            'optimization_config': self.config.copy()
        }
        
        if valid_results:
            best_performance = max(valid_results, 
                                 key=lambda x: x['performance'].get(self.primary_metric, -float('inf')))
            summary['best_performance'] = best_performance['performance']
        
        return summary

# Export main classes
__all__ = [
    'GridSearchOptimizer',
    'PerformanceMetrics'
]