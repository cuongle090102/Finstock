"""Strategy Performance Evaluator for Vietnamese Trading Systems.

This module provides comprehensive performance evaluation and walk-forward analysis
for trading strategies, with Vietnamese market-specific metrics and considerations.
"""

from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)
import warnings
warnings.filterwarnings('ignore')

from src.strategies.base_strategy import BaseStrategy
from src.strategies.ma_crossover_strategy import MovingAverageCrossoverStrategy
from src.strategies.breakout_strategy import BreakoutStrategy
from src.strategies.mean_reversion_strategy import MeanReversionStrategy
from src.strategies.momentum_strategy import MomentumStrategy

@dataclass
class PerformanceReport:
    """Comprehensive performance report for a trading strategy."""
    
    # Basic metrics
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # Risk metrics
    max_drawdown: float
    max_drawdown_duration: int  # days
    var_95: float  # Value at Risk 95%
    cvar_95: float  # Conditional VaR 95%
    
    # Trading metrics
    total_trades: int
    win_rate: float
    profit_factor: float
    avg_win: float
    avg_loss: float
    avg_trade_duration: float  # hours
    
    # Vietnamese market specific
    vn30_trades: int
    vn30_performance: float
    morning_session_performance: float
    afternoon_session_performance: float
    
    # Additional metrics
    recovery_factor: float
    expectancy: float
    kelly_criterion: float
    information_ratio: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'basic_metrics': {
                'total_return': self.total_return,
                'annualized_return': self.annualized_return,
                'volatility': self.volatility,
                'sharpe_ratio': self.sharpe_ratio,
                'sortino_ratio': self.sortino_ratio,
                'calmar_ratio': self.calmar_ratio
            },
            'risk_metrics': {
                'max_drawdown': self.max_drawdown,
                'max_drawdown_duration': self.max_drawdown_duration,
                'var_95': self.var_95,
                'cvar_95': self.cvar_95
            },
            'trading_metrics': {
                'total_trades': self.total_trades,
                'win_rate': self.win_rate,
                'profit_factor': self.profit_factor,
                'avg_win': self.avg_win,
                'avg_loss': self.avg_loss,
                'avg_trade_duration': self.avg_trade_duration
            },
            'vietnamese_specific': {
                'vn30_trades': self.vn30_trades,
                'vn30_performance': self.vn30_performance,
                'morning_session_performance': self.morning_session_performance,
                'afternoon_session_performance': self.afternoon_session_performance
            },
            'advanced_metrics': {
                'recovery_factor': self.recovery_factor,
                'expectancy': self.expectancy,
                'kelly_criterion': self.kelly_criterion,
                'information_ratio': self.information_ratio
            }
        }

class PerformanceEvaluator:
    """Comprehensive performance evaluation for trading strategies."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Evaluation settings
        self.risk_free_rate = config.get('risk_free_rate', 0.05)  # 5% annual risk-free rate
        self.trading_days_per_year = config.get('trading_days_per_year', 252)
        self.benchmark_return = config.get('benchmark_return', 0.08)  # VN-Index benchmark
        
        # Vietnamese market settings
        self.vn30_symbols = {
            'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'EIB', 'FPT', 'GAS', 'GVR', 'HDB',
            'HPG', 'KDH', 'KHG', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB',
            'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB'
        }
        
        # Commission and tax (Vietnamese market)
        self.commission_rate = config.get('commission_rate', 0.0015)  # 0.15%
        self.vn30_commission_rate = config.get('vn30_commission_rate', 0.001)  # 0.10%
        self.transaction_tax = config.get('transaction_tax', 0.001)  # 0.1% on sells
        
        self.logger = logging.getLogger('performance_evaluator')
    
    def create_strategy_instance(self, strategy_type: str, parameters: Dict[str, Any]) -> BaseStrategy:
        """Create strategy instance for evaluation."""
        
        base_params = {
            'portfolio_value': 1000000,
            'max_position_size': 0.05,
            'stop_loss_pct': 0.02,
            'take_profit_pct': 0.04,
            'kafka_servers': None  # Disable Kafka for backtesting
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
    
    def simulate_trading(self, strategy: BaseStrategy, market_data: pd.DataFrame) -> List[Dict]:
        """Simulate trading with the strategy and return trade list."""
        
        # Initialize strategy
        strategy.initialize()
        
        # Sort data by timestamp
        if 'timestamp' in market_data.columns:
            market_data = market_data.sort_values('timestamp')
        
        all_signals = []
        
        # Process data in chunks to simulate real-time
        chunk_size = 100
        for i in range(0, len(market_data), chunk_size):
            chunk = market_data.iloc[i:i+chunk_size]
            
            try:
                # Process chunk
                strategy.on_data(chunk)
                
                # Generate signals
                signals = strategy.generate_signals()
                if signals:
                    all_signals.extend(signals)
            except Exception as e:
                self.logger.error(f"Error processing chunk {i}: {e}")
                continue
        
        # Convert signals to trades
        trades = self.signals_to_trades(all_signals, market_data)
        
        # Cleanup
        strategy.cleanup()
        
        return trades
    
    def signals_to_trades(self, signals: List[Dict], market_data: pd.DataFrame) -> List[Dict]:
        """Convert signals to completed trades."""
        trades = []
        open_positions = {}  # symbol -> position info
        
        # Create price lookup for faster access
        price_lookup = {}
        if 'timestamp' in market_data.columns and 'close' in market_data.columns:
            for _, row in market_data.iterrows():
                price_lookup[row['timestamp']] = {
                    'close': row['close'],
                    'symbol': row.get('symbol', 'UNKNOWN')
                }
        
        for signal in signals:
            symbol = signal.get('symbol')
            signal_type = signal.get('signal_type')
            timestamp = signal.get('timestamp')
            price = signal.get('price', 0)
            confidence = signal.get('confidence', 0)
            
            if not symbol or not price:
                continue
            
            # Get actual price from market data if available
            if timestamp in price_lookup and price_lookup[timestamp]['symbol'] == symbol:
                actual_price = price_lookup[timestamp]['close']
            else:
                actual_price = price
            
            if signal_type == 'BUY' and symbol not in open_positions:
                # Open long position
                open_positions[symbol] = {
                    'entry_time': timestamp,
                    'entry_price': actual_price,
                    'signal_price': price,
                    'confidence': confidence,
                    'symbol': symbol,
                    'direction': 'long'
                }
                
            elif signal_type == 'SELL' and symbol in open_positions:
                # Close long position
                position = open_positions[symbol]
                
                # Calculate returns
                gross_return = (actual_price - position['entry_price']) / position['entry_price']
                
                # Calculate costs (Vietnamese market)
                is_vn30 = symbol in self.vn30_symbols
                commission = self.vn30_commission_rate if is_vn30 else self.commission_rate
                
                # Entry cost
                entry_cost = position['entry_price'] * commission
                # Exit cost (commission + tax)
                exit_cost = actual_price * (commission + self.transaction_tax)
                
                total_costs = (entry_cost + exit_cost) / position['entry_price']
                net_return = gross_return - total_costs
                
                # Determine session
                session = self.get_trading_session(timestamp)
                
                trade = {
                    'symbol': symbol,
                    'entry_time': position['entry_time'],
                    'exit_time': timestamp,
                    'entry_price': position['entry_price'],
                    'exit_price': actual_price,
                    'gross_return': gross_return,
                    'net_return': net_return,
                    'confidence': position['confidence'],
                    'duration_hours': (timestamp - position['entry_time']).total_seconds() / 3600,
                    'is_vn30': is_vn30,
                    'session': session,
                    'costs': total_costs
                }
                
                trades.append(trade)
                del open_positions[symbol]
        
        return trades
    
    def get_trading_session(self, timestamp: datetime) -> str:
        """Determine Vietnamese trading session."""
        try:
            hour = timestamp.hour
            minute = timestamp.minute
            time_decimal = hour + minute / 60.0
            
            if 9.0 <= time_decimal <= 11.5:
                return 'morning'
            elif 13.0 <= time_decimal <= 15.0:
                return 'afternoon'
            else:
                return 'off_hours'
        except (AttributeError, TypeError) as e:
            logger.warning(f"Failed to classify trading session for timestamp {timestamp}: {e}")
            return 'unknown'
    
    def calculate_performance_metrics(self, trades: List[Dict]) -> PerformanceReport:
        """Calculate comprehensive performance metrics from trades."""
        
        if not trades:
            return PerformanceReport(
                total_return=0.0, annualized_return=0.0, volatility=0.0,
                sharpe_ratio=0.0, sortino_ratio=0.0, calmar_ratio=0.0,
                max_drawdown=0.0, max_drawdown_duration=0, var_95=0.0, cvar_95=0.0,
                total_trades=0, win_rate=0.0, profit_factor=1.0,
                avg_win=0.0, avg_loss=0.0, avg_trade_duration=0.0,
                vn30_trades=0, vn30_performance=0.0,
                morning_session_performance=0.0, afternoon_session_performance=0.0,
                recovery_factor=0.0, expectancy=0.0, kelly_criterion=0.0, information_ratio=0.0
            )
        
        # Basic calculations
        returns = [trade['net_return'] for trade in trades]
        total_return = sum(returns)
        
        # Time period calculations
        first_trade = min(trade['entry_time'] for trade in trades)
        last_trade = max(trade['exit_time'] for trade in trades)
        period_days = max(1, (last_trade - first_trade).days)
        annualized_return = (1 + total_return) ** (365.25 / period_days) - 1
        
        # Risk metrics
        volatility = np.std(returns) * np.sqrt(self.trading_days_per_year) if len(returns) > 1 else 0
        
        # Sharpe ratio
        excess_return = annualized_return - self.risk_free_rate
        sharpe_ratio = excess_return / volatility if volatility > 0 else 0
        
        # Sortino ratio (downside deviation)
        negative_returns = [r for r in returns if r < 0]
        downside_deviation = np.std(negative_returns) * np.sqrt(self.trading_days_per_year) if negative_returns else 0
        sortino_ratio = excess_return / downside_deviation if downside_deviation > 0 else 0
        
        # Drawdown analysis
        cumulative_returns = np.cumsum(returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = running_max - cumulative_returns
        max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0
        
        # Max drawdown duration
        max_dd_duration = self.calculate_max_drawdown_duration(cumulative_returns)
        
        # Calmar ratio
        calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else 0
        
        # VaR and CVaR
        var_95 = np.percentile(returns, 5) if returns else 0  # 5th percentile (95% VaR)
        cvar_95 = np.mean([r for r in returns if r <= var_95]) if returns and var_95 < 0 else 0
        
        # Trading metrics
        win_trades = [r for r in returns if r > 0]
        loss_trades = [r for r in returns if r < 0]
        
        win_rate = len(win_trades) / len(returns) if returns else 0
        avg_win = np.mean(win_trades) if win_trades else 0
        avg_loss = abs(np.mean(loss_trades)) if loss_trades else 0
        profit_factor = (avg_win * len(win_trades)) / (avg_loss * len(loss_trades)) if loss_trades else float('inf')
        
        avg_trade_duration = np.mean([trade['duration_hours'] for trade in trades])
        
        # Vietnamese market specific
        vn30_trades_data = [trade for trade in trades if trade.get('is_vn30', False)]
        vn30_trades = len(vn30_trades_data)
        vn30_performance = sum(trade['net_return'] for trade in vn30_trades_data)
        
        morning_trades = [trade for trade in trades if trade.get('session') == 'morning']
        afternoon_trades = [trade for trade in trades if trade.get('session') == 'afternoon']
        
        morning_session_performance = sum(trade['net_return'] for trade in morning_trades) if morning_trades else 0
        afternoon_session_performance = sum(trade['net_return'] for trade in afternoon_trades) if afternoon_trades else 0
        
        # Advanced metrics
        recovery_factor = total_return / max_drawdown if max_drawdown > 0 else 0
        expectancy = avg_win * win_rate - avg_loss * (1 - win_rate)
        
        # Kelly criterion
        if avg_loss > 0:
            kelly_criterion = (avg_win * win_rate - avg_loss * (1 - win_rate)) / avg_win
        else:
            kelly_criterion = win_rate if win_rate > 0 else 0
        
        # Information ratio (vs benchmark)
        excess_return_vs_benchmark = annualized_return - self.benchmark_return
        tracking_error = np.std([r - self.benchmark_return/self.trading_days_per_year for r in returns]) * np.sqrt(self.trading_days_per_year)
        information_ratio = excess_return_vs_benchmark / tracking_error if tracking_error > 0 else 0
        
        return PerformanceReport(
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            max_drawdown_duration=max_dd_duration,
            var_95=var_95,
            cvar_95=cvar_95,
            total_trades=len(trades),
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            avg_trade_duration=avg_trade_duration,
            vn30_trades=vn30_trades,
            vn30_performance=vn30_performance,
            morning_session_performance=morning_session_performance,
            afternoon_session_performance=afternoon_session_performance,
            recovery_factor=recovery_factor,
            expectancy=expectancy,
            kelly_criterion=kelly_criterion,
            information_ratio=information_ratio
        )
    
    def calculate_max_drawdown_duration(self, cumulative_returns: np.ndarray) -> int:
        """Calculate maximum drawdown duration in days."""
        if len(cumulative_returns) == 0:
            return 0
        
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdowns = running_max - cumulative_returns
        
        # Find drawdown periods
        in_drawdown = drawdowns > 1e-6  # Small threshold for numerical precision
        
        if not any(in_drawdown):
            return 0
        
        # Calculate consecutive drawdown periods
        periods = []
        current_period = 0
        
        for is_dd in in_drawdown:
            if is_dd:
                current_period += 1
            else:
                if current_period > 0:
                    periods.append(current_period)
                    current_period = 0
        
        if current_period > 0:
            periods.append(current_period)
        
        return max(periods) if periods else 0
    
    def evaluate_strategy(self, strategy_type: str, parameters: Dict[str, Any], 
                         market_data: pd.DataFrame) -> Tuple[PerformanceReport, List[Dict]]:
        """Evaluate strategy performance with given parameters."""
        
        try:
            # Create strategy instance
            strategy = self.create_strategy_instance(strategy_type, parameters)
            
            # Run simulation
            trades = self.simulate_trading(strategy, market_data)
            
            # Calculate performance
            performance = self.calculate_performance_metrics(trades)
            
            return performance, trades
            
        except Exception as e:
            self.logger.error(f"Error evaluating strategy {strategy_type}: {e}")
            # Return empty performance report
            empty_performance = PerformanceReport(
                total_return=0.0, annualized_return=0.0, volatility=0.0,
                sharpe_ratio=0.0, sortino_ratio=0.0, calmar_ratio=0.0,
                max_drawdown=0.0, max_drawdown_duration=0, var_95=0.0, cvar_95=0.0,
                total_trades=0, win_rate=0.0, profit_factor=1.0,
                avg_win=0.0, avg_loss=0.0, avg_trade_duration=0.0,
                vn30_trades=0, vn30_performance=0.0,
                morning_session_performance=0.0, afternoon_session_performance=0.0,
                recovery_factor=0.0, expectancy=0.0, kelly_criterion=0.0, information_ratio=0.0
            )
            return empty_performance, []
    
    def compare_strategies(self, strategy_results: Dict[str, Tuple[PerformanceReport, List[Dict]]]) -> Dict[str, Any]:
        """Compare multiple strategy results."""
        
        if not strategy_results:
            return {'error': 'No strategy results provided'}
        
        comparison = {
            'strategies': list(strategy_results.keys()),
            'metrics_comparison': {},
            'rankings': {},
            'summary': {}
        }
        
        # Extract metrics for comparison
        metrics_data = {}
        for strategy_name, (performance, trades) in strategy_results.items():
            metrics_data[strategy_name] = performance.to_dict()
        
        # Compare key metrics
        key_metrics = [
            'total_return', 'annualized_return', 'sharpe_ratio', 'max_drawdown',
            'win_rate', 'profit_factor', 'total_trades'
        ]
        
        for metric in key_metrics:
            metric_values = {}
            for strategy_name in strategy_results.keys():
                # Navigate nested dict structure
                for category in metrics_data[strategy_name].values():
                    if isinstance(category, dict) and metric in category:
                        metric_values[strategy_name] = category[metric]
                        break
            
            if metric_values:
                comparison['metrics_comparison'][metric] = metric_values
                
                # Rank strategies by this metric
                if metric == 'max_drawdown':
                    # Lower is better for drawdown
                    ranked = sorted(metric_values.items(), key=lambda x: x[1])
                else:
                    # Higher is better for other metrics
                    ranked = sorted(metric_values.items(), key=lambda x: x[1], reverse=True)
                
                comparison['rankings'][metric] = [name for name, value in ranked]
        
        # Overall ranking (using Sharpe ratio as primary)
        if 'sharpe_ratio' in comparison['rankings']:
            comparison['overall_ranking'] = comparison['rankings']['sharpe_ratio']
        
        # Summary statistics
        comparison['summary'] = {
            'best_total_return': max(strategy_results.keys(), 
                                   key=lambda x: strategy_results[x][0].total_return),
            'best_sharpe_ratio': max(strategy_results.keys(),
                                   key=lambda x: strategy_results[x][0].sharpe_ratio),
            'lowest_drawdown': min(strategy_results.keys(),
                                 key=lambda x: strategy_results[x][0].max_drawdown),
            'most_trades': max(strategy_results.keys(),
                             key=lambda x: strategy_results[x][0].total_trades)
        }
        
        return comparison

# Export main classes
__all__ = [
    'PerformanceEvaluator',
    'PerformanceReport'
]