"""Walk-Forward Analysis for Vietnamese Trading Strategy Optimization.

This module provides walk-forward optimization and analysis to validate strategy
parameters across different time periods and market conditions.
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

from src.optimization.grid_search_optimizer import GridSearchOptimizer
from src.optimization.bayesian_optimizer import BayesianOptimizer
from src.optimization.genetic_optimizer import GeneticOptimizer
from src.optimization.performance_evaluator import PerformanceEvaluator, PerformanceReport

@dataclass
class WalkForwardWindow:
    """Represents a single walk-forward analysis window."""
    
    window_id: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    train_data: pd.DataFrame
    test_data: pd.DataFrame
    
    # Optimization results
    optimization_method: str = None
    best_parameters: Dict[str, Any] = None
    optimization_score: float = 0.0
    optimization_time: float = 0.0  # seconds
    
    # Out-of-sample performance
    oos_performance: PerformanceReport = None
    oos_trades: List[Dict] = None
    
    def __str__(self):
        return f"Window {self.window_id}: Train({self.train_start.date()}-{self.train_end.date()}) Test({self.test_start.date()}-{self.test_end.date()})"

class WalkForwardAnalyzer:
    """Walk-forward analysis for strategy optimization and validation."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Walk-forward settings
        self.train_period_months = config.get('train_period_months', 6)
        self.test_period_months = config.get('test_period_months', 1)
        self.step_months = config.get('step_months', 1)  # How much to advance each window
        self.min_train_days = config.get('min_train_days', 60)
        self.min_test_days = config.get('min_test_days', 20)
        
        # Optimization settings
        self.optimization_method = config.get('optimization_method', 'grid_search')
        self.optimization_metric = config.get('optimization_metric', 'sharpe_ratio')
        self.parallel_windows = config.get('parallel_windows', True)
        
        # Analysis settings
        self.stability_threshold = config.get('stability_threshold', 0.3)  # Parameter stability
        self.performance_threshold = config.get('performance_threshold', 0.0)  # Min acceptable performance
        
        # Results storage
        self.windows = []
        self.analysis_results = {}
        
        # Components
        self.performance_evaluator = PerformanceEvaluator(config.get('performance_config', {}))
        
        self.logger = logging.getLogger('walk_forward_analyzer')
    
    def create_walk_forward_windows(self, market_data: pd.DataFrame) -> List[WalkForwardWindow]:
        """Create walk-forward analysis windows from market data."""
        
        if 'timestamp' not in market_data.columns:
            raise ValueError("Market data must have 'timestamp' column")
        
        # Sort by timestamp
        market_data = market_data.sort_values('timestamp')
        
        # Get date range
        start_date = market_data['timestamp'].min()
        end_date = market_data['timestamp'].max()
        
        self.logger.info(f"Creating walk-forward windows from {start_date.date()} to {end_date.date()}")
        
        windows = []
        window_id = 1
        
        # Calculate initial training period
        train_start = start_date
        
        while True:
            # Calculate window dates
            train_end = train_start + timedelta(days=self.train_period_months * 30)
            test_start = train_end + timedelta(days=1)
            test_end = test_start + timedelta(days=self.test_period_months * 30)
            
            # Check if we have enough data
            if test_end > end_date:
                break
            
            # Extract data for this window
            train_data = market_data[
                (market_data['timestamp'] >= train_start) & 
                (market_data['timestamp'] <= train_end)
            ].copy()
            
            test_data = market_data[
                (market_data['timestamp'] >= test_start) & 
                (market_data['timestamp'] <= test_end)
            ].copy()
            
            # Validate minimum data requirements
            if len(train_data) < self.min_train_days or len(test_data) < self.min_test_days:
                self.logger.warning(f"Skipping window {window_id}: insufficient data")
                train_start += timedelta(days=self.step_months * 30)
                window_id += 1
                continue
            
            # Create window
            window = WalkForwardWindow(
                window_id=window_id,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                train_data=train_data,
                test_data=test_data
            )
            
            windows.append(window)
            self.logger.info(f"Created {window}")
            
            # Advance to next window
            train_start += timedelta(days=self.step_months * 30)
            window_id += 1
        
        self.logger.info(f"Created {len(windows)} walk-forward windows")
        return windows
    
    def optimize_window(self, window: WalkForwardWindow, strategy_type: str,
                       parameter_bounds: Dict[str, Tuple], parameter_types: Dict[str, str] = None) -> WalkForwardWindow:
        """Optimize parameters for a single walk-forward window."""
        
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Optimizing {window} using {self.optimization_method}")
            
            # Choose optimization method
            if self.optimization_method == 'grid_search':
                optimizer = GridSearchOptimizer(self.config.get('grid_search_config', {}))
                
                # Convert bounds to grid format
                param_grid = {}
                for param, bounds in parameter_bounds.items():
                    param_type = parameter_types.get(param, 'float') if parameter_types else 'float'
                    
                    if param_type == 'int':
                        param_grid[param] = list(range(int(bounds[0]), int(bounds[1]) + 1, 
                                                     max(1, (int(bounds[1]) - int(bounds[0])) // 5)))
                    else:  # float
                        param_grid[param] = list(np.linspace(bounds[0], bounds[1], 5))
                
                # Run optimization
                results = optimizer.optimize_strategy(strategy_type, window.train_data, param_grid)
                
                window.best_parameters = results.get('best_parameters', {})
                window.optimization_score = results.get('best_performance', {}).get(self.optimization_metric, 0)
                
            elif self.optimization_method == 'bayesian':
                optimizer = BayesianOptimizer(self.config.get('bayesian_config', {}))
                optimizer.set_parameter_space(parameter_bounds, parameter_types)
                
                # Define objective function
                def objective(strat_type, params, data):
                    perf, _ = self.performance_evaluator.evaluate_strategy(strat_type, params, data)
                    return getattr(perf, self.optimization_metric, 0)
                
                optimizer.set_objective_function(objective)
                
                # Run optimization
                results = optimizer.optimize(strategy_type, window.train_data)
                
                window.best_parameters = results.get('best_parameters', {})
                window.optimization_score = results.get('best_score', 0)
                
            elif self.optimization_method == 'genetic':
                optimizer = GeneticOptimizer(self.config.get('genetic_config', {}))
                optimizer.set_parameter_space(parameter_bounds, parameter_types)
                
                # Define objective function
                def objective(strat_type, params, data):
                    perf, _ = self.performance_evaluator.evaluate_strategy(strat_type, params, data)
                    return getattr(perf, self.optimization_metric, 0)
                
                optimizer.set_objective_function(objective)
                
                # Run optimization
                results = optimizer.optimize(strategy_type, window.train_data)
                
                window.best_parameters = results.get('best_parameters', {})
                window.optimization_score = results.get('best_fitness', 0)
                
            else:
                raise ValueError(f"Unknown optimization method: {self.optimization_method}")
            
            window.optimization_method = self.optimization_method
            window.optimization_time = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"Optimization completed for {window} in {window.optimization_time:.1f}s")
            self.logger.info(f"Best parameters: {window.best_parameters}")
            self.logger.info(f"Optimization score ({self.optimization_metric}): {window.optimization_score:.4f}")
            
        except Exception as e:
            self.logger.error(f"Optimization failed for {window}: {e}")
            window.best_parameters = {}
            window.optimization_score = 0.0
            window.optimization_time = (datetime.now() - start_time).total_seconds()
        
        return window
    
    def test_window(self, window: WalkForwardWindow, strategy_type: str) -> WalkForwardWindow:
        """Test optimized parameters on out-of-sample data."""
        
        if not window.best_parameters:
            self.logger.warning(f"No parameters to test for {window}")
            return window
        
        try:
            self.logger.info(f"Testing {window} on out-of-sample data")
            
            # Evaluate on test data
            performance, trades = self.performance_evaluator.evaluate_strategy(
                strategy_type, window.best_parameters, window.test_data
            )
            
            window.oos_performance = performance
            window.oos_trades = trades
            
            self.logger.info(f"Out-of-sample performance: {getattr(performance, self.optimization_metric, 0):.4f}")
            
        except Exception as e:
            self.logger.error(f"Testing failed for {window}: {e}")
        
        return window
    
    def run_walk_forward_analysis(self, strategy_type: str, market_data: pd.DataFrame,
                                parameter_bounds: Dict[str, Tuple], parameter_types: Dict[str, str] = None) -> Dict[str, Any]:
        """Run complete walk-forward analysis."""
        
        self.logger.info(f"Starting walk-forward analysis for {strategy_type}")
        
        # Create windows
        self.windows = self.create_walk_forward_windows(market_data)
        
        if not self.windows:
            return {'error': 'No valid walk-forward windows created'}
        
        # Process each window
        completed_windows = []
        
        for window in self.windows:
            try:
                # Optimize parameters
                window = self.optimize_window(window, strategy_type, parameter_bounds, parameter_types)
                
                # Test on out-of-sample data
                window = self.test_window(window, strategy_type)
                
                completed_windows.append(window)
                
            except Exception as e:
                self.logger.error(f"Failed to process {window}: {e}")
        
        self.windows = completed_windows
        
        # Analyze results
        self.analysis_results = self.analyze_walk_forward_results(strategy_type)
        
        return self.analysis_results
    
    def analyze_walk_forward_results(self, strategy_type: str) -> Dict[str, Any]:
        """Analyze walk-forward optimization results."""
        
        if not self.windows:
            return {'error': 'No windows to analyze'}
        
        # Extract results
        in_sample_scores = [w.optimization_score for w in self.windows if w.optimization_score is not None]
        oos_scores = []
        parameter_history = []
        
        for window in self.windows:
            if window.oos_performance:
                oos_scores.append(getattr(window.oos_performance, self.optimization_metric, 0))
            
            if window.best_parameters:
                parameter_history.append(window.best_parameters.copy())
        
        # Performance analysis
        performance_analysis = {
            'in_sample_performance': {
                'mean': np.mean(in_sample_scores) if in_sample_scores else 0,
                'std': np.std(in_sample_scores) if len(in_sample_scores) > 1 else 0,
                'min': min(in_sample_scores) if in_sample_scores else 0,
                'max': max(in_sample_scores) if in_sample_scores else 0
            },
            'out_of_sample_performance': {
                'mean': np.mean(oos_scores) if oos_scores else 0,
                'std': np.std(oos_scores) if len(oos_scores) > 1 else 0,
                'min': min(oos_scores) if oos_scores else 0,
                'max': max(oos_scores) if oos_scores else 0
            }
        }
        
        # Calculate overfitting metric
        if in_sample_scores and oos_scores:
            overfitting_ratio = (np.mean(in_sample_scores) - np.mean(oos_scores)) / abs(np.mean(in_sample_scores))
            performance_analysis['overfitting_ratio'] = overfitting_ratio
            performance_analysis['is_overfitted'] = overfitting_ratio > 0.3  # 30% threshold
        
        # Parameter stability analysis
        stability_analysis = self.analyze_parameter_stability(parameter_history)
        
        # Time-based analysis
        time_analysis = self.analyze_time_patterns()
        
        # Overall assessment
        assessment = self.assess_strategy_robustness()
        
        return {
            'strategy_type': strategy_type,
            'windows_analyzed': len(self.windows),
            'optimization_method': self.optimization_method,
            'performance_analysis': performance_analysis,
            'parameter_stability': stability_analysis,
            'time_analysis': time_analysis,
            'robustness_assessment': assessment,
            'detailed_windows': [self.window_summary(w) for w in self.windows]
        }
    
    def analyze_parameter_stability(self, parameter_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze stability of optimized parameters across windows."""
        
        if len(parameter_history) < 2:
            return {'error': 'Insufficient parameter history for stability analysis'}
        
        stability = {}
        
        # Get all parameter names
        all_params = set()
        for params in parameter_history:
            all_params.update(params.keys())
        
        for param_name in all_params:
            param_values = []
            for params in parameter_history:
                if param_name in params:
                    param_values.append(params[param_name])
            
            if len(param_values) < 2:
                continue
            
            # Calculate stability metrics
            if isinstance(param_values[0], (int, float)):
                # Numerical parameter
                mean_val = np.mean(param_values)
                std_val = np.std(param_values)
                cv = std_val / abs(mean_val) if mean_val != 0 else 0  # Coefficient of variation
                
                stability[param_name] = {
                    'type': 'numerical',
                    'mean': mean_val,
                    'std': std_val,
                    'coefficient_of_variation': cv,
                    'is_stable': cv < self.stability_threshold,
                    'min': min(param_values),
                    'max': max(param_values),
                    'trend': self.calculate_parameter_trend(param_values)
                }
            else:
                # Categorical parameter
                unique_values = list(set(param_values))
                most_common = max(set(param_values), key=param_values.count)
                consistency = param_values.count(most_common) / len(param_values)
                
                stability[param_name] = {
                    'type': 'categorical',
                    'unique_values': unique_values,
                    'most_common': most_common,
                    'consistency': consistency,
                    'is_stable': consistency > (1 - self.stability_threshold)
                }
        
        # Overall stability score
        stable_params = sum(1 for p in stability.values() if p.get('is_stable', False))
        overall_stability = stable_params / len(stability) if stability else 0
        
        return {
            'parameter_details': stability,
            'overall_stability_score': overall_stability,
            'stable_parameters': stable_params,
            'total_parameters': len(stability)
        }
    
    def calculate_parameter_trend(self, values: List[float]) -> str:
        """Calculate trend direction for parameter values."""
        if len(values) < 3:
            return 'insufficient_data'
        
        # Simple linear regression slope
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if slope > 0.01:
            return 'increasing'
        elif slope < -0.01:
            return 'decreasing'
        else:
            return 'stable'
    
    def analyze_time_patterns(self) -> Dict[str, Any]:
        """Analyze performance patterns over time."""
        
        if len(self.windows) < 3:
            return {'error': 'Insufficient windows for time analysis'}
        
        # Extract time series data
        window_dates = [w.test_start for w in self.windows]
        oos_performances = []
        
        for window in self.windows:
            if window.oos_performance:
                oos_performances.append(getattr(window.oos_performance, self.optimization_metric, 0))
            else:
                oos_performances.append(0)
        
        # Time-based analysis
        time_analysis = {
            'performance_trend': self.calculate_parameter_trend(oos_performances),
            'best_period': window_dates[np.argmax(oos_performances)] if oos_performances else None,
            'worst_period': window_dates[np.argmin(oos_performances)] if oos_performances else None,
            'performance_volatility': np.std(oos_performances) if len(oos_performances) > 1 else 0,
            'consistent_periods': sum(1 for p in oos_performances if p > self.performance_threshold)
        }
        
        # Seasonal analysis (if enough data)
        if len(self.windows) >= 12:  # At least a year
            monthly_performance = {}
            for window, performance in zip(self.windows, oos_performances):
                month = window.test_start.month
                if month not in monthly_performance:
                    monthly_performance[month] = []
                monthly_performance[month].append(performance)
            
            # Calculate average performance by month
            monthly_avg = {
                month: np.mean(performances) 
                for month, performances in monthly_performance.items()
            }
            
            time_analysis['seasonal_patterns'] = monthly_avg
        
        return time_analysis
    
    def assess_strategy_robustness(self) -> Dict[str, Any]:
        """Assess overall strategy robustness based on walk-forward results."""
        
        if not self.windows:
            return {'error': 'No windows to assess'}
        
        # Performance consistency
        oos_scores = []
        for window in self.windows:
            if window.oos_performance:
                oos_scores.append(getattr(window.oos_performance, self.optimization_metric, 0))
        
        performance_consistency = len([s for s in oos_scores if s > self.performance_threshold]) / len(oos_scores) if oos_scores else 0
        
        # Parameter stability (from previous analysis)
        stability_results = self.analyze_parameter_stability([w.best_parameters for w in self.windows if w.best_parameters])
        parameter_stability = stability_results.get('overall_stability_score', 0)
        
        # Overfitting assessment
        in_sample_scores = [w.optimization_score for w in self.windows if w.optimization_score is not None]
        overfitting_score = 0
        if in_sample_scores and oos_scores:
            overfitting_ratio = (np.mean(in_sample_scores) - np.mean(oos_scores)) / abs(np.mean(in_sample_scores))
            overfitting_score = max(0, 1 - abs(overfitting_ratio))  # Higher is better
        
        # Overall robustness score
        robustness_score = (performance_consistency * 0.4 + parameter_stability * 0.3 + overfitting_score * 0.3)
        
        # Recommendations
        recommendations = []
        if performance_consistency < 0.5:
            recommendations.append("Strategy shows inconsistent performance across time periods")
        if parameter_stability < 0.5:
            recommendations.append("Parameters are unstable across optimization windows")
        if overfitting_score < 0.7:
            recommendations.append("Strategy may be overfitted to in-sample data")
        if robustness_score > 0.7:
            recommendations.append("Strategy shows good robustness characteristics")
        
        return {
            'overall_robustness_score': robustness_score,
            'performance_consistency': performance_consistency,
            'parameter_stability': parameter_stability,
            'overfitting_resistance': overfitting_score,
            'assessment': 'robust' if robustness_score > 0.7 else 'moderate' if robustness_score > 0.5 else 'poor',
            'recommendations': recommendations
        }
    
    def window_summary(self, window: WalkForwardWindow) -> Dict[str, Any]:
        """Create summary of a single window."""
        summary = {
            'window_id': window.window_id,
            'train_period': f"{window.train_start.date()} to {window.train_end.date()}",
            'test_period': f"{window.test_start.date()} to {window.test_end.date()}",
            'optimization_method': window.optimization_method,
            'optimization_time': window.optimization_time,
            'best_parameters': window.best_parameters.copy() if window.best_parameters else {},
            'in_sample_score': window.optimization_score
        }
        
        if window.oos_performance:
            summary['out_of_sample_performance'] = {
                'metric_value': getattr(window.oos_performance, self.optimization_metric, 0),
                'total_return': window.oos_performance.total_return,
                'total_trades': window.oos_performance.total_trades,
                'win_rate': window.oos_performance.win_rate
            }
        
        return summary

# Export main classes
__all__ = [
    'WalkForwardAnalyzer',
    'WalkForwardWindow'
]