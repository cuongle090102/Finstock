"""Bayesian Hyperparameter Optimizer for Vietnamese Trading Strategies.

This module provides intelligent Bayesian optimization using Gaussian Processes
to efficiently find optimal strategy parameters with minimal evaluations.
"""

from typing import Dict, List, Any, Optional, Tuple, Callable
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from scipy.optimize import minimize

logger = logging.getLogger(__name__)
from scipy.stats import norm
import warnings
warnings.filterwarnings('ignore')

from src.optimization.grid_search_optimizer import PerformanceMetrics

class GaussianProcess:
    """Simple Gaussian Process implementation for Bayesian optimization."""
    
    def __init__(self, kernel_lengthscale: float = 1.0, kernel_variance: float = 1.0, 
                 noise_variance: float = 1e-6):
        self.lengthscale = kernel_lengthscale
        self.variance = kernel_variance
        self.noise = noise_variance
        
        # Training data
        self.X_train = None
        self.y_train = None
        self.K_inv = None
        
    def rbf_kernel(self, X1: np.ndarray, X2: np.ndarray) -> np.ndarray:
        """Radial Basis Function (RBF) kernel."""
        # Compute squared Euclidean distances
        sqdist = np.sum(X1**2, axis=1).reshape(-1, 1) + \
                np.sum(X2**2, axis=1) - 2 * np.dot(X1, X2.T)
        return self.variance * np.exp(-0.5 * sqdist / self.lengthscale**2)
    
    def fit(self, X: np.ndarray, y: np.ndarray):
        """Fit Gaussian Process to training data."""
        self.X_train = X.copy()
        self.y_train = y.copy()
        
        # Compute kernel matrix
        K = self.rbf_kernel(X, X)
        K += self.noise * np.eye(len(X))  # Add noise
        
        # Compute inverse for predictions
        try:
            self.K_inv = np.linalg.inv(K)
        except np.linalg.LinAlgError:
            # Add more noise if matrix is singular
            K += 1e-3 * np.eye(len(X))
            self.K_inv = np.linalg.inv(K)
    
    def predict(self, X_new: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Predict mean and variance at new points."""
        if self.X_train is None:
            raise ValueError("Model not fitted yet")
        
        # Kernel between training and new points
        K_star = self.rbf_kernel(self.X_train, X_new)
        
        # Kernel between new points
        K_star_star = self.rbf_kernel(X_new, X_new)
        
        # Mean prediction
        mu = K_star.T @ self.K_inv @ self.y_train
        
        # Variance prediction
        sigma_squared = np.diag(K_star_star) - np.diag(K_star.T @ self.K_inv @ K_star)
        sigma_squared = np.maximum(sigma_squared, 1e-10)  # Ensure positive
        
        return mu, np.sqrt(sigma_squared)

class AcquisitionFunction:
    """Acquisition functions for Bayesian optimization."""
    
    @staticmethod
    def expected_improvement(mu: np.ndarray, sigma: np.ndarray, 
                           f_best: float, xi: float = 0.01) -> np.ndarray:
        """Expected Improvement acquisition function."""
        with np.errstate(divide='warn'):
            imp = mu - f_best - xi
            Z = imp / sigma
            ei = imp * norm.cdf(Z) + sigma * norm.pdf(Z)
            ei[sigma == 0.0] = 0.0
        return ei
    
    @staticmethod
    def upper_confidence_bound(mu: np.ndarray, sigma: np.ndarray, 
                              beta: float = 2.0) -> np.ndarray:
        """Upper Confidence Bound acquisition function."""
        return mu + beta * sigma
    
    @staticmethod
    def probability_of_improvement(mu: np.ndarray, sigma: np.ndarray,
                                 f_best: float, xi: float = 0.01) -> np.ndarray:
        """Probability of Improvement acquisition function."""
        with np.errstate(divide='warn'):
            Z = (mu - f_best - xi) / sigma
            pi = norm.cdf(Z)
            pi[sigma == 0.0] = 0.0
        return pi

class BayesianOptimizer:
    """Bayesian hyperparameter optimizer using Gaussian Process."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Optimization settings
        self.n_initial_points = config.get('n_initial_points', 10)
        self.n_iterations = config.get('n_iterations', 50)
        self.acquisition_function = config.get('acquisition_function', 'expected_improvement')
        self.xi = config.get('exploration_weight', 0.01)
        self.beta = config.get('confidence_weight', 2.0)
        
        # Gaussian Process settings
        self.gp_lengthscale = config.get('gp_lengthscale', 1.0)
        self.gp_variance = config.get('gp_variance', 1.0)
        self.gp_noise = config.get('gp_noise', 1e-6)
        
        # Strategy evaluation
        self.objective_function = None
        self.parameter_bounds = {}
        self.parameter_types = {}  # 'int' or 'float'
        
        # Results storage
        self.X_history = []  # Parameter history
        self.y_history = []  # Objective values
        self.best_params = None
        self.best_score = -np.inf
        
        # Gaussian Process model
        self.gp = GaussianProcess(
            kernel_lengthscale=self.gp_lengthscale,
            kernel_variance=self.gp_variance,
            noise_variance=self.gp_noise
        )
        
        # Vietnamese market specific
        self.vn30_boost = config.get('vn30_performance_boost', 0.05)
        self.session_penalty = config.get('off_hours_penalty', 0.1)
        
        self.logger = logging.getLogger('bayesian_optimizer')
    
    def set_parameter_space(self, param_bounds: Dict[str, Tuple], param_types: Dict[str, str] = None):
        """Set parameter bounds and types for optimization."""
        self.parameter_bounds = param_bounds.copy()
        self.parameter_types = param_types or {k: 'float' for k in param_bounds.keys()}
        
        self.logger.info(f"Parameter space set: {len(param_bounds)} parameters")
        for param, bounds in param_bounds.items():
            param_type = self.parameter_types.get(param, 'float')
            self.logger.info(f"  {param} ({param_type}): {bounds}")
    
    def set_objective_function(self, obj_func: Callable):
        """Set the objective function to maximize."""
        self.objective_function = obj_func
        
    def normalize_parameters(self, params: Dict[str, Any]) -> np.ndarray:
        """Normalize parameters to [0, 1] range."""
        normalized = []
        for param_name in sorted(self.parameter_bounds.keys()):
            value = params[param_name]
            min_val, max_val = self.parameter_bounds[param_name]
            normalized_val = (value - min_val) / (max_val - min_val)
            normalized.append(normalized_val)
        return np.array(normalized)
    
    def denormalize_parameters(self, normalized: np.ndarray) -> Dict[str, Any]:
        """Convert normalized parameters back to original scale."""
        params = {}
        param_names = sorted(self.parameter_bounds.keys())
        
        for i, param_name in enumerate(param_names):
            min_val, max_val = self.parameter_bounds[param_name]
            value = normalized[i] * (max_val - min_val) + min_val
            
            # Handle integer parameters
            if self.parameter_types.get(param_name, 'float') == 'int':
                value = int(round(value))
            
            params[param_name] = value
        
        return params
    
    def generate_initial_points(self) -> List[Dict[str, Any]]:
        """Generate initial random points for exploration."""
        initial_points = []
        
        # Generate random points in normalized space
        n_params = len(self.parameter_bounds)
        random_points = np.random.random((self.n_initial_points, n_params))
        
        for point in random_points:
            params = self.denormalize_parameters(point)
            initial_points.append(params)
        
        # Add some edge cases and center point
        if self.n_initial_points >= 3:
            # Add center point
            center_point = np.array([0.5] * n_params)
            center_params = self.denormalize_parameters(center_point)
            initial_points[-1] = center_params
            
            # Add corner points if we have space
            if self.n_initial_points >= n_params + 2:
                for i in range(min(n_params, self.n_initial_points - 2)):
                    corner = np.array([0.5] * n_params)
                    corner[i] = 0.0  # One parameter at minimum
                    corner_params = self.denormalize_parameters(corner)
                    initial_points[i] = corner_params
        
        return initial_points
    
    def acquire_next_point(self) -> Dict[str, Any]:
        """Find next point to evaluate using acquisition function."""
        
        if len(self.X_history) < 2:
            # Not enough data for GP, return random point
            random_point = np.random.random(len(self.parameter_bounds))
            return self.denormalize_parameters(random_point)
        
        # Fit Gaussian Process
        X_train = np.array(self.X_history)
        y_train = np.array(self.y_history)
        self.gp.fit(X_train, y_train)
        
        # Optimize acquisition function
        best_x = None
        best_acq = -np.inf
        
        # Try multiple random restarts
        n_restarts = 20
        for _ in range(n_restarts):
            # Random starting point
            x0 = np.random.random(len(self.parameter_bounds))
            
            # Optimize acquisition function
            def negative_acquisition(x):
                x = x.reshape(1, -1)
                mu, sigma = self.gp.predict(x)
                
                if self.acquisition_function == 'expected_improvement':
                    acq = AcquisitionFunction.expected_improvement(mu, sigma, max(self.y_history), self.xi)
                elif self.acquisition_function == 'upper_confidence_bound':
                    acq = AcquisitionFunction.upper_confidence_bound(mu, sigma, self.beta)
                elif self.acquisition_function == 'probability_of_improvement':
                    acq = AcquisitionFunction.probability_of_improvement(mu, sigma, max(self.y_history), self.xi)
                else:
                    acq = mu  # Default to mean
                
                return -acq[0]  # Minimize negative acquisition
            
            # Bounds for optimization (normalized space)
            bounds = [(0.0, 1.0)] * len(self.parameter_bounds)
            
            try:
                result = minimize(negative_acquisition, x0, bounds=bounds, method='L-BFGS-B')
                if result.success and -result.fun > best_acq:
                    best_acq = -result.fun
                    best_x = result.x
            except (ValueError, RuntimeError, np.linalg.LinAlgError) as e:
                logger.debug(f"Optimization iteration failed: {e}")
                continue
        
        # Fallback to random if optimization failed
        if best_x is None:
            best_x = np.random.random(len(self.parameter_bounds))
        
        return self.denormalize_parameters(best_x)
    
    def optimize(self, strategy_type: str, market_data: pd.DataFrame) -> Dict[str, Any]:
        """Run Bayesian optimization."""
        
        if not self.objective_function:
            raise ValueError("Objective function not set")
        
        self.logger.info(f"Starting Bayesian optimization for {strategy_type}")
        self.logger.info(f"Parameters: {self.n_initial_points} initial + {self.n_iterations} iterations")
        
        # Reset history
        self.X_history = []
        self.y_history = []
        self.best_score = -np.inf
        self.best_params = None
        
        # Phase 1: Initial random exploration
        self.logger.info("Phase 1: Initial exploration")
        initial_points = self.generate_initial_points()
        
        for i, params in enumerate(initial_points):
            self.logger.info(f"Evaluating initial point {i+1}/{len(initial_points)}")
            
            try:
                # Evaluate objective function
                score = self.objective_function(strategy_type, params, market_data)
                
                # Store results
                normalized_params = self.normalize_parameters(params)
                self.X_history.append(normalized_params)
                self.y_history.append(score)
                
                # Update best
                if score > self.best_score:
                    self.best_score = score
                    self.best_params = params.copy()
                    self.logger.info(f"New best score: {score:.4f}")
                
            except Exception as e:
                self.logger.error(f"Error evaluating parameters {params}: {e}")
                # Add poor score to continue optimization
                normalized_params = self.normalize_parameters(params)
                self.X_history.append(normalized_params)
                self.y_history.append(-1.0)
        
        # Phase 2: Bayesian optimization iterations
        self.logger.info("Phase 2: Bayesian optimization")
        
        for iteration in range(self.n_iterations):
            self.logger.info(f"Iteration {iteration+1}/{self.n_iterations}")
            
            try:
                # Acquire next point
                next_params = self.acquire_next_point()
                
                # Evaluate objective
                score = self.objective_function(strategy_type, next_params, market_data)
                
                # Store results
                normalized_params = self.normalize_parameters(next_params)
                self.X_history.append(normalized_params)
                self.y_history.append(score)
                
                # Update best
                if score > self.best_score:
                    self.best_score = score
                    self.best_params = next_params.copy()
                    self.logger.info(f"New best score: {score:.4f} at iteration {iteration+1}")
                
                # Early stopping if converged
                if len(self.y_history) >= 10:
                    recent_improvement = max(self.y_history[-10:]) - max(self.y_history[-20:-10]) if len(self.y_history) >= 20 else float('inf')
                    if abs(recent_improvement) < 1e-4:
                        self.logger.info(f"Converged after {iteration+1} iterations")
                        break
                
            except Exception as e:
                self.logger.error(f"Error in iteration {iteration+1}: {e}")
        
        # Compile results
        results = {
            'strategy_type': strategy_type,
            'best_parameters': self.best_params.copy() if self.best_params else {},
            'best_score': self.best_score,
            'optimization_history': {
                'parameters': [self.denormalize_parameters(x) for x in self.X_history],
                'scores': self.y_history.copy(),
                'iterations': len(self.y_history)
            },
            'convergence_info': {
                'total_evaluations': len(self.y_history),
                'improvement_iterations': sum(1 for i, score in enumerate(self.y_history) 
                                            if i == 0 or score > max(self.y_history[:i])),
                'final_gp_confidence': self.estimate_final_confidence()
            }
        }
        
        self.logger.info(f"Optimization completed. Best score: {self.best_score:.4f}")
        return results
    
    def estimate_final_confidence(self) -> float:
        """Estimate confidence in the final result."""
        if len(self.X_history) < 5:
            return 0.0
        
        try:
            # Fit final GP model
            X_train = np.array(self.X_history)
            y_train = np.array(self.y_history)
            self.gp.fit(X_train, y_train)
            
            # Predict at best point
            best_normalized = self.normalize_parameters(self.best_params)
            mu, sigma = self.gp.predict(best_normalized.reshape(1, -1))
            
            # Confidence based on uncertainty
            confidence = 1.0 / (1.0 + sigma[0])
            return float(confidence)
            
        except Exception:
            return 0.5  # Default moderate confidence
    
    def suggest_next_experiments(self, n_suggestions: int = 3) -> List[Dict[str, Any]]:
        """Suggest next parameter combinations to try."""
        if len(self.X_history) < 2:
            return self.generate_initial_points()[:n_suggestions]
        
        suggestions = []
        for _ in range(n_suggestions):
            next_point = self.acquire_next_point()
            suggestions.append(next_point)
        
        return suggestions
    
    def get_parameter_importance(self) -> Dict[str, float]:
        """Analyze parameter importance from optimization history."""
        if len(self.X_history) < 10:
            return {param: 0.0 for param in self.parameter_bounds.keys()}
        
        importance = {}
        param_names = sorted(self.parameter_bounds.keys())
        
        try:
            # Calculate correlation between each parameter and performance
            X_array = np.array(self.X_history)
            y_array = np.array(self.y_history)
            
            for i, param_name in enumerate(param_names):
                param_values = X_array[:, i]
                correlation = np.corrcoef(param_values, y_array)[0, 1]
                importance[param_name] = abs(correlation) if not np.isnan(correlation) else 0.0
            
        except Exception as e:
            self.logger.error(f"Error calculating parameter importance: {e}")
            importance = {param: 0.0 for param in param_names}
        
        return importance

# Export main classes
__all__ = [
    'BayesianOptimizer',
    'GaussianProcess',
    'AcquisitionFunction'
]