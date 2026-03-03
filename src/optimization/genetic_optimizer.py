"""Genetic Algorithm Hyperparameter Optimizer for Vietnamese Trading Strategies.

This module provides evolutionary optimization using genetic algorithms to evolve
optimal strategy parameters through selection, crossover, and mutation operations.
"""

from typing import Dict, List, Any, Optional, Tuple, Callable
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import random

logger = logging.getLogger(__name__)
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy

class Individual:
    """Represents an individual (parameter set) in the genetic algorithm."""
    
    def __init__(self, genes: Dict[str, Any], fitness: float = None):
        self.genes = genes.copy()
        self.fitness = fitness
        self.age = 0
        self.generation = 0
        
    def copy(self):
        """Create a copy of the individual."""
        return Individual(self.genes.copy(), self.fitness)
    
    def __str__(self):
        return f"Individual(fitness={self.fitness}, genes={self.genes})"

class GeneticOptimizer:
    """Genetic Algorithm optimizer for trading strategy parameters."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Genetic algorithm parameters
        self.population_size = config.get('population_size', 50)
        self.n_generations = config.get('n_generations', 30)
        self.elite_ratio = config.get('elite_ratio', 0.1)  # Top 10% preserved
        self.mutation_rate = config.get('mutation_rate', 0.1)
        self.crossover_rate = config.get('crossover_rate', 0.8)
        
        # Selection methods
        self.selection_method = config.get('selection_method', 'tournament')
        self.tournament_size = config.get('tournament_size', 5)
        
        # Diversity and convergence
        self.diversity_threshold = config.get('diversity_threshold', 0.01)
        self.convergence_generations = config.get('convergence_generations', 5)
        self.adaptive_mutation = config.get('adaptive_mutation', True)
        
        # Vietnamese market specific
        self.vn30_preference = config.get('vn30_preference', True)
        self.session_awareness = config.get('session_awareness', True)
        
        # Parameter space
        self.parameter_bounds = {}
        self.parameter_types = {}  # 'int', 'float', 'choice'
        self.parameter_choices = {}  # For categorical parameters
        
        # Population and results
        self.population = []
        self.best_individual = None
        self.generation_history = []
        self.diversity_history = []
        
        # Evaluation function
        self.objective_function = None
        
        self.logger = logging.getLogger('genetic_optimizer')
    
    def set_parameter_space(self, param_bounds: Dict[str, Tuple], param_types: Dict[str, str] = None,
                           param_choices: Dict[str, List] = None):
        """Set parameter space for optimization."""
        self.parameter_bounds = param_bounds.copy()
        self.parameter_types = param_types or {k: 'float' for k in param_bounds.keys()}
        self.parameter_choices = param_choices or {}
        
        self.logger.info(f"Parameter space set: {len(param_bounds)} parameters")
    
    def set_objective_function(self, obj_func: Callable):
        """Set the objective function to maximize."""
        self.objective_function = obj_func
    
    def generate_random_individual(self) -> Individual:
        """Generate a random individual."""
        genes = {}
        
        for param_name, bounds in self.parameter_bounds.items():
            param_type = self.parameter_types.get(param_name, 'float')
            
            if param_type == 'choice':
                choices = self.parameter_choices.get(param_name, [])
                genes[param_name] = random.choice(choices) if choices else bounds[0]
            elif param_type == 'int':
                genes[param_name] = random.randint(int(bounds[0]), int(bounds[1]))
            else:  # float
                genes[param_name] = random.uniform(bounds[0], bounds[1])
        
        return Individual(genes)
    
    def initialize_population(self) -> List[Individual]:
        """Initialize the population with random individuals."""
        population = []
        
        # Generate random individuals
        for _ in range(self.population_size):
            individual = self.generate_random_individual()
            population.append(individual)
        
        # Add some known good configurations if available
        if hasattr(self, 'seed_individuals') and self.seed_individuals:
            for seed in self.seed_individuals[:self.population_size//4]:  # Up to 25% seeds
                individual = Individual(seed)
                population[len(population) % self.population_size] = individual
        
        return population
    
    def evaluate_population(self, population: List[Individual], strategy_type: str, 
                          market_data: pd.DataFrame) -> List[Individual]:
        """Evaluate fitness for all individuals in population."""
        
        def evaluate_individual(individual):
            try:
                fitness = self.objective_function(strategy_type, individual.genes, market_data)
                individual.fitness = fitness
                return individual
            except Exception as e:
                self.logger.error(f"Error evaluating individual {individual.genes}: {e}")
                individual.fitness = -1.0  # Poor fitness for failed evaluations
                return individual
        
        # Parallel evaluation
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_individual = {
                executor.submit(evaluate_individual, individual): individual 
                for individual in population if individual.fitness is None
            }
            
            for future in as_completed(future_to_individual):
                individual = future.result()
        
        return population
    
    def tournament_selection(self, population: List[Individual]) -> Individual:
        """Tournament selection method."""
        tournament = random.sample(population, min(self.tournament_size, len(population)))
        return max(tournament, key=lambda x: x.fitness or -float('inf'))
    
    def roulette_selection(self, population: List[Individual]) -> Individual:
        """Roulette wheel selection method."""
        # Ensure all fitness values are positive
        min_fitness = min(ind.fitness or 0 for ind in population)
        adjusted_fitness = [(ind.fitness or 0) - min_fitness + 1e-6 for ind in population]
        
        total_fitness = sum(adjusted_fitness)
        if total_fitness <= 0:
            return random.choice(population)
        
        pick = random.uniform(0, total_fitness)
        current = 0
        
        for i, fitness in enumerate(adjusted_fitness):
            current += fitness
            if current >= pick:
                return population[i]
        
        return population[-1]
    
    def select_parents(self, population: List[Individual], n_parents: int) -> List[Individual]:
        """Select parents for next generation."""
        parents = []
        
        for _ in range(n_parents):
            if self.selection_method == 'tournament':
                parent = self.tournament_selection(population)
            elif self.selection_method == 'roulette':
                parent = self.roulette_selection(population)
            else:
                parent = random.choice(population)
            
            parents.append(parent)
        
        return parents
    
    def crossover(self, parent1: Individual, parent2: Individual) -> Tuple[Individual, Individual]:
        """Create offspring through crossover."""
        if random.random() > self.crossover_rate:
            return parent1.copy(), parent2.copy()
        
        child1_genes = {}
        child2_genes = {}
        
        for param_name in self.parameter_bounds.keys():
            # Uniform crossover
            if random.random() < 0.5:
                child1_genes[param_name] = parent1.genes[param_name]
                child2_genes[param_name] = parent2.genes[param_name]
            else:
                child1_genes[param_name] = parent2.genes[param_name]
                child2_genes[param_name] = parent1.genes[param_name]
        
        # Arithmetic crossover for numerical parameters
        for param_name in self.parameter_bounds.keys():
            param_type = self.parameter_types.get(param_name, 'float')
            
            if param_type in ['int', 'float'] and random.random() < 0.3:  # 30% chance of arithmetic crossover
                alpha = random.random()
                val1 = parent1.genes[param_name]
                val2 = parent2.genes[param_name]
                
                new_val1 = alpha * val1 + (1 - alpha) * val2
                new_val2 = alpha * val2 + (1 - alpha) * val1
                
                # Ensure bounds
                bounds = self.parameter_bounds[param_name]
                new_val1 = max(bounds[0], min(bounds[1], new_val1))
                new_val2 = max(bounds[0], min(bounds[1], new_val2))
                
                if param_type == 'int':
                    new_val1 = int(round(new_val1))
                    new_val2 = int(round(new_val2))
                
                child1_genes[param_name] = new_val1
                child2_genes[param_name] = new_val2
        
        return Individual(child1_genes), Individual(child2_genes)
    
    def mutate(self, individual: Individual, mutation_rate: float = None) -> Individual:
        """Mutate an individual."""
        mutation_rate = mutation_rate or self.mutation_rate
        mutated = individual.copy()
        
        for param_name in self.parameter_bounds.keys():
            if random.random() < mutation_rate:
                param_type = self.parameter_types.get(param_name, 'float')
                bounds = self.parameter_bounds[param_name]
                
                if param_type == 'choice':
                    choices = self.parameter_choices.get(param_name, [bounds[0], bounds[1]])
                    mutated.genes[param_name] = random.choice(choices)
                elif param_type == 'int':
                    # Gaussian mutation for integers
                    current_val = mutated.genes[param_name]
                    mutation_strength = (bounds[1] - bounds[0]) * 0.1
                    new_val = int(round(current_val + random.gauss(0, mutation_strength)))
                    new_val = max(int(bounds[0]), min(int(bounds[1]), new_val))
                    mutated.genes[param_name] = new_val
                else:  # float
                    # Gaussian mutation for floats
                    current_val = mutated.genes[param_name]
                    mutation_strength = (bounds[1] - bounds[0]) * 0.1
                    new_val = current_val + random.gauss(0, mutation_strength)
                    new_val = max(bounds[0], min(bounds[1], new_val))
                    mutated.genes[param_name] = new_val
        
        return mutated
    
    def calculate_population_diversity(self, population: List[Individual]) -> float:
        """Calculate population diversity."""
        if len(population) < 2:
            return 0.0
        
        diversity_scores = []
        
        for param_name in self.parameter_bounds.keys():
            param_values = [ind.genes[param_name] for ind in population]
            
            if self.parameter_types.get(param_name, 'float') == 'choice':
                # For categorical parameters, count unique values
                unique_ratio = len(set(param_values)) / len(param_values)
                diversity_scores.append(unique_ratio)
            else:
                # For numerical parameters, calculate coefficient of variation
                if len(set(param_values)) == 1:
                    diversity_scores.append(0.0)
                else:
                    std_dev = np.std(param_values)
                    mean_val = np.mean(param_values)
                    cv = std_dev / abs(mean_val) if mean_val != 0 else std_dev
                    diversity_scores.append(min(cv, 1.0))  # Cap at 1.0
        
        return np.mean(diversity_scores) if diversity_scores else 0.0
    
    def adaptive_mutation_rate(self, generation: int, diversity: float) -> float:
        """Adjust mutation rate based on generation and diversity."""
        if not self.adaptive_mutation:
            return self.mutation_rate
        
        # Increase mutation if diversity is low
        base_rate = self.mutation_rate
        diversity_factor = 1.0 + (0.5 * (1.0 - diversity))  # Up to 50% increase
        
        # Decrease mutation in later generations
        generation_factor = 1.0 - (generation / self.n_generations) * 0.3  # Up to 30% decrease
        
        adaptive_rate = base_rate * diversity_factor * generation_factor
        return min(adaptive_rate, 0.5)  # Cap at 50%
    
    def optimize(self, strategy_type: str, market_data: pd.DataFrame) -> Dict[str, Any]:
        """Run genetic algorithm optimization."""
        
        if not self.objective_function:
            raise ValueError("Objective function not set")
        
        self.logger.info(f"Starting genetic algorithm optimization for {strategy_type}")
        self.logger.info(f"Population: {self.population_size}, Generations: {self.n_generations}")
        
        # Initialize
        self.population = self.initialize_population()
        self.generation_history = []
        self.diversity_history = []
        self.best_individual = None
        
        # Evolution loop
        for generation in range(self.n_generations):
            self.logger.info(f"Generation {generation + 1}/{self.n_generations}")
            
            # Evaluate population
            self.population = self.evaluate_population(self.population, strategy_type, market_data)
            
            # Sort by fitness
            self.population.sort(key=lambda x: x.fitness or -float('inf'), reverse=True)
            
            # Update best individual
            if not self.best_individual or self.population[0].fitness > self.best_individual.fitness:
                self.best_individual = self.population[0].copy()
                self.best_individual.generation = generation
                self.logger.info(f"New best fitness: {self.best_individual.fitness:.4f}")
            
            # Calculate diversity
            diversity = self.calculate_population_diversity(self.population)
            
            # Store generation statistics
            fitnesses = [ind.fitness for ind in self.population if ind.fitness is not None]
            gen_stats = {
                'generation': generation,
                'best_fitness': max(fitnesses) if fitnesses else 0,
                'mean_fitness': np.mean(fitnesses) if fitnesses else 0,
                'std_fitness': np.std(fitnesses) if len(fitnesses) > 1 else 0,
                'diversity': diversity
            }
            self.generation_history.append(gen_stats)
            self.diversity_history.append(diversity)
            
            # Check convergence
            if generation >= self.convergence_generations:
                recent_improvements = [
                    self.generation_history[i]['best_fitness'] - self.generation_history[i-1]['best_fitness']
                    for i in range(generation - self.convergence_generations + 1, generation + 1)
                ]
                if all(imp < 1e-4 for imp in recent_improvements):
                    self.logger.info(f"Converged after {generation + 1} generations")
                    break
            
            # Create next generation
            if generation < self.n_generations - 1:
                next_population = []
                
                # Elitism: preserve best individuals
                n_elite = max(1, int(self.population_size * self.elite_ratio))
                next_population.extend([ind.copy() for ind in self.population[:n_elite]])
                
                # Generate offspring
                adaptive_mut_rate = self.adaptive_mutation_rate(generation, diversity)
                
                while len(next_population) < self.population_size:
                    # Select parents
                    parent1 = self.tournament_selection(self.population)
                    parent2 = self.tournament_selection(self.population)
                    
                    # Crossover
                    child1, child2 = self.crossover(parent1, parent2)
                    
                    # Mutation
                    child1 = self.mutate(child1, adaptive_mut_rate)
                    child2 = self.mutate(child2, adaptive_mut_rate)
                    
                    # Add to next generation
                    next_population.append(child1)
                    if len(next_population) < self.population_size:
                        next_population.append(child2)
                
                # Update population
                self.population = next_population[:self.population_size]
                
                # Update generation counter
                for ind in self.population:
                    ind.generation = generation + 1
                    ind.age += 1
        
        # Compile results
        results = {
            'strategy_type': strategy_type,
            'best_parameters': self.best_individual.genes.copy() if self.best_individual else {},
            'best_fitness': self.best_individual.fitness if self.best_individual else 0,
            'optimization_history': {
                'generation_stats': self.generation_history.copy(),
                'diversity_history': self.diversity_history.copy(),
                'total_generations': len(self.generation_history)
            },
            'final_population': {
                'size': len(self.population),
                'fitness_distribution': [ind.fitness for ind in self.population if ind.fitness is not None],
                'diversity': self.calculate_population_diversity(self.population)
            },
            'algorithm_config': {
                'population_size': self.population_size,
                'mutation_rate': self.mutation_rate,
                'crossover_rate': self.crossover_rate,
                'selection_method': self.selection_method
            }
        }
        
        self.logger.info(f"Optimization completed. Best fitness: {self.best_individual.fitness:.4f}")
        return results
    
    def get_population_summary(self) -> Dict[str, Any]:
        """Get summary of current population."""
        if not self.population:
            return {'error': 'No population available'}
        
        fitnesses = [ind.fitness for ind in self.population if ind.fitness is not None]
        
        summary = {
            'population_size': len(self.population),
            'evaluated_individuals': len(fitnesses),
            'fitness_stats': {
                'best': max(fitnesses) if fitnesses else 0,
                'worst': min(fitnesses) if fitnesses else 0,
                'mean': np.mean(fitnesses) if fitnesses else 0,
                'std': np.std(fitnesses) if len(fitnesses) > 1 else 0
            },
            'diversity': self.calculate_population_diversity(self.population),
            'best_individual': {
                'parameters': self.best_individual.genes.copy() if self.best_individual else {},
                'fitness': self.best_individual.fitness if self.best_individual else 0,
                'generation': self.best_individual.generation if self.best_individual else 0
            }
        }
        
        return summary
    
    def analyze_parameter_evolution(self) -> Dict[str, Any]:
        """Analyze how parameters evolved over generations."""
        if not self.generation_history or not self.population:
            return {'error': 'Insufficient data for analysis'}
        
        analysis = {}
        
        for param_name in self.parameter_bounds.keys():
            param_type = self.parameter_types.get(param_name, 'float')
            
            # Get best parameter value from each generation
            if hasattr(self, 'generation_best_params'):
                param_evolution = [gen_best.get(param_name, 0) for gen_best in self.generation_best_params]
                
                analysis[param_name] = {
                    'type': param_type,
                    'evolution': param_evolution,
                    'final_value': self.best_individual.genes[param_name] if self.best_individual else 0,
                    'convergence_generation': self._find_convergence_generation(param_evolution)
                }
        
        return analysis
    
    def _find_convergence_generation(self, values: List) -> int:
        """Find generation where parameter converged."""
        if len(values) < 5:
            return len(values) - 1
        
        # Look for when parameter stopped changing significantly
        for i in range(4, len(values)):
            recent_std = np.std(values[i-4:i+1])
            if recent_std < 0.01:  # Converged when std < 1%
                return i
        
        return len(values) - 1

# Export main classes
__all__ = [
    'GeneticOptimizer',
    'Individual'
]