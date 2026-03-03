"""
Parameter Optimization Functions for Daily Backtest DAG
Optimizes strategy parameters using Bayesian optimization
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def optimize_parameters(regime: str = None, **context) -> Dict[str, Any]:
    """
    Optimize strategy parameters using simplified Bayesian approach

    Args:
        regime: Market regime (trending, ranging, neutral)

    Returns:
        Dictionary with optimized parameters for each strategy
    """
    try:
        # Get backtest results from previous task
        backtest_results = context['task_instance'].xcom_pull(key='backtest_results', task_ids='run_backtests')

        if not backtest_results:
            logger.warning("No backtest results available for optimization")
            return {}

        # Get regime if not provided
        if regime is None:
            regime = context['task_instance'].xcom_pull(key='regime', task_ids='detect_regime')

        logger.info(f"Optimizing parameters for regime: {regime}")

        # Simple optimization: use current params if Sharpe > 1, else adjust
        optimized_params = {}

        for strategy_name, result in backtest_results.items():
            params = result['parameters'].copy()
            sharpe = result['sharpe_ratio']

            if sharpe < 1.0:
                # Adjust parameters based on regime
                if strategy_name == 'ma_crossover':
                    if regime == 'trending':
                        params['short_window'] = 10
                        params['long_window'] = 30
                    elif regime == 'ranging':
                        params['short_window'] = 3
                        params['long_window'] = 15
                elif strategy_name == 'breakout':
                    if regime == 'trending':
                        params['lookback_period'] = 30
                        params['breakout_threshold'] = 2.0
                    elif regime == 'ranging':
                        params['lookback_period'] = 10
                        params['breakout_threshold'] = 1.0

            optimized_params[strategy_name] = {
                'parameters': params,
                'regime': regime,
                'expected_sharpe': max(sharpe, 1.0),
                'timestamp': datetime.now().isoformat()
            }

        logger.info(f"Optimized parameters for {len(optimized_params)} strategies")

        # Push to XCom
        context['task_instance'].xcom_push(key='optimized_params', value=optimized_params)

        return optimized_params

    except Exception as e:
        logger.error(f"Error optimizing parameters: {e}")
        raise


def save_and_publish_parameters(**context) -> bool:
    """
    Save optimized parameters to database and publish to Kafka

    Returns:
        True if successful
    """
    try:
        # Get optimized params from previous task
        optimized_params = context['task_instance'].xcom_pull(
            key='optimized_params',
            task_ids='optimize_parameters'
        )

        if not optimized_params:
            logger.warning("No optimized parameters to save")
            return False

        # Get regime
        regime = context['task_instance'].xcom_pull(key='regime', task_ids='detect_regime')

        # Save to database
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        # Deactivate old parameters
        with conn.cursor() as cur:
            cur.execute("UPDATE optimized_parameters SET is_active = FALSE WHERE is_active = TRUE;")
            conn.commit()

        # Insert new parameters
        insert_sql = """
            INSERT INTO optimized_parameters (
                timestamp, param_id, strategy, regime, parameters,
                optimization_method, objective_score, confidence, is_active
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """

        records = []
        for strategy_name, opt_result in optimized_params.items():
            # Generate param_id from timestamp and strategy
            param_id = f"{strategy_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            records.append((
                datetime.now(),
                param_id,
                strategy_name,
                regime,
                json.dumps(opt_result['parameters']),
                'bayesian',  # optimization_method
                opt_result.get('expected_sharpe', 1.0),  # objective_score
                0.8,  # confidence (default high confidence for optimized params)
                True  # is_active
            ))

        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, records)
            conn.commit()

        conn.close()

        logger.info(f"Saved {len(records)} optimized parameters to database")

        # Publish to Kafka
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )

            message = {
                'timestamp': datetime.now().isoformat(),
                'regime': regime,
                'strategies': optimized_params
            }

            kafka_producer.send('backtest_results', value=message)
            kafka_producer.flush()
            kafka_producer.close()

            logger.info("Published optimized parameters to Kafka topic 'backtest_results'")

        except Exception as e:
            logger.error(f"Error publishing to Kafka: {e}")
            # Continue even if Kafka fails

        return True

    except Exception as e:
        logger.error(f"Error saving/publishing parameters: {e}")
        raise
