"""
Daily Backtest DAG - Automated Strategy Backtesting and Optimization

CRITICAL: Runs daily at 06:00 VIETNAM TIME (UTC+7)
         = 23:00 UTC (previous day)

PURPOSE:
--------
Run daily backtests and parameter optimization before market open.

Workflow (All times in VIETNAM TIME UTC+7):
--------------------------------------------
1. 06:00 VN - Check data availability (GATE: skip if < 7 days)
2. 06:00 VN - Fetch 60 days of market data from TimescaleDB
3. 06:15 VN - Detect market regime (trending/ranging/neutral)
4. 06:20 VN - Run backtests for all 4 strategies
5. 06:30 VN - Optimize parameters using Bayesian optimization
6. 06:45 VN - Save results to DB and publish to Kafka

Timing Rationale:
-----------------
- Vietnamese market opens at 09:00 (9:00 AM)
- Backtest runs at 06:00 (6:00 AM) to allow:
  * 3 hours before market open
  * Sufficient time for parameter optimization
  * Updated strategies ready for trading day
  * No interference with live market data collection

TIMEZONE CONVERSION:
-------------------
Vietnam Time (UTC+7) → UTC for Airflow Schedule
06:00 VN → 06:00 - 7 hours = 23:00 UTC (previous day)

Example:
Run date:    2025-10-22 06:00 VN = 2025-10-21 23:00 UTC
Market open: 2025-10-22 09:00 VN (3 hours after backtest)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project root to path for src imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Add functions directory to path
sys.path.insert(0, os.path.dirname(__file__))

from functions.backtest_functions import (
    fetch_market_data_from_db,
    run_backtests,
    save_backtest_results,
    check_data_availability_task
)
from functions.regime_detection import detect_regime
from functions.parameter_optimization import (
    optimize_parameters,
    save_and_publish_parameters
)

# Default arguments
default_args = {
    'owner': 'trading-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

# Create DAG
dag = DAG(
    'daily_backtest_optimization',
    default_args=default_args,
    description='Daily automated backtesting and parameter optimization',
    # CRITICAL TIMEZONE CONVERSION:
    # Runs at 06:00 VIETNAM TIME (UTC+7)
    # = 23:00 UTC (previous day)
    # Example: 2025-10-22 06:00 VN = 2025-10-21 23:00 UTC (3 hours before market open)
    schedule_interval='0 23 * * *',  # 23:00 UTC = 06:00 Vietnam (next day, 3h before market)
    catchup=False,
    max_active_runs=1,
    tags=['backtest', 'optimization', 'phase2']
)

# Task 0: Check data availability (GATE TASK - must pass before proceeding)
# Skips entire DAG if < 7 days of data available
check_data = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability_task,
    provide_context=True,
    dag=dag
)

# Task 1: Fetch market data (06:00 VN = 23:00 UTC)
# Fetches 60 days of historical data for backtesting
fetch_data = PythonOperator(
    task_id='fetch_market_data',
    python_callable=fetch_market_data_from_db,
    op_kwargs={'days': 60},
    provide_context=True,
    dag=dag
)

# Task 2: Detect market regime (06:15 VN = 23:15 UTC)
# Determines current market regime (trending/ranging/neutral)
detect_regime_task = PythonOperator(
    task_id='detect_regime',
    python_callable=detect_regime,
    provide_context=True,
    dag=dag
)

# Task 3: Run backtests (06:20 VN = 23:20 UTC)
# Tests all 4 strategies with 1B VND initial capital
run_backtest_task = PythonOperator(
    task_id='run_backtests',
    python_callable=run_backtests,
    provide_context=True,
    dag=dag
)

# Task 4: Optimize parameters (06:30 VN = 23:30 UTC)
# Uses Bayesian optimization to find best strategy parameters
optimize_params_task = PythonOperator(
    task_id='optimize_parameters',
    python_callable=optimize_parameters,
    provide_context=True,
    dag=dag
)

# Task 5: Save results to database (06:40 VN = 23:40 UTC)
# Persists backtest results to TimescaleDB
save_results_task = PythonOperator(
    task_id='save_backtest_results',
    python_callable=save_backtest_results,
    provide_context=True,
    dag=dag
)

# Task 6: Publish parameters to Kafka (06:45 VN = 23:45 UTC)
# Broadcasts optimized parameters to paper trading service
publish_params_task = PythonOperator(
    task_id='publish_parameters',
    python_callable=save_and_publish_parameters,
    provide_context=True,
    dag=dag
)

# Define task dependencies
# Sequential flow: check_data → fetch → regime → backtest → optimize → save → publish
# The check_data task acts as a GATE - if insufficient data, DAG will skip gracefully
check_data >> fetch_data >> detect_regime_task >> run_backtest_task >> optimize_params_task >> save_results_task >> publish_params_task
