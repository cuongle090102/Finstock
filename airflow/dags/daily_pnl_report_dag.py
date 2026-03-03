"""
Daily P&L Report DAG - Automated Daily Performance Reporting

CRITICAL: Runs daily at 15:30 VIETNAM TIME (UTC+7)
         = 08:30 UTC (same day)

PURPOSE:
--------
Generate daily profit & loss report 30 minutes after market close.

Workflow (All times in VIETNAM TIME UTC+7):
--------------------------------------------
1. 15:30 VN - Fetch today's paper trades from TimescaleDB
2. 15:35 VN - Calculate P&L metrics (win rate, best/worst trades, etc.)
3. 15:40 VN - Save metrics to daily_pnl table
4. 15:45 VN - Export report to JSON file (for MinIO upload later)

Timing Rationale:
-----------------
- Vietnamese market closes at 15:00 (3:00 PM)
- Report runs at 15:30 (3:30 PM) to allow:
  * All closing orders to be processed
  * Final market data to be committed
  * 30-minute buffer for data settlement
  * Real-time daily performance visibility

TIMEZONE CONVERSION:
-------------------
Vietnam Time (UTC+7) → UTC for Airflow Schedule
15:30 VN → 15:30 - 7 hours = 08:30 UTC (same day)

Example:
Market close: 2025-10-22 15:00 VN
Report runs:  2025-10-22 15:30 VN = 2025-10-22 08:30 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

# Add project root to path for src imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Add functions directory to path
sys.path.insert(0, os.path.dirname(__file__))

from functions.pnl_report_functions import (
    fetch_daily_trades,
    calculate_pnl_metrics,
    save_pnl_to_database,
    export_report_to_file
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
    'execution_timeout': timedelta(minutes=15)
}

# Create DAG
dag = DAG(
    'daily_pnl_report',
    default_args=default_args,
    description='Daily P&L report generation after market close',
    # CRITICAL TIMEZONE CONVERSION:
    # Runs at 15:30 VIETNAM TIME (UTC+7)
    # = 08:30 UTC (same day)
    # Example: Market closes 2025-10-22 15:00 VN, report runs 2025-10-22 15:30 VN = 08:30 UTC
    schedule_interval='30 8 * * *',  # 08:30 UTC = 15:30 Vietnam (30 min after market close)
    catchup=False,
    max_active_runs=1,
    tags=['pnl', 'reporting', 'phase2']
)

# Task 1: Fetch today's trades (15:30 VN = 08:30 UTC)
fetch_trades = PythonOperator(
    task_id='fetch_daily_trades',
    python_callable=fetch_daily_trades,
    provide_context=True,
    dag=dag
)

# Task 2: Calculate P&L metrics (15:35 VN = 08:35 UTC)
calculate_metrics = PythonOperator(
    task_id='calculate_pnl_metrics',
    python_callable=calculate_pnl_metrics,
    provide_context=True,
    dag=dag
)

# Task 3: Save to database (15:40 VN = 08:40 UTC)
save_to_db = PythonOperator(
    task_id='save_pnl_to_database',
    python_callable=save_pnl_to_database,
    provide_context=True,
    dag=dag
)

# Task 4: Export report to file (15:45 VN = 08:45 UTC)
export_report = PythonOperator(
    task_id='export_report_to_file',
    python_callable=export_report_to_file,
    provide_context=True,
    dag=dag
)

# Define task dependencies
# Sequential flow: fetch → calculate → save → export
fetch_trades >> calculate_metrics >> save_to_db >> export_report
