"""
Monthly Archive DAG - Archive Old Market Data to MinIO
Task 2.3: Runs on 1st of each month at 00:00 (midnight)

Workflow:
1. 00:00 - Export old market data (older than 6 months) from TimescaleDB
2. 00:10 - Convert to Parquet format with Snappy compression
3. 00:20 - Upload to MinIO S3 bucket (finstock-archive)
4. 00:30 - Verify upload and create manifest file
5. 00:40 - Data will be auto-deleted by TimescaleDB retention policy
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

from functions.archive_functions import (
    export_old_market_data,
    convert_to_parquet,
    upload_to_minio,
    verify_and_create_manifest
)

# Default arguments
default_args = {
    'owner': 'trading-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2)
}

# Create DAG
dag = DAG(
    'monthly_archive',
    default_args=default_args,
    description='Monthly archival of old market data to MinIO',
    schedule_interval='0 0 1 * *',  # 00:00 on 1st of each month
    catchup=False,
    max_active_runs=1,
    tags=['archive', 'minio', 'phase2']
)

# Task 1: Export old market data from TimescaleDB (00:00)
export_data = PythonOperator(
    task_id='export_old_market_data',
    python_callable=export_old_market_data,
    provide_context=True,
    dag=dag
)

# Task 2: Convert to Parquet format (00:10)
convert_parquet = PythonOperator(
    task_id='convert_to_parquet',
    python_callable=convert_to_parquet,
    provide_context=True,
    dag=dag
)

# Task 3: Upload to MinIO (00:20)
upload_files = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    provide_context=True,
    dag=dag
)

# Task 4: Verify and create manifest (00:30)
verify_upload = PythonOperator(
    task_id='verify_and_create_manifest',
    python_callable=verify_and_create_manifest,
    provide_context=True,
    dag=dag
)

# Define task dependencies
# Sequential flow: export → convert → upload → verify
export_data >> convert_parquet >> upload_files >> verify_upload
