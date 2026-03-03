"""
Archive Functions for Monthly Archive DAG
Handles exporting old data from TimescaleDB to MinIO S3 storage
"""

import sys
sys.path.insert(0, '/opt/airflow/src')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import psycopg2
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def export_old_market_data(**context) -> str:
    """
    Export market data older than 6 months from TimescaleDB

    Returns:
        Path to exported CSV file
    """
    try:
        # Calculate cutoff date (6 months ago)
        cutoff_date = datetime.now() - timedelta(days=180)

        # Get execution date for this run (last month)
        execution_date = context['execution_date']
        archive_year_month = execution_date.strftime('%Y-%m')

        logger.info(f"Exporting data older than {cutoff_date.date()} for archive period {archive_year_month}")

        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        # Query old data from market_data_hot
        # Export data that is exactly in the 6-7 month old range
        # This ensures we archive data just before TimescaleDB retention policy deletes it
        query = """
            SELECT
                timestamp,
                symbol,
                open,
                high,
                low,
                close,
                volume,
                exchange,
                is_vn30
            FROM market_data_hot
            WHERE timestamp >= %s - INTERVAL '30 days'
              AND timestamp < %s
            ORDER BY timestamp, symbol;
        """

        df = pd.read_sql_query(query, conn, params=(cutoff_date, cutoff_date))
        conn.close()

        if len(df) == 0:
            logger.warning("No data to archive for this period")
            context['task_instance'].xcom_push(key='records_exported', value=0)
            return ""

        logger.info(f"Exported {len(df)} records for archival")

        # Create temp directory
        temp_dir = f"/tmp/archive_{archive_year_month}"
        os.makedirs(temp_dir, exist_ok=True)

        # Save to CSV
        csv_path = os.path.join(temp_dir, f"market_data_{archive_year_month}.csv")
        df.to_csv(csv_path, index=False)

        # Push to XCom
        context['task_instance'].xcom_push(key='csv_path', value=csv_path)
        context['task_instance'].xcom_push(key='records_exported', value=len(df))
        context['task_instance'].xcom_push(key='archive_period', value=archive_year_month)

        logger.info(f"Saved export to {csv_path}")

        return csv_path

    except Exception as e:
        logger.error(f"Error exporting old market data: {e}")
        raise


def convert_to_parquet(**context) -> List[str]:
    """
    Convert CSV data to Parquet format with Snappy compression
    Split into multiple files if data is large (target: 100MB per file)

    Returns:
        List of paths to Parquet files
    """
    try:
        # Get CSV path from previous task
        csv_path = context['task_instance'].xcom_pull(key='csv_path', task_ids='export_old_market_data')
        records_exported = context['task_instance'].xcom_pull(key='records_exported', task_ids='export_old_market_data')
        archive_period = context['task_instance'].xcom_pull(key='archive_period', task_ids='export_old_market_data')

        if not csv_path or records_exported == 0:
            logger.warning("No CSV data to convert")
            context['task_instance'].xcom_push(key='parquet_files', value=[])
            return []

        logger.info(f"Converting {csv_path} to Parquet format")

        # Read CSV
        df = pd.read_csv(csv_path, parse_dates=['timestamp'])

        # Calculate rows per file (target ~100MB per file)
        # Estimate: ~100 bytes per row → 1M rows per file
        rows_per_file = 1_000_000
        num_files = max(1, len(df) // rows_per_file)

        parquet_files = []
        temp_dir = os.path.dirname(csv_path)

        if num_files == 1:
            # Single file
            parquet_path = os.path.join(temp_dir, f"market_data_{archive_period}.parquet")

            # Convert to PyArrow Table for better control
            table = pa.Table.from_pandas(df)

            # Write with Snappy compression
            pq.write_table(
                table,
                parquet_path,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )

            parquet_files.append(parquet_path)
            logger.info(f"Created single Parquet file: {parquet_path}")

        else:
            # Multiple files
            for i in range(num_files):
                start_idx = i * rows_per_file
                end_idx = min((i + 1) * rows_per_file, len(df))

                df_chunk = df.iloc[start_idx:end_idx]
                parquet_path = os.path.join(temp_dir, f"market_data_{archive_period}_part_{i+1:03d}.parquet")

                table = pa.Table.from_pandas(df_chunk)
                pq.write_table(
                    table,
                    parquet_path,
                    compression='snappy',
                    use_dictionary=True,
                    write_statistics=True
                )

                parquet_files.append(parquet_path)

            logger.info(f"Created {len(parquet_files)} Parquet files")

        # Push to XCom
        context['task_instance'].xcom_push(key='parquet_files', value=parquet_files)

        # Clean up CSV file
        try:
            os.remove(csv_path)
            logger.info(f"Cleaned up CSV file: {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to remove CSV file: {e}")

        return parquet_files

    except Exception as e:
        logger.error(f"Error converting to Parquet: {e}")
        raise


def upload_to_minio(**context) -> bool:
    """
    Upload Parquet files to MinIO S3 bucket

    Returns:
        True if successful
    """
    try:
        # Get Parquet files from previous task
        parquet_files = context['task_instance'].xcom_pull(key='parquet_files', task_ids='convert_to_parquet')
        archive_period = context['task_instance'].xcom_pull(key='archive_period', task_ids='export_old_market_data')

        if not parquet_files:
            logger.warning("No Parquet files to upload")
            return False

        logger.info(f"Uploading {len(parquet_files)} files to MinIO")

        # Initialize MinIO client
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False  # Use HTTP for internal network
        )

        # Ensure bucket exists
        bucket_name = 'finstock-archive'
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")

        uploaded_files = []

        # Upload each Parquet file
        for parquet_path in parquet_files:
            filename = os.path.basename(parquet_path)
            object_name = f"market_data/{archive_period}/{filename}"

            minio_client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=parquet_path,
                content_type='application/octet-stream'
            )

            uploaded_files.append(object_name)
            logger.info(f"Uploaded: {object_name}")

        logger.info(f"Successfully uploaded {len(uploaded_files)} files to MinIO")

        # Push to XCom
        context['task_instance'].xcom_push(key='uploaded_files', value=uploaded_files)

        # Clean up Parquet files
        for parquet_path in parquet_files:
            try:
                os.remove(parquet_path)
                logger.info(f"Cleaned up Parquet file: {parquet_path}")
            except Exception as e:
                logger.warning(f"Failed to remove Parquet file: {e}")

        return True

    except S3Error as e:
        logger.error(f"MinIO S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error uploading to MinIO: {e}")
        raise


def verify_and_create_manifest(**context) -> str:
    """
    Verify uploaded files and create manifest file
    Manifest contains metadata about archived data

    Returns:
        Path to manifest file in MinIO
    """
    try:
        # Get uploaded files from previous task
        uploaded_files = context['task_instance'].xcom_pull(key='uploaded_files', task_ids='upload_to_minio')
        records_exported = context['task_instance'].xcom_pull(key='records_exported', task_ids='export_old_market_data')
        archive_period = context['task_instance'].xcom_pull(key='archive_period', task_ids='export_old_market_data')

        if not uploaded_files:
            logger.warning("No files to verify")
            return ""

        logger.info(f"Verifying {len(uploaded_files)} uploaded files")

        # Initialize MinIO client
        minio_client = Minio(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )

        bucket_name = 'finstock-archive'

        # Verify each file exists
        verified_files = []
        for object_name in uploaded_files:
            try:
                stat = minio_client.stat_object(bucket_name, object_name)
                verified_files.append({
                    'object_name': object_name,
                    'size_bytes': stat.size,
                    'last_modified': stat.last_modified.isoformat() if stat.last_modified else None,
                    'etag': stat.etag
                })
                logger.info(f"Verified: {object_name} ({stat.size} bytes)")
            except S3Error as e:
                logger.error(f"Failed to verify {object_name}: {e}")
                raise

        # Create manifest
        manifest = {
            'archive_period': archive_period,
            'created_at': datetime.now().isoformat(),
            'total_records': records_exported,
            'num_files': len(verified_files),
            'files': verified_files,
            'source_table': 'market_data_hot',
            'format': 'parquet',
            'compression': 'snappy'
        }

        # Save manifest to temp file
        manifest_path = f"/tmp/manifest_{archive_period}.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        # Upload manifest to MinIO
        manifest_object_name = f"market_data/{archive_period}/manifest.json"
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=manifest_object_name,
            file_path=manifest_path,
            content_type='application/json'
        )

        logger.info(f"Manifest uploaded: {manifest_object_name}")

        # Clean up temp manifest
        try:
            os.remove(manifest_path)
        except Exception as e:
            logger.warning(f"Failed to remove manifest file: {e}")

        # Push to XCom
        context['task_instance'].xcom_push(key='manifest_path', value=manifest_object_name)

        logger.info(f"Archive complete: {records_exported} records in {len(verified_files)} files")

        return manifest_object_name

    except Exception as e:
        logger.error(f"Error creating manifest: {e}")
        raise
