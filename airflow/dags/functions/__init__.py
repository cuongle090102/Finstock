"""
Functions module for Airflow DAGs
Contains helper functions for backtesting, regime detection, optimization, P&L reporting, and archival
"""

import sys
import os

# Add project root to path for src imports
# In Docker container, src is at /opt/airflow/src
# In local dev, src is at project_root/src
if os.path.exists('/opt/airflow/src'):
    # Running in Docker container
    if '/opt/airflow' not in sys.path:
        sys.path.insert(0, '/opt/airflow')
else:
    # Running locally
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from .backtest_functions import (
    fetch_market_data_from_db,
    run_backtests,
    save_backtest_results
)

from .regime_detection import detect_regime

from .parameter_optimization import (
    optimize_parameters,
    save_and_publish_parameters
)

from .pnl_report_functions import (
    fetch_daily_trades,
    calculate_pnl_metrics,
    save_pnl_to_database,
    export_report_to_file
)

from .archive_functions import (
    export_old_market_data,
    convert_to_parquet,
    upload_to_minio,
    verify_and_create_manifest
)

__all__ = [
    'fetch_market_data_from_db',
    'run_backtests',
    'save_backtest_results',
    'detect_regime',
    'optimize_parameters',
    'save_and_publish_parameters',
    'fetch_daily_trades',
    'calculate_pnl_metrics',
    'save_pnl_to_database',
    'export_report_to_file',
    'export_old_market_data',
    'convert_to_parquet',
    'upload_to_minio',
    'verify_and_create_manifest'
]
