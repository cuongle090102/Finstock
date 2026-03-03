"""
Data Readiness Check Module

Provides centralized data availability checking for:
- Airflow DAGs (backtest scheduling)
- Paper Trading Service (startup validation)

Ensures system only operates when sufficient historical data exists.
"""

import os
import psycopg2
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DataReadinessChecker:
    """
    Checks if sufficient historical data exists for backtesting and trading.

    Requirements:
    - Minimum 7 days of data for reliable backtesting
    - Data should be complete (no large gaps)
    - Sufficient symbols (at least 5 VN30 stocks)
    """

    MINIMUM_DAYS = 7
    MINIMUM_SYMBOLS = 5
    RECOMMENDED_DAYS = 30

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None
    ):
        """
        Initialize with database connection parameters.

        Args:
            host: PostgreSQL host (defaults to env var)
            port: PostgreSQL port (defaults to env var)
            database: Database name (defaults to env var)
            user: Database user (defaults to env var)
            password: Database password (defaults to env var)
        """
        self.db_config = {
            'host': host or os.getenv('POSTGRES_HOST', 'postgres'),
            'port': port or int(os.getenv('POSTGRES_PORT', 5432)),
            'database': database or os.getenv('POSTGRES_DB', 'finstock_market_data'),
            'user': user or os.getenv('POSTGRES_USER', 'finstock_user'),
            'password': password or os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        }

    def check_data_availability(
        self,
        required_days: int = None,
        required_symbols: int = None
    ) -> Tuple[bool, int, str]:
        """
        Check if sufficient data exists for backtesting/trading.

        Args:
            required_days: Minimum days needed (defaults to MINIMUM_DAYS)
            required_symbols: Minimum symbols needed (defaults to MINIMUM_SYMBOLS)

        Returns:
            Tuple of (ready: bool, days_available: int, message: str)

        Example:
            >>> checker = DataReadinessChecker()
            >>> ready, days, msg = checker.check_data_availability()
            >>> if not ready:
            >>>     logger.warning(msg)
        """
        required_days = required_days or self.MINIMUM_DAYS
        required_symbols = required_symbols or self.MINIMUM_SYMBOLS

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Query 1: Check date range and unique days
            cursor.execute("""
                SELECT
                    MIN(DATE(timestamp)) as oldest_date,
                    MAX(DATE(timestamp)) as newest_date,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days,
                    COUNT(*) as total_records
                FROM market_data_hot;
            """)

            result = cursor.fetchone()
            oldest_date, newest_date, unique_days, total_records = result

            # Query 2: Check symbols availability
            cursor.execute("""
                SELECT
                    COUNT(DISTINCT symbol) as total_symbols,
                    COUNT(DISTINCT CASE WHEN is_vn30 = true THEN symbol END) as vn30_symbols
                FROM market_data_hot
                WHERE timestamp >= NOW() - INTERVAL '7 days';
            """)

            symbol_result = cursor.fetchone()
            total_symbols, vn30_symbols = symbol_result

            # Query 3: Check for data gaps (days with very low record counts)
            cursor.execute("""
                SELECT
                    DATE(timestamp) as trade_date,
                    COUNT(*) as records_per_day
                FROM market_data_hot
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(timestamp)
                HAVING COUNT(*) < 10
                ORDER BY trade_date;
            """)

            gap_days = cursor.fetchall()

            cursor.close()
            conn.close()

            # Validation logic
            if total_records == 0:
                return False, 0, "No data available in database. Data collection may not have started yet."

            if unique_days < required_days:
                return (
                    False,
                    unique_days,
                    f"Insufficient data: {unique_days} days available, need {required_days}+ days. "
                    f"System in data collection phase (Day {unique_days}/{required_days})."
                )

            if total_symbols < required_symbols:
                return (
                    False,
                    unique_days,
                    f"Insufficient symbols: {total_symbols} symbols available, need {required_symbols}+ symbols. "
                    f"Data collection incomplete."
                )

            if len(gap_days) > 0:
                gap_dates = [str(day[0]) for day in gap_days[:3]]
                logger.warning(
                    f"Data quality warning: {len(gap_days)} days with low record counts. "
                    f"Examples: {', '.join(gap_dates)}"
                )

            # Data is ready
            status_msg = (
                f"Data ready: {unique_days} days available ({oldest_date} to {newest_date}), "
                f"{total_symbols} symbols ({vn30_symbols} VN30), "
                f"{total_records:,} total records."
            )

            if unique_days < self.RECOMMENDED_DAYS:
                status_msg += f" Note: {self.RECOMMENDED_DAYS}+ days recommended for optimal backtesting."

            return True, unique_days, status_msg

        except psycopg2.Error as e:
            logger.error(f"Database error checking data availability: {e}")
            return False, 0, f"Database connection error: {str(e)}"
        except Exception as e:
            logger.error(f"Error checking data availability: {e}")
            return False, 0, f"Error checking data: {str(e)}"

    def get_data_summary(self) -> Dict[str, Any]:
        """
        Get detailed summary of available data.

        Returns:
            Dictionary with data statistics
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Comprehensive data summary
            cursor.execute("""
                SELECT
                    MIN(timestamp) as oldest_timestamp,
                    MAX(timestamp) as newest_timestamp,
                    COUNT(DISTINCT DATE(timestamp)) as unique_days,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT CASE WHEN is_vn30 = true THEN symbol END) as vn30_count,
                    AVG(volume) as avg_volume
                FROM market_data_hot;
            """)

            result = cursor.fetchone()

            # Per-day breakdown
            cursor.execute("""
                SELECT
                    DATE(timestamp) as trade_date,
                    COUNT(*) as records,
                    COUNT(DISTINCT symbol) as symbols
                FROM market_data_hot
                GROUP BY DATE(timestamp)
                ORDER BY trade_date DESC
                LIMIT 10;
            """)

            daily_stats = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                'oldest_timestamp': result[0],
                'newest_timestamp': result[1],
                'unique_days': result[2],
                'unique_symbols': result[3],
                'total_records': result[4],
                'vn30_count': result[5],
                'avg_volume': float(result[6]) if result[6] else 0.0,
                'daily_breakdown': [
                    {
                        'date': str(day[0]),
                        'records': day[1],
                        'symbols': day[2]
                    }
                    for day in daily_stats
                ]
            }

        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {}

    def check_backtest_ready(self) -> Tuple[bool, str]:
        """
        Specific check for backtest readiness.

        Returns:
            Tuple of (ready: bool, message: str)
        """
        ready, days, msg = self.check_data_availability(
            required_days=self.MINIMUM_DAYS,
            required_symbols=self.MINIMUM_SYMBOLS
        )

        if not ready:
            return False, f"[BACKTEST SKIP] {msg}"

        return True, f"[BACKTEST READY] {msg}"

    def check_trading_ready(self, check_backtest_results: bool = True) -> Tuple[bool, str]:
        """
        Specific check for paper trading readiness.
        Requires both data availability AND backtest results.

        Args:
            check_backtest_results: If True, also check for backtest results

        Returns:
            Tuple of (ready: bool, message: str)
        """
        # First check data availability
        ready, days, msg = self.check_data_availability(
            required_days=self.MINIMUM_DAYS,
            required_symbols=self.MINIMUM_SYMBOLS
        )

        if not ready:
            return False, f"[TRADING WAIT] Data collection phase. {msg}"

        # Check if backtest results exist
        if check_backtest_results:
            has_backtest, backtest_msg = self._check_backtest_results_exist()
            if not has_backtest:
                return False, f"[TRADING WAIT] {msg} Waiting for backtest to complete. {backtest_msg}"

        return True, f"[TRADING READY] {msg}"

    def _check_backtest_results_exist(self) -> Tuple[bool, str]:
        """
        Check if backtest results exist (parameters have been optimized).

        Returns:
            Tuple of (exists: bool, message: str)
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Check if any backtest results exist from today or yesterday
            cursor.execute("""
                SELECT
                    COUNT(*) as result_count,
                    MAX(timestamp) as latest_backtest
                FROM backtest_results
                WHERE timestamp >= NOW() - INTERVAL '2 days';
            """)

            result = cursor.fetchone()
            result_count, latest_backtest = result

            cursor.close()
            conn.close()

            if result_count == 0:
                return False, "No backtest results found. Backtest will run at 06:00 daily."

            return True, f"Latest backtest: {latest_backtest}"

        except Exception as e:
            logger.error(f"Error checking backtest results: {e}")
            return False, f"Could not verify backtest results: {str(e)}"


# Convenience functions for direct imports
def check_data_ready(required_days: int = 7) -> Tuple[bool, int, str]:
    """
    Convenience function for quick data availability check.

    Args:
        required_days: Minimum days needed (default 7)

    Returns:
        Tuple of (ready: bool, days_available: int, message: str)

    Example:
        >>> from functions.data_readiness import check_data_ready
        >>> ready, days, msg = check_data_ready()
        >>> if ready:
        >>>     run_backtest()
    """
    checker = DataReadinessChecker()
    return checker.check_data_availability(required_days=required_days)


def check_backtest_ready() -> Tuple[bool, str]:
    """
    Check if system is ready for backtesting.

    Returns:
        Tuple of (ready: bool, message: str)
    """
    checker = DataReadinessChecker()
    return checker.check_backtest_ready()


def check_trading_ready(check_backtest_results: bool = True) -> Tuple[bool, str]:
    """
    Check if system is ready for paper trading.

    Args:
        check_backtest_results: If True, also verify backtest has run

    Returns:
        Tuple of (ready: bool, message: str)
    """
    checker = DataReadinessChecker()
    return checker.check_trading_ready(check_backtest_results=check_backtest_results)
