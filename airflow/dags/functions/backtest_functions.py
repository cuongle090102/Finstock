"""
Backtest Functions for Daily Backtest DAG
Provides data fetching, backtesting, and result saving capabilities
"""

import os
import sys
import uuid
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
import logging
import psycopg2
from psycopg2.extras import execute_batch
import json

# Add project root to path for src imports
# __file__ is /opt/airflow/dags/functions/backtest_functions.py
# We need /opt/airflow/ which is 3 levels up
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Also add /opt/airflow explicitly (alternative path)
airflow_root = '/opt/airflow'
if airflow_root not in sys.path:
    sys.path.insert(0, airflow_root)

# Add functions directory to path for data_readiness import
sys.path.insert(0, os.path.dirname(__file__))

from airflow.exceptions import AirflowSkipException

try:
    from data_readiness import check_backtest_ready
except ImportError:
    # Fallback: create a simple checker
    def check_backtest_ready():
        import psycopg2
        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
                user=os.getenv('POSTGRES_USER', 'finstock_user'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(DISTINCT DATE(timestamp)) FROM market_data_hot;")
            days = cur.fetchone()[0]
            conn.close()

            if days >= 7:
                return True, f"Data ready: {days} days available"
            else:
                return False, f"Insufficient data: {days}/7 days available"
        except Exception as e:
            return False, f"Error checking data: {str(e)}"

logger = logging.getLogger(__name__)


def fetch_market_data_from_db(days: int = 60, **context) -> pd.DataFrame:
    """
    Fetch historical market data from TimescaleDB
    Automatically adjusts to available data if less than requested days

    Args:
        days: Number of days to fetch (will use available data if less)

    Returns:
        DataFrame with market data
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        # Check available data first
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                MIN(timestamp) as oldest,
                MAX(timestamp) as newest,
                COUNT(DISTINCT DATE(timestamp)) as available_days,
                COUNT(*) as total_records
            FROM market_data_hot;
        """)
        result = cursor.fetchone()
        oldest, newest, available_days, total_records = result

        logger.info(f"Database has {total_records} records spanning {available_days} days ({oldest} to {newest})")

        # Validate minimum data requirement (relaxed to 5 days for basic backtesting)
        if available_days < 5:
            error_msg = f"Insufficient data: only {available_days} days available, need at least 5"
            logger.error(error_msg)
            conn.close()
            raise ValueError(error_msg)

        if available_days < 7:
            logger.warning(f"Limited data: only {available_days} days available, ideally need 7+ for reliable backtesting")

        # Use available data if less than requested
        actual_days = min(days, available_days)
        if actual_days < days:
            logger.warning(f"Using {actual_days} days instead of {days} (limited by available data)")

        # Fetch data (parameterized query to prevent SQL injection)
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
            WHERE timestamp >= NOW() - INTERVAL '%s days'
            ORDER BY timestamp ASC, symbol ASC;
        """

        df = pd.read_sql_query(query, conn, params=(int(actual_days),))
        conn.close()

        # Validate data
        if df.empty:
            raise ValueError("No market data found in database")

        if len(df) < 100:
            logger.warning(f"Limited data: only {len(df)} records available")

        logger.info(f"Fetched {len(df)} records for {df['symbol'].nunique()} symbols over {actual_days} days")

        # Push to XCom for next tasks
        context['task_instance'].xcom_push(key='data_records', value=len(df))
        context['task_instance'].xcom_push(key='symbols_count', value=df['symbol'].nunique())
        context['task_instance'].xcom_push(key='actual_days', value=actual_days)

        # Save to temporary file in Airflow logs directory
        temp_path = f"/opt/airflow/logs/market_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(temp_path, index=False)
        context['task_instance'].xcom_push(key='data_path', value=temp_path)

        return df

    except Exception as e:
        logger.error(f"Error fetching market data: {e}")
        raise


def _backtest_strategy(df: pd.DataFrame, strategy_name: str, params: Dict[str, Any],
                       initial_capital: float = 1_000_000_000) -> Dict[str, Any]:
    """Run a simple backtest on historical data using indicator-based signals.

    Works directly with real market data (no Kafka, no market-hours check).
    """
    all_strategy_returns = []
    total_trades = 0

    for symbol in df['symbol'].unique():
        sym = df[df['symbol'] == symbol].sort_values('timestamp').reset_index(drop=True).copy()
        if len(sym) < 30:
            continue

        # --- Generate signals based on strategy type ---
        sym['signal'] = 0

        if strategy_name == 'MA_Crossover':
            short_p = params.get('short_period', 5)
            long_p = params.get('long_period', 20)
            sym['sma_s'] = sym['close'].rolling(short_p).mean()
            sym['sma_l'] = sym['close'].rolling(long_p).mean()
            sym.loc[sym['sma_s'] > sym['sma_l'], 'signal'] = 1
            sym.loc[sym['sma_s'] < sym['sma_l'], 'signal'] = -1

        elif strategy_name == 'Breakout':
            lb = params.get('lookback_period', 20)
            sym['high_max'] = sym['high'].rolling(lb).max().shift(1)
            sym['low_min'] = sym['low'].rolling(lb).min().shift(1)
            sym.loc[sym['close'] > sym['high_max'], 'signal'] = 1
            sym.loc[sym['close'] < sym['low_min'], 'signal'] = -1

        elif strategy_name == 'Mean_Reversion':
            period = params.get('bb_period', 20)
            std_dev = params.get('bb_std_dev', 2.0)
            sym['sma'] = sym['close'].rolling(period).mean()
            sym['std'] = sym['close'].rolling(period).std()
            sym.loc[sym['close'] < sym['sma'] - std_dev * sym['std'], 'signal'] = 1
            sym.loc[sym['close'] > sym['sma'] + std_dev * sym['std'], 'signal'] = -1

        elif strategy_name == 'Momentum':
            roc_p = params.get('roc_short_period', 10)
            threshold = params.get('roc_threshold', 5.0)
            sym['roc'] = sym['close'].pct_change(roc_p) * 100
            sym.loc[sym['roc'] > threshold, 'signal'] = 1
            sym.loc[sym['roc'] < -threshold, 'signal'] = -1

        # Strategy return = yesterday's signal × today's return
        sym['daily_ret'] = sym['close'].pct_change()
        sym['strat_ret'] = sym['signal'].shift(1) * sym['daily_ret']
        sym['strat_ret'] = sym['strat_ret'].fillna(0)

        total_trades += int(sym['signal'].diff().abs().sum())
        all_strategy_returns.append(sym[['timestamp', 'strat_ret']].dropna())

    if not all_strategy_returns:
        return {
            'total_return': 0.0, 'sharpe_ratio': 0.0, 'max_drawdown': 0.0,
            'win_rate': 0.0, 'total_trades': 0, 'parameters': params,
            'timestamp': datetime.now().isoformat(),
        }

    # Equal-weight aggregate across symbols
    combined = pd.concat(all_strategy_returns)
    daily = combined.groupby('timestamp')['strat_ret'].mean().sort_index()

    total_return = float((1 + daily).prod() - 1)
    vol = float(daily.std() * np.sqrt(252)) if len(daily) > 1 else 0.0
    sharpe = float((daily.mean() * 252 - 0.03) / vol) if vol > 0 else 0.0

    cum = (1 + daily).cumprod()
    peak = cum.expanding().max()
    dd = (cum - peak) / peak
    max_dd = float(abs(dd.min())) if len(dd) > 0 else 0.0

    win_rate = float((daily > 0).sum() / len(daily)) if len(daily) > 0 else 0.0

    return {
        'total_return': total_return,
        'sharpe_ratio': sharpe,
        'max_drawdown': max_dd,
        'win_rate': win_rate,
        'total_trades': total_trades,
        'parameters': params,
        'timestamp': datetime.now().isoformat(),
    }


def run_backtests(strategy_params: Dict[str, Any] = None, **context) -> Dict[str, Any]:
    """
    Run backtests for all strategies on real market data from TimescaleDB.

    Uses simple indicator-based signal generation directly on historical data,
    bypassing the live-trading strategy classes (which require Kafka + market hours).
    """
    try:
        # Get data from previous task
        data_path = context['task_instance'].xcom_pull(key='data_path', task_ids='fetch_market_data')
        if not data_path:
            raise ValueError("XCom 'data_path' from fetch_market_data is None — upstream task may have failed")
        df = pd.read_csv(data_path, parse_dates=['timestamp'])

        logger.info(f"Running backtests on {len(df)} records for {df['symbol'].nunique()} symbols")

        # Default parameters for each strategy
        strategy_configs = {
            'MA_Crossover': {'short_period': 5, 'long_period': 20},
            'Breakout': {'lookback_period': 20, 'breakout_threshold': 1.5},
            'Mean_Reversion': {'bb_period': 20, 'bb_std_dev': 2.0, 'rsi_period': 14,
                               'rsi_oversold': 30, 'rsi_overbought': 70},
            'Momentum': {'roc_short_period': 10, 'roc_threshold': 5.0,
                         'macd_fast': 12, 'macd_slow': 26, 'macd_signal': 9},
        }

        # Override with custom parameters if provided
        if strategy_params:
            for name, params in strategy_params.items():
                if name in strategy_configs:
                    strategy_configs[name].update(params)

        results = {}

        for strategy_name, params in strategy_configs.items():
            logger.info(f"Running backtest for {strategy_name}")
            result = _backtest_strategy(df, strategy_name, params)
            results[strategy_name] = result

            logger.info(f"{strategy_name}: Return={result['total_return']:.2%}, "
                        f"Sharpe={result['sharpe_ratio']:.2f}, "
                        f"Trades={result['total_trades']}")

        logger.info(f"Completed backtests for {len(results)} strategies")

        # Push to XCom
        context['task_instance'].xcom_push(key='backtest_results', value=results)

        return results

    except Exception as e:
        logger.error(f"Error running backtests: {e}")
        raise


def save_backtest_results(regime: str = 'neutral', **context) -> bool:
    """
    Save backtest results to TimescaleDB

    Args:
        regime: Detected market regime

    Returns:
        True if successful
    """
    try:
        # Get results and regime from previous tasks
        results = context['task_instance'].xcom_pull(key='backtest_results', task_ids='run_backtests')
        regime = context['task_instance'].xcom_pull(key='regime', task_ids='detect_regime') or regime

        if not results:
            logger.warning("No backtest results to save")
            return False

        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        insert_sql = """
            INSERT INTO backtest_results (
                timestamp, run_id, strategy, regime, total_return, sharpe_ratio,
                max_drawdown, win_rate, total_trades, parameters, metrics
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """

        run_batch = uuid.uuid4().hex[:8]
        records = []
        for strategy_name, result in results.items():
            run_id = f"backtest_{strategy_name}_{run_batch}"
            records.append((
                datetime.now(),
                run_id,
                strategy_name,
                regime,
                result['total_return'],
                result['sharpe_ratio'],
                result['max_drawdown'],
                result['win_rate'],
                result['total_trades'],
                json.dumps(result['parameters']),
                json.dumps({
                    'timestamp': result['timestamp']
                })
            ))

        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, records)
            conn.commit()

        conn.close()

        logger.info(f"Saved {len(records)} backtest results to database")

        return True

    except Exception as e:
        logger.error(f"Error saving backtest results: {e}")
        raise


def check_data_availability_task(**context) -> bool:
    """
    Check if sufficient data exists for backtesting.
    This is a GATE task that runs before the backtest pipeline.

    If insufficient data (< 7 days), this task will:
    1. Log an informative message
    2. Skip the entire DAG gracefully using AirflowSkipException

    This prevents backtest from running prematurely during the
    data collection phase (Days 1-6).

    Returns:
        True if data is ready

    Raises:
        AirflowSkipException: If insufficient data (< 7 days)
    """
    try:
        logger.info("=" * 60)
        logger.info("CHECKING DATA AVAILABILITY FOR BACKTEST")
        logger.info("=" * 60)

        # Use data readiness checker
        ready, message = check_backtest_ready()

        if ready:
            logger.info(f"✓ {message}")
            logger.info("Proceeding with backtest pipeline...")
            return True
        else:
            # Not ready - skip entire DAG
            logger.warning(f"✗ {message}")
            logger.warning("=" * 60)
            logger.warning("SKIPPING BACKTEST - INSUFFICIENT DATA")
            logger.warning("Backtest will run automatically once 7+ days of data are available")
            logger.warning("Current data collection continues normally")
            logger.warning("=" * 60)

            # Raise AirflowSkipException to skip downstream tasks
            raise AirflowSkipException(message)

    except AirflowSkipException:
        # Re-raise skip exception
        raise
    except Exception as e:
        logger.error(f"Error checking data availability: {e}")
        # On error, we skip to be safe (don't run backtest with potentially corrupt data)
        raise AirflowSkipException(f"Data availability check failed: {str(e)}")
