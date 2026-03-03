"""
P&L Report Functions for Daily P&L Report DAG
Generates daily profit/loss reports from paper trading results
"""

import sys
import os
sys.path.insert(0, '/opt/airflow/src')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import psycopg2
from psycopg2.extras import execute_batch
import json

logger = logging.getLogger(__name__)


def fetch_daily_trades(**context) -> pd.DataFrame:
    """
    Fetch today's paper trades from TimescaleDB

    Returns:
        DataFrame with today's trades
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        # Get today's trades (Vietnam time)
        query = """
            SELECT
                timestamp,
                trade_id,
                order_id,
                symbol,
                side,
                quantity,
                price,
                commission,
                tax,
                total_cost,
                pnl,
                strategy,
                regime,
                exchange,
                is_vn30
            FROM paper_trades
            WHERE timestamp >= CURRENT_DATE
            ORDER BY timestamp ASC;
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        logger.info(f"Fetched {len(df)} trades for today")

        # Push to XCom
        context['task_instance'].xcom_push(key='trade_count', value=len(df))

        # Save to temporary file
        temp_path = f"/tmp/daily_trades_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(temp_path, index=False)
        context['task_instance'].xcom_push(key='trades_path', value=temp_path)

        return df

    except Exception as e:
        logger.error(f"Error fetching daily trades: {e}")
        raise


def calculate_pnl_metrics(**context) -> Dict[str, Any]:
    """
    Calculate P&L metrics from today's trades

    Returns:
        Dictionary with P&L metrics
    """
    try:
        # Get trades from previous task
        trades_path = context['task_instance'].xcom_pull(key='trades_path', task_ids='fetch_daily_trades')

        if not trades_path or not os.path.exists(trades_path):
            logger.warning("No trades file found, returning empty metrics")
            return {
                'date': datetime.now().date().isoformat(),
                'total_trades': 0,
                'total_pnl': 0.0,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0,
                'win_rate': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0,
                'total_commission': 0.0,
                'total_tax': 0.0,
                'net_pnl': 0.0,
                'strategies': {},
                'symbols': {}
            }

        df = pd.read_csv(trades_path, parse_dates=['timestamp'])

        if len(df) == 0:
            logger.info("No trades today, returning zero metrics")
            return {
                'date': datetime.now().date().isoformat(),
                'total_trades': 0,
                'total_pnl': 0.0,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0,
                'win_rate': 0.0,
                'best_trade': 0.0,
                'worst_trade': 0.0,
                'total_commission': 0.0,
                'total_tax': 0.0,
                'net_pnl': 0.0,
                'strategies': {},
                'symbols': {}
            }

        logger.info(f"Calculating P&L metrics for {len(df)} trades")

        # Overall metrics
        total_pnl = df['pnl'].sum()
        total_commission = df['commission'].sum()
        total_tax = df['tax'].sum()
        net_pnl = total_pnl - total_commission - total_tax

        # Separate buys and sells for realized/unrealized
        sells = df[df['side'] == 'SELL']
        realized_pnl = sells['pnl'].sum() if len(sells) > 0 else 0.0

        # For simplicity, unrealized is remaining PnL
        unrealized_pnl = total_pnl - realized_pnl

        # Win/loss counts and rate
        winning_trades = int((df['pnl'] > 0).sum())
        losing_trades = int((df['pnl'] <= 0).sum())
        win_rate = (winning_trades / len(df)) * 100 if len(df) > 0 else 0.0

        # Best/worst trades
        best_trade = df['pnl'].max() if len(df) > 0 else 0.0
        worst_trade = df['pnl'].min() if len(df) > 0 else 0.0

        # Strategy breakdown
        strategy_pnl = df.groupby('strategy')['pnl'].agg(['sum', 'count', 'mean']).to_dict('index')
        strategies = {
            strategy: {
                'total_pnl': float(stats['sum']),
                'trade_count': int(stats['count']),
                'avg_pnl': float(stats['mean'])
            }
            for strategy, stats in strategy_pnl.items()
        }

        # Symbol breakdown
        symbol_pnl = df.groupby('symbol')['pnl'].agg(['sum', 'count']).to_dict('index')
        symbols = {
            symbol: {
                'total_pnl': float(stats['sum']),
                'trade_count': int(stats['count'])
            }
            for symbol, stats in symbol_pnl.items()
        }

        metrics = {
            'date': datetime.now().date().isoformat(),
            'total_trades': int(len(df)),
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'total_pnl': float(total_pnl),
            'realized_pnl': float(realized_pnl),
            'unrealized_pnl': float(unrealized_pnl),
            'win_rate': float(win_rate),
            'best_trade': float(best_trade),
            'worst_trade': float(worst_trade),
            'total_commission': float(total_commission),
            'total_tax': float(total_tax),
            'net_pnl': float(net_pnl),
            'strategies': strategies,
            'symbols': symbols,
            'timestamp': datetime.now().isoformat()
        }

        logger.info(f"Calculated metrics: Total P&L={net_pnl:.2f}, Win Rate={win_rate:.2f}%")

        # Push to XCom
        context['task_instance'].xcom_push(key='pnl_metrics', value=metrics)

        return metrics

    except Exception as e:
        logger.error(f"Error calculating P&L metrics: {e}")
        raise


def save_pnl_to_database(**context) -> bool:
    """
    Save daily P&L metrics to TimescaleDB

    Returns:
        True if successful
    """
    try:
        # Get metrics from previous task
        metrics = context['task_instance'].xcom_pull(key='pnl_metrics', task_ids='calculate_pnl_metrics')

        if not metrics:
            logger.warning("No P&L metrics to save")
            return False

        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )

        insert_sql = """
            INSERT INTO daily_pnl (
                date, timestamp, total_trades, winning_trades, losing_trades,
                total_pnl, realized_pnl, unrealized_pnl, win_rate,
                best_trade, worst_trade, strategy_breakdown, regime_breakdown,
                portfolio_value, cash_balance
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (timestamp, date) DO UPDATE SET
                total_trades = EXCLUDED.total_trades,
                winning_trades = EXCLUDED.winning_trades,
                losing_trades = EXCLUDED.losing_trades,
                total_pnl = EXCLUDED.total_pnl,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                win_rate = EXCLUDED.win_rate,
                best_trade = EXCLUDED.best_trade,
                worst_trade = EXCLUDED.worst_trade,
                strategy_breakdown = EXCLUDED.strategy_breakdown,
                regime_breakdown = EXCLUDED.regime_breakdown,
                portfolio_value = EXCLUDED.portfolio_value,
                cash_balance = EXCLUDED.cash_balance;
        """

        # Use actual counts from calculate_pnl_metrics (not derived from win_rate)
        winning_trades = metrics.get('winning_trades', 0)
        losing_trades = metrics.get('losing_trades', 0)

        # For regime breakdown, we'll need to query paper_trades table
        # For now, use empty dict (can be enhanced later)
        regime_breakdown = {}

        with conn.cursor() as cur:
            cur.execute(insert_sql, (
                datetime.now().date(),
                datetime.now(),
                metrics['total_trades'],
                winning_trades,
                losing_trades,
                metrics['total_pnl'],
                metrics['realized_pnl'],
                metrics['unrealized_pnl'],
                metrics['win_rate'] / 100,  # Convert percentage to decimal (0-1)
                metrics['best_trade'],
                metrics['worst_trade'],
                json.dumps(metrics['strategies']),
                json.dumps(regime_breakdown),
                metrics['net_pnl'],  # Use net_pnl as portfolio_value approximation
                metrics['net_pnl']   # Use net_pnl as cash_balance approximation
            ))
            conn.commit()

        conn.close()

        logger.info(f"Saved daily P&L report to database for {metrics['date']}")

        return True

    except Exception as e:
        logger.error(f"Error saving P&L to database: {e}")
        raise


def export_report_to_file(**context) -> str:
    """
    Export P&L report to JSON file (could also be PDF)

    Returns:
        Path to exported file
    """
    try:
        # Get metrics from previous task
        metrics = context['task_instance'].xcom_pull(key='pnl_metrics', task_ids='calculate_pnl_metrics')

        if not metrics:
            logger.warning("No metrics to export")
            return ""

        # Create report directory if not exists
        report_dir = "/tmp/pnl_reports"
        os.makedirs(report_dir, exist_ok=True)

        # Export as JSON
        report_path = os.path.join(report_dir, f"pnl_report_{datetime.now().strftime('%Y%m%d')}.json")

        with open(report_path, 'w') as f:
            json.dump(metrics, f, indent=2)

        logger.info(f"Exported P&L report to {report_path}")

        # Push to XCom
        context['task_instance'].xcom_push(key='report_path', value=report_path)

        return report_path

    except Exception as e:
        logger.error(f"Error exporting report: {e}")
        raise
