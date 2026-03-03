#!/usr/bin/env python3
"""
System Status Checker

Quick script to check the current status of the data accumulation system.
Run this daily to monitor progress through the 3 phases.

Usage:
    python scripts/check_system_status.py
"""

import psycopg2
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import sys

# ANSI color codes for pretty output
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
BLUE = '\033[94m'
BOLD = '\033[1m'
RESET = '\033[0m'


def print_header(text: str):
    """Print a formatted header"""
    print(f"\n{BOLD}{BLUE}{'=' * 80}{RESET}")
    print(f"{BOLD}{BLUE}{text:^80}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 80}{RESET}\n")


def print_success(text: str):
    """Print success message"""
    print(f"{GREEN}✓{RESET} {text}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{YELLOW}⚠{RESET} {text}")


def print_error(text: str):
    """Print error message"""
    print(f"{RED}✗{RESET} {text}")


def print_info(text: str):
    """Print info message"""
    print(f"{BLUE}ℹ{RESET} {text}")


def connect_to_db():
    """Connect to TimescaleDB"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'finstock_market_data'),
            user=os.getenv('POSTGRES_USER', 'finstock_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'finstock_password')
        )
        return conn
    except Exception as e:
        print_error(f"Failed to connect to database: {e}")
        print_info("Make sure PostgreSQL is running: docker ps | grep postgres")
        sys.exit(1)


def check_data_availability(conn) -> Dict[str, Any]:
    """Check how many days of data we have"""
    cursor = conn.cursor()

    # Query data statistics
    cursor.execute("""
        SELECT
            MIN(DATE(timestamp)) as oldest_date,
            MAX(DATE(timestamp)) as newest_date,
            COUNT(DISTINCT DATE(timestamp)) as unique_days,
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT CASE WHEN is_vn30 = true THEN symbol END) as vn30_symbols
        FROM market_data_hot;
    """)

    result = cursor.fetchone()
    cursor.close()

    return {
        'oldest_date': result[0],
        'newest_date': result[1],
        'unique_days': result[2] or 0,
        'total_records': result[3] or 0,
        'unique_symbols': result[4] or 0,
        'vn30_symbols': result[5] or 0
    }


def check_backtest_results(conn) -> Dict[str, Any]:
    """Check if backtest has run"""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) as result_count,
            MAX(timestamp) as latest_backtest,
            COUNT(DISTINCT strategy) as strategy_count
        FROM backtest_results
        WHERE timestamp >= NOW() - INTERVAL '2 days';
    """)

    result = cursor.fetchone()
    cursor.close()

    return {
        'result_count': result[0] or 0,
        'latest_backtest': result[1],
        'strategy_count': result[2] or 0
    }


def check_paper_trades(conn) -> Dict[str, Any]:
    """Check if paper trading has executed trades"""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) as trade_count,
            MAX(timestamp) as latest_trade,
            COUNT(DISTINCT symbol) as symbols_traded,
            COUNT(CASE WHEN side = 'BUY' THEN 1 END) as buy_count,
            COUNT(CASE WHEN side = 'SELL' THEN 1 END) as sell_count
        FROM paper_trades
        WHERE timestamp >= NOW() - INTERVAL '7 days';
    """)

    result = cursor.fetchone()
    cursor.close()

    return {
        'trade_count': result[0] or 0,
        'latest_trade': result[1],
        'symbols_traded': result[2] or 0,
        'buy_count': result[3] or 0,
        'sell_count': result[4] or 0
    }


def determine_phase(data_stats: Dict, backtest_stats: Dict, trade_stats: Dict) -> int:
    """
    Determine which phase the system is in:
    Phase 1: Data Collection (< 7 days)
    Phase 2: Backtest Activation (>= 7 days, backtest ran)
    Phase 3: Live Trading (>= 7 days, backtest ran, trading active)
    """
    days = data_stats['unique_days']
    has_backtest = backtest_stats['result_count'] > 0
    has_trades = trade_stats['trade_count'] > 0

    if days < 7:
        return 1
    elif days >= 7 and has_backtest and has_trades:
        return 3
    elif days >= 7 and has_backtest:
        return 2.5  # Backtest done, waiting for trading
    elif days >= 7:
        return 2  # Enough data, waiting for backtest
    else:
        return 1


def print_phase_status(phase: float, data_stats: Dict):
    """Print current phase status"""
    days = data_stats['unique_days']

    if phase == 1:
        print_header(f"PHASE 1: DATA COLLECTION (Day {days}/7)")
        print_info("System is collecting market data")
        print_info(f"Progress: {days}/7 days ({(days/7)*100:.1f}%)")
        print_warning("Backtest will start automatically on Day 7")
        print_warning("Paper trading will activate after backtest completes")

    elif phase == 2:
        print_header(f"PHASE 2: BACKTEST READY (Day {days})")
        print_success(f"Data collection complete: {days} days available")
        print_warning("Backtest scheduled to run at 06:00 AM daily")
        print_info("Check Airflow UI: http://localhost:8080")
        print_info("Look for: daily_backtest_optimization DAG")

    elif phase == 2.5:
        print_header(f"PHASE 2: BACKTEST COMPLETED (Day {days})")
        print_success(f"Data available: {days} days")
        print_success("Backtest has run successfully!")
        print_warning("Paper trading should activate soon...")
        print_info("Check paper trading logs: docker logs paper-trading")

    elif phase == 3:
        print_header(f"PHASE 3: LIVE TRADING (Day {days})")
        print_success(f"Data collection: {days} days (ongoing)")
        print_success("Backtest: Running daily")
        print_success("Paper trading: ACTIVE ✓")
        print_info("System fully operational!")


def print_data_statistics(stats: Dict):
    """Print data availability statistics"""
    print(f"\n{BOLD}Data Availability:{RESET}")
    print(f"  Date Range: {stats['oldest_date']} to {stats['newest_date']}")
    print(f"  Unique Days: {stats['unique_days']}")
    print(f"  Total Records: {stats['total_records']:,}")
    print(f"  Unique Symbols: {stats['unique_symbols']}")
    print(f"  VN30 Symbols: {stats['vn30_symbols']}")

    # Data quality checks
    if stats['unique_days'] >= 7:
        print_success("Data requirement met: >= 7 days")
    else:
        print_warning(f"Need {7 - stats['unique_days']} more days")

    if stats['unique_symbols'] >= 5:
        print_success("Symbol requirement met: >= 5 symbols")
    else:
        print_warning(f"Need {5 - stats['unique_symbols']} more symbols")


def print_backtest_statistics(stats: Dict):
    """Print backtest statistics"""
    print(f"\n{BOLD}Backtest Status:{RESET}")

    if stats['result_count'] > 0:
        print_success(f"Backtest has run: {stats['result_count']} results")
        print(f"  Latest Run: {stats['latest_backtest']}")
        print(f"  Strategies: {stats['strategy_count']}")
    else:
        print_warning("No backtest results yet")
        print_info("Backtest runs daily at 06:00 AM (if >= 7 days of data)")


def print_trading_statistics(stats: Dict):
    """Print paper trading statistics"""
    print(f"\n{BOLD}Paper Trading Status:{RESET}")

    if stats['trade_count'] > 0:
        print_success(f"Paper trading active: {stats['trade_count']} trades")
        print(f"  Latest Trade: {stats['latest_trade']}")
        print(f"  Symbols Traded: {stats['symbols_traded']}")
        print(f"  Buy Orders: {stats['buy_count']}")
        print(f"  Sell Orders: {stats['sell_count']}")
    else:
        print_warning("No trades yet")
        print_info("Trading activates after backtest completes")


def print_next_steps(phase: float, data_stats: Dict):
    """Print what to do next"""
    print(f"\n{BOLD}Next Steps:{RESET}")

    days = data_stats['unique_days']

    if phase == 1:
        print_info(f"Wait for Day 7 ({7 - days} more days)")
        print_info("System will automatically activate backtest")
        print_info("Run this script daily to monitor progress")

    elif phase == 2:
        print_info("Wait for 06:00 AM (backtest scheduled time)")
        print_info("Check Airflow UI: http://localhost:8080")
        print_info("Manual trigger: Click on daily_backtest_optimization → Trigger DAG")

    elif phase == 2.5:
        print_info("Wait a few minutes for paper trading to activate")
        print_info("Check logs: docker logs paper-trading | grep 'PAPER TRADING ENABLED'")
        print_info("If not activating, restart: docker restart paper-trading")

    elif phase == 3:
        print_info("System is fully operational!")
        print_info("Monitor trades: docker logs paper-trading")
        print_info("Check P&L: Runs daily at 15:30")
        print_info("View dashboards: Superset & Grafana")


def print_useful_commands():
    """Print useful docker commands"""
    print(f"\n{BOLD}Useful Commands:{RESET}")
    print("\n# Check day count:")
    print("  docker exec -it postgres psql -U finstock_user -d finstock_market_data -c \\")
    print("    \"SELECT COUNT(DISTINCT DATE(timestamp)) as days FROM market_data_hot;\"")

    print("\n# Check VnStock producer logs:")
    print("  docker logs vnstock-producer | tail -20")

    print("\n# Check paper trading logs:")
    print("  docker logs paper-trading | tail -20")

    print("\n# Check Airflow scheduler:")
    print("  docker logs airflow-scheduler | tail -20")

    print("\n# Restart paper trading:")
    print("  docker restart paper-trading")

    print("\n# View all running services:")
    print("  docker ps")


def main():
    """Main function"""
    print_header("FINSTOCK SYSTEM STATUS CHECKER")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to database
    print_info("Connecting to database...")
    conn = connect_to_db()
    print_success("Database connected")

    # Check data availability
    print_info("Checking data availability...")
    data_stats = check_data_availability(conn)

    # Check backtest results
    print_info("Checking backtest results...")
    backtest_stats = check_backtest_results(conn)

    # Check paper trades
    print_info("Checking paper trades...")
    trade_stats = check_paper_trades(conn)

    conn.close()

    # Determine current phase
    phase = determine_phase(data_stats, backtest_stats, trade_stats)

    # Print status
    print_phase_status(phase, data_stats)
    print_data_statistics(data_stats)
    print_backtest_statistics(backtest_stats)
    print_trading_statistics(trade_stats)
    print_next_steps(phase, data_stats)
    print_useful_commands()

    print(f"\n{BOLD}{BLUE}{'=' * 80}{RESET}")
    print(f"{BOLD}Status check complete!{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 80}{RESET}\n")


if __name__ == '__main__':
    main()
