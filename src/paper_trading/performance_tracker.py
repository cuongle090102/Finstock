"""
Performance Tracker
Phase 6: Task 6.4 - Paper Trading System

Tracks and analyzes paper trading performance metrics.
"""

from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict
import structlog

logger = structlog.get_logger(__name__)


class PerformanceTracker:
    """
    Track paper trading performance

    Metrics:
    - P&L (realized, unrealized, total)
    - Win rate
    - Sharpe ratio
    - Maximum drawdown
    - Trade statistics
    - Commission and tax totals
    """

    def __init__(self, config: dict):
        self.config = config
        self.trades: List[Dict] = []
        self.daily_pnl: Dict[str, Decimal] = defaultdict(lambda: Decimal('0'))
        self.equity_curve: List[tuple] = []

        # Performance metrics
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_pnl = Decimal('0')
        self.total_commission = Decimal('0')
        self.total_tax = Decimal('0')
        self.max_drawdown = Decimal('0')
        self.peak_equity = Decimal('0')

        # Strategy breakdown
        self.strategy_performance: Dict[str, Dict] = defaultdict(lambda: {
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'pnl': Decimal('0'),
            'commission': Decimal('0')
        })

    def record_trade(self, order):
        """Record a completed trade"""
        from .paper_broker import OrderSide

        trade_record = {
            'trade_id': order.order_id,
            'timestamp': order.timestamp,
            'symbol': order.symbol,
            'side': order.side.value,
            'quantity': order.filled_quantity,
            'price': order.filled_price,
            'commission': order.commission,
            'tax': order.tax,
            'total_cost': order.total_cost,
            'strategy': order.strategy,
            'is_vn30': order.is_vn30,
            'exchange': order.exchange
        }

        self.trades.append(trade_record)
        self.total_trades += 1
        self.total_commission += order.commission
        self.total_tax += order.tax

        # Update strategy performance
        strategy = order.strategy if order.strategy else "unknown"
        self.strategy_performance[strategy]['trades'] += 1
        self.strategy_performance[strategy]['commission'] += order.commission

        logger.debug(
            "Trade recorded",
            trade_id=order.order_id,
            symbol=order.symbol,
            strategy=strategy,
            total_trades=self.total_trades
        )

    def record_realized_pnl(self, pnl: Decimal, strategy: str = ""):
        """Record realized P&L from closed position"""
        self.total_pnl += pnl

        if pnl > 0:
            self.winning_trades += 1
        elif pnl < 0:
            self.losing_trades += 1

        # Update strategy performance
        if strategy:
            self.strategy_performance[strategy]['pnl'] += pnl
            if pnl > 0:
                self.strategy_performance[strategy]['wins'] += 1
            else:
                self.strategy_performance[strategy]['losses'] += 1

        # Record daily P&L
        today = datetime.now().date().isoformat()
        self.daily_pnl[today] += pnl

    def update_equity(self, total_equity: Decimal):
        """Update equity curve and calculate drawdown"""
        timestamp = datetime.now()
        self.equity_curve.append((timestamp, total_equity))

        # Update peak equity
        if total_equity > self.peak_equity:
            self.peak_equity = total_equity

        # Calculate current drawdown
        if self.peak_equity > 0:
            current_drawdown = (self.peak_equity - total_equity) / self.peak_equity * 100
            if current_drawdown > self.max_drawdown:
                self.max_drawdown = current_drawdown

    def get_win_rate(self) -> float:
        """Calculate win rate"""
        total_closed = self.winning_trades + self.losing_trades
        if total_closed == 0:
            return 0.0
        return (self.winning_trades / total_closed) * 100

    def get_profit_factor(self) -> float:
        """
        Calculate profit factor (gross profit / gross loss)
        """
        gross_profit = sum(pnl for pnl in self.daily_pnl.values() if pnl > 0)
        gross_loss = abs(sum(pnl for pnl in self.daily_pnl.values() if pnl < 0))

        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0

        return float(gross_profit / gross_loss)

    def get_sharpe_ratio(self, risk_free_rate: float = 0.05) -> float:
        """
        Calculate Sharpe ratio (annualized)

        Args:
            risk_free_rate: Annual risk-free rate (default 5%)
        """
        if len(self.daily_pnl) < 2:
            return 0.0

        # Calculate daily returns
        daily_returns = [float(pnl) for pnl in self.daily_pnl.values()]

        # Calculate mean and std dev
        import statistics
        mean_return = statistics.mean(daily_returns)
        std_return = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0

        if std_return == 0:
            return 0.0

        # Annualize (assuming 252 trading days)
        daily_rf = risk_free_rate / 252
        sharpe = (mean_return - daily_rf) / std_return * (252 ** 0.5)

        return sharpe

    def get_daily_summary(self, date: str = None) -> Dict:
        """Get performance summary for a specific date"""
        if date is None:
            date = datetime.now().date().isoformat()

        trades_today = [t for t in self.trades if t['timestamp'].date().isoformat() == date]

        return {
            'date': date,
            'num_trades': len(trades_today),
            'pnl': float(self.daily_pnl.get(date, Decimal('0'))),
            'commission': float(sum(t['commission'] for t in trades_today)),
            'tax': float(sum(t['tax'] for t in trades_today)),
            'trades': trades_today
        }

    def get_weekly_summary(self) -> Dict:
        """Get performance summary for the past week"""
        week_ago = datetime.now() - timedelta(days=7)
        recent_trades = [t for t in self.trades if t['timestamp'] >= week_ago]

        # Calculate weekly P&L
        weekly_dates = [
            (datetime.now() - timedelta(days=i)).date().isoformat()
            for i in range(7)
        ]
        weekly_pnl = sum(self.daily_pnl.get(date, Decimal('0')) for date in weekly_dates)

        return {
            'period': 'last_7_days',
            'start_date': week_ago.date().isoformat(),
            'end_date': datetime.now().date().isoformat(),
            'num_trades': len(recent_trades),
            'total_pnl': float(weekly_pnl),
            'commission': float(sum(t['commission'] for t in recent_trades)),
            'tax': float(sum(t['tax'] for t in recent_trades)),
            'win_rate': self._calculate_win_rate(recent_trades)
        }

    def get_comprehensive_report(self) -> Dict:
        """Get comprehensive performance report"""
        return {
            'summary': {
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades,
                'win_rate': self.get_win_rate(),
                'profit_factor': self.get_profit_factor()
            },
            'pnl': {
                'total_pnl': float(self.total_pnl),
                'total_commission': float(self.total_commission),
                'total_tax': float(self.total_tax),
                'net_pnl': float(self.total_pnl - self.total_commission - self.total_tax),
                'max_drawdown': float(self.max_drawdown)
            },
            'risk_metrics': {
                'sharpe_ratio': self.get_sharpe_ratio(),
                'max_drawdown_pct': float(self.max_drawdown)
            },
            'strategy_breakdown': {
                strategy: {
                    'trades': stats['trades'],
                    'wins': stats['wins'],
                    'losses': stats['losses'],
                    'win_rate': (stats['wins'] / stats['trades'] * 100) if stats['trades'] > 0 else 0,
                    'pnl': float(stats['pnl']),
                    'commission': float(stats['commission'])
                }
                for strategy, stats in self.strategy_performance.items()
            },
            'daily_pnl': {
                date: float(pnl)
                for date, pnl in sorted(self.daily_pnl.items())
            }
        }

    def get_trade_statistics(self) -> Dict:
        """Get detailed trade statistics"""
        if not self.trades:
            return {'message': 'No trades recorded'}

        # Commission breakdown
        vn30_commission = sum(t['commission'] for t in self.trades if t.get('is_vn30', False))
        standard_commission = sum(t['commission'] for t in self.trades if not t.get('is_vn30', False))

        # Exchange breakdown
        from collections import Counter
        exchange_counts = Counter(t['exchange'] for t in self.trades)

        # Side breakdown
        buy_count = sum(1 for t in self.trades if t['side'] == 'BUY')
        sell_count = sum(1 for t in self.trades if t['side'] == 'SELL')

        return {
            'total_trades': len(self.trades),
            'buy_trades': buy_count,
            'sell_trades': sell_count,
            'commission': {
                'total': float(self.total_commission),
                'vn30': float(vn30_commission),
                'standard': float(standard_commission)
            },
            'tax': {
                'total': float(self.total_tax)
            },
            'exchange_breakdown': dict(exchange_counts),
            'avg_trade_size': float(sum(t['total_cost'] for t in self.trades) / len(self.trades))
        }

    def _calculate_win_rate(self, trades: List[Dict]) -> float:
        """Calculate win rate for a specific set of trades"""
        if not trades:
            return 0.0

        # This is simplified - in reality you'd need matched buy/sell pairs
        # For now, just count if trade value after costs is positive
        wins = sum(1 for t in trades if t['total_cost'] > 0)
        return (wins / len(trades)) * 100 if trades else 0.0

    def export_trades_to_csv(self, filename: str):
        """Export trade history to CSV"""
        import csv

        with open(filename, 'w', newline='') as csvfile:
            if not self.trades:
                logger.warning("No trades to export")
                return

            fieldnames = self.trades[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for trade in self.trades:
                writer.writerow({
                    k: str(v) if isinstance(v, Decimal) else v
                    for k, v in trade.items()
                })

        logger.info("Trades exported to CSV", filename=filename, count=len(self.trades))

    def reset(self):
        """Reset all performance tracking"""
        self.trades.clear()
        self.daily_pnl.clear()
        self.equity_curve.clear()
        self.strategy_performance.clear()

        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_pnl = Decimal('0')
        self.total_commission = Decimal('0')
        self.total_tax = Decimal('0')
        self.max_drawdown = Decimal('0')
        self.peak_equity = Decimal('0')

        logger.info("Performance tracker reset")
