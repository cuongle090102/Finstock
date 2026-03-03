#!/usr/bin/env python3
"""
Comprehensive Strategy Backtesting Framework for Vietnamese Trading System

This module provides advanced backtesting capabilities with Delta Lake integration,
comprehensive performance metrics, and Vietnamese market-specific features.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
import logging
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import time

try:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except ImportError:
    logging.warning("PySpark/Delta Lake not available - using CSV fallback")
    SPARK_AVAILABLE = False

# Strategy imports
from strategies.base_strategy import BaseStrategy, SignalType, MarketSession
from strategies.ma_crossover_strategy import MovingAverageCrossoverStrategy
from strategies.breakout_strategy import BreakoutStrategy
from strategies.mean_reversion_strategy import MeanReversionStrategy
from strategies.momentum_strategy import MomentumStrategy

@dataclass
class BacktestConfig:
    """Configuration for backtesting parameters"""
    start_date: datetime
    end_date: datetime
    initial_capital: float = 1000000.0  # 1M VND
    commission_rate: float = 0.0015  # 0.15% standard rate
    vn30_commission_rate: float = 0.001  # 0.10% VN30 rate
    tax_rate: float = 0.001  # 0.10% securities tax
    min_commission: float = 1000.0  # 1,000 VND minimum
    slippage: float = 0.0005  # 0.05% slippage
    symbols: Optional[List[str]] = None  # If None, uses all available
    benchmark_symbol: str = "VN30"  # Vietnamese market benchmark
    rebalance_frequency: str = "daily"  # daily, weekly, monthly
    position_sizing: str = "equal_weight"  # equal_weight, risk_parity, kelly
    max_position_size: float = 0.1  # 10% max per position
    
@dataclass
class Trade:
    """Individual trade record"""
    timestamp: datetime
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: float
    price: float
    commission: float
    tax: float
    strategy: str
    signal_confidence: float
    market_session: str
    
    @property
    def total_cost(self) -> float:
        """Total cost including commissions and tax"""
        return (self.quantity * self.price) + self.commission + self.tax
    
    @property
    def net_amount(self) -> float:
        """Net amount (positive for sells, negative for buys)"""
        base_amount = self.quantity * self.price
        if self.side == 'BUY':
            return -(base_amount + self.commission + self.tax)
        else:
            return base_amount - self.commission - self.tax

@dataclass
class Position:
    """Position tracking"""
    symbol: str
    quantity: float
    avg_cost: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float
    total_cost: float  # Including commissions
    last_price: float
    
    @property
    def total_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""
    # Return metrics
    total_return: float
    annualized_return: float
    cumulative_return: float
    
    # Risk metrics
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown: float
    max_drawdown_duration: int
    
    # Trading metrics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    
    # Vietnamese market specific
    vn30_exposure: float
    commission_paid: float
    tax_paid: float
    
    # Risk measures
    var_95: float  # Value at Risk 95%
    cvar_95: float  # Conditional VaR 95%
    beta: float  # Market beta
    alpha: float  # Market alpha

class HistoricalDataProvider:
    """Historical data access with Delta Lake and CSV fallback"""
    
    def __init__(self):
        self.spark = None
        self.data_cache = {}
        
        if SPARK_AVAILABLE:
            self._init_spark()
    
    def _init_spark(self):
        """Initialize Spark session with Delta Lake"""
        try:
            builder = SparkSession.builder.appName("FinStock_Backtesting") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            self.spark.sparkContext.setLogLevel("WARN")
            logging.info("Spark session initialized for backtesting")
            
        except Exception as e:
            logging.error(f"Failed to initialize Spark: {e}")
            self.spark = None
    
    def get_historical_data(self, 
                           symbols: List[str], 
                           start_date: datetime, 
                           end_date: datetime,
                           timeframe: str = "daily") -> pd.DataFrame:
        """Get historical OHLCV data"""
        
        cache_key = f"{'-'.join(symbols)}_{start_date}_{end_date}_{timeframe}"
        if cache_key in self.data_cache:
            return self.data_cache[cache_key]
        
        if self.spark:
            data = self._get_delta_lake_data(symbols, start_date, end_date, timeframe)
        else:
            data = self._get_csv_fallback_data(symbols, start_date, end_date, timeframe)
        
        self.data_cache[cache_key] = data
        return data
    
    def _get_delta_lake_data(self, symbols: List[str], start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """Get data from Delta Lake silver layer"""
        try:
            # Read from Delta Lake silver layer
            silver_path = "data/delta/silver/market_data"
            
            query = f"""
            SELECT timestamp, symbol, open, high, low, close, volume, adj_close
            FROM delta.`{silver_path}`
            WHERE symbol IN ({','.join([f"'{s}'" for s in symbols])})
            AND timestamp BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY timestamp, symbol
            """
            
            df_spark = self.spark.sql(query)
            df = df_spark.toPandas()
            
            if df.empty:
                logging.warning("No data found in Delta Lake - using fallback")
                return self._get_csv_fallback_data(symbols, start_date, end_date, timeframe)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index(['timestamp', 'symbol']).sort_index()
            
            logging.info(f"Loaded {len(df)} records from Delta Lake")
            return df
            
        except Exception as e:
            logging.error(f"Failed to read from Delta Lake: {e}")
            return self._get_csv_fallback_data(symbols, start_date, end_date, timeframe)
    
    def _get_csv_fallback_data(self, symbols: List[str], start_date: datetime, end_date: datetime, timeframe: str) -> pd.DataFrame:
        """Fallback to generate synthetic data for testing"""
        logging.warning("Using synthetic data for backtesting - not for production!")
        
        # Generate date range
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        dates = dates[dates.weekday < 5]  # Remove weekends
        
        data_list = []
        np.random.seed(42)  # For reproducible results
        
        for symbol in symbols:
            # Generate realistic price series
            initial_price = np.random.uniform(50000, 200000)  # VND price range
            returns = np.random.normal(0.001, 0.02, len(dates))  # 0.1% daily return, 2% volatility
            
            prices = [initial_price]
            for ret in returns[1:]:
                prices.append(prices[-1] * (1 + ret))
            
            for i, date in enumerate(dates):
                price = prices[i]
                # Generate OHLC from close price
                daily_range = price * np.random.uniform(0.005, 0.03)  # 0.5-3% daily range
                
                high = price + daily_range * np.random.uniform(0.3, 1.0)
                low = price - daily_range * np.random.uniform(0.3, 1.0)
                open_price = low + (high - low) * np.random.uniform(0.2, 0.8)
                
                volume = np.random.uniform(100000, 1000000)
                
                data_list.append({
                    'timestamp': date,
                    'symbol': symbol,
                    'open': round(open_price, -2),  # Round to nearest 100 VND
                    'high': round(high, -2),
                    'low': round(low, -2),
                    'close': round(price, -2),
                    'volume': int(volume),
                    'adj_close': round(price, -2)
                })
        
        df = pd.DataFrame(data_list)
        df = df.set_index(['timestamp', 'symbol']).sort_index()
        
        logging.info(f"Generated {len(df)} synthetic records for backtesting")
        return df

class StrategyBacktester:
    """Main backtesting engine"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.data_provider = HistoricalDataProvider()
        
        # State tracking
        self.trades: List[Trade] = []
        self.positions: Dict[str, Position] = {}
        self.portfolio_values: List[Tuple[datetime, float]] = []
        self.cash = config.initial_capital
        self.current_date = None
        
        # Performance tracking
        self.daily_returns = []
        self.benchmark_returns = []
        
        # Vietnamese market symbols
        self.vn30_symbols = {
            'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'EIB', 'FPT', 'GAS', 'GVR', 'HDB',
            'HPG', 'KDH', 'KHG', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB',
            'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB'
        }
    
    def run_backtest(self, strategy: BaseStrategy, symbols: List[str] = None) -> Dict[str, Any]:
        """Run comprehensive backtest for a strategy"""
        
        if symbols is None:
            symbols = self.config.symbols or ['VIC', 'VCB', 'HPG', 'VNM', 'GAS']  # Default VN30 subset
        
        logging.info(f"Starting backtest: {strategy.name} | {self.config.start_date} to {self.config.end_date}")
        start_time = time.time()
        
        # Reset state
        self._reset_state()
        
        # Load historical data
        historical_data = self.data_provider.get_historical_data(
            symbols, self.config.start_date, self.config.end_date
        )
        
        if historical_data.empty:
            raise ValueError("No historical data available for backtesting")
        
        # Get unique dates
        dates = historical_data.index.get_level_values('timestamp').unique().sort_values()
        
        # Run simulation day by day
        for date in dates:
            self.current_date = date
            daily_data = historical_data.loc[date]
            
            if isinstance(daily_data, pd.Series):
                # Single symbol case
                daily_data = daily_data.to_frame().T
                daily_data.index = [daily_data.index.name]
                daily_data.index.name = 'symbol'
            
            # Convert to format expected by strategy
            market_data = self._convert_to_strategy_format(daily_data, date)
            
            # Process data through strategy
            strategy.on_data(market_data)
            signals = strategy.generate_signals()
            
            # Execute signals
            for signal in signals:
                self._execute_signal(signal, daily_data)
            
            # Update positions with current prices
            self._update_positions(daily_data)
            
            # Record portfolio value
            portfolio_value = self._calculate_portfolio_value(daily_data)
            self.portfolio_values.append((date, portfolio_value))
        
        # Calculate final performance metrics
        performance = self._calculate_performance_metrics(historical_data)
        
        duration = time.time() - start_time
        logging.info(f"Backtest completed in {duration:.2f}s | Return: {performance.total_return:.2%}")
        
        return {
            'strategy_name': strategy.name,
            'config': asdict(self.config),
            'performance': asdict(performance),
            'trades': [asdict(t) for t in self.trades],
            'portfolio_values': self.portfolio_values,
            'positions': {k: asdict(v) for k, v in self.positions.items()},
            'final_cash': self.cash,
            'duration_seconds': duration
        }
    
    def _reset_state(self):
        """Reset backtester state"""
        self.trades.clear()
        self.positions.clear()
        self.portfolio_values.clear()
        self.cash = self.config.initial_capital
        self.daily_returns.clear()
        self.benchmark_returns.clear()
        self.current_date = None
    
    def _convert_to_strategy_format(self, daily_data: pd.DataFrame, date: datetime) -> pd.DataFrame:
        """Convert historical data to format expected by strategy"""
        records = []
        
        for symbol in daily_data.index:
            row = daily_data.loc[symbol]
            records.append({
                'timestamp': date,
                'symbol': symbol,
                'close': row['close'],
                'high': row['high'],
                'low': row['low'],
                'volume': row['volume']
            })
        
        return pd.DataFrame(records)
    
    def _execute_signal(self, signal: Dict[str, Any], market_data: pd.DataFrame):
        """Execute trading signal"""
        symbol = signal['symbol']
        signal_type = signal['signal_type']
        confidence = signal.get('confidence', 0.5)
        
        if symbol not in market_data.index:
            return
        
        current_price = market_data.loc[symbol, 'close']
        
        # Apply slippage
        if signal_type == 'BUY':
            execution_price = current_price * (1 + self.config.slippage)
        elif signal_type == 'SELL':
            execution_price = current_price * (1 - self.config.slippage)
        else:
            return  # HOLD signal
        
        # Calculate position size
        quantity = self._calculate_position_size(symbol, execution_price, signal_type, confidence)
        
        if quantity == 0:
            return
        
        # Calculate costs
        is_vn30 = symbol in self.vn30_symbols
        commission_rate = self.config.vn30_commission_rate if is_vn30 else self.config.commission_rate
        commission = max(quantity * execution_price * commission_rate, self.config.min_commission)
        tax = quantity * execution_price * self.config.tax_rate if signal_type == 'SELL' else 0.0
        
        # Check if we have enough cash for buy orders
        if signal_type == 'BUY':
            total_cost = quantity * execution_price + commission + tax
            if total_cost > self.cash:
                # Reduce quantity to fit available cash
                available_for_shares = self.cash - commission - tax
                if available_for_shares <= 0:
                    return
                quantity = int(available_for_shares / execution_price / 100) * 100  # Round to lot size
                if quantity == 0:
                    return
                # Recalculate with new quantity
                total_cost = quantity * execution_price + commission + tax
        
        # Execute trade
        trade = Trade(
            timestamp=self.current_date,
            symbol=symbol,
            side=signal_type,
            quantity=quantity,
            price=execution_price,
            commission=commission,
            tax=tax,
            strategy=signal.get('strategy', 'Unknown'),
            signal_confidence=confidence,
            market_session=signal.get('vietnamese_session', 'unknown')
        )
        
        self.trades.append(trade)
        
        # Update cash
        self.cash -= trade.net_amount
        
        # Update position
        self._update_position(trade, current_price)
    
    def _calculate_position_size(self, symbol: str, price: float, signal_type: str, confidence: float) -> int:
        """Calculate position size based on strategy"""
        
        if signal_type == 'SELL':
            # For sell signals, use current position quantity
            current_position = self.positions.get(symbol)
            if current_position and current_position.quantity > 0:
                return min(current_position.quantity, int(current_position.quantity * confidence))
            return 0
        
        # For buy signals
        if self.config.position_sizing == "equal_weight":
            # Equal weight across positions
            max_position_value = self.config.initial_capital * self.config.max_position_size
            max_quantity = int(max_position_value / price / 100) * 100  # Round to lot size
            
            # Scale by confidence
            target_quantity = int(max_quantity * confidence / 100) * 100
            
            return max(100, target_quantity)  # Minimum 100 shares (1 lot)
        
        elif self.config.position_sizing == "risk_parity":
            # Risk parity based on volatility (simplified)
            portfolio_value = self._calculate_current_portfolio_value()
            risk_budget = portfolio_value * 0.02  # 2% risk per position
            
            # Estimate volatility (simplified as 2% daily)
            estimated_volatility = 0.02
            position_size = risk_budget / (price * estimated_volatility)
            
            return int(position_size / 100) * 100  # Round to lot size
        
        else:
            # Default to equal weight
            return self._calculate_position_size(symbol, price, signal_type, confidence)
    
    def _update_position(self, trade: Trade, current_price: float):
        """Update position with new trade"""
        symbol = trade.symbol
        
        if symbol not in self.positions:
            self.positions[symbol] = Position(
                symbol=symbol,
                quantity=0,
                avg_cost=0,
                market_value=0,
                unrealized_pnl=0,
                realized_pnl=0,
                total_cost=0,
                last_price=current_price
            )
        
        position = self.positions[symbol]
        
        if trade.side == 'BUY':
            # Update average cost
            total_shares = position.quantity + trade.quantity
            if total_shares > 0:
                total_cost = (position.quantity * position.avg_cost) + trade.total_cost
                position.avg_cost = total_cost / total_shares
            
            position.quantity = total_shares
            position.total_cost += trade.total_cost
            
        elif trade.side == 'SELL':
            # Calculate realized P&L
            cost_basis = trade.quantity * position.avg_cost
            proceeds = (trade.quantity * trade.price) - trade.commission - trade.tax
            realized_pnl = proceeds - cost_basis
            
            position.realized_pnl += realized_pnl
            position.quantity -= trade.quantity
            
            if position.quantity <= 0:
                position.quantity = 0
                position.avg_cost = 0
        
        # Update market value and unrealized P&L
        position.last_price = current_price
        if position.quantity > 0:
            position.market_value = position.quantity * current_price
            position.unrealized_pnl = position.market_value - (position.quantity * position.avg_cost)
        else:
            position.market_value = 0
            position.unrealized_pnl = 0
    
    def _update_positions(self, market_data: pd.DataFrame):
        """Update all positions with current market prices"""
        for symbol, position in self.positions.items():
            if symbol in market_data.index and position.quantity > 0:
                current_price = market_data.loc[symbol, 'close']
                position.last_price = current_price
                position.market_value = position.quantity * current_price
                position.unrealized_pnl = position.market_value - (position.quantity * position.avg_cost)
    
    def _calculate_portfolio_value(self, market_data: pd.DataFrame) -> float:
        """Calculate current portfolio value"""
        total_value = self.cash
        
        for position in self.positions.values():
            if position.quantity > 0:
                total_value += position.market_value
        
        return total_value
    
    def _calculate_current_portfolio_value(self) -> float:
        """Calculate current portfolio value without market data"""
        total_value = self.cash
        
        for position in self.positions.values():
            if position.quantity > 0:
                total_value += position.market_value
        
        return total_value
    
    def _calculate_performance_metrics(self, historical_data: pd.DataFrame) -> PerformanceMetrics:
        """Calculate comprehensive performance metrics"""
        
        if len(self.portfolio_values) < 2:
            return self._empty_performance_metrics()
        
        # Calculate returns
        portfolio_df = pd.DataFrame(self.portfolio_values, columns=['date', 'value'])
        portfolio_df = portfolio_df.set_index('date')
        portfolio_df['returns'] = portfolio_df['value'].pct_change().dropna()
        
        # Basic return metrics
        total_return = (portfolio_df['value'].iloc[-1] / self.config.initial_capital) - 1
        days = (self.config.end_date - self.config.start_date).days
        years = days / 365.25
        annualized_return = ((1 + total_return) ** (1/years)) - 1 if years > 0 else 0
        
        # Risk metrics
        returns = portfolio_df['returns'].dropna()
        volatility = returns.std() * np.sqrt(252) if len(returns) > 1 else 0
        
        # Sharpe ratio (assume 3% risk-free rate)
        risk_free_rate = 0.03
        excess_returns = returns.mean() * 252 - risk_free_rate
        sharpe_ratio = excess_returns / volatility if volatility > 0 else 0
        
        # Sortino ratio
        downside_returns = returns[returns < 0]
        downside_deviation = downside_returns.std() * np.sqrt(252) if len(downside_returns) > 1 else volatility
        sortino_ratio = excess_returns / downside_deviation if downside_deviation > 0 else 0
        
        # Maximum drawdown
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Drawdown duration
        max_dd_duration = 0
        current_dd_duration = 0
        for dd in drawdown:
            if dd < 0:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0
        
        # Calmar ratio
        calmar_ratio = annualized_return / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # Trading metrics
        winning_trades = len([t for t in self.trades if self._trade_pnl(t) > 0])
        losing_trades = len([t for t in self.trades if self._trade_pnl(t) < 0])
        total_trades = len(self.trades)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        wins = [self._trade_pnl(t) for t in self.trades if self._trade_pnl(t) > 0]
        losses = [self._trade_pnl(t) for t in self.trades if self._trade_pnl(t) < 0]
        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 0
        
        # Vietnamese market specific
        vn30_trades = [t for t in self.trades if t.symbol in self.vn30_symbols]
        vn30_exposure = len(vn30_trades) / total_trades if total_trades > 0 else 0
        
        commission_paid = sum(t.commission for t in self.trades)
        tax_paid = sum(t.tax for t in self.trades)
        
        # Risk measures
        var_95 = returns.quantile(0.05) if len(returns) > 0 else 0
        cvar_95 = returns[returns <= var_95].mean() if len(returns) > 0 and var_95 < 0 else 0
        
        # Market beta/alpha (simplified - would need benchmark data)
        beta = 1.0  # Placeholder
        alpha = annualized_return - (risk_free_rate + beta * (0.08 - risk_free_rate))  # Assume 8% market return
        
        return PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            cumulative_return=total_return,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            max_drawdown_duration=max_dd_duration,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            vn30_exposure=vn30_exposure,
            commission_paid=commission_paid,
            tax_paid=tax_paid,
            var_95=var_95,
            cvar_95=cvar_95,
            beta=beta,
            alpha=alpha
        )
    
    def _trade_pnl(self, trade: Trade) -> float:
        """Calculate P&L for a completed trade pair (simplified)"""
        # This is a simplified calculation - in practice you'd match buy/sell pairs
        if trade.side == 'SELL':
            return trade.net_amount  # Approximate
        return 0
    
    def _empty_performance_metrics(self) -> PerformanceMetrics:
        """Return empty performance metrics"""
        return PerformanceMetrics(
            total_return=0, annualized_return=0, cumulative_return=0,
            volatility=0, sharpe_ratio=0, sortino_ratio=0, calmar_ratio=0,
            max_drawdown=0, max_drawdown_duration=0,
            total_trades=0, winning_trades=0, losing_trades=0, win_rate=0,
            avg_win=0, avg_loss=0, profit_factor=0,
            vn30_exposure=0, commission_paid=0, tax_paid=0,
            var_95=0, cvar_95=0, beta=0, alpha=0
        )

class MultiStrategyBacktester:
    """Backtester for comparing multiple strategies"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.results = {}
    
    def run_comparison(self, strategies: List[BaseStrategy], symbols: List[str] = None) -> Dict[str, Any]:
        """Run backtest comparison across multiple strategies"""
        
        logging.info(f"Running multi-strategy comparison with {len(strategies)} strategies")
        
        # Run backtests in parallel for speed
        with ThreadPoolExecutor(max_workers=min(len(strategies), 4)) as executor:
            future_to_strategy = {}
            
            for strategy in strategies:
                backtester = StrategyBacktester(self.config)
                future = executor.submit(backtester.run_backtest, strategy, symbols)
                future_to_strategy[future] = strategy.name
            
            for future in as_completed(future_to_strategy):
                strategy_name = future_to_strategy[future]
                try:
                    result = future.result()
                    self.results[strategy_name] = result
                except Exception as e:
                    logging.error(f"Strategy {strategy_name} failed: {e}")
                    self.results[strategy_name] = None
        
        # Generate comparison report
        comparison = self._generate_comparison_report()
        
        return {
            'individual_results': self.results,
            'comparison': comparison,
            'config': asdict(self.config)
        }
    
    def _generate_comparison_report(self) -> Dict[str, Any]:
        """Generate comparison report across strategies"""
        
        valid_results = {k: v for k, v in self.results.items() if v is not None}
        
        if not valid_results:
            return {}
        
        # Extract key metrics for comparison
        comparison_data = []
        
        for strategy_name, result in valid_results.items():
            perf = result['performance']
            comparison_data.append({
                'strategy': strategy_name,
                'total_return': perf['total_return'],
                'annualized_return': perf['annualized_return'],
                'volatility': perf['volatility'],
                'sharpe_ratio': perf['sharpe_ratio'],
                'max_drawdown': perf['max_drawdown'],
                'win_rate': perf['win_rate'],
                'total_trades': perf['total_trades'],
                'profit_factor': perf['profit_factor']
            })
        
        comparison_df = pd.DataFrame(comparison_data)
        
        # Rankings
        rankings = {
            'best_return': comparison_df.loc[comparison_df['total_return'].idxmax(), 'strategy'],
            'best_sharpe': comparison_df.loc[comparison_df['sharpe_ratio'].idxmax(), 'strategy'],
            'lowest_drawdown': comparison_df.loc[comparison_df['max_drawdown'].idxmax(), 'strategy'],  # Max because drawdown is negative
            'best_win_rate': comparison_df.loc[comparison_df['win_rate'].idxmax(), 'strategy']
        }
        
        return {
            'summary_table': comparison_df.to_dict('records'),
            'rankings': rankings,
            'avg_performance': {
                'avg_return': comparison_df['total_return'].mean(),
                'avg_sharpe': comparison_df['sharpe_ratio'].mean(),
                'avg_volatility': comparison_df['volatility'].mean(),
                'avg_max_drawdown': comparison_df['max_drawdown'].mean()
            }
        }

# Convenience functions for easy usage
def backtest_strategy(strategy: BaseStrategy, 
                     start_date: Union[str, datetime],
                     end_date: Union[str, datetime],
                     initial_capital: float = 1000000,
                     symbols: List[str] = None) -> Dict[str, Any]:
    """Convenient function to backtest a single strategy"""
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    config = BacktestConfig(
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        symbols=symbols
    )
    
    backtester = StrategyBacktester(config)
    return backtester.run_backtest(strategy, symbols)

def compare_strategies(strategies: List[BaseStrategy],
                      start_date: Union[str, datetime],
                      end_date: Union[str, datetime],
                      initial_capital: float = 1000000,
                      symbols: List[str] = None) -> Dict[str, Any]:
    """Convenient function to compare multiple strategies"""
    
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    config = BacktestConfig(
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        symbols=symbols
    )
    
    multi_backtester = MultiStrategyBacktester(config)
    return multi_backtester.run_comparison(strategies, symbols)