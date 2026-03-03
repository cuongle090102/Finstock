"""Enhanced base strategy class with Kafka integration and Vietnamese market support."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import json
import logging
from enum import Enum

import pytz
from kafka import KafkaProducer, KafkaConsumer

from src.utils.market_config import (
    MarketSession, VN_TIMEZONE, TRADING_HOURS, VN30_SYMBOLS,
    get_current_market_session as _get_market_session,
    is_market_open as _is_market_open,
)

class SignalType(Enum):
    """Trading signal types."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class BaseStrategy(ABC):
    """Enhanced abstract base class for trading strategies with Vietnamese market support."""
    
    def __init__(self, name: str, params: Dict[str, Any]):
        self.name = name
        self.params = params
        
        # Position management
        self.positions = {}
        self.max_position_size = params.get('max_position_size', 0.05)  # 5% of portfolio
        self.stop_loss_pct = params.get('stop_loss_pct', 0.02)  # 2% stop loss
        self.take_profit_pct = params.get('take_profit_pct', 0.04)  # 4% take profit
        
        # Performance tracking
        self.performance_metrics = {
            'total_signals': 0,
            'successful_signals': 0,
            'total_return': 0.0,
            'sharpe_ratio': 0.0,
            'max_drawdown': 0.0,
            'win_rate': 0.0
        }
        
        # Vietnamese market configuration (from centralized config)
        self.vn_timezone = VN_TIMEZONE
        self.trading_hours = TRADING_HOURS
        
        # Data buffers
        self.market_data_buffer = pd.DataFrame()
        self.indicator_buffer = {}
        self.signal_history = []

        # Trade tracking for performance calculation
        self.trade_history = []  # List of completed trades
        self.equity_curve = []  # Portfolio value over time
        self.initial_capital = params.get('portfolio_value', 1000000.0)
        
        # Kafka integration
        self.kafka_bootstrap_servers = params.get('kafka_servers', 'localhost:9092')
        self.kafka_producer = None
        self.kafka_consumer = None
        
        # Risk management
        self.portfolio_value = params.get('portfolio_value', 1000000.0)  # 1M VND default
        self.max_portfolio_risk = params.get('max_portfolio_risk', 0.02)  # 2% portfolio risk
        
        # Warmup mode — suppresses Kafka publishing during historical buffer pre-loading
        self.warmup_mode = False

        # Logging
        self.logger = logging.getLogger(f'strategy.{self.name}')
        
    def initialize(self):
        """Initialize strategy parameters and Kafka connections."""
        try:
            # Initialize Kafka producer for signals
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None
            )
            
            self.logger.info(f"Strategy {self.name} initialized successfully")
            self._initialize_strategy()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize strategy {self.name}: {e}")
            raise
    
    @abstractmethod
    def _initialize_strategy(self):
        """Initialize strategy-specific parameters and indicators."""
        pass
    
    def get_current_market_session(self) -> MarketSession:
        """Determine current Vietnamese market trading session."""
        return _get_market_session()

    def is_market_open(self) -> bool:
        """Check if Vietnamese market is currently open for trading."""
        return _is_market_open()
    
    def update_market_data(self, market_data: pd.DataFrame):
        """Update market data buffer and process new data."""
        try:
            # Add timestamp if not present
            if 'timestamp' not in market_data.columns:
                market_data['timestamp'] = datetime.now(self.vn_timezone)
            
            # Update buffer (keep last 1000 records)
            self.market_data_buffer = pd.concat([self.market_data_buffer, market_data]).tail(1000)
            
            # Process new data
            self.on_data(market_data)
            
        except Exception as e:
            self.logger.error(f"Error updating market data: {e}")
    
    @abstractmethod
    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data."""
        pass
    
    def calculate_position_size(self, symbol: str, price: float, confidence: float,
                                 is_vn30: bool = False, exchange: str = 'HOSE') -> float:
        """
        Calculate position size based on risk management rules and Vietnamese market compliance.

        Implements:
        - Kelly Criterion for optimal position sizing
        - Vietnamese lot size rules (multiples of 100 shares)
        - Max 10% of capital per position
        - 500M VND max order value limit
        - Commission deduction from available capital

        Args:
            symbol: Stock symbol
            price: Current price in VND
            confidence: Signal confidence (0.0 to 1.0)
            is_vn30: Whether stock is in VN30 index (affects commission)
            exchange: Exchange (HOSE/HNX/UPCOM) - affects commission and limits

        Returns:
            Position size in shares (float), compliant with Vietnamese regulations
        """
        try:
            # Vietnamese market parameters
            LOT_SIZE = 100  # Vietnamese standard lot size
            MAX_ORDER_VALUE_VND = 500_000_000  # 500M VND limit
            MAX_POSITION_PCT = 0.10  # 10% max of portfolio per position

            # Commission rates (Vietnamese market)
            if is_vn30:
                commission_rate = 0.0010  # 0.10% for VN30
            else:
                commission_rate = 0.0015  # 0.15% for standard stocks

            min_commission = 1000  # 1,000 VND minimum

            # 1. Calculate available capital after reserving for commission
            # Reserve commission both for entry and potential exit
            commission_reserve_rate = commission_rate * 2  # Buy + Sell
            available_capital = self.portfolio_value * (1 - commission_reserve_rate)

            # 2. Apply maximum position size limit (10% of capital)
            max_capital_for_position = available_capital * MAX_POSITION_PCT

            # 3. Kelly Criterion calculation
            # Kelly = (p * b - q) / b
            # where p = win probability (from confidence), q = 1-p, b = win/loss ratio
            # Simplified: Using confidence as proxy for Kelly percentage
            # Apply fractional Kelly (0.25x) for safety
            kelly_fraction = 0.25  # Use 1/4 Kelly for reduced risk
            kelly_adjusted_confidence = confidence * kelly_fraction

            # Capital to deploy based on Kelly
            kelly_capital = available_capital * kelly_adjusted_confidence

            # Take minimum of Kelly and max position limit
            capital_to_deploy = min(kelly_capital, max_capital_for_position)

            # 4. Calculate shares based on capital
            shares_raw = capital_to_deploy / price

            # 5. Apply portfolio risk limit (alternative sizing method)
            max_risk_amount = self.portfolio_value * self.max_portfolio_risk
            max_shares_by_risk = max_risk_amount / (price * self.stop_loss_pct)

            # Take minimum of both methods
            shares_before_lot = min(shares_raw, max_shares_by_risk)

            # 6. Round down to Vietnamese lot size (100 shares)
            shares_lots = int(shares_before_lot / LOT_SIZE)
            shares_compliant = float(shares_lots * LOT_SIZE)

            # 7. Check if shares is at least 1 lot
            if shares_compliant < LOT_SIZE:
                self.logger.warning(f"Position size for {symbol} below minimum lot size (100 shares)")
                return 0.0

            # 8. Check maximum order value (500M VND)
            order_value = shares_compliant * price
            if order_value > MAX_ORDER_VALUE_VND:
                # Scale down to max order value
                max_shares = MAX_ORDER_VALUE_VND / price
                shares_lots = int(max_shares / LOT_SIZE)
                shares_compliant = float(shares_lots * LOT_SIZE)
                self.logger.info(f"Position size for {symbol} capped at 500M VND limit: {shares_compliant} shares")

            # 9. Verify commission calculation
            estimated_commission = max(order_value * commission_rate, min_commission)
            total_cost = order_value + estimated_commission

            # Final validation: ensure we can afford it
            if total_cost > available_capital:
                self.logger.warning(f"Insufficient capital for {symbol}: need {total_cost:,.0f} VND, have {available_capital:,.0f} VND")
                return 0.0

            self.logger.debug(
                f"Position sizing for {symbol}: "
                f"{shares_compliant:.0f} shares @ {price:,.0f} VND = {order_value:,.0f} VND "
                f"(commission: {estimated_commission:,.0f} VND, total: {total_cost:,.0f} VND)"
            )

            return shares_compliant

        except Exception as e:
            self.logger.error(f"Error calculating position size for {symbol}: {e}")
            return 0.0
    
    def create_signal(self, symbol: str, signal_type: SignalType,
                     confidence: float, price: float,
                     is_vn30: bool = False, exchange: str = 'HOSE',
                     metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a standardized trading signal."""
        try:
            position_size = self.calculate_position_size(symbol, price, confidence, is_vn30, exchange)
            
            signal = {
                'timestamp': datetime.now(self.vn_timezone).isoformat(),
                'strategy': self.name,
                'symbol': symbol,
                'signal_type': signal_type.value,
                'confidence': confidence,
                'price': price,
                'position_size': position_size,
                'stop_loss': price * (1 - self.stop_loss_pct) if signal_type == SignalType.BUY else price * (1 + self.stop_loss_pct),
                'take_profit': price * (1 + self.take_profit_pct) if signal_type == SignalType.BUY else price * (1 - self.take_profit_pct),
                'vietnamese_session': self.get_current_market_session().value,
                'market_open': self.is_market_open(),
                'metadata': metadata or {}
            }
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error creating signal for {symbol}: {e}")
            return {}
    
    def set_warmup_mode(self, enabled: bool):
        """Enable/disable warmup mode. During warmup, signals are NOT published to Kafka."""
        self.warmup_mode = enabled

    def publish_signal(self, signal: Dict[str, Any]):
        """Publish signal to Kafka signals topic."""
        try:
            if not signal:
                return

            # Suppress Kafka publishing during strategy buffer warmup
            if self.warmup_mode:
                return

            # Only publish signals during market hours
            if not signal.get('market_open', False):
                self.logger.info(f"Market closed, signal for {signal.get('symbol')} not published")
                return
                
            # Publish to Kafka
            if self.kafka_producer:
                self.kafka_producer.send(
                    topic='signals',
                    key=signal['symbol'],
                    value=signal
                )
                
            # Store in signal history
            self.signal_history.append(signal)
            if len(self.signal_history) > 1000:  # Keep last 1000 signals
                self.signal_history = self.signal_history[-1000:]
                
            # Update performance metrics
            self.performance_metrics['total_signals'] += 1
            
            self.logger.info(f"Published signal: {signal['symbol']} {signal['signal_type']} @ {signal['price']}")
            
        except Exception as e:
            self.logger.error(f"Error publishing signal: {e}")
    
    @abstractmethod
    def generate_signals(self) -> List[Dict[str, Any]]:
        """Generate trading signals based on current market data."""
        pass
    
    def on_signal(self, signal: Dict[str, Any]):
        """Handle generated trading signal."""
        self.publish_signal(signal)
    
    def update_position(self, symbol: str, quantity: int, price: float):
        """Update position tracking."""
        if symbol not in self.positions:
            self.positions[symbol] = {
                'quantity': 0,
                'avg_price': 0.0,
                'unrealized_pnl': 0.0,
                'realized_pnl': 0.0
            }
        
        position = self.positions[symbol]
        
        if position['quantity'] == 0:
            # New position
            position['quantity'] = quantity
            position['avg_price'] = price
        else:
            # Update existing position
            total_cost = position['quantity'] * position['avg_price'] + quantity * price
            position['quantity'] += quantity
            if position['quantity'] != 0:
                position['avg_price'] = total_cost / position['quantity']
            else:
                position['avg_price'] = 0.0
    
    def calculate_performance(self) -> Dict[str, float]:
        """
        Calculate comprehensive strategy performance metrics.

        Implements ACTION_PLAN requirements:
        - Win rate (wins/total trades)
        - Total P&L from all trades
        - Sharpe ratio (annualized)
        - Maximum drawdown
        - Total return percentage
        - Profit factor (gross profit/gross loss)

        Returns:
            Dictionary with all performance metrics
        """
        try:
            # Return empty metrics if no trade history
            if not self.trade_history:
                return self.performance_metrics

            # 1. Calculate Win Rate
            winning_trades = [t for t in self.trade_history if t.get('pnl', 0) > 0]
            losing_trades = [t for t in self.trade_history if t.get('pnl', 0) < 0]
            total_trades = len(self.trade_history)

            win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0.0

            # 2. Calculate Total P&L
            total_pnl = sum(t.get('pnl', 0) for t in self.trade_history)

            # 3. Calculate Total Return Percentage
            total_return_pct = (total_pnl / self.initial_capital * 100) if self.initial_capital > 0 else 0.0

            # 4. Calculate Profit Factor (Gross Profit / Gross Loss)
            gross_profit = sum(t.get('pnl', 0) for t in winning_trades)
            gross_loss = abs(sum(t.get('pnl', 0) for t in losing_trades))
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

            # 5. Calculate Sharpe Ratio (Annualized)
            # Need returns series for Sharpe calculation
            if len(self.equity_curve) > 1:
                returns = []
                for i in range(1, len(self.equity_curve)):
                    if self.equity_curve[i-1] > 0:
                        ret = (self.equity_curve[i] - self.equity_curve[i-1]) / self.equity_curve[i-1]
                        returns.append(ret)

                if returns:
                    mean_return = np.mean(returns)
                    std_return = np.std(returns)

                    # Annualize: Assume daily returns, 252 trading days/year in Vietnam
                    # Vietnamese market: ~250 trading days/year
                    trading_days_per_year = 250

                    if std_return > 0:
                        sharpe_ratio = (mean_return / std_return) * np.sqrt(trading_days_per_year)
                    else:
                        sharpe_ratio = 0.0
                else:
                    sharpe_ratio = 0.0
            else:
                sharpe_ratio = 0.0

            # 6. Calculate Maximum Drawdown
            max_drawdown = 0.0
            max_drawdown_pct = 0.0

            if len(self.equity_curve) > 1:
                peak = self.equity_curve[0]
                max_dd = 0.0

                for value in self.equity_curve:
                    if value > peak:
                        peak = value

                    drawdown = peak - value
                    if drawdown > max_dd:
                        max_dd = drawdown
                        max_drawdown_pct = (drawdown / peak * 100) if peak > 0 else 0.0

                max_drawdown = max_dd

            # 7. Additional metrics
            avg_win = gross_profit / len(winning_trades) if len(winning_trades) > 0 else 0.0
            avg_loss = gross_loss / len(losing_trades) if len(losing_trades) > 0 else 0.0
            avg_trade_pnl = total_pnl / total_trades if total_trades > 0 else 0.0

            # Expectancy (average profit per trade)
            expectancy = avg_trade_pnl

            # Update performance metrics
            self.performance_metrics.update({
                # Core metrics (ACTION_PLAN required)
                'win_rate': win_rate * 100,  # Convert to percentage
                'total_pnl': total_pnl,
                'total_return_pct': total_return_pct,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'max_drawdown_pct': max_drawdown_pct,
                'profit_factor': profit_factor,

                # Additional useful metrics
                'total_trades': total_trades,
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades),
                'gross_profit': gross_profit,
                'gross_loss': gross_loss,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'avg_trade_pnl': avg_trade_pnl,
                'expectancy': expectancy,

                # Signal metrics
                'total_signals': len(self.signal_history),
                'successful_signals': sum(1 for s in self.signal_history
                                         if s.get('metadata', {}).get('successful', False))
            })

            return self.performance_metrics

        except Exception as e:
            self.logger.error(f"Error calculating performance: {e}")
            return self.performance_metrics

    def validate_signal(self, signal: Dict[str, Any]) -> bool:
        """
        Validate trading signal for Vietnamese market compliance.

        Implements ACTION_PLAN requirements:
        - Check all required fields exist
        - Validate symbol format (3-4 uppercase letters)
        - Validate side is BUY or SELL
        - Validate price (positive, multiple of 100 VND, within range)
        - Validate timestamp format
        - Check Vietnamese market hours
        - Validate confidence score (0-100)

        Args:
            signal: Trading signal dictionary

        Returns:
            True if signal is valid, False otherwise
        """
        try:
            # 1. Check required fields exist
            required_fields = ['symbol', 'signal_type', 'price', 'timestamp']
            for field in required_fields:
                if field not in signal:
                    self.logger.warning(f"Signal missing required field: {field}")
                    return False

            # 2. Validate symbol format (3-4 uppercase letters)
            symbol = signal['symbol']
            if not isinstance(symbol, str):
                self.logger.warning(f"Symbol must be string, got {type(symbol)}")
                return False

            if not (3 <= len(symbol) <= 4):
                self.logger.warning(f"Symbol length must be 3-4 characters, got {len(symbol)}")
                return False

            if not symbol.isupper():
                self.logger.warning(f"Symbol must be uppercase, got {symbol}")
                return False

            if not symbol.isalpha():
                self.logger.warning(f"Symbol must contain only letters, got {symbol}")
                return False

            # 3. Validate signal_type/side (BUY or SELL)
            signal_type = signal.get('signal_type', signal.get('side'))
            valid_types = ['BUY', 'SELL', SignalType.BUY.value, SignalType.SELL.value]

            if signal_type not in valid_types:
                self.logger.warning(f"Invalid signal_type: {signal_type}. Must be BUY or SELL")
                return False

            # 4. Validate price
            price = signal['price']

            # Price must be numeric
            if not isinstance(price, (int, float)):
                self.logger.warning(f"Price must be numeric, got {type(price)}")
                return False

            # Price must be positive
            if price <= 0:
                self.logger.warning(f"Price must be positive, got {price}")
                return False

            # Price must be multiple of 100 VND (Vietnamese tick size)
            if price % 100 != 0:
                self.logger.warning(f"Price must be multiple of 100 VND, got {price}")
                return False

            # Price must be within reasonable range (1,000 - 1,000,000 VND)
            MIN_PRICE_VND = 1000
            MAX_PRICE_VND = 1000000

            if price < MIN_PRICE_VND:
                self.logger.warning(f"Price {price} below minimum {MIN_PRICE_VND} VND")
                return False

            if price > MAX_PRICE_VND:
                self.logger.warning(f"Price {price} above maximum {MAX_PRICE_VND} VND")
                return False

            # 5. Validate timestamp format
            timestamp = signal['timestamp']

            # Try to parse timestamp
            try:
                if isinstance(timestamp, str):
                    # Try ISO format
                    parsed_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                elif isinstance(timestamp, datetime):
                    parsed_time = timestamp
                else:
                    self.logger.warning(f"Invalid timestamp type: {type(timestamp)}")
                    return False
            except (ValueError, AttributeError) as e:
                self.logger.warning(f"Failed to parse timestamp {timestamp}: {e}")
                return False

            # 6. Check Vietnamese market hours (9:00-11:30, 13:00-15:00 UTC+7)
            # Convert to Vietnamese timezone
            vn_time = parsed_time.astimezone(self.vn_timezone)
            hour_decimal = vn_time.hour + vn_time.minute / 60.0

            # Morning session: 9:00 - 11:30
            morning_valid = (self.trading_hours['morning_start'] <= hour_decimal <= self.trading_hours['morning_end'])

            # Afternoon session: 13:00 - 15:00
            afternoon_valid = (self.trading_hours['afternoon_start'] <= hour_decimal <= self.trading_hours['afternoon_end'])

            if not (morning_valid or afternoon_valid):
                self.logger.warning(
                    f"Signal timestamp {vn_time.strftime('%H:%M')} outside Vietnamese market hours "
                    f"(9:00-11:30, 13:00-15:00)"
                )
                return False

            # Check if it's a weekend
            if vn_time.weekday() >= 5:  # Saturday=5, Sunday=6
                self.logger.warning(f"Signal timestamp is on weekend: {vn_time.strftime('%A')}")
                return False

            # 7. Validate confidence score if present (0-100)
            if 'confidence' in signal:
                confidence = signal['confidence']

                if not isinstance(confidence, (int, float)):
                    self.logger.warning(f"Confidence must be numeric, got {type(confidence)}")
                    return False

                # Accept both 0-1 and 0-100 ranges
                if 0 <= confidence <= 1:
                    # Normalized range (0-1), this is fine
                    pass
                elif 0 <= confidence <= 100:
                    # Percentage range (0-100), this is fine
                    pass
                else:
                    self.logger.warning(f"Confidence must be 0-1 or 0-100, got {confidence}")
                    return False

            # 8. Additional optional validations
            # Validate position_size if present
            if 'position_size' in signal:
                position_size = signal['position_size']
                if position_size < 0:
                    self.logger.warning(f"Position size must be non-negative, got {position_size}")
                    return False

                # Check lot size (100 shares)
                if position_size > 0 and position_size % 100 != 0:
                    self.logger.warning(f"Position size must be multiple of 100, got {position_size}")
                    return False

            # All validations passed
            return True

        except Exception as e:
            self.logger.error(f"Error validating signal: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """Get strategy status information."""
        return {
            'name': self.name,
            'market_session': self.get_current_market_session().value,
            'market_open': self.is_market_open(),
            'positions_count': len(self.positions),
            'signals_generated': len(self.signal_history),
            'performance': self.calculate_performance(),
            'buffer_size': len(self.market_data_buffer)
        }
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if getattr(self, 'kafka_producer', None):
                self.kafka_producer.flush()
                self.kafka_producer.close()
            if getattr(self, 'kafka_consumer', None):
                self.kafka_consumer.close()

            if hasattr(self, 'logger'):
                self.logger.info(f"Strategy {self.name} cleaned up successfully")

        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"Error during cleanup: {e}")

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.cleanup()
