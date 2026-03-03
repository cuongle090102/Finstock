"""
Paper Trading Broker
Phase 6: Task 6.4 - Paper Trading System

Main broker interface for paper trading with Vietnamese market rules.
"""

import uuid
from typing import Dict, List, Optional
from datetime import datetime, time
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import structlog

from .order_simulator import OrderSimulator
from .market_simulator import MarketSimulator
from .fill_calculator import FillCalculator
from .performance_tracker import PerformanceTracker

logger = structlog.get_logger(__name__)


class OrderSide(Enum):
    """Order side"""
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    """Order status"""
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class Position:
    """Trading position"""
    symbol: str
    quantity: int
    avg_entry_price: Decimal
    current_price: Decimal
    market_value: Decimal = field(init=False)
    unrealized_pnl: Decimal = field(init=False)
    sector: str = ""
    exchange: str = "HOSE"
    is_vn30: bool = False
    entry_time: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        self.market_value = Decimal(self.quantity) * self.current_price
        self.unrealized_pnl = (self.current_price - self.avg_entry_price) * Decimal(self.quantity)

    def update_price(self, new_price: Decimal):
        """Update current price and recalculate metrics"""
        self.current_price = new_price
        self.market_value = Decimal(self.quantity) * self.current_price
        self.unrealized_pnl = (self.current_price - self.avg_entry_price) * Decimal(self.quantity)


@dataclass
class Order:
    """Trading order"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: int
    price: Decimal
    status: OrderStatus
    timestamp: datetime
    filled_quantity: int = 0
    filled_price: Optional[Decimal] = None
    commission: Decimal = Decimal('0')
    tax: Decimal = Decimal('0')
    total_cost: Decimal = Decimal('0')
    realized_pnl: Decimal = Decimal('0')
    exchange: str = "HOSE"
    is_vn30: bool = False
    strategy: str = ""


class PaperBroker:
    """
    Paper Trading Broker for Vietnamese Market

    Simulates realistic order execution with:
    - Vietnamese market hours (9:00-11:30, 13:00-15:00 UTC+7)
    - Commission rates (0.10% VN30, 0.15% standard)
    - Securities tax (0.10% on sells)
    - Price limits (±7%, ±10%, ±15%)
    - Lot size rules (100 shares per lot)
    """

    def __init__(self, config: Dict):
        self.config = config
        self.initial_capital = Decimal(str(config.get('initial_capital', 1_000_000_000)))
        self.cash = self.initial_capital
        self.positions: Dict[str, Position] = {}
        self.orders: List[Order] = []
        self.trade_history: List[Dict] = []

        # Initialize components
        self.order_simulator = OrderSimulator(config)
        self.market_simulator = MarketSimulator(config)
        self.fill_calculator = FillCalculator(config)
        self.performance_tracker = PerformanceTracker(config)

        # Limits
        self.max_order_size = Decimal(str(config.get('limits', {}).get('max_order_size', 500_000_000)))
        self.max_daily_orders = config.get('limits', {}).get('max_daily_orders', 1000)
        self.max_position_per_symbol = Decimal(str(config.get('limits', {}).get('max_position_per_symbol', 0.20)))

        # Daily counters
        self.daily_order_count = 0
        self.current_date = datetime.now().date()

    def is_market_open(self) -> bool:
        """Check if Vietnamese market is open"""
        from src.utils.market_config import is_market_open
        return is_market_open()

    def calculate_commission(self, value: Decimal, is_vn30: bool = False) -> Decimal:
        """
        Calculate commission for Vietnamese market
        VN30: 0.10%, Standard: 0.15%, Min: 1,000 VND
        """
        commission_rate = Decimal('0.0010') if is_vn30 else Decimal('0.0015')
        commission = value * commission_rate
        min_commission = Decimal('1000')
        return max(commission, min_commission)

    def calculate_tax(self, value: Decimal, side: OrderSide) -> Decimal:
        """
        Calculate securities transaction tax
        0.10% on sells only
        """
        if side == OrderSide.SELL:
            return value * Decimal('0.001')
        return Decimal('0')

    def validate_order(self, symbol: str, side: OrderSide, quantity: int, price: Decimal,
                      is_vn30: bool = False) -> tuple[bool, str]:
        """Validate order against limits and rules"""

        # Validate basic input types and ranges
        if not isinstance(symbol, str) or not symbol.isalpha() or not (2 <= len(symbol) <= 4):
            return False, f"Invalid symbol format: {symbol!r} (must be 2-4 letters)"

        if not isinstance(quantity, int) or quantity <= 0:
            return False, f"Quantity must be a positive integer, got {quantity!r}"

        if not isinstance(price, Decimal) or price <= 0:
            return False, f"Price must be a positive Decimal, got {price!r}"

        if not isinstance(side, OrderSide):
            return False, f"Side must be OrderSide enum, got {type(side).__name__}"

        # Check market hours
        if not self.is_market_open():
            return False, "Market is closed"

        # Check daily order limit
        if self.daily_order_count >= self.max_daily_orders:
            return False, f"Daily order limit reached ({self.max_daily_orders})"

        # Check lot size (100 shares per lot)
        if quantity % 100 != 0:
            return False, "Order quantity must be in multiples of 100 shares"

        # Check order value limit
        order_value = Decimal(quantity) * price
        if order_value > self.max_order_size:
            return False, f"Order size exceeds limit ({self.max_order_size:,.0f} VND)"

        # Check buying power for BUY orders
        if side == OrderSide.BUY:
            commission = self.calculate_commission(order_value, is_vn30)
            total_cost = order_value + commission
            if total_cost > self.cash:
                return False, f"Insufficient cash (need: {total_cost:,.0f}, have: {self.cash:,.0f})"

        # Check position for SELL orders
        if side == OrderSide.SELL:
            if symbol not in self.positions:
                return False, f"No position in {symbol}"
            if self.positions[symbol].quantity < quantity:
                return False, f"Insufficient shares (have: {self.positions[symbol].quantity}, need: {quantity})"

        # Check position concentration for BUY orders
        if side == OrderSide.BUY:
            portfolio_value = self.get_portfolio_value()
            if symbol in self.positions:
                current_position_value = self.positions[symbol].market_value
            else:
                current_position_value = Decimal('0')

            new_position_value = current_position_value + order_value
            position_pct = new_position_value / portfolio_value if portfolio_value > 0 else Decimal('0')

            if position_pct > self.max_position_per_symbol:
                return False, f"Position would exceed {self.max_position_per_symbol*100}% of portfolio"

        return True, "OK"

    def place_order(self, symbol: str, side: OrderSide, quantity: int, price: Decimal,
                   exchange: str = "HOSE", is_vn30: bool = False, strategy: str = "") -> Optional[Order]:
        """Place a paper trading order"""

        # Validate order
        is_valid, message = self.validate_order(symbol, side, quantity, price, is_vn30)
        if not is_valid:
            logger.warning("Order rejected", symbol=symbol, reason=message)
            return None

        # Create order
        order = Order(
            order_id=str(uuid.uuid4()),
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            status=OrderStatus.PENDING,
            timestamp=datetime.now(),
            exchange=exchange,
            is_vn30=is_vn30,
            strategy=strategy
        )

        # Simulate order execution
        filled_order = self.order_simulator.simulate_execution(order, self.market_simulator)

        if filled_order.status == OrderStatus.FILLED:
            self._process_fill(filled_order)
            self.daily_order_count += 1

        self.orders.append(filled_order)

        logger.info(
            "Order placed",
            order_id=order.order_id,
            symbol=symbol,
            side=side.value,
            quantity=quantity,
            price=float(price),
            status=filled_order.status.value
        )

        return filled_order

    def _process_fill(self, order: Order):
        """Process filled order"""
        order_value = order.filled_price * Decimal(order.filled_quantity)

        # Calculate costs
        order.commission = self.calculate_commission(order_value, order.is_vn30)
        order.tax = self.calculate_tax(order_value, order.side)
        order.total_cost = order_value + order.commission + order.tax

        if order.side == OrderSide.BUY:
            # Update cash
            self.cash -= order.total_cost

            # Update position
            if order.symbol in self.positions:
                pos = self.positions[order.symbol]
                total_quantity = pos.quantity + order.filled_quantity
                total_cost = (pos.avg_entry_price * Decimal(pos.quantity) +
                            order.filled_price * Decimal(order.filled_quantity))
                pos.quantity = total_quantity
                pos.avg_entry_price = total_cost / Decimal(total_quantity)
                pos.update_price(order.filled_price)
            else:
                self.positions[order.symbol] = Position(
                    symbol=order.symbol,
                    quantity=order.filled_quantity,
                    avg_entry_price=order.filled_price,
                    current_price=order.filled_price,
                    exchange=order.exchange,
                    is_vn30=order.is_vn30
                )

        elif order.side == OrderSide.SELL:
            # Calculate realized P&L before updating position
            if order.symbol in self.positions:
                pos = self.positions[order.symbol]
                order.realized_pnl = (
                    (order.filled_price - pos.avg_entry_price) * Decimal(order.filled_quantity)
                    - order.commission - order.tax
                )

            # Update cash
            self.cash += (order_value - order.commission - order.tax)

            # Update position
            if order.symbol in self.positions:
                pos = self.positions[order.symbol]
                pos.quantity -= order.filled_quantity

                # Remove position if fully sold
                if pos.quantity == 0:
                    del self.positions[order.symbol]
                else:
                    pos.update_price(order.filled_price)

        # Track trade
        self.trade_history.append({
            'trade_id': order.order_id,
            'timestamp': order.timestamp,
            'symbol': order.symbol,
            'side': order.side.value,
            'quantity': order.filled_quantity,
            'price': float(order.filled_price),
            'commission': float(order.commission),
            'tax': float(order.tax),
            'total_cost': float(order.total_cost),
            'strategy': order.strategy
        })

        # Update performance tracker
        self.performance_tracker.record_trade(order)

    def get_portfolio_value(self) -> Decimal:
        """Get total portfolio value (cash + positions)"""
        positions_value = sum(pos.market_value for pos in self.positions.values())
        return self.cash + positions_value

    def get_portfolio_summary(self) -> Dict:
        """Get comprehensive portfolio summary"""
        total_value = self.get_portfolio_value()
        positions_value = sum(pos.market_value for pos in self.positions.values())
        total_pnl = sum(pos.unrealized_pnl for pos in self.positions.values())

        return {
            'timestamp': datetime.now().isoformat(),
            'cash': float(self.cash),
            'positions_value': float(positions_value),
            'total_value': float(total_value),
            'initial_capital': float(self.initial_capital),
            'total_pnl': float(total_pnl),
            'total_return_pct': float((total_value - self.initial_capital) / self.initial_capital * 100),
            'num_positions': len(self.positions),
            'num_trades': len(self.trade_history),
            'daily_order_count': self.daily_order_count,
            'positions': [
                {
                    'symbol': pos.symbol,
                    'quantity': pos.quantity,
                    'avg_entry_price': float(pos.avg_entry_price),
                    'current_price': float(pos.current_price),
                    'market_value': float(pos.market_value),
                    'unrealized_pnl': float(pos.unrealized_pnl),
                    'unrealized_pnl_pct': float(pos.unrealized_pnl / (pos.avg_entry_price * Decimal(pos.quantity)) * 100),
                    'exchange': pos.exchange,
                    'is_vn30': pos.is_vn30
                }
                for pos in self.positions.values()
            ]
        }

    def reset_daily_counters(self):
        """Reset daily counters (call at start of new trading day)"""
        today = datetime.now().date()
        if today != self.current_date:
            self.daily_order_count = 0
            self.current_date = today
            logger.info("Daily counters reset", date=str(today))
