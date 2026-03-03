"""
Order Execution Simulator
Phase 6: Task 6.4 - Paper Trading System

Simulates realistic order execution with latency, slippage, and fill probability.
"""

import random
import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Optional
import structlog

logger = structlog.get_logger(__name__)


class OrderSimulator:
    """
    Simulates order execution with realistic characteristics:
    - Execution latency (50ms default)
    - Slippage (0.05% default)
    - Fill probability (95% default)
    - Market impact (based on order size)
    """

    def __init__(self, config: dict):
        self.config = config
        simulation_config = config.get('simulation', {})

        self.slippage_bps = simulation_config.get('slippage_bps', 5)  # 0.05%
        self.market_impact_factor = Decimal(str(simulation_config.get('market_impact_factor', 0.001)))  # 0.1% per 100M VND
        self.latency_ms = simulation_config.get('latency_ms', 50)
        self.fill_probability = simulation_config.get('fill_probability', 0.95)

    def simulate_execution(self, order, market_simulator):
        """
        Simulate order execution

        Args:
            order: Order object to execute
            market_simulator: MarketSimulator instance

        Returns:
            Updated order with execution details
        """
        from .paper_broker import OrderStatus, OrderSide

        # Simulate execution latency
        execution_delay = timedelta(milliseconds=self.latency_ms)
        order.timestamp = order.timestamp + execution_delay

        # Check fill probability
        if random.random() > self.fill_probability:
            order.status = OrderStatus.REJECTED
            logger.warning(
                "Order rejected by simulator",
                order_id=order.order_id,
                symbol=order.symbol,
                reason="Fill probability check failed"
            )
            return order

        # Get current market price
        market_price = market_simulator.get_current_price(order.symbol, order.exchange)
        if market_price is None:
            market_price = order.price  # Fall back to order price

        # Calculate slippage
        slippage = self.calculate_slippage(order, market_price)

        # Calculate market impact
        market_impact = self.calculate_market_impact(order, market_price)

        # Calculate fill price
        fill_price = self.calculate_fill_price(
            order.price,
            market_price,
            slippage,
            market_impact,
            order.side
        )

        # Check price limits (Vietnamese market: ±7%, ±10%, ±15% depending on exchange)
        if not self.is_within_price_limits(market_price, fill_price, order.exchange):
            order.status = OrderStatus.REJECTED
            logger.warning(
                "Order rejected - price limit violation",
                order_id=order.order_id,
                symbol=order.symbol,
                market_price=float(market_price),
                fill_price=float(fill_price)
            )
            return order

        # Fill order
        order.status = OrderStatus.FILLED
        order.filled_quantity = order.quantity
        order.filled_price = fill_price

        logger.info(
            "Order filled",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side.value,
            quantity=order.quantity,
            order_price=float(order.price),
            fill_price=float(fill_price),
            slippage_bps=float(slippage * 10000),
            market_impact_bps=float(market_impact * 10000)
        )

        return order

    def calculate_slippage(self, order, market_price: Decimal) -> Decimal:
        """Calculate slippage based on order and market conditions"""
        # Base slippage
        base_slippage = Decimal(str(self.slippage_bps)) / Decimal('10000')

        # Add randomness (±50% of base slippage)
        random_factor = Decimal(str(random.uniform(0.5, 1.5)))
        slippage = base_slippage * random_factor

        return slippage

    def calculate_market_impact(self, order, market_price: Decimal) -> Decimal:
        """
        Calculate market impact based on order size
        Larger orders have more market impact
        """
        order_value = Decimal(order.quantity) * market_price

        # Market impact increases with order size
        # Formula: impact = market_impact_factor * (order_value / 100M VND)
        impact = self.market_impact_factor * (order_value / Decimal('100000000'))

        # Cap market impact at 2%
        max_impact = Decimal('0.02')
        impact = min(impact, max_impact)

        return impact

    def calculate_fill_price(
        self,
        order_price: Decimal,
        market_price: Decimal,
        slippage: Decimal,
        market_impact: Decimal,
        side
    ) -> Decimal:
        """
        Calculate final fill price

        For BUY: fill price is higher (worse for buyer)
        For SELL: fill price is lower (worse for seller)
        """
        from .paper_broker import OrderSide

        total_impact = slippage + market_impact

        if side == OrderSide.BUY:
            # Pay more for buy orders
            fill_price = market_price * (Decimal('1') + total_impact)
        else:
            # Receive less for sell orders
            fill_price = market_price * (Decimal('1') - total_impact)

        # Round to nearest 100 VND (Vietnamese tick size)
        fill_price = (fill_price / Decimal('100')).quantize(Decimal('1')) * Decimal('100')

        return fill_price

    def is_within_price_limits(self, reference_price: Decimal, fill_price: Decimal, exchange: str) -> bool:
        """
        Check if fill price is within Vietnamese market price limits

        HOSE: ±7%
        HNX: ±10%
        UPCOM: ±15%
        """
        price_limits = {
            'HOSE': Decimal('0.07'),
            'HNX': Decimal('0.10'),
            'UPCOM': Decimal('0.15')
        }

        limit = price_limits.get(exchange, Decimal('0.10'))

        upper_limit = reference_price * (Decimal('1') + limit)
        lower_limit = reference_price * (Decimal('1') - limit)

        return lower_limit <= fill_price <= upper_limit

    async def simulate_execution_async(self, order, market_simulator):
        """Async version of simulate_execution"""
        # Simulate network/execution latency
        await asyncio.sleep(self.latency_ms / 1000)

        return self.simulate_execution(order, market_simulator)
