"""
Fill Price Calculator
Phase 6: Task 6.4 - Paper Trading System

Calculates realistic fill prices considering Vietnamese market microstructure.
"""

from decimal import Decimal
from typing import Optional
import structlog

logger = structlog.get_logger(__name__)


class FillCalculator:
    """
    Calculate fill prices for paper trading orders

    Considers:
    - Market microstructure
    - Bid-ask spread
    - Order book depth
    - Vietnamese market rules
    - Price improvement opportunities
    """

    def __init__(self, config: dict):
        self.config = config

    def calculate_fill_price(
        self,
        order_price: Decimal,
        order_side: str,
        symbol: str,
        quantity: int,
        market_simulator
    ) -> Decimal:
        """
        Calculate realistic fill price

        Args:
            order_price: Limit price specified in order
            order_side: "BUY" or "SELL"
            symbol: Stock symbol
            quantity: Order quantity
            market_simulator: MarketSimulator instance

        Returns:
            Calculated fill price
        """
        # Get market data
        spread_data = market_simulator.get_bid_ask_spread(symbol)
        market_depth = market_simulator.get_market_depth(symbol)

        if order_side == "BUY":
            fill_price = self._calculate_buy_fill(
                order_price,
                quantity,
                spread_data,
                market_depth
            )
        else:
            fill_price = self._calculate_sell_fill(
                order_price,
                quantity,
                spread_data,
                market_depth
            )

        # Ensure price is valid
        fill_price = self._validate_and_round_price(fill_price)

        return fill_price

    def _calculate_buy_fill(
        self,
        limit_price: Decimal,
        quantity: int,
        spread_data: dict,
        market_depth: dict
    ) -> Decimal:
        """
        Calculate fill price for buy order

        For market orders or aggressive limit orders, fill at ask.
        For passive limit orders, may get price improvement.
        """
        ask_price = spread_data['ask']
        mid_price = spread_data['mid']

        # If limit price is at or above ask, fill at ask (aggressive)
        if limit_price >= ask_price:
            return ask_price

        # If limit price is between mid and ask, partial improvement
        if mid_price <= limit_price < ask_price:
            # Get some price improvement
            improvement = (ask_price - limit_price) * Decimal('0.3')  # 30% improvement
            return ask_price - improvement

        # If limit price is below mid, fill at limit (if order would fill)
        # This represents joining the bid side
        if limit_price >= spread_data['bid']:
            return limit_price

        # Order too far from market, wouldn't fill immediately
        return limit_price

    def _calculate_sell_fill(
        self,
        limit_price: Decimal,
        quantity: int,
        spread_data: dict,
        market_depth: dict
    ) -> Decimal:
        """
        Calculate fill price for sell order

        For market orders or aggressive limit orders, fill at bid.
        For passive limit orders, may get price improvement.
        """
        bid_price = spread_data['bid']
        mid_price = spread_data['mid']

        # If limit price is at or below bid, fill at bid (aggressive)
        if limit_price <= bid_price:
            return bid_price

        # If limit price is between bid and mid, partial improvement
        if bid_price < limit_price <= mid_price:
            # Get some price improvement
            improvement = (limit_price - bid_price) * Decimal('0.3')  # 30% improvement
            return bid_price + improvement

        # If limit price is above mid, fill at limit (if order would fill)
        # This represents joining the ask side
        if limit_price <= spread_data['ask']:
            return limit_price

        # Order too far from market, wouldn't fill immediately
        return limit_price

    def _validate_and_round_price(self, price: Decimal) -> Decimal:
        """
        Validate and round price to Vietnamese tick size

        Vietnamese market tick size: 100 VND
        """
        # Round to nearest 100 VND
        rounded_price = (price / Decimal('100')).quantize(Decimal('1')) * Decimal('100')

        # Ensure minimum price
        min_price = Decimal('100')
        rounded_price = max(rounded_price, min_price)

        return rounded_price

    def calculate_average_fill_price(
        self,
        orders: list,
        order_side: str,
        market_depth: dict
    ) -> Decimal:
        """
        Calculate average fill price when order walks through order book

        Used for large orders that consume multiple price levels.
        """
        if order_side == "BUY":
            levels = market_depth['asks']
        else:
            levels = market_depth['bids']

        total_quantity = sum(order.quantity for order in orders)
        filled_quantity = 0
        weighted_price_sum = Decimal('0')

        for level in levels:
            level_price = Decimal(str(level['price']))
            level_quantity = level['quantity']

            # How much can we fill at this level?
            fill_at_level = min(total_quantity - filled_quantity, level_quantity)

            if fill_at_level > 0:
                weighted_price_sum += level_price * Decimal(fill_at_level)
                filled_quantity += fill_at_level

            if filled_quantity >= total_quantity:
                break

        if filled_quantity > 0:
            average_price = weighted_price_sum / Decimal(filled_quantity)
            return self._validate_and_round_price(average_price)

        # If can't fill from order book, return None
        return Decimal('0')

    def estimate_price_impact(
        self,
        symbol: str,
        quantity: int,
        order_side: str,
        market_simulator
    ) -> Decimal:
        """
        Estimate price impact of order

        Args:
            symbol: Stock symbol
            quantity: Order quantity
            order_side: "BUY" or "SELL"
            market_simulator: MarketSimulator instance

        Returns:
            Estimated price impact in basis points (bps)
        """
        current_price = market_simulator.get_current_price(symbol)
        order_value = Decimal(quantity) * current_price

        # Simple impact model: impact = k * sqrt(order_value / ADV)
        # where ADV = Average Daily Volume (assumed 1B VND for estimation)
        assumed_adv = Decimal('1000000000')  # 1B VND

        # Impact factor (k) - calibrated for Vietnamese market
        k = Decimal('0.1')

        impact_pct = k * (order_value / assumed_adv).sqrt()

        # Convert to basis points
        impact_bps = impact_pct * Decimal('10000')

        # Cap at 200 bps (2%)
        impact_bps = min(impact_bps, Decimal('200'))

        return impact_bps

    def get_execution_quality_score(
        self,
        order_price: Decimal,
        fill_price: Decimal,
        order_side: str,
        market_mid: Decimal
    ) -> float:
        """
        Calculate execution quality score (0-100)

        100 = best possible execution
        0 = worst execution

        Compares fill price to:
        - Order limit price
        - Market mid price
        - Expected slippage
        """
        if order_side == "BUY":
            # For buy: lower fill price is better
            if fill_price <= order_price:
                # Got price at or better than limit
                improvement = (order_price - fill_price) / order_price * 100
                score = 100 + float(improvement * 100)  # Can exceed 100 for exceptional fills
            else:
                # Filled above limit (shouldn't happen in limit orders)
                degradation = (fill_price - order_price) / order_price * 100
                score = 100 - float(degradation * 100)
        else:  # SELL
            # For sell: higher fill price is better
            if fill_price >= order_price:
                # Got price at or better than limit
                improvement = (fill_price - order_price) / order_price * 100
                score = 100 + float(improvement * 100)
            else:
                # Filled below limit
                degradation = (order_price - fill_price) / order_price * 100
                score = 100 - float(degradation * 100)

        # Normalize to 0-100 range
        score = max(0, min(score, 100))

        return score
