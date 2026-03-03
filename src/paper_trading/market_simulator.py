"""
Market Conditions Simulator
Phase 6: Task 6.4 - Paper Trading System

Simulates market conditions and provides realistic price data.
"""

import random
from decimal import Decimal
from typing import Dict, Optional
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger(__name__)


class MarketSimulator:
    """
    Simulates market conditions for paper trading

    Features:
    - Price generation based on volatility
    - Bid-ask spread simulation
    - Volume simulation
    - Market depth simulation
    """

    def __init__(self, config: dict):
        self.config = config
        self.market_data_cache: Dict[str, Dict] = {}
        self.last_prices: Dict[str, Decimal] = {}

        # Default prices for common VN30 stocks (in VND)
        self.default_prices = {
            'VIC': Decimal('45000'),   # Vingroup
            'VHM': Decimal('75000'),   # Vinhomes
            'VNM': Decimal('82000'),   # Vinamilk
            'VCB': Decimal('88000'),   # Vietcombank
            'HPG': Decimal('24000'),   # Hoa Phat
            'VPB': Decimal('19500'),   # VPBank
            'MSN': Decimal('87000'),   # Masan
            'GAS': Decimal('91000'),   # PetroVietnam Gas
            'TCB': Decimal('23500'),   # Techcombank
            'BID': Decimal('46000'),   # BIDV
            'CTG': Decimal('32000'),   # VietinBank
            'MWG': Decimal('62000'),   # Mobile World
            'SAB': Decimal('215000'),  # Sabeco
            'PLX': Decimal('52000'),   # Petrolimex
            'VJC': Decimal('125000'),  # VietJet
        }

        # Initialize with default prices
        for symbol, price in self.default_prices.items():
            self.last_prices[symbol] = price

    def get_current_price(self, symbol: str, exchange: str = "HOSE") -> Optional[Decimal]:
        """
        Get current market price for a symbol

        In a real implementation, this would fetch from market data feed.
        For paper trading, we simulate price movements.
        """
        if symbol not in self.last_prices:
            # Use default or generate random price
            if symbol in self.default_prices:
                self.last_prices[symbol] = self.default_prices[symbol]
            else:
                # Generate random price between 10,000 and 100,000 VND
                self.last_prices[symbol] = Decimal(random.randint(10000, 100000))

        # Simulate small price movement (±0.5%)
        base_price = self.last_prices[symbol]
        movement_pct = Decimal(str(random.uniform(-0.005, 0.005)))
        new_price = base_price * (Decimal('1') + movement_pct)

        # Round to nearest 100 VND (tick size)
        new_price = (new_price / Decimal('100')).quantize(Decimal('1')) * Decimal('100')

        # Update cache
        self.last_prices[symbol] = new_price

        return new_price

    def get_bid_ask_spread(self, symbol: str, exchange: str = "HOSE") -> Dict[str, Decimal]:
        """
        Get bid-ask spread

        Typical spreads:
        - HOSE VN30: 0.05-0.1%
        - HOSE non-VN30: 0.1-0.2%
        - HNX: 0.2-0.5%
        - UPCOM: 0.5-1.0%
        """
        current_price = self.get_current_price(symbol, exchange)

        # Determine spread based on exchange and liquidity
        if exchange == "HOSE":
            spread_pct = Decimal('0.001')  # 0.1%
        elif exchange == "HNX":
            spread_pct = Decimal('0.003')  # 0.3%
        else:  # UPCOM
            spread_pct = Decimal('0.007')  # 0.7%

        spread = current_price * spread_pct

        # Round to nearest 100 VND
        spread = (spread / Decimal('100')).quantize(Decimal('1')) * Decimal('100')
        spread = max(spread, Decimal('100'))  # Minimum 100 VND spread

        bid = current_price - spread / Decimal('2')
        ask = current_price + spread / Decimal('2')

        return {
            'bid': bid,
            'ask': ask,
            'spread': spread,
            'mid': current_price
        }

    def get_market_depth(self, symbol: str, levels: int = 5) -> Dict:
        """
        Simulate market depth (order book)

        Returns:
            Dictionary with bid/ask levels
        """
        current_price = self.get_current_price(symbol)
        spread_data = self.get_bid_ask_spread(symbol)

        bid_levels = []
        ask_levels = []

        # Generate bid levels (below current price)
        for i in range(levels):
            price = spread_data['bid'] - Decimal(i * 100)
            quantity = random.randint(1000, 50000)  # 10 to 500 lots
            bid_levels.append({'price': float(price), 'quantity': quantity})

        # Generate ask levels (above current price)
        for i in range(levels):
            price = spread_data['ask'] + Decimal(i * 100)
            quantity = random.randint(1000, 50000)
            ask_levels.append({'price': float(price), 'quantity': quantity})

        return {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'bids': bid_levels,
            'asks': ask_levels
        }

    def simulate_price_movement(self, symbol: str, volatility: float = 0.02) -> Decimal:
        """
        Simulate realistic price movement using random walk

        Args:
            symbol: Stock symbol
            volatility: Daily volatility (default 2%)

        Returns:
            New simulated price
        """
        current_price = self.get_current_price(symbol)

        # Use geometric Brownian motion for price simulation
        # ΔS = μ * S * Δt + σ * S * √Δt * Z
        # where Z ~ N(0,1)

        dt = 1 / (6.5 * 60)  # 1 minute in trading day (6.5 hours)
        drift = 0  # No drift for paper trading
        volatility_decimal = Decimal(str(volatility))

        random_shock = Decimal(str(random.gauss(0, 1)))
        price_change = current_price * volatility_decimal * Decimal(str(dt ** 0.5)) * random_shock

        new_price = current_price + price_change

        # Ensure price doesn't go negative or too extreme
        min_price = current_price * Decimal('0.90')  # -10% max
        max_price = current_price * Decimal('1.10')  # +10% max
        new_price = max(min_price, min(max_price, new_price))

        # Round to tick size
        new_price = (new_price / Decimal('100')).quantize(Decimal('1')) * Decimal('100')

        self.last_prices[symbol] = new_price

        return new_price

    def get_market_status(self) -> Dict:
        """Get simulated market status"""
        now = datetime.now()

        return {
            'timestamp': now.isoformat(),
            'market_open': self._is_market_open(now),
            'session': self._get_trading_session(now),
            'symbols_tracked': len(self.last_prices),
            'last_update': now.isoformat()
        }

    def _is_market_open(self, dt: datetime) -> bool:
        """Check if market is open at given time"""
        from src.utils.market_config import is_market_open
        return is_market_open(dt)

    def _get_trading_session(self, dt: datetime) -> str:
        """Get current trading session"""
        from src.utils.market_config import get_current_market_session
        return get_current_market_session(dt).value.upper()

    def update_from_real_data(self, symbol: str, price: Decimal):
        """
        Update price from real market data feed

        This method allows integration with real market data when available.
        """
        self.last_prices[symbol] = price
        logger.debug("Price updated from real data", symbol=symbol, price=float(price))

    def reset_prices(self):
        """Reset all prices to defaults"""
        self.last_prices = {symbol: price for symbol, price in self.default_prices.items()}
        logger.info("Market prices reset to defaults")
