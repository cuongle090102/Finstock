"""
Full System Integration Tests
Phase 6: Task 6.7 - Testing & Validation

End-to-end tests for the trading system.
Tests requiring external services (DB, Redis, Kafka) are marked with pytest.mark.integration.
"""

import pytest
import sys
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# Mock market open so tests run regardless of time of day
@patch('src.utils.market_config.is_market_open', return_value=True)
class TestPaperTradingIntegration:
    """Test paper trading order execution (no external deps needed)"""

    def test_paper_trading_buy_order(self, mock_market):
        """Test paper trading BUY order execution"""
        from src.paper_trading.paper_broker import PaperBroker, OrderSide, OrderStatus

        config = {
            'initial_capital': 1_000_000_000,
            'commission_rate': 0.0015,
            'tax_rate': 0.001,
            'slippage_rate': 0.001,
            'fill_probability': 1.0,
        }

        broker = PaperBroker(config)

        order = broker.place_order(
            symbol="VIC",
            side=OrderSide.BUY,
            quantity=1000,
            price=Decimal("45000"),
            exchange="HOSE",
            is_vn30=True,
            strategy="test"
        )

        assert order is not None, "Order placement failed"
        assert order.symbol == "VIC"
        assert order.quantity == 1000
        assert order.side == OrderSide.BUY

    def test_paper_trading_sell_order(self, mock_market):
        """Test paper trading SELL after BUY"""
        from src.paper_trading.paper_broker import PaperBroker, OrderSide

        config = {
            'initial_capital': 1_000_000_000,
            'commission_rate': 0.0015,
            'tax_rate': 0.001,
            'slippage_rate': 0.001,
            'fill_probability': 1.0,
        }

        broker = PaperBroker(config)

        # Buy first
        buy_order = broker.place_order(
            symbol="VNM",
            side=OrderSide.BUY,
            quantity=500,
            price=Decimal("82000"),
            exchange="HOSE",
            is_vn30=True,
            strategy="test_integration"
        )
        assert buy_order is not None

        # Then sell
        sell_order = broker.place_order(
            symbol="VNM",
            side=OrderSide.SELL,
            quantity=500,
            price=Decimal("83000"),
            exchange="HOSE",
            strategy="test_integration"
        )
        assert sell_order is not None

    def test_end_to_end_trade_flow(self, mock_market):
        """Test complete trade flow with portfolio checks"""
        from src.paper_trading.paper_broker import PaperBroker, OrderSide

        config = {
            'initial_capital': 1_000_000_000,
            'commission_rate': 0.0015,
            'tax_rate': 0.001,
            'slippage_rate': 0.001,
            'fill_probability': 1.0,
        }

        broker = PaperBroker(config)

        # Get initial portfolio
        initial_summary = broker.get_portfolio_summary()
        initial_cash = initial_summary["cash"]

        # Place BUY order
        buy_order = broker.place_order(
            symbol="VNM",
            side=OrderSide.BUY,
            quantity=500,
            price=Decimal("82000"),
            exchange="HOSE",
            is_vn30=True,
            strategy="test_integration"
        )

        assert buy_order is not None

        # Check portfolio updated
        summary = broker.get_portfolio_summary()
        assert summary["cash"] < initial_cash  # Cash reduced
        assert summary["num_positions"] == 1
        assert len(summary["positions"]) == 1

        # Place SELL order
        sell_order = broker.place_order(
            symbol="VNM",
            side=OrderSide.SELL,
            quantity=500,
            price=Decimal("83000"),
            exchange="HOSE",
            strategy="test_integration"
        )

        assert sell_order is not None

        # Check position closed
        final_summary = broker.get_portfolio_summary()
        assert final_summary["num_positions"] == 0
        assert final_summary["num_trades"] == 2


class TestPerformanceIntegration:
    """Test performance of integrated system"""

    def test_order_latency(self):
        """Test order placement latency"""
        import time
        from src.paper_trading.paper_broker import PaperBroker, OrderSide

        config = {
            'initial_capital': 1_000_000_000,
            'commission_rate': 0.0015,
            'tax_rate': 0.001,
            'slippage_rate': 0.001,
            'fill_probability': 1.0,
        }

        broker = PaperBroker(config)

        start = time.perf_counter_ns()

        order = broker.place_order(
            symbol="HPG",
            side=OrderSide.BUY,
            quantity=1000,
            price=Decimal("24000"),
            exchange="HOSE",
            is_vn30=True
        )

        elapsed_ns = time.perf_counter_ns() - start
        elapsed_ms = elapsed_ns / 1_000_000

        assert elapsed_ms < 100, f"Order latency {elapsed_ms:.2f}ms exceeds 100ms target"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
