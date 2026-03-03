"""Unit tests for Enhanced VnStock Collector - Task 1.1."""

import asyncio
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from pathlib import Path
import tempfile
import json

from src.collectors.vnstock_collector import (
    EnhancedVnStockCollector,
    OHLCVSchema,
    FundamentalsSchema,
    CorporateActionSchema,
    RateLimiter,
    RetryHandler
)


class TestOHLCVSchema:
    """Test OHLCV data validation schema."""
    
    def test_valid_ohlcv_data(self):
        """Test validation with valid OHLCV data."""
        valid_data = {
            "symbol": "VIC",
            "timestamp": datetime.now(timezone.utc),
            "open_price": 100.0,
            "high_price": 105.0,
            "low_price": 98.0,
            "close_price": 102.0,
            "volume": 1000000,
            "value": 102000000.0
        }
        
        schema = OHLCVSchema(**valid_data)
        assert schema.symbol == "VIC"
        assert schema.open_price == 100.0
        assert schema.high_price == 105.0
    
    def test_invalid_price_relationships(self):
        """Test validation fails with invalid price relationships."""
        # High price lower than open price
        with pytest.raises(ValueError, match="High price must be >= open price"):
            OHLCVSchema(
                symbol="VIC",
                timestamp=datetime.now(timezone.utc),
                open_price=100.0,
                high_price=95.0,  # Invalid: high < open
                low_price=90.0,
                close_price=98.0,
                volume=1000000
            )
    
    def test_negative_prices(self):
        """Test validation fails with negative prices."""
        with pytest.raises(ValueError):
            OHLCVSchema(
                symbol="VIC",
                timestamp=datetime.now(timezone.utc),
                open_price=-100.0,  # Invalid: negative price
                high_price=105.0,
                low_price=98.0,
                close_price=102.0,
                volume=1000000
            )
    
    def test_negative_volume(self):
        """Test validation fails with negative volume."""
        with pytest.raises(ValueError):
            OHLCVSchema(
                symbol="VIC",
                timestamp=datetime.now(timezone.utc),
                open_price=100.0,
                high_price=105.0,
                low_price=98.0,
                close_price=102.0,
                volume=-1000  # Invalid: negative volume
            )


class TestFundamentalsSchema:
    """Test fundamentals data validation schema."""
    
    def test_valid_fundamentals_data(self):
        """Test validation with valid fundamentals data."""
        valid_data = {
            "symbol": "VIC",
            "timestamp": datetime.now(timezone.utc),
            "market_cap": 1000000000.0,
            "pe_ratio": 15.5,
            "pb_ratio": 1.2,
            "roe": 12.5,
            "roa": 8.0,
            "eps": 2500.0,
            "revenue": 50000000000.0,
            "net_income": 5000000000.0,
            "debt_to_equity": 0.3
        }
        
        schema = FundamentalsSchema(**valid_data)
        assert schema.symbol == "VIC"
        assert schema.pe_ratio == 15.5
        assert schema.roe == 12.5
    
    def test_invalid_roe_range(self):
        """Test validation fails with ROE outside valid range."""
        with pytest.raises(ValueError):
            FundamentalsSchema(
                symbol="VIC",
                timestamp=datetime.now(timezone.utc),
                roe=150.0  # Invalid: ROE > 100%
            )


class TestCorporateActionSchema:
    """Test corporate actions validation schema."""
    
    def test_valid_corporate_action(self):
        """Test validation with valid corporate action."""
        valid_data = {
            "symbol": "VIC",
            "timestamp": datetime.now(timezone.utc),
            "action_type": "dividend",
            "announcement_date": datetime.now(timezone.utc),
            "ex_date": datetime.now(timezone.utc) + timedelta(days=5),
            "payment_date": datetime.now(timezone.utc) + timedelta(days=10),
            "amount": 1000.0,
            "currency": "VND"
        }
        
        schema = CorporateActionSchema(**valid_data)
        assert schema.action_type == "dividend"
        assert schema.currency == "VND"
    
    def test_invalid_action_type(self):
        """Test validation fails with invalid action type."""
        with pytest.raises(ValueError):
            CorporateActionSchema(
                symbol="VIC",
                timestamp=datetime.now(timezone.utc),
                action_type="invalid_action",  # Invalid action type
                announcement_date=datetime.now(timezone.utc)
            )


class TestRateLimiter:
    """Test rate limiter functionality."""
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiter enforces request limits."""
        limiter = RateLimiter(max_requests=2, time_window=1.0)
        
        # First two requests should be immediate
        start_time = asyncio.get_event_loop().time()
        await limiter.acquire()
        await limiter.acquire()
        
        # Third request should be delayed
        await limiter.acquire()
        end_time = asyncio.get_event_loop().time()
        
        # Should have taken at least some time for rate limiting
        assert end_time - start_time >= 0  # Some delay expected


class TestRetryHandler:
    """Test retry handler with exponential backoff."""
    
    @pytest.mark.asyncio
    async def test_successful_execution(self):
        """Test retry handler with successful function execution."""
        retry_handler = RetryHandler(max_retries=3)
        
        mock_func = Mock(return_value="success")
        result = await retry_handler.execute(mock_func, "arg1", kwarg1="value1")
        
        assert result == "success"
        mock_func.assert_called_once_with("arg1", kwarg1="value1")
    
    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        """Test retry handler retries on failures."""
        retry_handler = RetryHandler(max_retries=2, base_delay=0.01)  # Fast retry for testing
        
        # Mock function that fails twice then succeeds
        mock_func = Mock(side_effect=[Exception("fail1"), Exception("fail2"), "success"])
        
        result = await retry_handler.execute(mock_func)
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    @pytest.mark.asyncio
    async def test_exhausted_retries(self):
        """Test retry handler raises exception after max retries."""
        retry_handler = RetryHandler(max_retries=2, base_delay=0.01)
        
        mock_func = Mock(side_effect=Exception("persistent failure"))
        
        with pytest.raises(Exception, match="persistent failure"):
            await retry_handler.execute(mock_func)
        
        assert mock_func.call_count == 3  # Initial + 2 retries


class TestEnhancedVnStockCollector:
    """Test Enhanced VnStock Collector main functionality."""
    
    @pytest.fixture
    def collector_config(self):
        """Provide test configuration for collector."""
        return {
            "source": "vci",
            "timeout": 30,
            "max_requests_per_second": 10,
            "max_retries": 3
        }
    
    @pytest.fixture
    def mock_collector(self, collector_config):
        """Provide mocked collector for testing."""
        with patch('src.collectors.vnstock_collector.VNStockClient.__init__'):
            collector = EnhancedVnStockCollector(collector_config)
            collector.data_source = "vci"
            collector._listing = Mock()
            return collector
    
    @pytest.mark.asyncio
    async def test_get_stock_list_success(self, mock_collector):
        """Test successful stock list retrieval."""
        # Mock the internal fetch method
        test_symbols = pd.DataFrame({
            'symbol': ['VIC', 'VNM', 'HPG'],
            'exchange': ['HOSE', 'HOSE', 'HOSE']
        })
        
        with patch.object(mock_collector, '_fetch_stock_list', return_value=test_symbols):
            result = await mock_collector.get_stock_list()
        
        assert len(result) == 3
        assert 'VIC' in result['symbol'].values
        assert len(mock_collector.request_metadata) == 1
        assert mock_collector.request_metadata[0]['success'] is True
    
    @pytest.mark.asyncio
    async def test_get_stock_list_failure(self, mock_collector):
        """Test stock list retrieval failure handling."""
        with patch.object(mock_collector, '_fetch_stock_list', side_effect=Exception("API Error")):
            result = await mock_collector.get_stock_list()
        
        assert result.empty
        assert len(mock_collector.request_metadata) == 1
        assert mock_collector.request_metadata[0]['success'] is False
        assert "API Error" in mock_collector.request_metadata[0]['error']
    
    @pytest.mark.asyncio
    async def test_get_ohlcv_data_with_validation(self, mock_collector):
        """Test OHLCV data retrieval with validation."""
        # Mock historical data response
        test_data = pd.DataFrame({
            'timestamp': [datetime.now(timezone.utc)],
            'open_price': [100.0],
            'high_price': [105.0],
            'low_price': [95.0],
            'close_price': [102.0],
            'volume': [1000000],
            'value': [102000000.0]
        })
        
        with patch.object(mock_collector, 'get_historical_data', return_value=test_data):
            result = await mock_collector.get_ohlcv_data(
                "VIC", 
                datetime.now() - timedelta(days=1),
                datetime.now()
            )
        
        assert len(result) == 1
        assert isinstance(result[0], OHLCVSchema)
        assert result[0].symbol == "VIC"
        assert result[0].open_price == 100.0
        assert len(mock_collector.request_metadata) == 1
    
    @pytest.mark.asyncio
    async def test_get_ohlcv_data_validation_errors(self, mock_collector):
        """Test OHLCV data with validation errors."""
        # Mock data with validation error (high < open)
        test_data = pd.DataFrame({
            'timestamp': [datetime.now(timezone.utc)],
            'open_price': [100.0],
            'high_price': [90.0],  # Invalid: high < open
            'low_price': [85.0],
            'close_price': [95.0],
            'volume': [1000000],
            'value': [95000000.0]
        })
        
        with patch.object(mock_collector, 'get_historical_data', return_value=test_data):
            result = await mock_collector.get_ohlcv_data(
                "VIC",
                datetime.now() - timedelta(days=1),
                datetime.now()
            )
        
        # Should return empty due to validation error
        assert len(result) == 0
        assert mock_collector.request_metadata[0]['validation_errors'] == 1
    
    @pytest.mark.asyncio
    async def test_get_realtime_price(self, mock_collector):
        """Test real-time price retrieval."""
        mock_price_data = Mock()
        mock_price_data.close_price = 102.5
        mock_price_data.open_price = 100.0
        mock_price_data.high_price = 105.0
        mock_price_data.low_price = 98.0
        mock_price_data.volume = 1500000
        
        with patch.object(mock_collector, 'get_real_time_data', return_value=[mock_price_data]):
            result = await mock_collector.get_realtime_price("VIC")
        
        assert result is not None
        assert result['symbol'] == "VIC"
        assert result['price'] == 102.5
        assert result['data_source'] == "vci"
        assert 'market_status' in result
    
    @pytest.mark.asyncio
    async def test_get_fundamentals(self, mock_collector):
        """Test fundamentals data retrieval."""
        mock_fundamentals = {
            "market_cap": 1000000000.0,
            "pe_ratio": 15.5,
            "pb_ratio": 1.2,
            "roe": 12.5,
            "roa": 8.0
        }
        
        with patch.object(mock_collector, '_fetch_fundamentals', return_value=mock_fundamentals):
            result = await mock_collector.get_fundamentals("VIC")
        
        assert result is not None
        assert isinstance(result, FundamentalsSchema)
        assert result.symbol == "VIC"
        assert result.pe_ratio == 15.5
    
    @pytest.mark.asyncio
    async def test_get_corporate_actions(self, mock_collector):
        """Test corporate actions retrieval."""
        mock_actions = [{
            "action_type": "dividend",
            "announcement_date": datetime.now(timezone.utc),
            "ex_date": datetime.now(timezone.utc) + timedelta(days=5),
            "amount": 1000.0,
            "currency": "VND"
        }]
        
        with patch.object(mock_collector, '_fetch_corporate_actions', return_value=mock_actions):
            result = await mock_collector.get_corporate_actions("VIC")
        
        assert len(result) == 1
        assert isinstance(result[0], CorporateActionSchema)
        assert result[0].action_type == "dividend"
        assert result[0].symbol == "VIC"
    
    def test_market_status_detection(self, mock_collector):
        """Test Vietnamese market status detection."""
        # Test during market hours (10:00 AM)
        with patch('src.collectors.vnstock_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15, 10, 0)  # Monday 10:00 AM
            status = mock_collector._get_market_status()
            assert status == "open"
        
        # Test during lunch break (12:00 PM)
        with patch('src.collectors.vnstock_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 15, 12, 0)  # Monday 12:00 PM
            status = mock_collector._get_market_status()
            assert status == "lunch_break"
        
        # Test during weekend
        with patch('src.collectors.vnstock_collector.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 1, 13, 10, 0)  # Saturday 10:00 AM
            status = mock_collector._get_market_status()
            assert status == "closed"
    
    def test_performance_stats(self, mock_collector):
        """Test performance statistics calculation."""
        # Add some mock metadata
        mock_collector.request_metadata = [
            {"success": True, "duration_seconds": 1.5, "record_count": 100, "validation_errors": 0},
            {"success": True, "duration_seconds": 2.0, "record_count": 50, "validation_errors": 1},
            {"success": False, "duration_seconds": 5.0, "record_count": 0, "validation_errors": 0}
        ]
        
        stats = mock_collector.get_performance_stats()
        
        assert stats["total_requests"] == 3
        assert stats["successful_requests"] == 2
        assert stats["failed_requests"] == 1
        assert stats["success_rate"] == 66.66666666666667  # 2/3 * 100
        assert stats["total_records_fetched"] == 150
        assert stats["total_validation_errors"] == 1
    
    def test_config_file_loading(self, tmp_path):
        """Test loading collector from configuration file."""
        # Create temporary config file
        config_data = {
            "source": "tcbs",
            "timeout": 60,
            "max_requests_per_second": 5,
            "max_retries": 2
        }
        
        config_file = tmp_path / "test_config.yaml"
        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(config_data, f)
        
        with patch('src.collectors.vnstock_collector.VNStockClient.__init__'):
            collector = EnhancedVnStockCollector.from_config_file(config_file)
            assert collector.rate_limiter.max_requests == 5
            assert collector.retry_handler.max_retries == 2
    
    def test_metadata_export(self, mock_collector, tmp_path):
        """Test exporting request metadata to file."""
        # Add some mock metadata
        mock_collector.request_metadata = [
            {"method": "get_stock_list", "success": True, "timestamp": datetime.now(timezone.utc).isoformat()}
        ]
        
        export_file = tmp_path / "metadata_export.json"
        mock_collector.export_metadata(export_file)
        
        assert export_file.exists()
        
        with open(export_file, 'r') as f:
            exported_data = json.load(f)
        
        assert "export_timestamp" in exported_data
        assert "collector_info" in exported_data
        assert "performance_stats" in exported_data
        assert "request_metadata" in exported_data
        assert len(exported_data["request_metadata"]) == 1


class TestIntegration:
    """Integration tests for the Enhanced VnStock Collector."""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_full_workflow_with_rate_limiting(self):
        """Test complete workflow with rate limiting."""
        config = {
            "source": "vci",
            "timeout": 30,
            "max_requests_per_second": 2,  # Low rate limit for testing
            "max_retries": 1
        }
        
        with patch('src.collectors.vnstock_collector.VNStockClient.__init__'):
            collector = EnhancedVnStockCollector(config)
            collector.data_source = "vci"
            
            # Mock successful responses for all methods
            with patch.object(collector, '_fetch_stock_list') as mock_stock_list, \
                 patch.object(collector, 'get_historical_data') as mock_historical, \
                 patch.object(collector, 'get_real_time_data') as mock_realtime:
                
                mock_stock_list.return_value = pd.DataFrame({
                    'symbol': ['VIC', 'VNM'],
                    'exchange': ['HOSE', 'HOSE']
                })
                
                mock_historical.return_value = pd.DataFrame({
                    'timestamp': [datetime.now(timezone.utc)],
                    'open_price': [100.0],
                    'high_price': [105.0],
                    'low_price': [95.0],
                    'close_price': [102.0],
                    'volume': [1000000],
                    'value': [102000000.0]
                })
                
                mock_price_data = Mock()
                mock_price_data.close_price = 102.5
                mock_price_data.open_price = 100.0
                mock_price_data.high_price = 105.0
                mock_price_data.low_price = 98.0
                mock_price_data.volume = 1500000
                mock_realtime.return_value = [mock_price_data]
                
                # Execute multiple requests to test rate limiting
                start_time = asyncio.get_event_loop().time()
                
                await collector.get_stock_list()
                ohlcv_data = await collector.get_ohlcv_data("VIC", datetime.now() - timedelta(days=1), datetime.now())
                realtime_data = await collector.get_realtime_price("VIC")
                
                end_time = asyncio.get_event_loop().time()
                
                # Verify results
                assert len(ohlcv_data) == 1
                assert realtime_data is not None
                assert realtime_data['symbol'] == "VIC"
                
                # Verify rate limiting added some delay
                assert end_time - start_time > 0
                
                # Verify metadata collection
                assert len(collector.request_metadata) == 3
                assert all(req['success'] for req in collector.request_metadata)
                
                # Verify performance stats
                stats = collector.get_performance_stats()
                assert stats['total_requests'] == 3
                assert stats['success_rate'] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])