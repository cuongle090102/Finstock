"""Enhanced Vietnamese market data extractor with multi-source support."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.ingestion.base import DataExtractor, DataSource, MarketDataPoint
from src.ingestion.vnstock_client import VNStockClient
from src.ingestion.vnquant_client import VNQuantClient
from src.utils.logging import StructuredLogger


class VietnameseMarketExtractor(DataExtractor):
    """Enhanced data extractor specifically for Vietnamese market sources."""
    
    # Default source priority for Vietnamese market
    DEFAULT_SOURCE_PRIORITY = ["vnstock", "vnquant"]
    
    # VN30 symbols for quick validation
    VN30_SYMBOLS = [
        "VIC", "VNM", "HPG", "VCB", "FPT", "GAS", "BID", "CTG", "MSN", "PLX",
        "VRE", "TPB", "MWG", "SSI", "POW", "TCB", "VHM", "ACB", "SAB", "KDH",
        "PDR", "DGC", "VJC", "SBT", "NVL", "PNJ", "DHG", "VIB", "EIB", "HDB"
    ]
    
    def __init__(self, sources: Optional[List[DataSource]] = None):
        """Initialize Vietnamese market data extractor.
        
        Args:
            sources: List of data sources (if None, will create default sources)
        """
        if sources is None:
            sources = self._create_default_sources()
        
        super().__init__(sources)
        self.logger = StructuredLogger("VietnameseMarketExtractor")
        
    def _create_default_sources(self) -> List[DataSource]:
        """Create default Vietnamese data sources using vnstock and vnquant.
        
        Returns:
            List of configured data sources
        """
        sources = []
        
        # VNStock - Primary source (reliable and comprehensive)
        vnstock_config = {
            "source": "vci",  # VCI source for vnstock
            "timeout": 30
        }
        sources.append(VNStockClient(vnstock_config))
        
        # VNQuant - Secondary source (CAFE data provider)
        vnquant_config = {
            "data_source": "cafe",  # CAFE is more reliable than VND
            "timeout": 30
        }
        sources.append(VNQuantClient(vnquant_config))
        
        # Add TCBS version of VNStock for screening capabilities
        vnstock_tcbs_config = {
            "source": "tcbs",  # TCBS source for screening
            "timeout": 30
        }
        vnstock_tcbs = VNStockClient(vnstock_tcbs_config)
        vnstock_tcbs.name = "vnstock_tcbs"  # Rename to distinguish
        sources.append(vnstock_tcbs)
        
        return sources
    
    def connect_all_sources(self) -> Dict[str, bool]:
        """Connect to all data sources.
        
        Returns:
            Dictionary with connection status for each source
        """
        connection_results = {}
        
        for source_name, source in self.sources.items():
            try:
                self.logger.info(f"Connecting to {source_name}...")
                success = source.connect()
                connection_results[source_name] = success
                
                if success:
                    self.logger.info(f"Successfully connected to {source_name}")
                else:
                    self.logger.error(f"Failed to connect to {source_name}")
                    
            except Exception as e:
                self.logger.error(f"Error connecting to {source_name}: {e}")
                connection_results[source_name] = False
        
        connected_count = sum(connection_results.values())
        total_count = len(connection_results)
        
        self.logger.info(f"Connected to {connected_count}/{total_count} data sources")
        return connection_results
    
    def get_vn30_historical_data(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> Dict[str, pd.DataFrame]:
        """Fetch historical data for all VN30 stocks.
        
        Args:
            start_date: Start date
            end_date: End date
            interval: Data interval
            
        Returns:
            Dictionary mapping symbols to DataFrames
        """
        self.logger.info(f"Fetching VN30 historical data from {start_date} to {end_date}")
        
        results = {}
        
        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit tasks for all VN30 symbols
            future_to_symbol = {
                executor.submit(
                    self.get_historical_data,
                    symbol,
                    start_date,
                    end_date,
                    interval,
                    self.DEFAULT_SOURCE_PRIORITY
                ): symbol
                for symbol in self.VN30_SYMBOLS
            }
            
            # Collect results
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    data = future.result()
                    if not data.empty:
                        results[symbol] = data
                        self.logger.info(f"Successfully fetched data for {symbol}: {len(data)} records")
                    else:
                        self.logger.warning(f"No data available for {symbol}")
                except Exception as e:
                    self.logger.error(f"Error fetching data for {symbol}: {e}")
        
        self.logger.info(f"Completed VN30 data fetch: {len(results)}/{len(self.VN30_SYMBOLS)} symbols")
        return results
    
    def get_multi_source_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        reconcile: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """Fetch data from multiple sources for comparison and reconciliation.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            reconcile: Whether to perform data reconciliation
            
        Returns:
            Dictionary mapping source names to DataFrames
        """
        self.logger.info(f"Fetching multi-source data for {symbol}")
        
        results = {}
        
        # Fetch from all available sources
        for source_name, source in self.sources.items():
            try:
                data = source.get_historical_data(symbol, start_date, end_date)
                if not data.empty:
                    results[source_name] = data
                    self.logger.info(f"{source_name}: {len(data)} records")
                else:
                    self.logger.warning(f"{source_name}: No data available")
            except Exception as e:
                self.logger.error(f"Error fetching from {source_name}: {e}")
        
        if reconcile and len(results) > 1:
            reconciled_data = self._reconcile_data(results)
            results["reconciled"] = reconciled_data
        
        return results
    
    def _reconcile_data(self, source_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Reconcile data from multiple sources.
        
        Args:
            source_data: Dictionary mapping source names to DataFrames
            
        Returns:
            Reconciled DataFrame
        """
        self.logger.info("Reconciling data from multiple sources")
        
        if not source_data:
            return pd.DataFrame()
        
        # Use the source with most data as base
        base_source = max(source_data.keys(), key=lambda k: len(source_data[k]))
        base_data = source_data[base_source].copy()
        
        self.logger.info(f"Using {base_source} as base source with {len(base_data)} records")
        
        # Add data quality scores
        base_data["data_sources"] = 1
        base_data["source_consensus"] = 1.0
        
        # Compare with other sources
        for source_name, data in source_data.items():
            if source_name == base_source:
                continue
            
            # Merge on timestamp
            merged = pd.merge(
                base_data[["timestamp", "close_price", "volume"]],
                data[["timestamp", "close_price", "volume"]],
                on="timestamp",
                suffixes=("_base", f"_{source_name}"),
                how="inner"
            )
            
            if not merged.empty:
                # Calculate price differences
                price_diff = abs(merged["close_price_base"] - merged[f"close_price_{source_name}"]) / merged["close_price_base"]
                
                # Update consensus scores for matching records
                matching_timestamps = merged["timestamp"]
                base_data.loc[base_data["timestamp"].isin(matching_timestamps), "data_sources"] += 1
                
                # Lower consensus score for high price differences
                high_diff_timestamps = merged[price_diff > 0.02]["timestamp"]  # >2% difference
                base_data.loc[base_data["timestamp"].isin(high_diff_timestamps), "source_consensus"] *= 0.8
        
        # Add reconciliation metadata
        base_data["reconciliation_timestamp"] = datetime.now()
        base_data["total_sources_available"] = len(source_data)
        
        self.logger.info(f"Reconciliation complete: {len(base_data)} records")
        return base_data
    
    def validate_data_quality(self, data: pd.DataFrame, symbol: str) -> Dict[str, Any]:
        """Validate data quality for Vietnamese market data.
        
        Args:
            data: DataFrame to validate
            symbol: Stock symbol
            
        Returns:
            Data quality report
        """
        if data.empty:
            return {
                "symbol": symbol,
                "status": "empty",
                "issues": ["No data available"],
                "quality_score": 0.0
            }
        
        issues = []
        quality_score = 1.0
        
        # Check for missing values
        missing_data = data.isnull().sum()
        if missing_data.any():
            issues.append(f"Missing values: {missing_data.to_dict()}")
            quality_score *= 0.9
        
        # Check for price consistency (High >= Open,Close >= Low)
        price_inconsistencies = (
            (data["high_price"] < data["open_price"]) |
            (data["high_price"] < data["close_price"]) |
            (data["low_price"] > data["open_price"]) |
            (data["low_price"] > data["close_price"])
        ).sum()
        
        if price_inconsistencies > 0:
            issues.append(f"Price inconsistencies: {price_inconsistencies} records")
            quality_score *= 0.8
        
        # Check for zero volumes (suspicious)
        zero_volumes = (data["volume"] == 0).sum()
        if zero_volumes > len(data) * 0.1:  # More than 10% zero volumes
            issues.append(f"High zero volume count: {zero_volumes} records")
            quality_score *= 0.9
        
        # Check for gaps in data (Vietnamese market trades 4 hours/day, 5 days/week)
        if len(data) > 1:
            data_sorted = data.sort_values("timestamp")
            date_gaps = pd.date_range(
                start=data_sorted["timestamp"].min(),
                end=data_sorted["timestamp"].max(),
                freq="D"
            )
            
            # Filter for business days (rough approximation)
            business_days = pd.bdate_range(
                start=data_sorted["timestamp"].min(),
                end=data_sorted["timestamp"].max()
            )
            
            expected_records = len(business_days)
            actual_records = len(data_sorted)
            
            if actual_records < expected_records * 0.9:  # Missing more than 10%
                missing_days = expected_records - actual_records
                issues.append(f"Data gaps: {missing_days} missing business days")
                quality_score *= 0.8
        
        # Check for outliers (price movements > 10% in a day)
        if len(data) > 1:
            data_sorted = data.sort_values("timestamp")
            price_changes = data_sorted["close_price"].pct_change().abs()
            extreme_moves = (price_changes > 0.1).sum()
            
            if extreme_moves > len(data) * 0.05:  # More than 5% extreme moves
                issues.append(f"High volatility: {extreme_moves} days with >10% moves")
                quality_score *= 0.95
        
        return {
            "symbol": symbol,
            "status": "valid" if quality_score > 0.7 else "poor_quality",
            "issues": issues,
            "quality_score": quality_score,
            "record_count": len(data),
            "date_range": {
                "start": data["timestamp"].min().isoformat() if not data.empty else None,
                "end": data["timestamp"].max().isoformat() if not data.empty else None
            }
        }
    
    def get_market_overview(self) -> Dict[str, Any]:
        """Get comprehensive Vietnamese market overview.
        
        Returns:
            Market overview from multiple sources
        """
        overview = {
            "timestamp": datetime.now().isoformat(),
            "sources": {}
        }
        
        # Get data from each source
        for source_name, source in self.sources.items():
            try:
                if hasattr(source, "get_market_overview"):
                    data = source.get_market_overview()
                    if data:
                        overview["sources"][source_name] = data
                elif hasattr(source, "get_market_indices"):
                    data = source.get_market_indices()
                    if data:
                        overview["sources"][source_name] = data
            except Exception as e:
                self.logger.error(f"Error getting market overview from {source_name}: {e}")
        
        return overview
    
    def get_comprehensive_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check on all Vietnamese market sources.
        
        Returns:
            Detailed health status
        """
        health_status = self.health_check_all()
        
        # Add Vietnamese market specific checks
        health_status["vietnamese_market_status"] = {
            "vn30_sources_healthy": 0,
            "real_time_capable": 0,
            "historical_data_available": 0
        }
        
        for source_name, source_status in health_status["sources"].items():
            if source_status["status"] == "healthy":
                health_status["vietnamese_market_status"]["vn30_sources_healthy"] += 1
                
                # Check capabilities
                source = self.sources[source_name]
                
                # Test historical data capability
                try:
                    test_data = source.get_historical_data(
                        "VIC",
                        datetime.now() - timedelta(days=5),
                        datetime.now()
                    )
                    if not test_data.empty:
                        health_status["vietnamese_market_status"]["historical_data_available"] += 1
                except (Exception) as e:
                    self.logger.debug(f"Health check historical data unavailable for {source_name}: {e}")
                
                # Check real-time capability (SSI has WebSocket)
                if hasattr(source, "_connect_websocket"):
                    health_status["vietnamese_market_status"]["real_time_capable"] += 1
        
        # Add recommendations
        healthy_sources = health_status["healthy_sources"]
        if healthy_sources == 0:
            health_status["recommendations"] = [
                "No data sources available - check API configurations",
                "Verify network connectivity",
                "Check API credentials and rate limits"
            ]
        elif healthy_sources == 1:
            health_status["recommendations"] = [
                "Only one data source available - consider enabling backup sources",
                "Implement data source redundancy for production"
            ]
        else:
            health_status["recommendations"] = [
                "Multiple sources available - good redundancy",
                "Consider implementing data reconciliation"
            ]
        
        return health_status