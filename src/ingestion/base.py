"""Base classes for data ingestion."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel

from src.utils.logging import StructuredLogger


class MarketDataPoint(BaseModel):
    """Standard market data point structure."""
    
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    value: Optional[float] = None  # Trade value in VND
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class DataSource(ABC):
    """Abstract base class for market data sources."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize data source.
        
        Args:
            name: Data source name
            config: Configuration dictionary
        """
        self.name = name
        self.config = config
        self.logger = StructuredLogger(f"DataSource.{name}")
        self._session = None
        
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the data source."""
        pass
    
    @abstractmethod
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> pd.DataFrame:
        """Fetch historical market data.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Data interval (1D, 1H, 5M, etc.)
            
        Returns:
            DataFrame with historical data
        """
        pass
    
    @abstractmethod
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch real-time market data.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of market data points
        """
        pass
    
    @abstractmethod
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists in this data source.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on the data source.
        
        Returns:
            Health status dictionary
        """
        try:
            start_time = datetime.now()
            connected = self.connect()
            end_time = datetime.now()
            
            latency = (end_time - start_time).total_seconds()
            
            return {
                "source": self.name,
                "status": "healthy" if connected else "unhealthy",
                "latency_seconds": latency,
                "timestamp": end_time.isoformat(),
                "connected": connected
            }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "source": self.name,
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "connected": False
            }
    
    def normalize_data(self, raw_data: Any) -> pd.DataFrame:
        """Normalize raw data to standard format.
        
        Args:
            raw_data: Raw data from the source
            
        Returns:
            Normalized DataFrame
        """
        # Default implementation - should be overridden by subclasses
        if isinstance(raw_data, pd.DataFrame):
            return raw_data
        return pd.DataFrame(raw_data)
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


class DataExtractor:
    """Main data extraction orchestrator."""
    
    def __init__(self, sources: List[DataSource]):
        """Initialize data extractor.
        
        Args:
            sources: List of data sources
        """
        self.sources = {source.name: source for source in sources}
        self.logger = StructuredLogger("DataExtractor")
        
    def add_source(self, source: DataSource) -> None:
        """Add a data source.
        
        Args:
            source: Data source to add
        """
        self.sources[source.name] = source
        self.logger.info(f"Added data source: {source.name}")
    
    def remove_source(self, source_name: str) -> None:
        """Remove a data source.
        
        Args:
            source_name: Name of source to remove
        """
        if source_name in self.sources:
            del self.sources[source_name]
            self.logger.info(f"Removed data source: {source_name}")
    
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D",
        source_priority: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Fetch historical data with fallback sources.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Data interval
            source_priority: Ordered list of preferred sources
            
        Returns:
            DataFrame with historical data
        """
        sources_to_try = source_priority or list(self.sources.keys())
        
        for source_name in sources_to_try:
            if source_name not in self.sources:
                continue
                
            source = self.sources[source_name]
            
            try:
                self.logger.info(f"Trying source {source_name} for {symbol}")
                data = source.get_historical_data(symbol, start_date, end_date, interval)
                
                if not data.empty:
                    self.logger.info(
                        f"Successfully fetched {len(data)} records from {source_name}"
                    )
                    return data
                else:
                    self.logger.warning(f"No data returned from {source_name}")
                    
            except Exception as e:
                self.logger.error(f"Error fetching from {source_name}: {e}")
                continue
        
        self.logger.error(f"Failed to fetch data for {symbol} from all sources")
        return pd.DataFrame()
    
    def get_real_time_data(
        self,
        symbols: List[str],
        source_priority: Optional[List[str]] = None
    ) -> List[MarketDataPoint]:
        """Fetch real-time data with fallback sources.
        
        Args:
            symbols: List of stock symbols
            source_priority: Ordered list of preferred sources
            
        Returns:
            List of market data points
        """
        sources_to_try = source_priority or list(self.sources.keys())
        
        for source_name in sources_to_try:
            if source_name not in self.sources:
                continue
                
            source = self.sources[source_name]
            
            try:
                self.logger.debug(f"Fetching real-time data from {source_name}")
                data = source.get_real_time_data(symbols)
                
                if data:
                    self.logger.debug(
                        f"Fetched {len(data)} real-time data points from {source_name}"
                    )
                    return data
                    
            except Exception as e:
                self.logger.error(f"Error fetching real-time data from {source_name}: {e}")
                continue
        
        self.logger.error("Failed to fetch real-time data from all sources")
        return []
    
    def health_check_all(self) -> Dict[str, Any]:
        """Perform health check on all sources.
        
        Returns:
            Health status for all sources
        """
        results = {}
        
        for source_name, source in self.sources.items():
            results[source_name] = source.health_check()
        
        # Calculate overall health
        healthy_sources = sum(1 for r in results.values() if r["status"] == "healthy")
        total_sources = len(results)
        
        overall_status = {
            "overall_status": "healthy" if healthy_sources > 0 else "unhealthy",
            "healthy_sources": healthy_sources,
            "total_sources": total_sources,
            "sources": results,
            "timestamp": datetime.now().isoformat()
        }
        
        return overall_status