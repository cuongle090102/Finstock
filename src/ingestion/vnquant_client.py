"""VNQuant data source implementation for Vietnamese market data."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from vnquant import DataLoader

from .base import DataSource, MarketDataPoint


class VNQuantClient(DataSource):
    """VNQuant API client for Vietnamese market data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize VNQuant client.
        
        Args:
            config: Configuration dictionary with keys:
                - data_source: Data source ('cafe', 'vnd')
                - timeout: Request timeout in seconds
        """
        super().__init__("vnquant", config)
        self.data_source = config.get("data_source", "cafe")
        self.timeout = config.get("timeout", 30)
        self._connection_tested = False
        
    def connect(self) -> bool:
        """Establish connection to VNQuant.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test connection by fetching a small amount of recent data
            test_symbol = 'VIC'
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now().replace(day=1)).strftime('%Y-%m-%d')  # First day of current month
            
            loader = DataLoader(
                symbols=test_symbol,
                start=start_date,
                end=end_date,
                data_source=self.data_source
            )
            
            test_data = loader.download()
            
            if test_data is not None and not test_data.empty:
                self.logger.info(
                    f"Connected to VNQuant ({self.data_source}) - "
                    f"Test data: {len(test_data)} records"
                )
                self._connection_tested = True
                return True
            else:
                self.logger.error(f"Failed to fetch test data from VNQuant ({self.data_source})")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to connect to VNQuant ({self.data_source}): {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to VNQuant."""
        self._connection_tested = False
        self.logger.info(f"Disconnected from VNQuant ({self.data_source})")
    
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> pd.DataFrame:
        """Fetch historical market data from VNQuant.
        
        Args:
            symbol: Stock symbol (e.g., 'VIC', 'VNM', 'VNINDEX')
            start_date: Start date
            end_date: End date
            interval: Data interval (only 1D supported by VNQuant)
            
        Returns:
            DataFrame with historical data
        """
        if not self._connection_tested:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # VNQuant only supports daily data
            if interval != "1D":
                self.logger.warning(f"VNQuant only supports 1D interval, got {interval}")
            
            loader = DataLoader(
                symbols=symbol.upper(),
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                data_source=self.data_source
            )
            
            data = loader.download()
            
            if data is not None and not data.empty:
                # Normalize data to standard format
                normalized_data = self._normalize_vnquant_data(data, symbol)
                
                self.logger.info(
                    f"Fetched {len(normalized_data)} records for {symbol} "
                    f"from {start_date.date()} to {end_date.date()}"
                )
                return normalized_data
            else:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch latest available market data from VNQuant.
        
        Note: VNQuant doesn't provide true real-time data, so we fetch the most recent data.
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            List of market data points
        """
        if not self._connection_tested:
            if not self.connect():
                return []
        
        data_points = []
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        for symbol in symbols:
            try:
                loader = DataLoader(
                    symbols=symbol.upper(),
                    start=current_date,
                    end=current_date,
                    data_source=self.data_source
                )
                
                latest_data = loader.download()
                
                if latest_data is not None and not latest_data.empty:
                    # Get the most recent record
                    latest_record = latest_data.iloc[-1]
                    
                    # Extract data based on VNQuant's multi-level column structure
                    symbol_upper = symbol.upper()
                    
                    try:
                        # VNQuant data has multi-level columns with symbol names
                        open_val = self._extract_value(latest_record, ['open'], symbol_upper)
                        high_val = self._extract_value(latest_record, ['high'], symbol_upper)
                        low_val = self._extract_value(latest_record, ['low'], symbol_upper)
                        close_val = self._extract_value(latest_record, ['close'], symbol_upper)
                        volume_val = self._extract_value(latest_record, ['volume_match'], symbol_upper)
                        value_val = self._extract_value(latest_record, ['value_match'], symbol_upper)
                        
                        data_point = MarketDataPoint(
                            symbol=symbol_upper,
                            timestamp=datetime.now(),
                            open_price=float(open_val) if open_val else 0.0,
                            high_price=float(high_val) if high_val else 0.0,
                            low_price=float(low_val) if low_val else 0.0,
                            close_price=float(close_val) if close_val else 0.0,
                            volume=int(volume_val) if volume_val else 0,
                            value=float(value_val) if value_val else None
                        )
                        
                        data_points.append(data_point)
                        
                    except Exception as parse_error:
                        self.logger.error(f"Error parsing data for {symbol}: {parse_error}")
                        continue
                    
            except Exception as e:
                self.logger.error(f"Error fetching latest data for {symbol}: {e}")
                continue
        
        self.logger.debug(f"Fetched latest data for {len(data_points)} symbols")
        return data_points
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists by attempting to fetch data.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Try to fetch a small amount of recent data
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now().replace(day=1)).strftime('%Y-%m-%d')
            
            loader = DataLoader(
                symbols=symbol.upper(),
                start=start_date,
                end=end_date,
                data_source=self.data_source
            )
            
            data = loader.download()
            
            # Consider symbol valid if we get any data
            is_valid = data is not None and not data.empty
            
            if is_valid:
                self.logger.debug(f"Symbol {symbol} validated successfully")
            else:
                self.logger.debug(f"Symbol {symbol} validation failed - no data returned")
            
            return is_valid
            
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {e}")
            return False
    
    def get_multiple_symbols_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch data for multiple symbols at once.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with multi-symbol data
        """
        if not self._connection_tested:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # VNQuant can handle multiple symbols in one request
            loader = DataLoader(
                symbols=symbols,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                data_source=self.data_source
            )
            
            data = loader.download()
            
            if data is not None and not data.empty:
                self.logger.info(
                    f"Fetched multi-symbol data: {len(data)} records "
                    f"for {len(symbols)} symbols"
                )
                return data
            else:
                self.logger.warning("No multi-symbol data returned")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching multi-symbol data: {e}")
            return pd.DataFrame()
    
    def _normalize_vnquant_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize VNQuant data to standard format.
        
        Args:
            data: Raw data from VNQuant
            symbol: Stock symbol
            
        Returns:
            Normalized DataFrame
        """
        try:
            normalized = pd.DataFrame()
            
            # Add symbol column
            normalized['symbol'] = symbol.upper()
            
            # Handle timestamp - VNQuant uses date index
            if hasattr(data.index, 'name') and data.index.name == 'date':
                normalized['timestamp'] = pd.to_datetime(data.index)
            elif 'date' in data.columns:
                normalized['timestamp'] = pd.to_datetime(data['date'])
            else:
                # Try to use index as timestamp
                normalized['timestamp'] = pd.to_datetime(data.index)
            
            # VNQuant has multi-level columns with attributes and symbols
            symbol_upper = symbol.upper()
            
            # Map OHLCV columns
            normalized['open_price'] = self._extract_column(data, 'open', symbol_upper)
            normalized['high_price'] = self._extract_column(data, 'high', symbol_upper)
            normalized['low_price'] = self._extract_column(data, 'low', symbol_upper)
            normalized['close_price'] = self._extract_column(data, 'close', symbol_upper)
            normalized['volume'] = self._extract_column(data, 'volume_match', symbol_upper)
            
            # Handle value column (trading value in VND)
            value_col = self._extract_column(data, 'value_match', symbol_upper)
            normalized['value'] = value_col if value_col is not None else None
            
            # Ensure numeric types
            for col in ['open_price', 'high_price', 'low_price', 'close_price']:
                normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0.0)
            
            normalized['volume'] = pd.to_numeric(normalized['volume'], errors='coerce').fillna(0).astype(int)
            
            if normalized['value'] is not None:
                normalized['value'] = pd.to_numeric(normalized['value'], errors='coerce')
            
            # Sort by timestamp
            normalized = normalized.sort_values('timestamp').reset_index(drop=True)
            
            return normalized
            
        except Exception as e:
            self.logger.error(f"Error normalizing VNQuant data: {e}")
            # Return basic structure if normalization fails
            return pd.DataFrame({
                'symbol': [symbol.upper()] * len(data),
                'timestamp': pd.to_datetime(data.index) if hasattr(data, 'index') else [datetime.now()],
                'open_price': [0.0] * len(data),
                'high_price': [0.0] * len(data),
                'low_price': [0.0] * len(data),
                'close_price': [0.0] * len(data),
                'volume': [0] * len(data),
                'value': [None] * len(data)
            })
    
    def _extract_column(self, data: pd.DataFrame, column_name: str, symbol: str) -> pd.Series:
        """Extract a specific column from VNQuant's multi-level column structure.
        
        Args:
            data: VNQuant DataFrame
            column_name: Column to extract (e.g., 'open', 'close')
            symbol: Stock symbol
            
        Returns:
            Extracted column as Series
        """
        try:
            # VNQuant uses multi-level columns like ('open', 'VIC')
            if hasattr(data.columns, 'levels'):
                # Multi-level columns
                for col in data.columns:
                    if isinstance(col, tuple) and len(col) >= 2:
                        if col[0] == column_name and col[1] == symbol:
                            return data[col]
            
            # Try direct column access
            if column_name in data.columns:
                return data[column_name]
            
            # Try with symbol prefix/suffix
            possible_names = [
                f"{column_name}_{symbol}",
                f"{symbol}_{column_name}",
                column_name
            ]
            
            for name in possible_names:
                if name in data.columns:
                    return data[name]
            
            self.logger.warning(f"Column {column_name} not found for symbol {symbol}")
            return pd.Series([0.0] * len(data), index=data.index)
            
        except Exception as e:
            self.logger.error(f"Error extracting column {column_name}: {e}")
            return pd.Series([0.0] * len(data), index=data.index)
    
    def _extract_value(self, record: pd.Series, column_names: List[str], symbol: str) -> Any:
        """Extract a value from a VNQuant record.
        
        Args:
            record: Single record from VNQuant data
            column_names: Possible column names to try
            symbol: Stock symbol
            
        Returns:
            Extracted value or None
        """
        try:
            # Try multi-level column access
            for col_name in column_names:
                # Try tuple format (attribute, symbol)
                if (col_name, symbol) in record.index:
                    return record[(col_name, symbol)]
                
                # Try direct column name
                if col_name in record.index:
                    return record[col_name]
                
                # Try with symbol variations
                variations = [f"{col_name}_{symbol}", f"{symbol}_{col_name}"]
                for var in variations:
                    if var in record.index:
                        return record[var]
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting value for {column_names}: {e}")
            return None
    
    def get_supported_symbols(self) -> List[str]:
        """Get list of commonly supported symbols for VNQuant.
        
        Note: VNQuant doesn't provide a symbol listing API, so we return known VN30 symbols.
        
        Returns:
            List of supported stock symbols
        """
        # VN30 symbols that are known to work with VNQuant
        vn30_symbols = [
            'ACB', 'BCM', 'BID', 'CTG', 'DGC', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG',
            'LPB', 'MBB', 'MSN', 'MWG', 'PLX', 'SAB', 'SHB', 'SSB', 'SSI', 'STB',
            'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
        ]
        
        # Add major indices
        indices = ['VNINDEX', 'HNX-INDEX', 'UPCOM-INDEX']
        
        return vn30_symbols + indices
    
    def get_market_summary(self) -> Dict[str, Any]:
        """Get market summary information.
        
        Returns:
            Dictionary with market summary
        """
        try:
            summary = {
                'source': 'vnquant',
                'data_source': self.data_source,
                'timestamp': datetime.now().isoformat()
            }
            
            # Get VNINDEX data if available
            try:
                vnindex_data = self.get_historical_data(
                    'VNINDEX',
                    datetime.now(),
                    datetime.now(),
                    '1D'
                )
                
                if not vnindex_data.empty:
                    latest = vnindex_data.iloc[-1]
                    summary['vnindex'] = {
                        'close': float(latest['close_price']),
                        'volume': int(latest['volume']),
                        'timestamp': latest['timestamp'].isoformat()
                    }
            except (KeyError, IndexError, ValueError, AttributeError) as e:
                self.logger.warning(f"Failed to fetch VNINDEX data: {e}")
            
            # Add supported symbols count
            supported_symbols = self.get_supported_symbols()
            summary['supported_symbols'] = len(supported_symbols)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting market summary: {e}")
            return {
                'source': 'vnquant',
                'data_source': self.data_source,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }