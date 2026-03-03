"""VNStock data source implementation for Vietnamese market data."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from vnstock import Vnstock, Listing, Quote, Screener

from .base import DataSource, MarketDataPoint


class VNStockClient(DataSource):
    """VNStock API client for Vietnamese market data."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize VNStock client.
        
        Args:
            config: Configuration dictionary with keys:
                - source: Data source ('vci', 'tcbs', 'msn')
                - timeout: Request timeout in seconds
        """
        super().__init__("vnstock", config)
        self.data_source = config.get("source", "vci")
        self.timeout = config.get("timeout", 30)
        self._vnstock = None
        self._listing = None
        self._screener = None
        
    def connect(self) -> bool:
        """Establish connection to VNStock.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self._vnstock = Vnstock(source=self.data_source.upper())
            self._listing = Listing(source=self.data_source)
            
            # Initialize screener if using TCBS source
            if self.data_source.lower() == 'tcbs':
                self._screener = Screener(source='tcbs')
            
            # Test connection by fetching a simple listing
            test_listing = self._listing.all_symbols()
            if test_listing is not None and not test_listing.empty:
                self.logger.info(
                    f"Connected to VNStock ({self.data_source}) - "
                    f"Found {len(test_listing)} symbols"
                )
                return True
            else:
                self.logger.error("Failed to fetch symbol listing")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to connect to VNStock: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close connection to VNStock."""
        self._vnstock = None
        self._listing = None
        self._screener = None
        self.logger.info("Disconnected from VNStock")
    
    def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> pd.DataFrame:
        """Fetch historical market data from VNStock.
        
        Args:
            symbol: Stock symbol (e.g., 'VIC', 'VNM', 'VNINDEX')
            start_date: Start date
            end_date: End date
            interval: Data interval (1D, 1H, 5M, etc.)
            
        Returns:
            DataFrame with historical data
        """
        if not self._vnstock:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            # Create stock instance
            if symbol.upper() in ['VNINDEX', 'HNX-INDEX', 'UPCOM-INDEX']:
                # Handle market indices
                stock = self._vnstock.stock(symbol=symbol, source=self.data_source)
            else:
                # Handle regular stocks
                stock = self._vnstock.stock(symbol=symbol.upper(), source=self.data_source)
            
            # Fetch historical data
            data = stock.quote.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval=interval
            )
            
            if data is not None and not data.empty:
                # Normalize column names to match our standard format
                data = self._normalize_vnstock_data(data, symbol)
                
                self.logger.info(
                    f"Fetched {len(data)} records for {symbol} "
                    f"from {start_date.date()} to {end_date.date()}"
                )
                return data
            else:
                self.logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_real_time_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch real-time market data from VNStock.

        Uses price_board (live intraday data) during market hours.
        Falls back to quote.history for the previous trading day when market is closed.

        Args:
            symbols: List of stock symbols

        Returns:
            List of market data points
        """
        if not self._vnstock:
            if not self.connect():
                return []

        # Try price_board first — works during and after market hours
        try:
            return self._get_price_board_data(symbols)
        except Exception as e:
            self.logger.warning(f"price_board failed ({e}), falling back to history")
            return self._get_history_data(symbols)

    def _get_price_board_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fetch live data via trading.price_board (intraday, all symbols in one call)."""
        stock = self._vnstock.stock(symbol=symbols[0].upper(), source=self.data_source)
        pb = stock.trading.price_board(symbols_list=[s.upper() for s in symbols])

        if pb is None or pb.empty:
            return []

        # Flatten multi-level columns: ('match', 'match_price') → 'match_match_price'
        pb.columns = ['_'.join(c).strip() for c in pb.columns.values]

        data_points = []
        now = datetime.now()

        for _, row in pb.iterrows():
            try:
                symbol = str(row.get('listing_symbol', '')).upper()
                close = float(row.get('match_match_price', 0) or 0)
                if not symbol or close <= 0:
                    continue

                open_p = float(row.get('match_open_price', close) or close)
                high_p = float(row.get('match_highest', close) or close)
                low_p = float(row.get('match_lowest', close) or close)
                volume = int(row.get('match_accumulated_volume', 0) or 0)

                # Clamp OHLC consistency: at market open price_board high/low may lag
                high_p = max(high_p, open_p, close)
                low_p = min(low_p, open_p, close)
                if low_p <= 0:
                    low_p = close

                data_points.append(MarketDataPoint(
                    symbol=symbol,
                    timestamp=now,
                    open_price=open_p,
                    high_price=high_p,
                    low_price=low_p,
                    close_price=close,
                    volume=volume,
                    value=None,
                ))
            except Exception as e:
                self.logger.debug(f"Skipping row in price_board: {e}")
                continue

        self.logger.debug(f"Fetched real-time data for {len(data_points)} symbols")
        return data_points

    def _get_history_data(self, symbols: List[str]) -> List[MarketDataPoint]:
        """Fallback: fetch previous close via quote.history (market closed)."""
        from datetime import timedelta
        data_points = []
        # Look back up to 5 days to find the last trading day
        for days_back in range(1, 6):
            date_str = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            for symbol in symbols:
                try:
                    stock = self._vnstock.stock(symbol=symbol.upper(), source=self.data_source)
                    latest_data = stock.quote.history(
                        start=date_str, end=date_str, interval='1D'
                    )
                    if latest_data is not None and not latest_data.empty:
                        r = latest_data.iloc[-1]
                        # quote.history returns prices in thousands of VND — multiply to full VND
                        data_points.append(MarketDataPoint(
                            symbol=symbol.upper(),
                            timestamp=datetime.now(),
                            open_price=float(r['open']) * 1000,
                            high_price=float(r['high']) * 1000,
                            low_price=float(r['low']) * 1000,
                            close_price=float(r['close']) * 1000,
                            volume=int(r['volume']),
                            value=None,
                        ))
                except Exception as e:
                    self.logger.error(f"Error fetching history for {symbol}: {e}")
            if data_points:
                break
        self.logger.debug(f"Fetched history data for {len(data_points)} symbols")
        return data_points
    
    def validate_symbol(self, symbol: str) -> bool:
        """Validate if symbol exists in VNStock.
        
        Args:
            symbol: Stock symbol to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not self._listing:
            if not self.connect():
                return False
        
        try:
            # Get all symbols
            all_symbols = self._listing.all_symbols()
            
            if all_symbols is not None and not all_symbols.empty:
                # Check if symbol exists (case-insensitive)
                symbol_upper = symbol.upper()
                
                # Check in 'symbol' column if it exists
                if 'symbol' in all_symbols.columns:
                    return symbol_upper in all_symbols['symbol'].str.upper().values
                
                # Fallback: check in first column
                first_col = all_symbols.iloc[:, 0]
                return symbol_upper in first_col.str.upper().values
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error validating symbol {symbol}: {e}")
            return False
    
    def get_all_symbols(self) -> pd.DataFrame:
        """Get all available symbols from VNStock.
        
        Returns:
            DataFrame with all symbols
        """
        if not self._listing:
            if not self.connect():
                return pd.DataFrame()
        
        try:
            symbols = self._listing.all_symbols()
            
            if symbols is not None and not symbols.empty:
                self.logger.info(f"Retrieved {len(symbols)} symbols from VNStock")
                return symbols
            else:
                self.logger.warning("No symbols retrieved from VNStock")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error getting symbols: {e}")
            return pd.DataFrame()
    
    def get_vn30_symbols(self) -> List[str]:
        """Get VN30 index symbols.
        
        Returns:
            List of VN30 stock symbols
        """
        if not self._listing:
            if not self.connect():
                return []
        
        try:
            vn30 = self._listing.symbols_by_group(group='VN30')
            
            if vn30 is not None and not vn30.empty:
                # Extract symbols from the result
                if 'symbol' in vn30.columns:
                    symbols = vn30['symbol'].tolist()
                elif len(vn30.columns) > 0:
                    symbols = vn30.iloc[:, 0].tolist()
                else:
                    symbols = vn30.tolist() if isinstance(vn30, pd.Series) else []
                
                self.logger.info(f"Retrieved {len(symbols)} VN30 symbols")
                return symbols
            else:
                self.logger.warning("No VN30 symbols retrieved")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting VN30 symbols: {e}")
            return []
    
    def screen_stocks(self, params: Optional[Dict] = None, limit: int = 50) -> pd.DataFrame:
        """Screen stocks using TCBS screener.
        
        Args:
            params: Screening parameters
            limit: Maximum number of results
            
        Returns:
            DataFrame with screened stocks
        """
        if not self._screener or self.data_source.lower() != 'tcbs':
            self.logger.warning("Stock screening only available with TCBS source")
            return pd.DataFrame()
        
        try:
            default_params = {"exchangeName": "HOSE,HNX,UPCOM"}
            screen_params = params or default_params
            
            results = self._screener.stock(
                params=screen_params,
                limit=limit
            )
            
            if results is not None and not results.empty:
                self.logger.info(f"Screened {len(results)} stocks")
                return results
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error screening stocks: {e}")
            return pd.DataFrame()
    
    def _normalize_vnstock_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize VNStock data to standard format.
        
        Args:
            data: Raw data from VNStock
            symbol: Stock symbol
            
        Returns:
            Normalized DataFrame
        """
        try:
            normalized = pd.DataFrame()
            
            # Add symbol column
            normalized['symbol'] = symbol.upper()
            
            # Handle timestamp/date column
            if 'time' in data.columns:
                normalized['timestamp'] = pd.to_datetime(data['time'])
            elif data.index.name == 'time' or hasattr(data.index, 'name'):
                normalized['timestamp'] = pd.to_datetime(data.index)
            else:
                # Fallback to current date
                normalized['timestamp'] = datetime.now()
            
            # Map OHLCV columns
            column_mapping = {
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'volume'
            }
            
            for vnstock_col, standard_col in column_mapping.items():
                if vnstock_col in data.columns:
                    normalized[standard_col] = data[vnstock_col].astype(float)
                else:
                    self.logger.warning(f"Column {vnstock_col} not found in data")
                    normalized[standard_col] = 0.0
            
            # Handle volume as integer
            if 'volume' in normalized.columns:
                normalized['volume'] = normalized['volume'].fillna(0).astype(int)
            
            # Add value column if available (in VND)
            normalized['value'] = None
            
            # Sort by timestamp
            normalized = normalized.sort_values('timestamp').reset_index(drop=True)
            
            return normalized
            
        except Exception as e:
            self.logger.error(f"Error normalizing data: {e}")
            return data  # Return original data if normalization fails
    
    def get_market_summary(self) -> Dict[str, Any]:
        """Get market summary information.
        
        Returns:
            Dictionary with market summary
        """
        try:
            summary = {
                'source': 'vnstock',
                'data_source': self.data_source,
                'timestamp': datetime.now().isoformat()
            }
            
            # Get VN-Index data if available
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
            
            # Get symbol count
            try:
                symbols = self.get_all_symbols()
                summary['total_symbols'] = len(symbols) if not symbols.empty else 0
            except (ValueError, AttributeError, ConnectionError) as e:
                self.logger.warning(f"Failed to fetch symbol count: {e}")
                summary['total_symbols'] = 0
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting market summary: {e}")
            return {
                'source': 'vnstock',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }