"""Enhanced VnStock collector with rate limiting, validation, and comprehensive features for Task 1.1."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
import json
import yaml
from pathlib import Path

import pandas as pd
import numpy as np
from pydantic import BaseModel, validator, Field
from vnstock import Vnstock, Listing, Quote, Screener
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.ingestion.vnstock_client import VNStockClient
from src.utils.logging import StructuredLogger


# Pydantic schemas for data validation
class OHLCVSchema(BaseModel):
    """OHLCV data validation schema."""
    symbol: str
    timestamp: datetime
    open_price: float = Field(ge=0, description="Opening price must be non-negative")
    high_price: float = Field(ge=0, description="High price must be non-negative") 
    low_price: float = Field(ge=0, description="Low price must be non-negative")
    close_price: float = Field(ge=0, description="Closing price must be non-negative")
    volume: int = Field(ge=0, description="Volume must be non-negative")
    value: Optional[float] = Field(ge=0, description="Value must be non-negative")
    
    @validator('high_price')
    def high_must_be_highest(cls, v, values):
        """High price must be >= open and close."""
        if 'open_price' in values and v < values['open_price']:
            raise ValueError('High price must be >= open price')
        if 'close_price' in values and v < values['close_price']:
            raise ValueError('High price must be >= close price')
        return v
    
    @validator('low_price')
    def low_must_be_lowest(cls, v, values):
        """Low price must be <= open and close."""
        if 'open_price' in values and v > values['open_price']:
            raise ValueError('Low price must be <= open price')
        if 'close_price' in values and v > values['close_price']:
            raise ValueError('Low price must be <= close price')
        return v

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class FundamentalsSchema(BaseModel):
    """Fundamentals data validation schema."""
    symbol: str
    timestamp: datetime
    market_cap: Optional[float] = Field(ge=0, description="Market cap must be non-negative")
    pe_ratio: Optional[float] = Field(ge=0, description="PE ratio must be non-negative")
    pb_ratio: Optional[float] = Field(ge=0, description="PB ratio must be non-negative")
    roe: Optional[float] = Field(ge=-100, le=100, description="ROE must be between -100 and 100")
    roa: Optional[float] = Field(ge=-100, le=100, description="ROA must be between -100 and 100")
    eps: Optional[float] = None
    revenue: Optional[float] = Field(ge=0, description="Revenue must be non-negative")
    net_income: Optional[float] = None
    debt_to_equity: Optional[float] = Field(ge=0, description="Debt to equity must be non-negative")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class CorporateActionSchema(BaseModel):
    """Corporate actions data validation schema."""
    symbol: str
    timestamp: datetime
    action_type: str = Field(..., pattern="^(dividend|stock_split|rights_issue|bonus|merger)$")
    announcement_date: datetime
    ex_date: Optional[datetime] = None
    record_date: Optional[datetime] = None
    payment_date: Optional[datetime] = None
    ratio: Optional[str] = None
    amount: Optional[float] = Field(ge=0, description="Amount must be non-negative")
    currency: str = Field(default="VND", pattern="^(VND|USD)$")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, max_requests: int = 10, time_window: float = 1.0):
        """Initialize rate limiter.
        
        Args:
            max_requests: Maximum requests per time window
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.logger = StructuredLogger("RateLimiter")
    
    async def acquire(self):
        """Acquire permission to make a request."""
        now = time.time()
        
        # Remove old requests outside the time window
        self.requests = [req_time for req_time in self.requests if now - req_time < self.time_window]
        
        # Check if we can make a request
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                self.logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
                return await self.acquire()
        
        # Record the request
        self.requests.append(now)


class RetryHandler:
    """Exponential backoff retry handler."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        """Initialize retry handler.
        
        Args:
            max_retries: Maximum number of retries
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.logger = StructuredLogger("RetryHandler")
    
    async def execute(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If all retries are exhausted
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.max_retries:
                    self.logger.error(f"All {self.max_retries} retries exhausted for {func.__name__}: {e}")
                    break
                
                delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                self.logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {delay}s")
                
                if asyncio.iscoroutinefunction(func):
                    await asyncio.sleep(delay)
                else:
                    time.sleep(delay)
        
        raise last_exception


class EnhancedVnStockCollector(VNStockClient):
    """Enhanced VnStock collector with rate limiting, validation, and comprehensive features."""

    def __init__(self, config: Dict[str, Any], kafka_producer: Optional[KafkaProducer] = None):
        """Initialize enhanced VnStock collector.

        Args:
            config: Configuration dictionary with keys:
                - source: Data source ('vci', 'tcbs', 'msn')
                - timeout: Request timeout in seconds
                - max_requests_per_second: Rate limiting (default: 10)
                - max_retries: Maximum retries (default: 3)
                - cache_ttl: Cache TTL in seconds (default: 3600)
                - kafka_bootstrap_servers: Kafka servers (default: localhost:9092)
                - kafka_topic: Kafka topic for market data (default: market_data)
            kafka_producer: Optional pre-configured Kafka producer
        """
        super().__init__(config)

        # Ensure data_source is set (in case parent init is mocked in tests)
        if not hasattr(self, 'data_source'):
            self.data_source = config.get("source", "vci")

        # Rate limiting
        max_rps = config.get("max_requests_per_second", 10)
        self.rate_limiter = RateLimiter(max_requests=max_rps, time_window=1.0)

        # Retry handling
        max_retries = config.get("max_retries", 3)
        self.retry_handler = RetryHandler(max_retries=max_retries)

        # Enhanced logging
        self.logger = StructuredLogger("EnhancedVnStockCollector")

        # Session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Request metadata storage
        self.request_metadata = []

        # Kafka integration (Task 1.1)
        self.kafka_producer = kafka_producer
        self.kafka_topic = config.get("kafka_topic", "market_data")
        self.kafka_enabled = config.get("kafka_enabled", True)

        if self.kafka_enabled and not self.kafka_producer:
            # Create Kafka producer if not provided
            kafka_servers = config.get("kafka_bootstrap_servers", "localhost:9092")
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=kafka_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=5
                )
                self.logger.info(f"Kafka producer connected to {kafka_servers}")
            except Exception as e:
                self.logger.error(f"Failed to create Kafka producer: {e}")
                self.kafka_enabled = False

        self.logger.info(f"Enhanced VnStock collector initialized with source: {self.data_source}, Kafka: {self.kafka_enabled}")

    def _send_to_kafka(self, data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Send data to Kafka topic (Task 1.1 integration).

        Args:
            data: Data dictionary to send
            key: Optional message key (defaults to symbol)

        Returns:
            True if successful, False otherwise
        """
        if not self.kafka_enabled or not self.kafka_producer:
            return False

        try:
            # Extract symbol as key if not provided
            if key is None:
                key = data.get('symbol', 'unknown')

            # Send to Kafka
            future = self.kafka_producer.send(
                topic=self.kafka_topic,
                key=key,
                value=data
            )

            # Wait for acknowledgment (with timeout)
            record_metadata = future.get(timeout=10)

            self.logger.debug(
                f"Sent to Kafka: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            self.logger.error(f"Kafka send error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to send to Kafka: {e}")
            return False

    async def get_stock_list(self) -> pd.DataFrame:
        """Fetch all available stock symbols with rate limiting and validation.
        
        Returns:
            DataFrame with stock symbols and metadata
        """
        await self.rate_limiter.acquire()
        
        start_time = datetime.now(timezone.utc)
        
        try:
            result = await self.retry_handler.execute(self._fetch_stock_list)
            
            # Store request metadata
            self._store_request_metadata(
                method="get_stock_list",
                parameters={},
                start_time=start_time,
                success=True,
                record_count=len(result) if result is not None else 0
            )
            
            self.logger.info(f"Successfully fetched {len(result)} stock symbols")
            return result
            
        except Exception as e:
            self._store_request_metadata(
                method="get_stock_list",
                parameters={},
                start_time=start_time,
                success=False,
                error=str(e)
            )
            self.logger.error(f"Failed to fetch stock list: {e}")
            return pd.DataFrame()
    
    def _fetch_stock_list(self) -> pd.DataFrame:
        """Internal method to fetch stock list."""
        if not self._listing:
            if not self.connect():
                raise ConnectionError("Failed to connect to VnStock")
        
        symbols = self._listing.all_symbols()
        if symbols is None or symbols.empty:
            raise ValueError("No symbols returned from VnStock")
        
        return symbols
    
    async def get_ohlcv_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1D"
    ) -> List[OHLCVSchema]:
        """Fetch historical OHLCV data with validation.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            interval: Data interval
            
        Returns:
            List of validated OHLCV data points
        """
        await self.rate_limiter.acquire()
        
        start_time = datetime.now(timezone.utc)
        parameters = {
            "symbol": symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "interval": interval
        }
        
        try:
            # Fetch raw data using parent class method
            raw_data = await self.retry_handler.execute(
                self.get_historical_data,
                symbol, start_date, end_date, interval
            )
            
            if raw_data.empty:
                self.logger.warning(f"No OHLCV data returned for {symbol}")
                return []
            
            # Validate and convert to schema objects
            validated_data = []
            validation_errors = 0
            
            for _, row in raw_data.iterrows():
                try:
                    ohlcv = OHLCVSchema(
                        symbol=symbol.upper(),
                        timestamp=pd.to_datetime(row['timestamp']),
                        open_price=float(row['open_price']),
                        high_price=float(row['high_price']),
                        low_price=float(row['low_price']),
                        close_price=float(row['close_price']),
                        volume=int(row['volume']),
                        value=float(row['value']) if row.get('value') and not pd.isna(row['value']) else None
                    )
                    validated_data.append(ohlcv)
                except Exception as validation_error:
                    validation_errors += 1
                    self.logger.warning(f"Validation error for {symbol} row: {validation_error}")
            
            # Send to Kafka (Task 1.1)
            kafka_sent_count = 0
            if self.kafka_enabled and validated_data:
                for ohlcv in validated_data:
                    kafka_message = {
                        "symbol": ohlcv.symbol,
                        "timestamp": ohlcv.timestamp.isoformat(),
                        "open_price": ohlcv.open_price,
                        "high_price": ohlcv.high_price,
                        "low_price": ohlcv.low_price,
                        "close_price": ohlcv.close_price,
                        "volume": ohlcv.volume,
                        "value": ohlcv.value,
                        "data_source": "vnstock",
                        "collector": "enhanced_vnstock",
                        "data_type": "ohlcv"
                    }
                    if self._send_to_kafka(kafka_message, key=symbol):
                        kafka_sent_count += 1

            # Store request metadata
            self._store_request_metadata(
                method="get_ohlcv_data",
                parameters=parameters,
                start_time=start_time,
                success=True,
                record_count=len(validated_data),
                validation_errors=validation_errors,
                kafka_sent=kafka_sent_count
            )

            self.logger.info(
                f"Successfully fetched and validated {len(validated_data)} OHLCV records for {symbol}"
                f" ({validation_errors} validation errors, {kafka_sent_count} sent to Kafka)"
            )

            return validated_data
            
        except Exception as e:
            self._store_request_metadata(
                method="get_ohlcv_data",
                parameters=parameters,
                start_time=start_time,
                success=False,
                error=str(e)
            )
            self.logger.error(f"Failed to fetch OHLCV data for {symbol}: {e}")
            return []
    
    async def get_realtime_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current market price with metadata.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with current price data and metadata
        """
        await self.rate_limiter.acquire()
        
        start_time = datetime.now(timezone.utc)
        parameters = {"symbol": symbol}
        
        try:
            # Use parent class real-time data method
            real_time_data = await self.retry_handler.execute(
                self.get_real_time_data,
                [symbol]
            )
            
            if not real_time_data:
                self.logger.warning(f"No real-time data returned for {symbol}")
                return None
            
            price_data = real_time_data[0]  # First (and only) symbol
            
            result = {
                "symbol": symbol.upper(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "price": price_data.close_price,
                "open": price_data.open_price,
                "high": price_data.high_price,
                "low": price_data.low_price,
                "volume": price_data.volume,
                "change": None,  # Calculate if previous close available
                "change_percent": None,
                "data_source": self.data_source,
                "market_status": self._get_market_status(),
                "collector": "enhanced_vnstock",
                "data_type": "realtime"
            }

            # Send to Kafka (Task 1.1)
            kafka_sent = False
            if self.kafka_enabled:
                kafka_sent = self._send_to_kafka(result, key=symbol)

            # Store request metadata
            self._store_request_metadata(
                method="get_realtime_price",
                parameters=parameters,
                start_time=start_time,
                success=True,
                record_count=1,
                kafka_sent=1 if kafka_sent else 0
            )

            self.logger.debug(f"Successfully fetched real-time price for {symbol}: {result['price']}, Kafka: {kafka_sent}")
            return result
            
        except Exception as e:
            self._store_request_metadata(
                method="get_realtime_price",
                parameters=parameters,
                start_time=start_time,
                success=False,
                error=str(e)
            )
            self.logger.error(f"Failed to fetch real-time price for {symbol}: {e}")
            return None
    
    async def get_fundamentals(self, symbol: str) -> Optional[FundamentalsSchema]:
        """Get fundamental data with validation.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Validated fundamentals data
        """
        await self.rate_limiter.acquire()
        
        start_time = datetime.now(timezone.utc)
        parameters = {"symbol": symbol}
        
        try:
            # Fetch fundamental data (implementation depends on vnstock API)
            fundamentals_data = await self.retry_handler.execute(
                self._fetch_fundamentals,
                symbol
            )
            
            if not fundamentals_data:
                self.logger.warning(f"No fundamentals data returned for {symbol}")
                return None
            
            # Validate using schema
            validated_fundamentals = FundamentalsSchema(
                symbol=symbol.upper(),
                timestamp=datetime.now(timezone.utc),
                **fundamentals_data
            )
            
            # Store request metadata
            self._store_request_metadata(
                method="get_fundamentals",
                parameters=parameters,
                start_time=start_time,
                success=True,
                record_count=1
            )
            
            self.logger.info(f"Successfully fetched fundamentals for {symbol}")
            return validated_fundamentals
            
        except Exception as e:
            self._store_request_metadata(
                method="get_fundamentals",
                parameters=parameters,
                start_time=start_time,
                success=False,
                error=str(e)
            )
            self.logger.error(f"Failed to fetch fundamentals for {symbol}: {e}")
            return None
    
    async def get_corporate_actions(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[CorporateActionSchema]:
        """Get corporate actions with validation.
        
        Args:
            symbol: Stock symbol
            start_date: Start date for corporate actions
            end_date: End date for corporate actions
            
        Returns:
            List of validated corporate actions
        """
        await self.rate_limiter.acquire()
        
        start_time = datetime.now(timezone.utc)
        parameters = {
            "symbol": symbol,
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None
        }
        
        try:
            # Fetch corporate actions data
            actions_data = await self.retry_handler.execute(
                self._fetch_corporate_actions,
                symbol, start_date, end_date
            )
            
            if not actions_data:
                self.logger.info(f"No corporate actions found for {symbol}")
                return []
            
            # Validate each corporate action
            validated_actions = []
            validation_errors = 0
            
            for action in actions_data:
                try:
                    validated_action = CorporateActionSchema(
                        symbol=symbol.upper(),
                        timestamp=datetime.now(timezone.utc),
                        **action
                    )
                    validated_actions.append(validated_action)
                except Exception as validation_error:
                    validation_errors += 1
                    self.logger.warning(f"Validation error for {symbol} corporate action: {validation_error}")
            
            # Store request metadata
            self._store_request_metadata(
                method="get_corporate_actions",
                parameters=parameters,
                start_time=start_time,
                success=True,
                record_count=len(validated_actions),
                validation_errors=validation_errors
            )
            
            self.logger.info(
                f"Successfully fetched {len(validated_actions)} corporate actions for {symbol}"
                f" ({validation_errors} validation errors)"
            )
            
            return validated_actions
            
        except Exception as e:
            self._store_request_metadata(
                method="get_corporate_actions",
                parameters=parameters,
                start_time=start_time,
                success=False,
                error=str(e)
            )
            self.logger.error(f"Failed to fetch corporate actions for {symbol}: {e}")
            return []
    
    def _fetch_fundamentals(self, symbol: str) -> Dict[str, Any]:
        """Internal method to fetch fundamentals using vnstock API."""
        try:
            # Try to get fundamental data using vnstock
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            
            # Get financial ratios
            ratios_data = {}
            try:
                # Get financial ratios if available
                ratios_df = stock.finance.ratio(period='year', lang='en')
                if not ratios_df.empty:
                    latest_ratios = ratios_df.iloc[-1]  # Get most recent data
                    
                    ratios_data = {
                        "pe_ratio": latest_ratios.get('pe', None),
                        "pb_ratio": latest_ratios.get('pb', None),
                        "roe": latest_ratios.get('roe', None),
                        "roa": latest_ratios.get('roa', None),
                        "eps": latest_ratios.get('eps', None),
                        "revenue": latest_ratios.get('revenue', None),
                        "net_income": latest_ratios.get('netIncome', None),
                        "debt_to_equity": latest_ratios.get('debtToEquity', None)
                    }
            except Exception as ratio_error:
                self.logger.debug(f"Could not fetch ratios for {symbol}: {ratio_error}")
                ratios_data = {}
            
            # Try to get market cap and other basic info
            try:
                overview_df = stock.finance.income_statement(period='year', lang='en')
                if not overview_df.empty:
                    latest_overview = overview_df.iloc[-1]
                    ratios_data.update({
                        "market_cap": latest_overview.get('marketCap', None),
                        "revenue": latest_overview.get('revenue', ratios_data.get('revenue')),
                        "net_income": latest_overview.get('netIncome', ratios_data.get('net_income'))
                    })
            except Exception as overview_error:
                self.logger.debug(f"Could not fetch overview for {symbol}: {overview_error}")
            
            # Fill in missing values with None
            default_fundamentals = {
                "market_cap": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "roe": None,
                "roa": None,
                "eps": None,
                "revenue": None,
                "net_income": None,
                "debt_to_equity": None
            }
            
            # Update with actual data
            default_fundamentals.update(ratios_data)
            
            self.logger.debug(f"Successfully fetched fundamentals for {symbol}")
            return default_fundamentals
            
        except Exception as e:
            self.logger.warning(f"Error fetching fundamentals for {symbol}: {e}")
            # Return default structure with None values
            return {
                "market_cap": None,
                "pe_ratio": None,
                "pb_ratio": None,
                "roe": None,
                "roa": None,
                "eps": None,
                "revenue": None,
                "net_income": None,
                "debt_to_equity": None
            }
    
    def _fetch_corporate_actions(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Internal method to fetch corporate actions using vnstock API."""
        try:
            # Try to get corporate actions/events using vnstock
            stock = Vnstock().stock(symbol=symbol, source='VCI')
            
            actions = []
            
            try:
                # Get dividend information if available
                dividend_df = stock.finance.dividend()
                if not dividend_df.empty:
                    for _, row in dividend_df.iterrows():
                        # Filter by date range if specified
                        action_date = pd.to_datetime(row.get('exRightDate', row.get('date', None)))
                        if action_date and start_date and action_date < pd.to_datetime(start_date):
                            continue
                        if action_date and end_date and action_date > pd.to_datetime(end_date):
                            continue
                        
                        actions.append({
                            "action_type": "dividend",
                            "announcement_date": action_date,
                            "ex_date": pd.to_datetime(row.get('exRightDate', None)),
                            "record_date": pd.to_datetime(row.get('recordDate', None)),
                            "payment_date": pd.to_datetime(row.get('paymentDate', None)),
                            "amount": row.get('cashDividend', row.get('amount', None)),
                            "currency": "VND",
                            "description": f"Cash dividend payment for {symbol}"
                        })
                        
            except Exception as dividend_error:
                self.logger.debug(f"Could not fetch dividend data for {symbol}: {dividend_error}")
            
            try:
                # Try to get other corporate events (splits, rights issues, etc.)
                # Note: vnstock may not have comprehensive corporate actions API
                # This is a best-effort implementation
                events_df = stock.finance.events() if hasattr(stock.finance, 'events') else None
                if events_df is not None and not events_df.empty:
                    for _, row in events_df.iterrows():
                        event_date = pd.to_datetime(row.get('date', None))
                        if event_date and start_date and event_date < pd.to_datetime(start_date):
                            continue
                        if event_date and end_date and event_date > pd.to_datetime(end_date):
                            continue
                        
                        event_type = row.get('type', 'other').lower()
                        if 'split' in event_type:
                            action_type = 'stock_split'
                        elif 'right' in event_type:
                            action_type = 'rights_issue'
                        elif 'bonus' in event_type:
                            action_type = 'bonus'
                        else:
                            action_type = 'other'
                        
                        actions.append({
                            "action_type": action_type,
                            "announcement_date": event_date,
                            "description": row.get('description', f"{action_type.replace('_', ' ').title()} for {symbol}"),
                            "ratio": row.get('ratio', None),
                            "currency": "VND"
                        })
                        
            except Exception as events_error:
                self.logger.debug(f"Could not fetch events data for {symbol}: {events_error}")
            
            self.logger.debug(f"Successfully fetched {len(actions)} corporate actions for {symbol}")
            return actions
            
        except Exception as e:
            self.logger.warning(f"Error fetching corporate actions for {symbol}: {e}")
            return []
    
    def _get_market_status(self) -> str:
        """Get current Vietnamese market status."""
        now = datetime.now()
        weekday = now.weekday()
        hour = now.hour
        minute = now.minute
        
        # Vietnamese market hours: 9:00-11:30, 13:00-15:00 (weekdays)
        if weekday >= 5:  # Weekend
            return "closed"
        
        # Check trading hours
        morning_open = (hour == 9 and minute >= 0) or (hour == 10) or (hour == 11 and minute <= 30)
        afternoon_open = (hour == 13) or (hour == 14) or (hour == 15 and minute <= 0)
        
        if morning_open or afternoon_open:
            return "open"
        elif hour < 9:
            return "pre_market"
        elif 11 < hour < 13:
            return "lunch_break"
        else:
            return "closed"
    
    def _store_request_metadata(
        self,
        method: str,
        parameters: Dict[str, Any],
        start_time: datetime,
        success: bool,
        record_count: int = 0,
        validation_errors: int = 0,
        kafka_sent: int = 0,
        error: str = None
    ):
        """Store request metadata for monitoring and debugging."""
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        
        metadata = {
            "timestamp": start_time.isoformat(),
            "method": method,
            "parameters": parameters,
            "duration_seconds": duration,
            "success": success,
            "record_count": record_count,
            "validation_errors": validation_errors,
            "kafka_sent": kafka_sent,
            "data_source": self.data_source,
            "source_identifier": "vnstock_enhanced",
            "response_status": "success" if success else "error",
            "error": error
        }
        
        self.request_metadata.append(metadata)
        
        # Keep only last 1000 requests in memory
        if len(self.request_metadata) > 1000:
            self.request_metadata = self.request_metadata[-1000:]
        
        # Log structured metadata
        self.logger.info("Request completed", **metadata)
    
    def get_request_metadata(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent request metadata for monitoring.
        
        Args:
            limit: Maximum number of recent requests to return
            
        Returns:
            List of request metadata dictionaries
        """
        return self.request_metadata[-limit:]
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics.
        
        Returns:
            Dictionary with performance metrics
        """
        if not self.request_metadata:
            return {"total_requests": 0}
        
        successful_requests = [r for r in self.request_metadata if r["success"]]
        failed_requests = [r for r in self.request_metadata if not r["success"]]
        
        durations = [r["duration_seconds"] for r in self.request_metadata]
        
        return {
            "total_requests": len(self.request_metadata),
            "successful_requests": len(successful_requests),
            "failed_requests": len(failed_requests),
            "success_rate": len(successful_requests) / len(self.request_metadata) * 100,
            "average_duration": np.mean(durations) if durations else 0,
            "median_duration": np.median(durations) if durations else 0,
            "total_records_fetched": sum(r.get("record_count", 0) for r in successful_requests),
            "total_validation_errors": sum(r.get("validation_errors", 0) for r in self.request_metadata),
            "data_source": self.data_source
        }
    
    @classmethod
    def from_config_file(cls, config_path: Union[str, Path]) -> 'EnhancedVnStockCollector':
        """Create collector from configuration file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Configured EnhancedVnStockCollector instance
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return cls(config)
    
    def export_metadata(self, file_path: Union[str, Path]) -> None:
        """Export request metadata to JSON file.

        Args:
            file_path: Path to export file
        """
        file_path = Path(file_path)

        export_data = {
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "collector_info": {
                "data_source": self.data_source,
                "collector_type": "EnhancedVnStockCollector",
                "kafka_enabled": self.kafka_enabled
            },
            "performance_stats": self.get_performance_stats(),
            "request_metadata": self.request_metadata
        }

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Request metadata exported to {file_path}")

    def close(self):
        """Close Kafka producer and cleanup resources (Task 1.1)."""
        if self.kafka_producer:
            try:
                self.kafka_producer.flush()
                self.kafka_producer.close()
                self.logger.info("Kafka producer closed successfully")
            except Exception as e:
                self.logger.error(f"Error closing Kafka producer: {e}")

    def __del__(self):
        """Cleanup on deletion."""
        self.close()
