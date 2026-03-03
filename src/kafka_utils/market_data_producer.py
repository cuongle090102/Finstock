"""
Market Data Producer for Vietnamese Trading System
Phase 2: Messaging Layer Implementation - Task 2.2.2

This producer handles real-time OHLCV market data streaming with:
- Integration with Phase 1 VnStock and VnQuant collectors
- High-throughput streaming optimized for Vietnamese market
- Symbol-based partitioning for parallel processing
- Market session awareness (morning/afternoon sessions)
- Vietnamese market specific validations
- Real-time data quality monitoring
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Union, Set
from dataclasses import dataclass
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.kafka_utils.base_producer import BaseKafkaProducer, ProducerConfig, MessageMetadata, HealthCheckMixin
from src.schemas.market_schemas import MarketDataSchema
from src.utils.logging import StructuredLogger


@dataclass
class MarketDataConfig:
    """Configuration for market data producer"""
    # Data sources
    vnstock_enabled: bool = True
    vnquant_enabled: bool = True
    primary_source: str = "vnstock"  # Primary data source
    fallback_enabled: bool = True
    
    # Vietnamese market settings
    vn30_symbols_only: bool = False
    include_indices: bool = True
    market_hours_only: bool = True
    
    # Streaming settings
    real_time_interval: int = 1  # seconds
    batch_interval: int = 5  # seconds
    max_batch_size: int = 100
    
    # Data quality
    price_change_threshold: float = 0.15  # 15% max change per update
    volume_threshold: int = 0  # Minimum volume to stream
    data_freshness_threshold: int = 300  # 5 minutes max age
    
    # Partitioning strategy
    partition_by_symbol: bool = True
    vn30_high_priority: bool = True
    
    # Monitoring
    publish_metrics: bool = True
    metric_interval: int = 60  # seconds


class MarketDataProducer(BaseKafkaProducer, HealthCheckMixin):
    """High-performance market data producer for Vietnamese equities"""
    
    def __init__(self, producer_config: ProducerConfig, market_config: MarketDataConfig,
                 vnstock_collector=None, vnquant_collector=None):
        super().__init__(producer_config, "market_data")
        
        self.market_config = market_config
        self.logger = StructuredLogger("MarketDataProducer")
        
        # Initialize data collectors (use provided instances or create new ones)
        self.vnstock_collector = vnstock_collector
        self.vnquant_collector = vnquant_collector
        self._init_collectors()
        
        # Vietnamese market sessions (UTC+7)
        self.market_sessions = {
            "morning": {"start": "02:00", "end": "04:30"},    # 9:00-11:30 UTC+7
            "afternoon": {"start": "06:00", "end": "08:00"}   # 13:00-15:00 UTC+7
        }
        
        # Symbol management
        self.active_symbols: Set[str] = set()
        self.vn30_symbols: Set[str] = set()
        self.symbol_metadata: Dict[str, Dict] = {}
        
        # Data quality tracking
        self.data_quality_stats = {
            "messages_validated": 0,
            "validation_failures": 0,
            "price_anomalies": 0,
            "stale_data_rejected": 0
        }
        
        # Streaming state
        self.is_streaming = False
        self.streaming_task = None
        self.last_data_cache = {}  # Cache for change detection
        
        # Performance tracking
        self.performance_stats = {
            "symbols_processed": 0,
            "data_points_collected": 0,
            "collection_time_total": 0.0,
            "streaming_errors": 0
        }
        
        # Initialize symbol lists
        asyncio.create_task(self._initialize_symbols())
        
        self.logger.info("MarketDataProducer initialized",
            vnstock_enabled=market_config.vnstock_enabled,
            vnquant_enabled=market_config.vnquant_enabled,
            primary_source=market_config.primary_source,
            real_time_interval=market_config.real_time_interval,
            vn30_symbols_only=market_config.vn30_symbols_only
        )
    
    def _init_collectors(self):
        """Initialize data collectors"""
        try:
            # Only create collectors if not provided and enabled
            if self.market_config.vnstock_enabled and not self.vnstock_collector:
                from src.collectors.vnstock_collector import EnhancedVnStockCollector
                self.vnstock_collector = EnhancedVnStockCollector()
                self.logger.info("VnStock collector initialized")
            
            if self.market_config.vnquant_enabled and not self.vnquant_collector:
                from src.collectors.vnquant_collector import EnhancedVnQuantCollector
                self.vnquant_collector = EnhancedVnQuantCollector()
                self.logger.info("VnQuant collector initialized")
                
            # Log if collectors were provided externally
            if self.vnstock_collector and self.market_config.vnstock_enabled:
                self.logger.info("Using provided VnStock collector instance")
            if self.vnquant_collector and self.market_config.vnquant_enabled:
                self.logger.info("Using provided VnQuant collector instance")
                
        except Exception as e:
            self.logger.error("Failed to initialize collectors", error=str(e))
            raise
    
    async def _initialize_symbols(self):
        """Initialize symbol lists and metadata"""
        try:
            # Get VN30 symbols if available
            if self.vnstock_collector:
                try:
                    # This would need to be implemented in the collector
                    # For now, use a static list of VN30 symbols
                    self.vn30_symbols = {
                        "VIC", "VCB", "VHM", "VNM", "HPG", "BID", "CTG", "FPT", 
                        "GAS", "MSN", "PLX", "POW", "SAB", "SBT", "SSI", "TCB",
                        "TPB", "VRE", "VJC", "ACB", "MWG", "VPB", "EIB", "HDB",
                        "KDH", "NVL", "PDR", "STB", "VHC", "VCI"
                    }
                    
                    if self.market_config.vn30_symbols_only:
                        self.active_symbols = self.vn30_symbols.copy()
                    else:
                        # Add more symbols here - could get from listing API
                        self.active_symbols = self.vn30_symbols.copy()
                        # Add additional symbols as needed
                        
                except Exception as e:
                    self.logger.warning("Could not get symbol list from collector", error=str(e))
                    # Fallback to VN30
                    self.active_symbols = self.vn30_symbols
            
            # Initialize symbol metadata
            for symbol in self.active_symbols:
                self.symbol_metadata[symbol] = {
                    "is_vn30": symbol in self.vn30_symbols,
                    "last_update": None,
                    "last_price": None,
                    "data_quality_score": 1.0
                }
            
            self.logger.info("Symbol initialization completed",
                total_symbols=len(self.active_symbols),
                vn30_symbols=len(self.vn30_symbols),
                vn30_only_mode=self.market_config.vn30_symbols_only
            )
            
        except Exception as e:
            self.logger.error("Symbol initialization failed", error=str(e))
            # Set default symbols to ensure system can operate
            self.active_symbols = {"VIC", "VCB", "VHM", "VNM", "HPG"}
            self.vn30_symbols = self.active_symbols.copy()
    
    def _is_market_hours(self) -> bool:
        """Check if current time is within Vietnamese market hours"""
        if not self.market_config.market_hours_only:
            return True
        
        now = datetime.now(timezone.utc)
        current_time = now.strftime("%H:%M")
        
        # Check morning session (02:00-04:30 UTC = 09:00-11:30 UTC+7)
        morning_start = self.market_sessions["morning"]["start"]
        morning_end = self.market_sessions["morning"]["end"]
        
        # Check afternoon session (06:00-08:00 UTC = 13:00-15:00 UTC+7)  
        afternoon_start = self.market_sessions["afternoon"]["start"]
        afternoon_end = self.market_sessions["afternoon"]["end"]
        
        in_morning = morning_start <= current_time <= morning_end
        in_afternoon = afternoon_start <= current_time <= afternoon_end
        
        return in_morning or in_afternoon
    
    def _get_partition_key(self, symbol: str) -> str:
        """Get partition key for symbol-based partitioning"""
        if not self.market_config.partition_by_symbol:
            return "default"
        
        # VN30 symbols get high priority partitions
        if self.market_config.vn30_high_priority and symbol in self.vn30_symbols:
            return f"vn30_{symbol}"
        
        return f"standard_{symbol}"
    
    async def collect_market_data(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Collect market data from configured sources"""
        start_time = time.time()
        collected_data = []
        
        try:
            # Primary source collection
            primary_data = await self._collect_from_source(
                symbols, self.market_config.primary_source
            )
            collected_data.extend(primary_data)
            
            # Fallback source if primary fails or data is incomplete
            if self.market_config.fallback_enabled and len(primary_data) < len(symbols):
                missing_symbols = [s for s in symbols if not any(d.get("symbol") == s for d in primary_data)]
                if missing_symbols:
                    fallback_source = "vnquant" if self.market_config.primary_source == "vnstock" else "vnstock"
                    fallback_data = await self._collect_from_source(missing_symbols, fallback_source)
                    collected_data.extend(fallback_data)
            
            # Update performance stats
            collection_time = time.time() - start_time
            self.performance_stats["symbols_processed"] += len(symbols)
            self.performance_stats["data_points_collected"] += len(collected_data)
            self.performance_stats["collection_time_total"] += collection_time
            
            self.logger.debug("Market data collection completed",
                symbols_requested=len(symbols),
                data_points_collected=len(collected_data),
                collection_time_ms=collection_time * 1000,
                primary_source=self.market_config.primary_source
            )
            
            return collected_data
            
        except Exception as e:
            self.performance_stats["streaming_errors"] += 1
            self.logger.error("Market data collection failed",
                symbols=symbols,
                error=str(e),
                error_type=type(e).__name__
            )
            return []
    
    async def _collect_from_source(self, symbols: List[str], source: str) -> List[Dict[str, Any]]:
        """Collect data from specific source"""
        if source == "vnstock" and self.vnstock_collector:
            return await self._collect_from_vnstock(symbols)
        elif source == "vnquant" and self.vnquant_collector:
            return await self._collect_from_vnquant(symbols)
        else:
            self.logger.warning(f"Source {source} not available or not configured")
            return []
    
    async def _collect_from_vnstock(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Collect data from VnStock using actual API methods"""
        data = []
        
        try:
            # Collect data for each symbol individually (VnStock methods are async)
            for symbol in symbols:
                try:
                    # Use the actual async method from VnStock collector
                    price_data = await self.vnstock_collector.get_realtime_price(symbol)
                    
                    if price_data:
                        # Convert to standard format
                        market_data = self._normalize_vnstock_data(symbol, price_data)
                        if market_data:
                            data.append(market_data)
                            self.logger.debug(f"VnStock data collected for {symbol}")
                    else:
                        self.logger.debug(f"No VnStock data available for {symbol}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to get data for {symbol} from VnStock",
                        symbol=symbol,
                        error=str(e)
                    )
                
                # Rate limiting between symbols
                await asyncio.sleep(0.5)
                
        except Exception as e:
            self.logger.error("VnStock data collection failed", error=str(e))
        
        return data
    
    async def _collect_from_vnquant(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Collect data from VnQuant using actual API methods"""
        data = []
        
        try:
            # Get recent date range for historical data
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            
            # Collect data for each symbol individually (VnQuant methods are async)
            for symbol in symbols:
                try:
                    # Use the actual async method from VnQuant collector
                    historical_data = await self.vnquant_collector.get_historical_data(
                        symbol, start_date, end_date
                    )
                    
                    if historical_data is not None and not historical_data.empty:
                        # Get most recent data point
                        market_data = self._normalize_vnquant_data(symbol, historical_data)
                        if market_data:
                            data.append(market_data)
                            self.logger.debug(f"VnQuant data collected for {symbol}")
                    else:
                        self.logger.debug(f"No VnQuant data available for {symbol}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to get data for {symbol} from VnQuant",
                        symbol=symbol,
                        error=str(e)
                    )
                
                # Rate limiting between symbols (VnQuant has stricter limits)
                await asyncio.sleep(2.0)
                
        except Exception as e:
            self.logger.error("VnQuant data collection failed", error=str(e))
        
        return data
    
    def _normalize_vnstock_data(self, symbol: str, data: Any) -> Optional[Dict[str, Any]]:
        """Normalize VnStock data to standard format"""
        try:
            # VnStock get_realtime_price returns a dict, not DataFrame
            if not data:
                return None
            
            # Handle both dict and DataFrame formats
            if isinstance(data, dict):
                latest = data
            elif hasattr(data, 'iloc') and len(data) > 0:
                latest = data.iloc[-1] if len(data) > 1 else data.iloc[0]
            else:
                self.logger.warning(f"Unexpected VnStock data format for {symbol}: {type(data)}")
                return None
            
            # Extract price data with fallbacks
            price = float(latest.get("price", latest.get("close", 0)))
            
            # Handle None values safely
            def safe_float(value, default=0.0):
                if value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
                    
            def safe_int(value, default=0):
                if value is None:
                    return default
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            return {
                "symbol": symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "vnstock",
                "data_type": "real_time",
                "ohlcv": {
                    "open": safe_float(latest.get("open"), price),
                    "high": safe_float(latest.get("high"), price),
                    "low": safe_float(latest.get("low"), price),
                    "close": safe_float(latest.get("close"), price),
                    "volume": safe_int(latest.get("volume", latest.get("totalVolume")))
                },
                "market_data": {
                    "bid": safe_float(latest.get("bid", latest.get("bidPrice")), price * 0.999),
                    "ask": safe_float(latest.get("ask", latest.get("askPrice")), price * 1.001),
                    "change": safe_float(latest.get("change", latest.get("priceChange"))),
                    "change_percent": safe_float(latest.get("change_percent", latest.get("percentChange")))
                },
                "raw_data": latest  # Keep original data for debugging
            }
            
        except Exception as e:
            self.logger.error("VnStock data normalization failed",
                symbol=symbol,
                error=str(e),
                data_sample=str(data)[:200] if data else "None"
            )
            return None
    
    def _normalize_vnquant_data(self, symbol: str, data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Normalize VnQuant data to standard format"""
        try:
            if data is None or data.empty:
                return None
            
            # Get most recent row
            latest = data.iloc[-1]
            
            # Convert Series to dict if needed
            if hasattr(latest, 'to_dict'):
                latest_dict = latest.to_dict()
            else:
                latest_dict = latest
            
            return {
                "symbol": symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "vnquant",
                "data_type": "historical",
                "ohlcv": {
                    "open": float(latest_dict.get("open", latest_dict.get("Open", 0))),
                    "high": float(latest_dict.get("high", latest_dict.get("High", 0))),
                    "low": float(latest_dict.get("low", latest_dict.get("Low", 0))),
                    "close": float(latest_dict.get("close", latest_dict.get("Close", 0))),
                    "volume": int(latest_dict.get("volume", latest_dict.get("Volume", 0)))
                },
                "market_data": {
                    "value": float(latest_dict.get("value", latest_dict.get("Value", 0))),
                    "change": float(latest_dict.get("change", latest_dict.get("Change", 0))),
                    "change_percent": float(latest_dict.get("change_percent", latest_dict.get("PercentChange", 0)))
                },
                "raw_data": latest_dict  # Keep original data for debugging
            }
            
        except Exception as e:
            self.logger.error("VnQuant data normalization failed",
                symbol=symbol,
                error=str(e),
                data_sample=str(data.head() if hasattr(data, 'head') else data)[:200] if data is not None else "None"
            )
            return None
    
    async def process_message(self, value: dict, metadata: MessageMetadata) -> dict:
        """Process market data message before sending"""
        # Add Vietnamese market specific processing
        processed_data = value.copy()
        
        # Add market session information
        processed_data["market_session"] = self._get_current_market_session()
        
        # Add symbol metadata
        symbol = value.get("symbol", "")
        if symbol in self.symbol_metadata:
            processed_data["symbol_metadata"] = {
                "is_vn30": self.symbol_metadata[symbol]["is_vn30"],
                "data_quality_score": self.symbol_metadata[symbol]["data_quality_score"]
            }
        
        return processed_data
    
    def validate_message(self, value: dict) -> bool:
        """Validate market data message"""
        try:
            self.data_quality_stats["messages_validated"] += 1
            
            # Basic structure validation
            required_fields = ["symbol", "timestamp", "source", "ohlcv"]
            for field in required_fields:
                if field not in value:
                    self.data_quality_stats["validation_failures"] += 1
                    return False
            
            # OHLCV validation
            ohlcv = value.get("ohlcv", {})
            required_ohlcv = ["open", "high", "low", "close", "volume"]
            for field in required_ohlcv:
                if field not in ohlcv:
                    self.data_quality_stats["validation_failures"] += 1
                    return False
            
            # Price relationship validation
            o, h, l, c = ohlcv["open"], ohlcv["high"], ohlcv["low"], ohlcv["close"]
            if not (l <= o <= h and l <= c <= h):
                self.data_quality_stats["validation_failures"] += 1
                return False
            
            # Price change validation (detect anomalies)
            symbol = value["symbol"]
            if symbol in self.last_data_cache:
                last_price = self.last_data_cache[symbol].get("close", c)
                change_percent = abs(c - last_price) / last_price if last_price > 0 else 0
                
                if change_percent > self.market_config.price_change_threshold:
                    self.data_quality_stats["price_anomalies"] += 1
                    self.logger.warning("Price anomaly detected",
                        symbol=symbol,
                        last_price=last_price,
                        current_price=c,
                        change_percent=change_percent
                    )
                    return False
            
            # Update cache
            self.last_data_cache[symbol] = ohlcv
            
            # Volume validation
            if ohlcv["volume"] < self.market_config.volume_threshold:
                return False  # Skip low volume updates
            
            # Data freshness validation
            message_time = datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
            age_seconds = (datetime.now(timezone.utc) - message_time).total_seconds()
            
            if age_seconds > self.market_config.data_freshness_threshold:
                self.data_quality_stats["stale_data_rejected"] += 1
                return False
            
            return True
            
        except Exception as e:
            self.logger.error("Message validation failed",
                error=str(e),
                message_sample=str(value)[:200]
            )
            self.data_quality_stats["validation_failures"] += 1
            return False
    
    def _get_current_market_session(self) -> str:
        """Get current market session"""
        if not self._is_market_hours():
            return "closed"
        
        now = datetime.now(timezone.utc)
        current_time = now.strftime("%H:%M")
        
        morning_start = self.market_sessions["morning"]["start"]
        morning_end = self.market_sessions["morning"]["end"]
        
        if morning_start <= current_time <= morning_end:
            return "morning"
        else:
            return "afternoon"
    
    async def start_streaming(self):
        """Start real-time market data streaming"""
        if self.is_streaming:
            self.logger.warning("Streaming already active")
            return
        
        self.is_streaming = True
        self.streaming_task = asyncio.create_task(self._streaming_loop())
        
        self.logger.info("Market data streaming started",
            symbols_count=len(self.active_symbols),
            interval_seconds=self.market_config.real_time_interval,
            market_hours_only=self.market_config.market_hours_only
        )
    
    async def stop_streaming(self):
        """Stop real-time market data streaming"""
        if not self.is_streaming:
            return
        
        self.is_streaming = False
        
        if self.streaming_task:
            self.streaming_task.cancel()
            try:
                await self.streaming_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Market data streaming stopped")
    
    async def _streaming_loop(self):
        """Main streaming loop"""
        symbols_list = list(self.active_symbols)
        batch_size = self.market_config.max_batch_size
        
        while self.is_streaming:
            try:
                # Check market hours
                if not self._is_market_hours():
                    self.logger.debug("Outside market hours, sleeping...")
                    await asyncio.sleep(60)  # Check every minute during off-hours
                    continue
                
                # Process symbols in batches
                for i in range(0, len(symbols_list), batch_size):
                    batch_symbols = symbols_list[i:i + batch_size]
                    
                    # Collect data
                    market_data_list = await self.collect_market_data(batch_symbols)
                    
                    # Send to Kafka
                    if market_data_list:
                        await self._send_market_data_batch(market_data_list)
                
                # Wait before next collection
                await asyncio.sleep(self.market_config.real_time_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.performance_stats["streaming_errors"] += 1
                self.logger.error("Streaming loop error", error=str(e))
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _send_market_data_batch(self, market_data_list: List[Dict[str, Any]]):
        """Send batch of market data to Kafka"""
        tasks = []
        
        for market_data in market_data_list:
            if self.validate_message(market_data):
                symbol = market_data["symbol"]
                partition_key = self._get_partition_key(symbol)
                
                task = self.send_message_async(
                    key=partition_key,
                    value=market_data,
                    source="market_data_stream",
                    headers={
                        "symbol": symbol,
                        "data_type": market_data.get("data_type", "real_time"),
                        "market_session": self._get_current_market_session()
                    }
                )
                tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            
            self.logger.debug("Market data batch sent",
                total_messages=len(tasks),
                successful=success_count,
                failed=len(tasks) - success_count
            )
    
    async def send_single_symbol_data(self, symbol: str) -> bool:
        """Send data for a single symbol (for testing/manual triggers)"""
        market_data_list = await self.collect_market_data([symbol])
        
        if market_data_list:
            await self._send_market_data_batch(market_data_list)
            return True
        return False
    
    def get_streaming_stats(self) -> Dict[str, Any]:
        """Get comprehensive streaming statistics"""
        base_metrics = self.get_metrics()
        
        streaming_stats = {
            "streaming_active": self.is_streaming,
            "market_hours_active": self._is_market_hours(),
            "current_session": self._get_current_market_session(),
            "active_symbols_count": len(self.active_symbols),
            "vn30_symbols_count": len(self.vn30_symbols),
            "performance": self.performance_stats.copy(),
            "data_quality": self.data_quality_stats.copy(),
            "avg_collection_time_ms": (
                (self.performance_stats["collection_time_total"] / self.performance_stats["symbols_processed"] * 1000)
                if self.performance_stats["symbols_processed"] > 0 else 0
            )
        }
        
        return {**base_metrics, **streaming_stats}


# Export
__all__ = ["MarketDataProducer", "MarketDataConfig"]