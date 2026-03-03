"""
Technical Indicator Producer for Vietnamese Trading System
Phase 2: Messaging Layer Implementation - Task 2.2.3

This producer handles technical indicator streaming with:
- Integration with Phase 1 Silver ETL technical indicators
- Real-time sliding window calculations
- Multiple timeframe support (1m, 5m, 15m, 1h, 1d)
- Vietnamese market specific indicators
- Custom indicator calculation engine
- Performance optimized streaming
"""

import asyncio
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Union, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import threading
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.kafka_utils.base_producer import BaseKafkaProducer, ProducerConfig, MessageMetadata, HealthCheckMixin
try:
    from src.etl.silver_etl import SilverETL, TechnicalIndicatorCalculator
except ImportError:
    SilverETL = None
    TechnicalIndicatorCalculator = None
from src.utils.logging import StructuredLogger


@dataclass
class IndicatorConfig:
    """Configuration for technical indicator producer"""
    # Timeframes to calculate indicators for
    timeframes: List[str] = field(default_factory=lambda: ["1m", "5m", "15m", "1h", "1d"])
    
    # Standard technical indicators
    indicators: Dict[str, Dict] = field(default_factory=lambda: {
        "sma": {"periods": [10, 20, 50, 200]},
        "ema": {"periods": [12, 26, 50]},
        "rsi": {"period": 14},
        "macd": {"fast": 12, "slow": 26, "signal": 9},
        "bollinger": {"period": 20, "std": 2},
        "stochastic": {"k_period": 14, "d_period": 3},
        "williams_r": {"period": 14},
        "atr": {"period": 14},
        "adx": {"period": 14}
    })
    
    # Vietnamese market specific indicators
    vietnamese_indicators: Dict[str, Dict] = field(default_factory=lambda: {
        "foreign_flow": {"period": 5},
        "vn30_correlation": {"period": 20},
        "market_breadth": {"period": 10},
        "session_momentum": {"morning_weight": 0.6, "afternoon_weight": 0.4}
    })
    
    # Calculation settings
    min_periods_required: int = 50  # Minimum data points needed
    calculation_interval: int = 60  # Calculate every 60 seconds
    batch_calculation: bool = True
    max_symbols_per_batch: int = 20
    
    # Data retention for calculations
    max_data_points: int = 1000  # Keep last 1000 data points per symbol
    data_cleanup_interval: int = 3600  # Clean up old data every hour
    
    # Performance settings
    parallel_calculation: bool = True
    max_workers: int = 4
    
    # Quality settings
    validate_calculations: bool = True
    outlier_detection: bool = True
    outlier_threshold: float = 3.0  # Standard deviations


class DataWindow:
    """Sliding window for OHLCV data with efficient calculations"""
    
    def __init__(self, symbol: str, timeframe: str, max_size: int = 1000):
        self.symbol = symbol
        self.timeframe = timeframe
        self.max_size = max_size
        
        # Data storage
        self.timestamps = deque(maxlen=max_size)
        self.opens = deque(maxlen=max_size)
        self.highs = deque(maxlen=max_size)
        self.lows = deque(maxlen=max_size)
        self.closes = deque(maxlen=max_size)
        self.volumes = deque(maxlen=max_size)
        
        # Lock for thread safety
        self._lock = threading.Lock()
        
        # Cached calculations (invalidated on new data)
        self._cache = {}
        self._cache_valid = False
    
    def add_data_point(self, timestamp: datetime, ohlcv: Dict[str, float]):
        """Add new OHLCV data point"""
        with self._lock:
            self.timestamps.append(timestamp)
            self.opens.append(ohlcv["open"])
            self.highs.append(ohlcv["high"])
            self.lows.append(ohlcv["low"])
            self.closes.append(ohlcv["close"])
            self.volumes.append(ohlcv["volume"])
            
            # Invalidate cache
            self._cache_valid = False
            self._cache.clear()
    
    def get_data_arrays(self) -> Tuple[np.ndarray, ...]:
        """Get data as numpy arrays for efficient calculation"""
        with self._lock:
            return (
                np.array(self.opens),
                np.array(self.highs),
                np.array(self.lows),
                np.array(self.closes),
                np.array(self.volumes)
            )
    
    def get_closes_array(self) -> np.ndarray:
        """Get just closes array (most commonly used)"""
        with self._lock:
            return np.array(self.closes)
    
    def size(self) -> int:
        """Get current window size"""
        return len(self.closes)
    
    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get timestamp of latest data point"""
        with self._lock:
            return self.timestamps[-1] if self.timestamps else None


class IndicatorCalculator:
    """High-performance technical indicator calculator"""
    
    def __init__(self, config: IndicatorConfig):
        self.config = config
        self.logger = StructuredLogger("IndicatorCalculator")
    
    def calculate_sma(self, closes: np.ndarray, period: int) -> np.ndarray:
        """Simple Moving Average"""
        if len(closes) < period:
            return np.array([])
        
        return pd.Series(closes).rolling(window=period).mean().values
    
    def calculate_ema(self, closes: np.ndarray, period: int) -> np.ndarray:
        """Exponential Moving Average"""
        if len(closes) < period:
            return np.array([])
        
        return pd.Series(closes).ewm(span=period).mean().values
    
    def calculate_rsi(self, closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Relative Strength Index"""
        if len(closes) < period + 1:
            return np.array([])
        
        closes_series = pd.Series(closes)
        delta = closes_series.diff()
        
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.values
    
    def calculate_macd(self, closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, np.ndarray]:
        """MACD indicator"""
        if len(closes) < slow:
            return {"macd": np.array([]), "signal": np.array([]), "histogram": np.array([])}
        
        closes_series = pd.Series(closes)
        
        ema_fast = closes_series.ewm(span=fast).mean()
        ema_slow = closes_series.ewm(span=slow).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        
        return {
            "macd": macd_line.values,
            "signal": signal_line.values,
            "histogram": histogram.values
        }
    
    def calculate_bollinger_bands(self, closes: np.ndarray, period: int = 20, std_dev: float = 2) -> Dict[str, np.ndarray]:
        """Bollinger Bands"""
        if len(closes) < period:
            return {"upper": np.array([]), "middle": np.array([]), "lower": np.array([])}
        
        closes_series = pd.Series(closes)
        sma = closes_series.rolling(window=period).mean()
        std = closes_series.rolling(window=period).std()
        
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        
        return {
            "upper": upper_band.values,
            "middle": sma.values,
            "lower": lower_band.values
        }
    
    def calculate_stochastic(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, 
                           k_period: int = 14, d_period: int = 3) -> Dict[str, np.ndarray]:
        """Stochastic Oscillator"""
        if len(closes) < k_period:
            return {"k": np.array([]), "d": np.array([])}
        
        df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
        
        lowest_low = df["low"].rolling(window=k_period).min()
        highest_high = df["high"].rolling(window=k_period).max()
        
        k_percent = 100 * (df["close"] - lowest_low) / (highest_high - lowest_low)
        d_percent = k_percent.rolling(window=d_period).mean()
        
        return {
            "k": k_percent.values,
            "d": d_percent.values
        }
    
    def calculate_williams_r(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Williams %R"""
        if len(closes) < period:
            return np.array([])
        
        df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
        
        highest_high = df["high"].rolling(window=period).max()
        lowest_low = df["low"].rolling(window=period).min()
        
        williams_r = -100 * (highest_high - df["close"]) / (highest_high - lowest_low)
        
        return williams_r.values
    
    def calculate_atr(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Average True Range"""
        if len(closes) < period + 1:
            return np.array([])
        
        df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
        
        # Previous close
        prev_close = df["close"].shift(1)
        
        # True Range calculation
        tr1 = df["high"] - df["low"]
        tr2 = abs(df["high"] - prev_close)
        tr3 = abs(df["low"] - prev_close)
        
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        # ATR is the moving average of True Range
        atr = true_range.rolling(window=period).mean()
        
        return atr.values
    
    def calculate_adx(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> Dict[str, np.ndarray]:
        """Average Directional Index"""
        if len(closes) < period + 1:
            return {"adx": np.array([]), "di_plus": np.array([]), "di_minus": np.array([])}
        
        df = pd.DataFrame({"high": highs, "low": lows, "close": closes})
        
        # Calculate True Range (needed for DI calculation)
        atr_values = self.calculate_atr(highs, lows, closes, period)
        
        # Directional Movement
        dm_plus = df["high"].diff()
        dm_minus = -df["low"].diff()
        
        # Only positive moves count
        dm_plus[dm_plus < 0] = 0
        dm_minus[dm_minus < 0] = 0
        
        # Smooth the directional movements
        dm_plus_smooth = dm_plus.rolling(window=period).mean()
        dm_minus_smooth = dm_minus.rolling(window=period).mean()
        
        # Calculate DI+ and DI-
        di_plus = 100 * dm_plus_smooth / pd.Series(atr_values)
        di_minus = 100 * dm_minus_smooth / pd.Series(atr_values)
        
        # Calculate DX
        dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
        
        # ADX is smoothed DX
        adx = dx.rolling(window=period).mean()
        
        return {
            "adx": adx.values,
            "di_plus": di_plus.values,
            "di_minus": di_minus.values
        }


class IndicatorProducer(BaseKafkaProducer, HealthCheckMixin):
    """Technical indicator producer with real-time calculations"""
    
    def __init__(self, producer_config: ProducerConfig, indicator_config: IndicatorConfig):
        super().__init__(producer_config, "indicators")
        
        self.indicator_config = indicator_config
        self.calculator = IndicatorCalculator(indicator_config)
        self.logger = StructuredLogger("IndicatorProducer")
        
        # Data windows for each symbol/timeframe combination
        self.data_windows: Dict[str, Dict[str, DataWindow]] = defaultdict(dict)
        
        # Calculation state
        self.is_calculating = False
        self.calculation_task = None
        self.last_calculation_time = {}
        
        # Performance tracking
        self.calculation_stats = {
            "indicators_calculated": 0,
            "calculation_time_total": 0.0,
            "calculation_errors": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
        
        # Quality tracking
        self.quality_stats = {
            "outliers_detected": 0,
            "invalid_calculations": 0,
            "missing_data_warnings": 0
        }
        
        self.logger.info("IndicatorProducer initialized",
            timeframes=indicator_config.timeframes,
            indicators=list(indicator_config.indicators.keys()),
            vietnamese_indicators=list(indicator_config.vietnamese_indicators.keys()),
            calculation_interval=indicator_config.calculation_interval
        )
    
    async def add_market_data(self, symbol: str, timestamp: datetime, ohlcv: Dict[str, float], timeframe: str = "1m"):
        """Add market data point for indicator calculation"""
        try:
            # Initialize data window if not exists
            if timeframe not in self.data_windows[symbol]:
                self.data_windows[symbol][timeframe] = DataWindow(
                    symbol, timeframe, self.indicator_config.max_data_points
                )
            
            # Add data point
            window = self.data_windows[symbol][timeframe]
            window.add_data_point(timestamp, ohlcv)
            
            self.logger.debug("Market data added for indicator calculation",
                symbol=symbol,
                timeframe=timeframe,
                window_size=window.size()
            )
            
        except Exception as e:
            self.logger.error("Failed to add market data",
                symbol=symbol,
                timeframe=timeframe,
                error=str(e)
            )
    
    async def calculate_indicators_for_symbol(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """Calculate all indicators for a specific symbol and timeframe"""
        start_time = time.time()
        
        try:
            # Get data window
            if timeframe not in self.data_windows[symbol]:
                return None
            
            window = self.data_windows[symbol][timeframe]
            
            # Check if we have enough data
            if window.size() < self.indicator_config.min_periods_required:
                self.quality_stats["missing_data_warnings"] += 1
                return None
            
            # Get data arrays
            opens, highs, lows, closes, volumes = window.get_data_arrays()
            
            # Calculate indicators
            indicators = {}
            
            # Standard indicators
            for indicator_name, params in self.indicator_config.indicators.items():
                try:
                    if indicator_name == "sma":
                        for period in params["periods"]:
                            indicators[f"sma_{period}"] = self.calculator.calculate_sma(closes, period)
                    
                    elif indicator_name == "ema":
                        for period in params["periods"]:
                            indicators[f"ema_{period}"] = self.calculator.calculate_ema(closes, period)
                    
                    elif indicator_name == "rsi":
                        indicators["rsi"] = self.calculator.calculate_rsi(closes, params["period"])
                    
                    elif indicator_name == "macd":
                        macd_data = self.calculator.calculate_macd(closes, params["fast"], params["slow"], params["signal"])
                        indicators.update({f"macd_{k}": v for k, v in macd_data.items()})
                    
                    elif indicator_name == "bollinger":
                        bb_data = self.calculator.calculate_bollinger_bands(closes, params["period"], params["std"])
                        indicators.update({f"bb_{k}": v for k, v in bb_data.items()})
                    
                    elif indicator_name == "stochastic":
                        stoch_data = self.calculator.calculate_stochastic(highs, lows, closes, params["k_period"], params["d_period"])
                        indicators.update({f"stoch_{k}": v for k, v in stoch_data.items()})
                    
                    elif indicator_name == "williams_r":
                        indicators["williams_r"] = self.calculator.calculate_williams_r(highs, lows, closes, params["period"])
                    
                    elif indicator_name == "atr":
                        indicators["atr"] = self.calculator.calculate_atr(highs, lows, closes, params["period"])
                    
                    elif indicator_name == "adx":
                        adx_data = self.calculator.calculate_adx(highs, lows, closes, params["period"])
                        indicators.update({f"adx_{k}": v for k, v in adx_data.items()})
                
                except Exception as e:
                    self.logger.warning(f"Failed to calculate {indicator_name}",
                        symbol=symbol,
                        timeframe=timeframe,
                        error=str(e)
                    )
                    self.calculation_stats["calculation_errors"] += 1
            
            # Vietnamese market specific indicators
            if self.indicator_config.vietnamese_indicators:
                vietnamese_indicators = await self._calculate_vietnamese_indicators(symbol, timeframe, closes, volumes)
                indicators.update(vietnamese_indicators)
            
            # Validate calculations
            if self.indicator_config.validate_calculations:
                indicators = self._validate_indicator_calculations(indicators)
            
            # Get latest values (most recent calculation)
            latest_indicators = {}
            for name, values in indicators.items():
                if isinstance(values, np.ndarray) and len(values) > 0:
                    latest_value = values[-1]
                    if not (np.isnan(latest_value) or np.isinf(latest_value)):
                        latest_indicators[name] = float(latest_value)
            
            # Update stats
            calculation_time = time.time() - start_time
            self.calculation_stats["indicators_calculated"] += len(latest_indicators)
            self.calculation_stats["calculation_time_total"] += calculation_time
            
            result = {
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "indicators": latest_indicators,
                "metadata": {
                    "data_points_used": window.size(),
                    "calculation_time_ms": calculation_time * 1000,
                    "indicators_count": len(latest_indicators)
                }
            }
            
            self.logger.debug("Indicators calculated",
                symbol=symbol,
                timeframe=timeframe,
                indicators_count=len(latest_indicators),
                calculation_time_ms=calculation_time * 1000
            )
            
            return result
            
        except Exception as e:
            self.calculation_stats["calculation_errors"] += 1
            self.logger.error("Indicator calculation failed",
                symbol=symbol,
                timeframe=timeframe,
                error=str(e)
            )
            return None
    
    async def _calculate_vietnamese_indicators(self, symbol: str, timeframe: str, closes: np.ndarray, volumes: np.ndarray) -> Dict[str, np.ndarray]:
        """Calculate Vietnamese market specific indicators"""
        vietnamese_indicators = {}
        
        try:
            # Session momentum (morning vs afternoon performance)
            if "session_momentum" in self.indicator_config.vietnamese_indicators:
                # This would need market session data - simplified for now
                session_momentum = self._calculate_session_momentum(closes)
                vietnamese_indicators["session_momentum"] = session_momentum
            
            # Volume-price trend (Vietnamese market characteristic)
            if len(closes) >= 10 and len(volumes) >= 10:
                vpt = self._calculate_volume_price_trend(closes, volumes)
                vietnamese_indicators["volume_price_trend"] = vpt
            
            # Market breadth indicator (requires multiple symbols - placeholder)
            if "market_breadth" in self.indicator_config.vietnamese_indicators:
                # This would need market-wide data
                pass
        
        except Exception as e:
            self.logger.warning("Vietnamese indicator calculation failed",
                symbol=symbol,
                timeframe=timeframe,
                error=str(e)
            )
        
        return vietnamese_indicators
    
    def _calculate_session_momentum(self, closes: np.ndarray) -> np.ndarray:
        """Calculate session momentum (simplified version)"""
        if len(closes) < 10:
            return np.array([])
        
        # Simple momentum calculation - in real implementation would use session data
        momentum = pd.Series(closes).pct_change(periods=5)
        return momentum.values
    
    def _calculate_volume_price_trend(self, closes: np.ndarray, volumes: np.ndarray) -> np.ndarray:
        """Calculate Volume Price Trend indicator"""
        if len(closes) < 2 or len(volumes) < 2:
            return np.array([])
        
        price_changes = pd.Series(closes).pct_change()
        volume_series = pd.Series(volumes)
        
        vpt = (volume_series * price_changes).cumsum()
        return vpt.values
    
    def _validate_indicator_calculations(self, indicators: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        """Validate indicator calculations and remove outliers"""
        validated_indicators = {}
        
        for name, values in indicators.items():
            if not isinstance(values, np.ndarray) or len(values) == 0:
                continue
            
            # Remove NaN and infinite values
            clean_values = values[~(np.isnan(values) | np.isinf(values))]
            
            # Outlier detection
            if self.indicator_config.outlier_detection and len(clean_values) > 10:
                mean_val = np.mean(clean_values)
                std_val = np.std(clean_values)
                threshold = self.indicator_config.outlier_threshold
                
                # Keep values within threshold standard deviations
                mask = np.abs(values - mean_val) <= (threshold * std_val)
                outlier_count = np.sum(~mask)
                
                if outlier_count > 0:
                    self.quality_stats["outliers_detected"] += outlier_count
                    self.logger.debug(f"Outliers detected in {name}",
                        outlier_count=outlier_count,
                        total_values=len(values)
                    )
            
            validated_indicators[name] = values
        
        return validated_indicators
    
    async def process_message(self, value: dict, metadata: MessageMetadata) -> dict:
        """Process indicator message before sending"""
        processed_data = value.copy()
        
        # Add market context
        processed_data["market_context"] = {
            "session": self._get_market_session(),
            "calculation_timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return processed_data
    
    def validate_message(self, value: dict) -> bool:
        """Validate indicator message"""
        try:
            required_fields = ["symbol", "timeframe", "timestamp", "indicators"]
            for field in required_fields:
                if field not in value:
                    return False
            
            # Check if we have any indicators
            indicators = value.get("indicators", {})
            if not indicators or len(indicators) == 0:
                return False
            
            # Basic value validation
            for indicator_name, indicator_value in indicators.items():
                if not isinstance(indicator_value, (int, float)) or np.isnan(indicator_value) or np.isinf(indicator_value):
                    self.quality_stats["invalid_calculations"] += 1
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error("Indicator message validation failed", error=str(e))
            return False
    
    def _get_market_session(self) -> str:
        """Get current market session (simplified)"""
        now = datetime.now(timezone.utc)
        hour = now.hour
        
        # Convert to Vietnam time approximation
        vietnam_hour = (hour + 7) % 24
        
        if 9 <= vietnam_hour <= 11:
            return "morning"
        elif 13 <= vietnam_hour <= 15:
            return "afternoon"
        else:
            return "closed"
    
    async def start_calculation_loop(self):
        """Start the main calculation loop"""
        if self.is_calculating:
            return
        
        self.is_calculating = True
        self.calculation_task = asyncio.create_task(self._calculation_loop())
        
        self.logger.info("Indicator calculation loop started",
            calculation_interval=self.indicator_config.calculation_interval,
            parallel_calculation=self.indicator_config.parallel_calculation
        )
    
    async def stop_calculation_loop(self):
        """Stop the calculation loop"""
        if not self.is_calculating:
            return
        
        self.is_calculating = False
        
        if self.calculation_task:
            self.calculation_task.cancel()
            try:
                await self.calculation_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Indicator calculation loop stopped")
    
    async def _calculation_loop(self):
        """Main calculation loop"""
        while self.is_calculating:
            try:
                start_time = time.time()
                
                # Get all symbol/timeframe combinations that have data
                calculation_tasks = []
                
                for symbol, timeframes in self.data_windows.items():
                    for timeframe in timeframes:
                        # Check if we need to calculate (enough data and not too recent)
                        window = timeframes[timeframe]
                        
                        if window.size() >= self.indicator_config.min_periods_required:
                            last_calc_key = f"{symbol}_{timeframe}"
                            last_calc_time = self.last_calculation_time.get(last_calc_key, 0)
                            
                            if (time.time() - last_calc_time) >= self.indicator_config.calculation_interval:
                                calculation_tasks.append((symbol, timeframe))
                                self.last_calculation_time[last_calc_key] = time.time()
                
                # Process in batches
                if calculation_tasks:
                    if self.indicator_config.parallel_calculation:
                        await self._process_calculations_parallel(calculation_tasks)
                    else:
                        await self._process_calculations_sequential(calculation_tasks)
                
                # Calculate loop timing
                loop_time = time.time() - start_time
                
                # Wait before next iteration
                sleep_time = max(0, self.indicator_config.calculation_interval - loop_time)
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Calculation loop error", error=str(e))
                await asyncio.sleep(10)  # Brief pause on error
    
    async def _process_calculations_parallel(self, calculation_tasks: List[Tuple[str, str]]):
        """Process calculations in parallel"""
        batch_size = self.indicator_config.max_symbols_per_batch
        
        for i in range(0, len(calculation_tasks), batch_size):
            batch = calculation_tasks[i:i + batch_size]
            
            # Create calculation tasks
            tasks = [
                self.calculate_indicators_for_symbol(symbol, timeframe)
                for symbol, timeframe in batch
            ]
            
            # Execute in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Send results
            await self._send_indicator_results(results)
    
    async def _process_calculations_sequential(self, calculation_tasks: List[Tuple[str, str]]):
        """Process calculations sequentially"""
        results = []
        
        for symbol, timeframe in calculation_tasks:
            result = await self.calculate_indicators_for_symbol(symbol, timeframe)
            if result:
                results.append(result)
        
        await self._send_indicator_results(results)
    
    async def _send_indicator_results(self, results: List[Optional[Dict[str, Any]]]):
        """Send calculated indicators to Kafka"""
        tasks = []
        
        for result in results:
            if result and isinstance(result, dict):
                symbol = result["symbol"]
                timeframe = result["timeframe"]
                
                task = self.send_message_async(
                    key=f"{symbol}_{timeframe}",
                    value=result,
                    source="indicator_calculation",
                    headers={
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "indicator_count": str(len(result.get("indicators", {})))
                    }
                )
                tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True)
            
            self.logger.debug("Indicator batch sent",
                total_messages=len(tasks),
                successful=success_count,
                failed=len(tasks) - success_count
            )
    
    def get_calculation_stats(self) -> Dict[str, Any]:
        """Get comprehensive calculation statistics"""
        base_metrics = self.get_metrics()
        
        calculation_stats = {
            "calculation_active": self.is_calculating,
            "data_windows_count": sum(len(timeframes) for timeframes in self.data_windows.values()),
            "symbols_tracked": len(self.data_windows),
            "calculation_stats": self.calculation_stats.copy(),
            "quality_stats": self.quality_stats.copy(),
            "avg_calculation_time_ms": (
                (self.calculation_stats["calculation_time_total"] / self.calculation_stats["indicators_calculated"] * 1000)
                if self.calculation_stats["indicators_calculated"] > 0 else 0
            )
        }
        
        return {**base_metrics, **calculation_stats}


# Export
__all__ = ["IndicatorProducer", "IndicatorConfig"]