"""Moving Average Crossover Strategy implementation with Vietnamese market support."""

from typing import Dict, Any, List
from src.strategies.base_strategy import BaseStrategy, SignalType
import pandas as pd
import numpy as np
from datetime import datetime

class MovingAverageCrossoverStrategy(BaseStrategy):
    """Moving Average Crossover trading strategy for Vietnamese market.
    
    Generates BUY signals on golden cross (short MA > long MA)
    Generates SELL signals on death cross (short MA < long MA)
    Includes volume confirmation and Vietnamese market session filtering.
    """
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__("MA_Crossover", params)
        
        # Moving average parameters
        self.short_period = params.get('short_period', 10)
        self.long_period = params.get('long_period', 20)
        self.ema_alpha = params.get('ema_alpha', 0.1)  # EMA smoothing factor
        
        # Signal parameters
        self.min_volume_ratio = params.get('min_volume_ratio', 1.2)  # Volume confirmation
        self.price_change_threshold = params.get('price_change_threshold', 0.005)  # 0.5% minimum price change
        self.confidence_base = params.get('confidence_base', 0.7)  # Base confidence level
        
        # Multi-timeframe support
        self.timeframes = params.get('timeframes', ['5min', '15min'])
        
        # Internal buffers
        self.price_buffer = {}
        self.volume_buffer = {}
        self.ma_buffer = {}
        self.last_signals = {}
        
        # Vietnamese market specific (centralized)
        from src.utils.market_config import VN30_SYMBOLS
        self.vn30_symbols = VN30_SYMBOLS
        
    def _initialize_strategy(self):
        """Initialize strategy-specific parameters and indicators."""
        self.logger.info(f"Initializing MA Crossover Strategy with periods: {self.short_period}/{self.long_period}")
        
        # Initialize buffers for each timeframe
        for timeframe in self.timeframes:
            self.price_buffer[timeframe] = pd.DataFrame()
            self.volume_buffer[timeframe] = pd.DataFrame()
            self.ma_buffer[timeframe] = {}
    
    def calculate_moving_averages(self, prices: pd.Series, volumes: pd.Series = None):
        """Calculate Simple and Exponential Moving Averages with volume weighting."""
        if len(prices) < max(self.short_period, self.long_period):
            return None, None, None, None
            
        # Simple Moving Averages
        sma_short = prices.rolling(window=self.short_period).mean()
        sma_long = prices.rolling(window=self.long_period).mean()
        
        # Exponential Moving Averages
        ema_short = prices.ewm(span=self.short_period, adjust=False).mean()
        ema_long = prices.ewm(span=self.long_period, adjust=False).mean()
        
        return sma_short, sma_long, ema_short, ema_long
    
    def calculate_volume_confirmation(self, volumes: pd.Series, window: int = 20):
        """Calculate volume-based confirmation signals."""
        if len(volumes) < window:
            return pd.Series([1.0] * len(volumes), index=volumes.index)
            
        volume_ma = volumes.rolling(window=window).mean()
        volume_ratio = volumes / volume_ma
        
        return volume_ratio
    
    def detect_crossover_signals(self, short_ma: pd.Series, long_ma: pd.Series, 
                                prices: pd.Series, volumes: pd.Series = None):
        """Detect golden cross and death cross signals with confirmations."""
        signals = []
        
        if len(short_ma) < 2 or len(long_ma) < 2:
            return signals
            
        # Calculate crossovers
        ma_diff = short_ma - long_ma
        ma_diff_prev = ma_diff.shift(1)
        
        # Volume confirmation
        volume_confirmation = pd.Series([1.0] * len(prices), index=prices.index)
        if volumes is not None:
            volume_confirmation = self.calculate_volume_confirmation(volumes)
        
        # Price change confirmation
        price_change = prices.pct_change().abs()
        
        for i in range(1, len(ma_diff)):
            current_time = ma_diff.index[i]
            
            # Skip if not enough data
            if pd.isna(ma_diff.iloc[i]) or pd.isna(ma_diff_prev.iloc[i]):
                continue
                
            # Golden Cross: short MA crosses above long MA
            if ma_diff_prev.iloc[i] <= 0 and ma_diff.iloc[i] > 0:
                # Confirmations
                volume_conf = volume_confirmation.iloc[i] >= self.min_volume_ratio
                price_conf = price_change.iloc[i] >= self.price_change_threshold
                
                confidence = self.confidence_base
                if volume_conf:
                    confidence += 0.15
                if price_conf:
                    confidence += 0.1
                    
                confidence = min(confidence, 1.0)
                
                signals.append({
                    'timestamp': current_time,
                    'signal_type': SignalType.BUY,
                    'confidence': confidence,
                    'price': prices.iloc[i],
                    'short_ma': short_ma.iloc[i],
                    'long_ma': long_ma.iloc[i],
                    'volume_ratio': volume_confirmation.iloc[i],
                    'price_change': price_change.iloc[i],
                    'crossover_type': 'golden_cross'
                })
                
            # Death Cross: short MA crosses below long MA
            elif ma_diff_prev.iloc[i] >= 0 and ma_diff.iloc[i] < 0:
                # Confirmations
                volume_conf = volume_confirmation.iloc[i] >= self.min_volume_ratio
                price_conf = price_change.iloc[i] >= self.price_change_threshold
                
                confidence = self.confidence_base
                if volume_conf:
                    confidence += 0.15
                if price_conf:
                    confidence += 0.1
                    
                confidence = min(confidence, 1.0)
                
                signals.append({
                    'timestamp': current_time,
                    'signal_type': SignalType.SELL,
                    'confidence': confidence,
                    'price': prices.iloc[i],
                    'short_ma': short_ma.iloc[i],
                    'long_ma': long_ma.iloc[i],
                    'volume_ratio': volume_confirmation.iloc[i],
                    'price_change': price_change.iloc[i],
                    'crossover_type': 'death_cross'
                })
        
        return signals
    
    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data and update indicators."""
        try:
            if market_data.empty:
                return
                
            # Process each symbol in the market data
            for symbol in market_data.get('symbol', pd.Series()).unique():
                if pd.isna(symbol):
                    continue
                    
                symbol_data = market_data[market_data['symbol'] == symbol].copy()
                
                if symbol_data.empty:
                    continue
                    
                # Update price and volume buffers
                if 'close' in symbol_data.columns:
                    prices = symbol_data.set_index('timestamp')['close'] if 'timestamp' in symbol_data.columns else symbol_data['close']
                    volumes = symbol_data.set_index('timestamp')['volume'] if 'volume' in symbol_data.columns and 'timestamp' in symbol_data.columns else symbol_data.get('volume', pd.Series())
                    
                    # Update buffer for 5min timeframe (default)
                    timeframe = '5min'
                    if symbol not in self.price_buffer.get(timeframe, {}):
                        self.price_buffer.setdefault(timeframe, {})[symbol] = pd.Series(dtype=float)
                        self.volume_buffer.setdefault(timeframe, {})[symbol] = pd.Series(dtype=float)
                    
                    # Append new data and keep last 200 points
                    self.price_buffer[timeframe][symbol] = pd.concat([self.price_buffer[timeframe][symbol], prices]).tail(200)
                    if not volumes.empty:
                        self.volume_buffer[timeframe][symbol] = pd.concat([self.volume_buffer[timeframe][symbol], volumes]).tail(200)
                    
                    # Calculate moving averages
                    if len(self.price_buffer[timeframe][symbol]) >= max(self.short_period, self.long_period):
                        sma_short, sma_long, ema_short, ema_long = self.calculate_moving_averages(
                            self.price_buffer[timeframe][symbol],
                            self.volume_buffer[timeframe].get(symbol, pd.Series())
                        )
                        
                        if sma_short is not None:
                            self.ma_buffer.setdefault(timeframe, {})[symbol] = {
                                'sma_short': sma_short,
                                'sma_long': sma_long,
                                'ema_short': ema_short,
                                'ema_long': ema_long,
                                'last_update': datetime.now(self.vn_timezone)
                            }
                            
                            # Generate signals if market is open
                            if self.is_market_open():
                                signals = self.detect_crossover_signals(
                                    sma_short, sma_long,
                                    self.price_buffer[timeframe][symbol],
                                    self.volume_buffer[timeframe].get(symbol, pd.Series())
                                )
                                
                                # Process and publish new signals
                                for signal_data in signals:
                                    if self._is_new_signal(symbol, signal_data):
                                        signal = self.create_signal(
                                            symbol=symbol,
                                            signal_type=signal_data['signal_type'],
                                            confidence=signal_data['confidence'],
                                            price=signal_data['price'],
                                            metadata={
                                                'strategy_type': 'MA_Crossover',
                                                'short_period': self.short_period,
                                                'long_period': self.long_period,
                                                'short_ma': signal_data['short_ma'],
                                                'long_ma': signal_data['long_ma'],
                                                'volume_ratio': signal_data['volume_ratio'],
                                                'price_change': signal_data['price_change'],
                                                'crossover_type': signal_data['crossover_type'],
                                                'timeframe': timeframe,
                                                'vn30_member': symbol in self.vn30_symbols
                                            }
                                        )
                                        
                                        if signal:
                                            self.publish_signal(signal)
                                            self.last_signals[symbol] = signal_data
                                            
        except Exception as e:
            self.logger.error(f"Error processing market data in MA Crossover strategy: {e}")
    
    def _is_new_signal(self, symbol: str, signal_data: Dict) -> bool:
        """Check if this is a new signal to avoid duplicates."""
        if symbol not in self.last_signals:
            return True
            
        last_signal = self.last_signals[symbol]

        # Block ALL signals (BUY or SELL) for the same symbol within the cooldown window.
        # Previously the direction-change bypass caused rapid BUY→SELL→BUY spam on tick data.
        try:
            import pandas as pd
            def _tz_naive(ts):
                t = pd.to_datetime(ts)
                return t.tz_localize(None) if t.tzinfo else t
            time_diff = (_tz_naive(signal_data['timestamp']) - _tz_naive(last_signal.get('timestamp', signal_data['timestamp']))).total_seconds()
        except (TypeError, ValueError):
            time_diff = 9999  # Can't compare — treat as new signal

        return time_diff > 300  # 5-minute hard cooldown regardless of direction
    
    def generate_signal(self, market_data: pd.DataFrame = None) -> Dict[str, Any]:
        """Generate a single trading signal based on market data.

        This method is called by AdaptiveStrategy. It processes the data
        and returns the most recent signal, or None if no signal.
        """
        if market_data is not None and not market_data.empty:
            self.on_data(market_data)

        signals = self.generate_signals()
        return signals[-1] if signals else None

    def generate_signals(self) -> List[Dict[str, Any]]:
        """Generate trading signals based on current market data."""
        signals = []

        try:
            # Only generate signals during Vietnamese market hours
            if not self.is_market_open():
                return signals
                
            current_session = self.get_current_market_session()
            
            for timeframe in self.timeframes:
                if timeframe not in self.ma_buffer:
                    continue
                    
                for symbol, ma_data in self.ma_buffer[timeframe].items():
                    if not ma_data or 'sma_short' not in ma_data:
                        continue
                        
                    sma_short = ma_data['sma_short']
                    sma_long = ma_data['sma_long']
                    
                    if len(sma_short) < 2 or len(sma_long) < 2:
                        continue
                        
                    # Get latest values
                    latest_short = sma_short.iloc[-1]
                    latest_long = sma_long.iloc[-1]
                    latest_price = self.price_buffer[timeframe][symbol].iloc[-1] if len(self.price_buffer[timeframe][symbol]) > 0 else 0
                    
                    if pd.isna(latest_short) or pd.isna(latest_long) or latest_price == 0:
                        continue
                        
                    # Determine signal based on MA position
                    ma_spread = (latest_short - latest_long) / latest_long
                    
                    # Strong golden cross
                    if latest_short > latest_long and ma_spread > 0.01:  # 1% spread
                        confidence = min(0.8 + ma_spread * 10, 1.0)
                        
                        signal = self.create_signal(
                            symbol=symbol,
                            signal_type=SignalType.BUY,
                            confidence=confidence,
                            price=latest_price,
                            metadata={
                                'strategy_type': 'MA_Crossover_Current',
                                'short_ma': latest_short,
                                'long_ma': latest_long,
                                'ma_spread': ma_spread,
                                'market_session': current_session.value,
                                'timeframe': timeframe,
                                'vn30_member': symbol in self.vn30_symbols
                            }
                        )
                        
                        if signal:
                            signals.append(signal)
                            
                    # Strong death cross
                    elif latest_short < latest_long and abs(ma_spread) > 0.01:
                        confidence = min(0.8 + abs(ma_spread) * 10, 1.0)
                        
                        signal = self.create_signal(
                            symbol=symbol,
                            signal_type=SignalType.SELL,
                            confidence=confidence,
                            price=latest_price,
                            metadata={
                                'strategy_type': 'MA_Crossover_Current',
                                'short_ma': latest_short,
                                'long_ma': latest_long,
                                'ma_spread': ma_spread,
                                'market_session': current_session.value,
                                'timeframe': timeframe,
                                'vn30_member': symbol in self.vn30_symbols
                            }
                        )
                        
                        if signal:
                            signals.append(signal)
                            
        except Exception as e:
            self.logger.error(f"Error generating MA Crossover signals: {e}")
            
        return signals
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get detailed strategy status information."""
        base_status = self.get_status()
        
        # Add strategy-specific information
        ma_status = {
            'short_period': self.short_period,
            'long_period': self.long_period,
            'timeframes': self.timeframes,
            'symbols_tracking': len(self.price_buffer.get('5min', {})),
            'last_signal_count': len(self.last_signals),
            'vn30_coverage': sum(1 for symbol in self.price_buffer.get('5min', {}).keys() if symbol in self.vn30_symbols),
            'buffer_health': {
                timeframe: {
                    'symbols': len(self.price_buffer.get(timeframe, {})),
                    'avg_buffer_size': np.mean([len(prices) for prices in self.price_buffer.get(timeframe, {}).values()]) if self.price_buffer.get(timeframe) else 0
                }
                for timeframe in self.timeframes
            }
        }
        
        base_status.update({'ma_crossover_details': ma_status})
        return base_status
