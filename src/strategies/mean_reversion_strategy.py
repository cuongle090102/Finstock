"""Mean Reversion Strategy implementation with Vietnamese market support.

This strategy identifies and trades mean reversion opportunities using Bollinger Bands
and RSI, optimized for Vietnamese market characteristics and VN30 stocks.
"""

from typing import Dict, Any, List, Optional, Tuple
from src.strategies.base_strategy import BaseStrategy, SignalType
from src.technical_analysis.indicators import TechnicalIndicators
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class MeanReversionStrategy(BaseStrategy):
    """Mean Reversion trading strategy for Vietnamese market.
    
    Uses Bollinger Bands and RSI to identify oversold/overbought conditions
    Generates BUY signals when price is oversold and near lower Bollinger Band
    Generates SELL signals when price is overbought and near upper Bollinger Band
    """
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__("Mean_Reversion", params)
        
        # Bollinger Bands parameters
        self.bb_period = params.get('bb_period', 20)
        self.bb_std_dev = params.get('bb_std_dev', 2.0)
        self.bb_squeeze_threshold = params.get('bb_squeeze_threshold', 0.015)  # 1.5% squeeze detection
        
        # RSI parameters
        self.rsi_period = params.get('rsi_period', 14)
        self.rsi_oversold = params.get('rsi_oversold', 30)
        self.rsi_overbought = params.get('rsi_overbought', 70)
        self.rsi_extreme_oversold = params.get('rsi_extreme_oversold', 20)
        self.rsi_extreme_overbought = params.get('rsi_extreme_overbought', 80)
        
        # Mean reversion parameters
        self.mean_reversion_threshold = params.get('mean_reversion_threshold', 0.02)  # 2% from mean
        self.volume_confirmation_multiplier = params.get('volume_confirmation_multiplier', 1.3)
        self.trend_filter_period = params.get('trend_filter_period', 50)  # Long-term trend filter
        
        # Signal validation
        self.min_reversion_probability = params.get('min_reversion_probability', 0.6)
        self.max_trend_strength = params.get('max_trend_strength', 0.7)  # Max trend to trade against
        self.signal_cooldown_minutes = params.get('signal_cooldown_minutes', 15)
        
        # Vietnamese market optimization
        self.vn30_rsi_adjustment = params.get('vn30_rsi_adjustment', 5)  # VN30 stocks more volatile
        self.lunch_break_filter = params.get('lunch_break_filter', True)  # Avoid lunch break signals
        self.morning_session_boost = params.get('morning_session_boost', 1.1)
        
        # Data buffers
        self.price_buffers = {}
        self.volume_buffers = {}
        self.bb_buffers = {}
        self.rsi_buffers = {}
        self.last_signals = {}
        
        # VN30 symbols (centralized)
        from src.utils.market_config import VN30_SYMBOLS
        self.vn30_symbols = VN30_SYMBOLS
        
    def _initialize_strategy(self):
        """Initialize strategy-specific parameters and indicators."""
        self.logger.info(f"Initializing Mean Reversion Strategy:")
        self.logger.info(f"  Bollinger Bands: {self.bb_period}-period, {self.bb_std_dev} std dev")
        self.logger.info(f"  RSI: {self.rsi_period}-period, oversold: {self.rsi_oversold}, overbought: {self.rsi_overbought}")
        self.logger.info(f"  Volume confirmation: {self.volume_confirmation_multiplier}x")
    
    def calculate_bollinger_bands(self, prices: pd.Series) -> Dict[str, pd.Series]:
        """Calculate Bollinger Bands for mean reversion analysis."""
        if len(prices) < self.bb_period:
            return {
                'upper': pd.Series([np.nan] * len(prices), index=prices.index),
                'middle': pd.Series([np.nan] * len(prices), index=prices.index),
                'lower': pd.Series([np.nan] * len(prices), index=prices.index),
                'width': pd.Series([np.nan] * len(prices), index=prices.index),
                'position': pd.Series([np.nan] * len(prices), index=prices.index)
            }
        
        upper, middle, lower = TechnicalIndicators.bollinger_bands(prices, self.bb_period, self.bb_std_dev)
        
        # Calculate band width (volatility measure)
        width = (upper - lower) / middle
        
        # Calculate price position within bands (0 = lower band, 1 = upper band)
        position = (prices - lower) / (upper - lower)
        
        return {
            'upper': upper,
            'middle': middle,
            'lower': lower,
            'width': width,
            'position': position
        }
    
    def calculate_rsi_levels(self, prices: pd.Series, symbol: str = None) -> Dict[str, float]:
        """Calculate RSI with Vietnamese market adjustments."""
        if len(prices) < self.rsi_period + 1:
            return {
                'rsi': np.nan,
                'oversold_level': self.rsi_oversold,
                'overbought_level': self.rsi_overbought,
                'is_oversold': False,
                'is_overbought': False,
                'is_extreme': False
            }
        
        rsi = TechnicalIndicators.rsi(prices, self.rsi_period)
        current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else np.nan
        
        # Adjust RSI levels for VN30 stocks (more volatile)
        oversold_level = self.rsi_oversold
        overbought_level = self.rsi_overbought
        
        if symbol and symbol in self.vn30_symbols:
            oversold_level += self.vn30_rsi_adjustment
            overbought_level -= self.vn30_rsi_adjustment
        
        return {
            'rsi': current_rsi,
            'rsi_series': rsi,
            'oversold_level': oversold_level,
            'overbought_level': overbought_level,
            'is_oversold': current_rsi <= oversold_level if not pd.isna(current_rsi) else False,
            'is_overbought': current_rsi >= overbought_level if not pd.isna(current_rsi) else False,
            'is_extreme': (current_rsi <= self.rsi_extreme_oversold or 
                          current_rsi >= self.rsi_extreme_overbought) if not pd.isna(current_rsi) else False
        }
    
    def detect_bollinger_squeeze(self, bb_data: Dict[str, pd.Series]) -> Dict[str, Any]:
        """Detect Bollinger Band squeeze conditions (low volatility)."""
        if 'width' not in bb_data or len(bb_data['width']) < 20:
            return {'is_squeeze': False, 'squeeze_intensity': 0.0}
        
        current_width = bb_data['width'].iloc[-1]
        avg_width = bb_data['width'].tail(20).mean()
        
        squeeze_intensity = avg_width - current_width if not pd.isna(current_width) else 0
        is_squeeze = current_width < self.bb_squeeze_threshold if not pd.isna(current_width) else False
        
        return {
            'is_squeeze': is_squeeze,
            'squeeze_intensity': squeeze_intensity,
            'current_width': current_width,
            'avg_width': avg_width
        }
    
    def calculate_trend_filter(self, prices: pd.Series) -> Dict[str, Any]:
        """Calculate trend filter to avoid trading against strong trends."""
        if len(prices) < self.trend_filter_period:
            return {
                'trend_strength': 0.0,
                'trend_direction': 'neutral',
                'allow_mean_reversion': True
            }
        
        # Calculate long-term moving average
        long_ma = prices.rolling(self.trend_filter_period).mean()
        short_ma = prices.rolling(int(self.trend_filter_period / 2)).mean()
        
        if pd.isna(long_ma.iloc[-1]) or pd.isna(short_ma.iloc[-1]):
            return {'trend_strength': 0.0, 'trend_direction': 'neutral', 'allow_mean_reversion': True}
        
        # Calculate trend strength and direction
        ma_diff = (short_ma.iloc[-1] - long_ma.iloc[-1]) / long_ma.iloc[-1]
        trend_strength = abs(ma_diff)
        trend_direction = 'up' if ma_diff > 0 else 'down' if ma_diff < 0 else 'neutral'
        
        # Allow mean reversion only if trend isn't too strong
        allow_mean_reversion = trend_strength < self.max_trend_strength
        
        return {
            'trend_strength': trend_strength,
            'trend_direction': trend_direction,
            'allow_mean_reversion': allow_mean_reversion,
            'long_ma': long_ma.iloc[-1],
            'short_ma': short_ma.iloc[-1]
        }
    
    def detect_mean_reversion_signals(self, symbol: str, prices: pd.Series, volumes: pd.Series = None) -> List[Dict]:
        """Detect mean reversion trading opportunities."""
        signals = []
        
        if len(prices) < max(self.bb_period, self.rsi_period, self.trend_filter_period) + 5:
            return signals
        
        # Calculate indicators
        bb_data = self.calculate_bollinger_bands(prices)
        rsi_data = self.calculate_rsi_levels(prices, symbol)
        trend_data = self.calculate_trend_filter(prices)
        squeeze_data = self.detect_bollinger_squeeze(bb_data)
        
        # Skip if trend is too strong
        if not trend_data['allow_mean_reversion']:
            return signals
        
        # Current price and position
        current_price = prices.iloc[-1]
        bb_position = bb_data['position'].iloc[-1] if not pd.isna(bb_data['position'].iloc[-1]) else 0.5
        
        # Volume confirmation
        volume_confirmed = True
        if volumes is not None and len(volumes) >= 20:
            avg_volume = volumes.tail(20).mean()
            current_volume = volumes.iloc[-1]
            volume_confirmed = current_volume >= (avg_volume * self.volume_confirmation_multiplier)
        
        # Session adjustments
        current_session = self.get_current_market_session()
        session_multiplier = 1.0
        
        # Skip lunch break signals if enabled
        if self.lunch_break_filter and current_session.value == 'lunch_break':
            return signals
        
        # Morning session boost
        if current_session.value == 'morning':
            session_multiplier = self.morning_session_boost
        
        # OVERSOLD CONDITION (BUY signal)
        oversold_conditions = [
            rsi_data['is_oversold'],  # RSI oversold
            bb_position < 0.2,  # Price near lower Bollinger Band
            current_price < bb_data['lower'].iloc[-1] * 1.005,  # Price within 0.5% of lower band
        ]
        
        if all(oversold_conditions):
            confidence = self._calculate_mean_reversion_confidence(
                'BUY', bb_data, rsi_data, trend_data, squeeze_data, volume_confirmed, symbol, session_multiplier
            )
            
            if confidence >= self.min_reversion_probability:
                signals.append({
                    'timestamp': prices.index[-1],
                    'signal_type': SignalType.BUY,
                    'confidence': confidence,
                    'price': current_price,
                    'signal_reason': 'oversold_mean_reversion',
                    'rsi': rsi_data['rsi'],
                    'bb_position': bb_position,
                    'bb_lower': bb_data['lower'].iloc[-1],
                    'bb_upper': bb_data['upper'].iloc[-1],
                    'bb_middle': bb_data['middle'].iloc[-1],
                    'trend_strength': trend_data['trend_strength'],
                    'volume_confirmed': volume_confirmed,
                    'is_squeeze': squeeze_data['is_squeeze'],
                    'session_multiplier': session_multiplier
                })
        
        # OVERBOUGHT CONDITION (SELL signal)
        overbought_conditions = [
            rsi_data['is_overbought'],  # RSI overbought
            bb_position > 0.8,  # Price near upper Bollinger Band
            current_price > bb_data['upper'].iloc[-1] * 0.995,  # Price within 0.5% of upper band
        ]
        
        if all(overbought_conditions):
            confidence = self._calculate_mean_reversion_confidence(
                'SELL', bb_data, rsi_data, trend_data, squeeze_data, volume_confirmed, symbol, session_multiplier
            )
            
            if confidence >= self.min_reversion_probability:
                signals.append({
                    'timestamp': prices.index[-1],
                    'signal_type': SignalType.SELL,
                    'confidence': confidence,
                    'price': current_price,
                    'signal_reason': 'overbought_mean_reversion',
                    'rsi': rsi_data['rsi'],
                    'bb_position': bb_position,
                    'bb_lower': bb_data['lower'].iloc[-1],
                    'bb_upper': bb_data['upper'].iloc[-1],
                    'bb_middle': bb_data['middle'].iloc[-1],
                    'trend_strength': trend_data['trend_strength'],
                    'volume_confirmed': volume_confirmed,
                    'is_squeeze': squeeze_data['is_squeeze'],
                    'session_multiplier': session_multiplier
                })
        
        return signals
    
    def _calculate_mean_reversion_confidence(self, signal_type: str, bb_data: Dict, rsi_data: Dict,
                                           trend_data: Dict, squeeze_data: Dict, volume_conf: bool,
                                           symbol: str, session_multiplier: float) -> float:
        """Calculate confidence score for mean reversion signals."""
        confidence = 0.5  # Base confidence
        
        # RSI extremity boost
        if signal_type == 'BUY':
            rsi_extremity = max(0, rsi_data['oversold_level'] - rsi_data['rsi']) / rsi_data['oversold_level']
            confidence += rsi_extremity * 0.2
        else:
            rsi_extremity = max(0, rsi_data['rsi'] - rsi_data['overbought_level']) / (100 - rsi_data['overbought_level'])
            confidence += rsi_extremity * 0.2
        
        # Bollinger Band position boost
        bb_position = bb_data['position'].iloc[-1]
        if signal_type == 'BUY':
            bb_extremity = max(0, 0.2 - bb_position) / 0.2  # How close to lower band
        else:
            bb_extremity = max(0, bb_position - 0.8) / 0.2  # How close to upper band
        confidence += bb_extremity * 0.15
        
        # Volume confirmation
        if volume_conf:
            confidence += 0.1
        
        # Bollinger squeeze boost (low volatility = higher reversion probability)
        if squeeze_data['is_squeeze']:
            confidence += 0.1
        
        # Trend filter - penalize against-trend trades
        if trend_data['trend_direction'] == 'up' and signal_type == 'SELL':
            confidence *= 0.8
        elif trend_data['trend_direction'] == 'down' and signal_type == 'BUY':
            confidence *= 0.8
        
        # VN30 stock boost (more liquid, better mean reversion)
        if symbol in self.vn30_symbols:
            confidence += 0.05
        
        # Session adjustment
        confidence *= session_multiplier
        
        # Extreme RSI boost
        if rsi_data['is_extreme']:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _is_new_signal(self, symbol: str, signal_data: Dict) -> bool:
        """Check if this is a new signal to avoid duplicates."""
        if symbol not in self.last_signals:
            return True
        
        last_signal = self.last_signals[symbol]

        # Time-based cooldown — normalize both timestamps to tz-naive UTC for safe subtraction
        try:
            import pandas as pd
            ts_new = pd.to_datetime(signal_data['timestamp']).tz_localize(None) if pd.to_datetime(signal_data['timestamp']).tzinfo else pd.to_datetime(signal_data['timestamp'])
            ts_last_raw = last_signal.get('timestamp', signal_data['timestamp'])
            ts_last = pd.to_datetime(ts_last_raw).tz_localize(None) if pd.to_datetime(ts_last_raw).tzinfo else pd.to_datetime(ts_last_raw)
            time_diff = (ts_new - ts_last).total_seconds()
            if time_diff < self.signal_cooldown_minutes * 60:
                return False
        except (TypeError, ValueError):
            pass  # Can't compare — allow signal
        
        # Signal type change allows new signal
        return last_signal.get('signal_type') != signal_data['signal_type']
    
    def on_data(self, market_data: pd.DataFrame):
        """Process incoming market data and generate mean reversion signals."""
        try:
            if market_data.empty:
                return
            
            # Process each symbol
            for symbol in market_data.get('symbol', pd.Series()).unique():
                if pd.isna(symbol):
                    continue
                
                symbol_data = market_data[market_data['symbol'] == symbol].copy()
                if symbol_data.empty or 'close' not in symbol_data.columns:
                    continue
                
                # Initialize buffers for new symbols
                if symbol not in self.price_buffers:
                    self.price_buffers[symbol] = pd.Series(dtype=float)
                    self.volume_buffers[symbol] = pd.Series(dtype=float)
                
                # Update data buffers
                if 'timestamp' in symbol_data.columns:
                    new_prices = symbol_data.set_index('timestamp')['close']
                    new_volumes = symbol_data.set_index('timestamp').get('volume', pd.Series())
                else:
                    new_prices = symbol_data['close']
                    new_volumes = symbol_data.get('volume', pd.Series())
                
                # Append new data (keep last 150 points for efficiency)
                self.price_buffers[symbol] = pd.concat([self.price_buffers[symbol], new_prices]).tail(150)
                if not new_volumes.empty:
                    self.volume_buffers[symbol] = pd.concat([self.volume_buffers[symbol], new_volumes]).tail(150)
                
                # Generate signals if market is open and sufficient data
                min_required = max(self.bb_period, self.rsi_period, self.trend_filter_period) + 5
                if (self.is_market_open() and len(self.price_buffers[symbol]) >= min_required):
                    
                    signals = self.detect_mean_reversion_signals(
                        symbol,
                        self.price_buffers[symbol],
                        self.volume_buffers.get(symbol, pd.Series())
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
                                    'strategy_type': 'Mean_Reversion',
                                    'signal_reason': signal_data['signal_reason'],
                                    'rsi': signal_data['rsi'],
                                    'rsi_oversold': self.rsi_oversold,
                                    'rsi_overbought': self.rsi_overbought,
                                    'bb_position': signal_data['bb_position'],
                                    'bb_lower': signal_data['bb_lower'],
                                    'bb_upper': signal_data['bb_upper'],
                                    'bb_middle': signal_data['bb_middle'],
                                    'trend_strength': signal_data['trend_strength'],
                                    'volume_confirmed': signal_data['volume_confirmed'],
                                    'is_squeeze': signal_data['is_squeeze'],
                                    'vn30_member': symbol in self.vn30_symbols,
                                    'session_multiplier': signal_data['session_multiplier']
                                }
                            )
                            
                            if signal:
                                self.publish_signal(signal)
                                self.last_signals[symbol] = signal_data
        
        except Exception as e:
            self.logger.error(f"Error processing market data in Mean Reversion strategy: {e}")
    
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
        """Generate mean reversion signals based on current market data."""
        signals = []

        try:
            if not self.is_market_open():
                return signals
            
            current_session = self.get_current_market_session()
            min_required = max(self.bb_period, self.rsi_period, self.trend_filter_period) + 5
            
            for symbol, prices in self.price_buffers.items():
                if len(prices) < min_required:
                    continue
                
                volumes = self.volume_buffers.get(symbol, pd.Series())
                reversion_signals = self.detect_mean_reversion_signals(symbol, prices, volumes)
                
                for signal_data in reversion_signals:
                    signal = self.create_signal(
                        symbol=symbol,
                        signal_type=signal_data['signal_type'],
                        confidence=signal_data['confidence'],
                        price=signal_data['price'],
                        metadata={
                            'strategy_type': 'Mean_Reversion_Scan',
                            'signal_reason': signal_data['signal_reason'],
                            'rsi': signal_data['rsi'],
                            'bb_position': signal_data['bb_position'],
                            'market_session': current_session.value,
                            'vn30_member': symbol in self.vn30_symbols
                        }
                    )
                    
                    if signal:
                        signals.append(signal)
        
        except Exception as e:
            self.logger.error(f"Error generating Mean Reversion signals: {e}")
        
        return signals
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get detailed mean reversion strategy status."""
        base_status = self.get_status()
        
        reversion_status = {
            'bb_period': self.bb_period,
            'bb_std_dev': self.bb_std_dev,
            'rsi_period': self.rsi_period,
            'rsi_levels': f"Oversold: {self.rsi_oversold}, Overbought: {self.rsi_overbought}",
            'symbols_tracking': len(self.price_buffers),
            'vn30_coverage': sum(1 for symbol in self.price_buffers.keys() if symbol in self.vn30_symbols),
            'recent_signals': len(self.last_signals),
            'trend_filter_period': self.trend_filter_period,
            'min_reversion_probability': self.min_reversion_probability,
            'volume_confirmation_multiplier': f"{self.volume_confirmation_multiplier}x",
            'avg_data_points': np.mean([len(prices) for prices in self.price_buffers.values()]) if self.price_buffers else 0
        }
        
        base_status.update({'mean_reversion_details': reversion_status})
        return base_status