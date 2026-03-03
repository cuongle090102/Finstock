"""Technical Analysis package for Vietnamese Market Trading.

This package provides comprehensive technical analysis indicators optimized for
Vietnamese market characteristics and real-time streaming data processing.
"""

from .indicators import (
    TechnicalIndicators,
    IndicatorCalculator,
    IndicatorType,
    calculate_indicator_confidence
)

__version__ = "1.0.0"
__author__ = "Finstock Trading System"

__all__ = [
    'TechnicalIndicators',
    'IndicatorCalculator',
    'IndicatorType', 
    'calculate_indicator_confidence'
]
