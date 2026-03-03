"""
Regime Detection Functions for Daily Backtest DAG
Uses production-grade RegimeDetector for consistency with live trading
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
import logging

from src.regime.regime_detector import RegimeDetector, MarketRegime

logger = logging.getLogger(__name__)


def detect_regime(**context) -> str:
    """
    Detect market regime using production RegimeDetector
    Ensures consistency between backtesting and live trading

    Returns:
        'trending', 'ranging', or 'neutral'
    """
    try:
        # Get data from previous task
        data_path = context['task_instance'].xcom_pull(key='data_path', task_ids='fetch_market_data')
        if not data_path:
            raise ValueError("XCom 'data_path' from fetch_market_data is None — upstream task may have failed")
        df = pd.read_csv(data_path, parse_dates=['timestamp'])

        logger.info(f"Detecting regime from {len(df)} records for {df['symbol'].nunique()} symbols")

        # Initialize production-grade regime detector
        detector = RegimeDetector(
            adx_period=14,
            adx_trending_threshold=25.0,
            adx_ranging_threshold=20.0,
            volatility_window=20,
            price_action_window=10
        )

        # Aggregate data for overall market view
        # Use mean close, max high, min low across all symbols by timestamp
        market_df = df.groupby('timestamp').agg({
            'open': 'mean',
            'high': 'max',
            'low': 'min',
            'close': 'mean',
            'volume': 'sum'
        }).reset_index()

        if len(market_df) < 30:
            logger.warning("Insufficient data for regime detection, defaulting to neutral")
            regime = 'neutral'
            confidence = 0.5
        else:
            # Use production RegimeDetector
            result = detector.detect_regime(market_df)

            regime = result.regime.value  # Convert enum to string: 'trending', 'ranging', 'neutral'
            confidence = result.confidence

            logger.info(f"Detected regime: {regime} (confidence: {confidence:.2f})")
            logger.info(f"Details - ADX: {result.indicators.get('adx', 0):.2f}, "
                       f"Volatility Ratio: {result.indicators.get('volatility_ratio', 0):.2f}")

        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(key='regime', value=regime)
        context['task_instance'].xcom_push(key='regime_confidence', value=confidence)

        return regime

    except Exception as e:
        logger.error(f"Error detecting regime: {e}")
        # Default to neutral on error
        context['task_instance'].xcom_push(key='regime', value='neutral')
        context['task_instance'].xcom_push(key='regime_confidence', value=0.5)
        return 'neutral'
