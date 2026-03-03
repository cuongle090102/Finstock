"""Market data schemas for Bronze layer ETL validation - Task 1.4."""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType, TimestampType, BooleanType, DecimalType
)


class MarketDataSchema:
    """Schema for market OHLCV data."""
    
    @staticmethod
    def get_spark_schema() -> StructType:
        """Get PySpark schema for market data."""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("open_price", DoubleType(), False),
            StructField("high_price", DoubleType(), False),
            StructField("low_price", DoubleType(), False),
            StructField("close_price", DoubleType(), False),
            StructField("volume", LongType(), True),
            StructField("value", DoubleType(), True),
            StructField("data_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("file_path", StringType(), True)
        ])


class FundamentalsSchema:
    """Schema for fundamental data."""
    
    @staticmethod
    def get_spark_schema() -> StructType:
        """Get PySpark schema for fundamental data."""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("report_date", TimestampType(), False),
            StructField("pe_ratio", DoubleType(), True),
            StructField("pb_ratio", DoubleType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("net_income", DoubleType(), True),
            StructField("total_assets", DoubleType(), True),
            StructField("total_equity", DoubleType(), True),
            StructField("debt_to_equity", DoubleType(), True),
            StructField("roe", DoubleType(), True),
            StructField("roa", DoubleType(), True),
            StructField("data_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("file_path", StringType(), True)
        ])


class CorporateActionsSchema:
    """Schema for corporate actions data."""
    
    @staticmethod
    def get_spark_schema() -> StructType:
        """Get PySpark schema for corporate actions."""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("action_type", StringType(), False),
            StructField("announcement_date", TimestampType(), False),
            StructField("ex_date", TimestampType(), True),
            StructField("record_date", TimestampType(), True),
            StructField("payment_date", TimestampType(), True),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("ratio", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("file_path", StringType(), True)
        ])


class NewsSchema:
    """Schema for news data."""
    
    @staticmethod
    def get_spark_schema() -> StructType:
        """Get PySpark schema for news data."""
        return StructType([
            StructField("article_id", StringType(), False),
            StructField("title", StringType(), False),
            StructField("content", StringType(), False),
            StructField("published_at", TimestampType(), False),
            StructField("author", StringType(), True),
            StructField("category", StringType(), True),
            StructField("tags", StringType(), True),  # JSON string of tags array
            StructField("related_symbols", StringType(), True),  # JSON string of symbols array
            StructField("sentiment_score", DoubleType(), True),
            StructField("importance_level", StringType(), True),
            StructField("data_source", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("file_path", StringType(), True)
        ])