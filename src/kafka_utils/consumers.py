"""Kafka consumers for strategies, risk, and Delta Lake persistence (Task 1.2)."""

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp
from delta.tables import DeltaTable
import pandas as pd

from config.delta_lake_config import DeltaLakeConfig
from src.utils.logging import StructuredLogger


class MarketDataConsumer:
    """Consumer for market data to write to Delta Lake Bronze layer (Task 1.2)."""

    def __init__(self,
                 bootstrap_servers: str,
                 group_id: str = "market_data_consumer",
                 spark: Optional[SparkSession] = None,
                 batch_size: int = 100):
        """Initialize market data consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            spark: SparkSession for Delta Lake writes (created if not provided)
            batch_size: Number of messages to batch before writing to Delta Lake
        """
        self.logger = StructuredLogger("MarketDataConsumer")

        # Kafka consumer setup
        self.consumer = KafkaConsumer(
            'market_data',
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=500
        )

        # Spark session for Delta Lake
        self.spark = spark or DeltaLakeConfig.create_spark_session("MarketDataConsumer")

        # Delta Lake configuration
        self.bronze_path = DeltaLakeConfig.BRONZE_TABLES["market_data"]

        # Batching configuration
        self.batch_size = batch_size
        self.message_buffer = []

        # Performance tracking
        self.messages_processed = 0
        self.messages_written = 0
        self.errors = 0

        self.logger.info(f"MarketDataConsumer initialized: group_id={group_id}, batch_size={batch_size}")

    def _create_market_data_schema(self) -> StructType:
        """Create schema for market data Delta table."""
        return StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("open_price", DoubleType(), True),
            StructField("high_price", DoubleType(), True),
            StructField("low_price", DoubleType(), True),
            StructField("close_price", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("value", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("data_source", StringType(), True),
            StructField("collector", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("market_status", StringType(), True),
            StructField("data_quality_score", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), False)
        ])

    def _write_batch_to_delta(self, messages: List[Dict[str, Any]]) -> bool:
        """Write batch of messages to Delta Lake Bronze layer.

        Args:
            messages: List of message dictionaries

        Returns:
            True if successful, False otherwise
        """
        if not messages:
            return True

        try:
            # Convert messages to DataFrame
            df = self.spark.createDataFrame(messages, schema=self._create_market_data_schema())

            # Add ingestion timestamp
            df = df.withColumn("ingestion_timestamp", current_timestamp())

            # Convert timestamp string to timestamp type if needed
            if df.schema["timestamp"].dataType == StringType():
                df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

            # Write to Delta Lake (append mode)
            df.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("symbol", "data_source") \
                .save(self.bronze_path)

            self.messages_written += len(messages)
            self.logger.info(f"Wrote {len(messages)} messages to Delta Lake Bronze: {self.bronze_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to write batch to Delta Lake: {e}")
            self.errors += 1
            return False

    def consume_market_data(self, max_messages: Optional[int] = None, timeout_ms: int = 5000):
        """Consume market data from Kafka and write to Delta Lake (Task 1.2).

        Args:
            max_messages: Maximum messages to consume (None for infinite)
            timeout_ms: Poll timeout in milliseconds
        """
        self.logger.info("Starting market data consumption...")

        try:
            messages_consumed = 0

            for message in self.consumer:
                try:
                    # Extract message data
                    data = message.value

                    # Normalize data for Delta Lake schema
                    normalized_data = {
                        "symbol": data.get("symbol", "UNKNOWN"),
                        "timestamp": data.get("timestamp"),
                        "open_price": data.get("open_price") or data.get("open"),
                        "high_price": data.get("high_price") or data.get("high"),
                        "low_price": data.get("low_price") or data.get("low"),
                        "close_price": data.get("close_price") or data.get("close") or data.get("price"),
                        "volume": data.get("volume"),
                        "value": data.get("value"),
                        "change": data.get("change"),
                        "change_percent": data.get("change_percent"),
                        "data_source": data.get("data_source", "unknown"),
                        "collector": data.get("collector", "unknown"),
                        "data_type": data.get("data_type", "unknown"),
                        "market_status": data.get("market_status"),
                        "data_quality_score": data.get("data_quality_score")
                    }

                    # Add to buffer
                    self.message_buffer.append(normalized_data)
                    self.messages_processed += 1

                    # Write batch if buffer is full
                    if len(self.message_buffer) >= self.batch_size:
                        self._write_batch_to_delta(self.message_buffer)
                        self.message_buffer = []

                    messages_consumed += 1

                    # Check if we've reached max_messages
                    if max_messages and messages_consumed >= max_messages:
                        break

                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    self.errors += 1
                    continue

            # Write remaining messages
            if self.message_buffer:
                self._write_batch_to_delta(self.message_buffer)
                self.message_buffer = []

            self.logger.info(f"Consumption complete: processed={self.messages_processed}, written={self.messages_written}, errors={self.errors}")

        except KeyboardInterrupt:
            self.logger.info("Consumer interrupted by user")
            # Write remaining messages before exit
            if self.message_buffer:
                self._write_batch_to_delta(self.message_buffer)

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            raise
        finally:
            self.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        return {
            "messages_processed": self.messages_processed,
            "messages_written": self.messages_written,
            "errors": self.errors,
            "buffer_size": len(self.message_buffer),
            "bronze_path": self.bronze_path
        }

    def close(self):
        """Close consumer and cleanup resources."""
        try:
            self.consumer.close()
            self.logger.info("Market data consumer closed")
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")


class StrategyConsumer:
    """Consumer for strategy engines to receive market data (Task 1.2)."""

    def __init__(self, bootstrap_servers: str, group_id: str, callback: Optional[Callable] = None):
        """Initialize strategy consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            callback: Callback function to process messages
        """
        self.logger = StructuredLogger("StrategyConsumer")

        self.consumer = KafkaConsumer(
            'market_data', 'indicators',
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        self.callback = callback
        self.messages_processed = 0
        self.logger.info(f"StrategyConsumer initialized: group_id={group_id}")

    def consume_market_data(self, max_messages: Optional[int] = None):
        """Consume market data for trading strategies (Task 1.2).

        Args:
            max_messages: Maximum messages to consume (None for infinite)
        """
        self.logger.info("Starting market data consumption for strategies...")

        try:
            for message in self.consumer:
                try:
                    data = message.value

                    # Call callback if provided
                    if self.callback:
                        self.callback(data)

                    self.messages_processed += 1

                    if max_messages and self.messages_processed >= max_messages:
                        break

                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            self.logger.info("Strategy consumer interrupted by user")
        finally:
            self.close()

    def close(self):
        """Close consumer."""
        try:
            self.consumer.close()
            self.logger.info("Strategy consumer closed")
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")


class RiskMonitorConsumer:
    """Consumer for risk monitoring (Task 1.2)."""

    def __init__(self, bootstrap_servers: str, callback: Optional[Callable] = None):
        """Initialize risk monitor consumer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            callback: Callback function for risk events
        """
        self.logger = StructuredLogger("RiskMonitorConsumer")

        self.consumer = KafkaConsumer(
            'signals', 'orders',
            bootstrap_servers=bootstrap_servers,
            group_id='risk_monitor',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        self.callback = callback
        self.events_monitored = 0
        self.logger.info("RiskMonitorConsumer initialized")

    def monitor_risk_events(self, max_events: Optional[int] = None):
        """Monitor signals and orders for risk assessment (Task 1.2).

        Args:
            max_events: Maximum events to monitor (None for infinite)
        """
        self.logger.info("Starting risk event monitoring...")

        try:
            for message in self.consumer:
                try:
                    data = message.value

                    # Call callback if provided
                    if self.callback:
                        self.callback(data)

                    self.events_monitored += 1

                    if max_events and self.events_monitored >= max_events:
                        break

                except Exception as e:
                    self.logger.error(f"Error processing risk event: {e}")
                    continue

        except KeyboardInterrupt:
            self.logger.info("Risk monitor interrupted by user")
        finally:
            self.close()

    def close(self):
        """Close consumer."""
        try:
            self.consumer.close()
            self.logger.info("Risk monitor consumer closed")
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")
