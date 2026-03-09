import os
import sys

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, when, lit

from schemas.event_schema import event_schema, VALID_EVENT_TYPES

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# =============================================================================
# Ingestion Stream logic — F3.2
# Purpose: Consume Kafka bytes, applying Schema & Validation (dead-letter-queue)
# =============================================================================


def read_raw_events(spark) -> DataFrame:
    """
    F3.2.1 — Read Kafka `raw-events` into Structured Streaming DataFrame.
    """
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "raw-events")
        # Start reading from latest messages
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return df_kafka


def process_ingestion(df_raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse JSON bytes, Flatten columns, and Validate Events (Route to DLQ).
    """

    # F3.2.2 — Parse JSON -> StructType
    parsed_df = df_raw.select(
        # Cast raw bytes back to String, then map to predefined Schema
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    # F3.2.3 — Flatten nested fields -> flat columns
    flat_df = (
        parsed_df.withColumn("user_id", col("user.user_id"))
        .withColumn("user_segment", col("user.segment"))
        .withColumn("country", col("user.country"))
        .withColumn("device", col("user.device"))
        .withColumn("product_id", col("product.product_id"))
        .withColumn("sku", col("product.sku"))
        .withColumn("category", col("product.category"))
        .withColumn("brand", col("product.brand"))
        .withColumn("sale_price", col("product.sale_price"))
        .withColumn("quantity", col("event_data.quantity"))
        .withColumn("position", col("event_data.position_in_list"))
        .withColumn("time_on_page_sec", col("event_data.time_on_page_sec"))
        .withColumn("search_keyword", col("event_data.search_keyword"))
        .withColumn("payment_method", col("event_data.payment_method"))
        .withColumn("order_id", col("event_data.order_id"))
        .withColumn("app_version", col("metadata.app_version"))
        # We no longer need the big nested JSON properties
        .drop("user", "product", "event_data", "metadata")
    )

    # F3.2.4 — Validate constraints & Add Error Label
    validation_df = flat_df.withColumn(
        "error_reason",
        when(col("event_id").isNull(), lit("Null Event ID"))
        .when(col("user_id").isNull(), lit("Null User ID"))
        .when(~col("event_type").isin(VALID_EVENT_TYPES), lit("Invalid Event Type"))
        .otherwise(lit(None)),
    )

    # Split the DataFrame into completely Clean ones (Valid) versus Corrupted ones (DLQ)
    clean_events_df = validation_df.filter(col("error_reason").isNull()).drop(
        "error_reason"
    )
    dead_letter_df = validation_df.filter(col("error_reason").isNotNull())

    return clean_events_df, dead_letter_df
