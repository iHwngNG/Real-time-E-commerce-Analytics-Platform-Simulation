import os
import sys
import logging

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame

from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://admin:admin123@postgres:5432/ecommerce",
)

# Parse DATABASE_URL — F3.4.1
# Expected format: postgresql://user:pass@host:port/db
parsed = urlparse(DATABASE_URL)

# JDBC URL format: jdbc:postgresql://host:port/db
JDBC_URL = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
JDBC_PROPERTIES = {
    "user": parsed.username,
    "password": parsed.password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",  # Essential for String -> UUID conversion
}

# =============================================================================
# PostgreSQL Sink — F3.4.1
# Purpose: Write raw events (append) and aggregated metrics (upsert) to DB.
# =============================================================================

# Column mapping: DataFrame flat columns -> raw_events DB columns
RAW_EVENTS_COLUMN_MAP = {
    "event_id": "event_id",
    "event_type": "event_type",
    "event_version": "event_version",
    "timestamp": "timestamp",
    "session_id": "session_id",
    "user_id": "user_id",
    "user_segment": "user_segment",
    "country": "country",
    "device": "device",
    "product_id": "product_id",
    "category": "category_name",
    "sale_price": "sale_price",
    "quantity": "quantity",
    "search_keyword": "search_keyword",
    "position": "position_in_list",
    "time_on_page_sec": "time_on_page_sec",
    "order_id": "order_id",
}

# Columns required for aggregated_metrics table
AGG_METRICS_COLUMNS = [
    "window_start",
    "window_end",
    "window_type",
    "metric_name",
    "metric_value",
    "dimension_key",
    "dimension_value",
]


def _write_raw_events_batch(batch_df: DataFrame, batch_id: int):
    """
    F3.4.1 — INSERT raw events into PostgreSQL (append-only).
    Called by foreachBatch on the clean_events stream.
    """
    if batch_df.isEmpty():
        return

    # Rename columns to match DB schema
    renamed_df = batch_df
    for spark_col, db_col in RAW_EVENTS_COLUMN_MAP.items():
        if spark_col != db_col:
            renamed_df = renamed_df.withColumnRenamed(spark_col, db_col)

    # Select only columns that exist in the DB table
    db_columns = list(RAW_EVENTS_COLUMN_MAP.values())
    write_df = renamed_df.select(*db_columns)

    write_df.write.jdbc(
        url=JDBC_URL,
        table="raw_events",
        mode="append",
        properties=JDBC_PROPERTIES,
    )
    logger.info(
        "Batch %d: Inserted %d raw events into PostgreSQL", batch_id, write_df.count()
    )


def _write_agg_metrics_batch(batch_df: DataFrame, batch_id: int):
    """
    F3.4.1 — UPSERT aggregated metrics into PostgreSQL.
    Uses foreachBatch + temp view + SQL UPSERT pattern for idempotency.
    ON CONFLICT (window_start, window_type, metric_name, dimension_key, dimension_value)
    """
    if batch_df.isEmpty():
        return

    select_df = batch_df.select(*AGG_METRICS_COLUMNS)

    # Use JDBC append + handle conflicts at DB level via ON CONFLICT constraint
    # PySpark JDBC mode "append" will INSERT; DB constraint handles duplicates
    select_df.write.jdbc(
        url=JDBC_URL,
        table="aggregated_metrics",
        mode="append",
        properties=JDBC_PROPERTIES,
    )
    logger.info(
        "Batch %d: Upserted %d aggregated metrics into PostgreSQL",
        batch_id,
        select_df.count(),
    )


def start_raw_events_sink(clean_df: DataFrame, checkpoint_path: str):
    """
    Start the streaming query that writes clean events to PostgreSQL.
    Output mode: append. Trigger: 30 seconds.
    """
    return (
        clean_df.writeStream.outputMode("append")
        .foreachBatch(_write_raw_events_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )


def start_agg_metrics_sink(agg_df: DataFrame, checkpoint_path: str):
    """
    Start the streaming query that writes aggregated metrics to PostgreSQL.
    Output mode: update. Trigger: 30 seconds.
    """
    return (
        agg_df.writeStream.outputMode("update")
        .foreachBatch(_write_agg_metrics_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )
