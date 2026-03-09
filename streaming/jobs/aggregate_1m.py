import os
import sys

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
    window,
    lit,
)

# =============================================================================
# Windowed Aggregation — F3.3.1 (Tumbling Window 1 Minute)
# Purpose: Calculate per-minute real-time KPIs from clean event stream.
# Metrics:
#   1. Event count per event_type per minute
#   2. Revenue: SUM(sale_price * quantity) WHERE event_type = 'purchase'
#   3. Active users: COUNT(DISTINCT user_id) per minute
# =============================================================================

WATERMARK_DURATION = os.environ.get("SPARK_WATERMARK", "2 minutes")
WINDOW_DURATION = "1 minute"


def build_event_count_per_type(df: DataFrame) -> DataFrame:
    """
    F3.3.1 — Event count per event_type per 1-minute tumbling window.

    Returns DataFrame with columns:
        window_start, window_end, event_type, event_count
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION),
            col("event_type"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1m").alias("window_type"),
            lit("event_count").alias("metric_name"),
            col("event_count").cast("double").alias("metric_value"),
            lit("event_type").alias("dimension_key"),
            col("event_type").alias("dimension_value"),
        )
    )


def build_revenue_per_minute(df: DataFrame) -> DataFrame:
    """
    F3.3.1 — Revenue per 1-minute tumbling window.
    Only purchase events contribute to revenue.

    Returns DataFrame with columns:
        window_start, window_end, total_revenue
    """
    purchase_df = df.filter(col("event_type") == "purchase")

    return (
        purchase_df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(window(col("timestamp"), WINDOW_DURATION))
        .agg(
            spark_sum(
                when(
                    col("sale_price").isNotNull() & col("quantity").isNotNull(),
                    col("sale_price") * col("quantity"),
                ).otherwise(lit(0.0))
            ).alias("total_revenue")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1m").alias("window_type"),
            lit("revenue").alias("metric_name"),
            col("total_revenue").cast("double").alias("metric_value"),
            lit(None).cast("string").alias("dimension_key"),
            lit(None).cast("string").alias("dimension_value"),
        )
    )


def build_active_users_per_minute(df: DataFrame) -> DataFrame:
    """
    F3.3.1 — Active users (distinct user_id) per 1-minute tumbling window.

    Returns DataFrame with columns:
        window_start, window_end, active_users
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(window(col("timestamp"), WINDOW_DURATION))
        .agg(countDistinct("user_id").alias("active_users"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1m").alias("window_type"),
            lit("active_users").alias("metric_name"),
            col("active_users").cast("double").alias("metric_value"),
            lit(None).cast("string").alias("dimension_key"),
            lit(None).cast("string").alias("dimension_value"),
        )
    )
