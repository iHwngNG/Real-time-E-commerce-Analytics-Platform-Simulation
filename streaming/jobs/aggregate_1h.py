import os
import sys

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    when,
    window,
    lit,
)

# =============================================================================
# Windowed Aggregation — F3.3.3 (Tumbling Window 1 Hour)
# Purpose: Calculate hourly business intelligence metrics.
# Metrics:
#   1. Revenue per category
#   2. Top 10 products by purchase count
#   3. User segment distribution
# =============================================================================

WATERMARK_DURATION = os.environ.get("SPARK_WATERMARK", "2 minutes")
WINDOW_DURATION = "1 hour"


def build_revenue_per_category(df: DataFrame) -> DataFrame:
    """
    F3.3.3 — Revenue per category per 1-hour tumbling window.
    Only purchase events contribute to revenue.

    Returns DataFrame with columns:
        window_start, window_end, category, revenue
    """
    purchase_df = df.filter(col("event_type") == "purchase")

    return (
        purchase_df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION),
            col("category"),
        )
        .agg(
            spark_sum(
                when(
                    col("sale_price").isNotNull() & col("quantity").isNotNull(),
                    col("sale_price") * col("quantity"),
                ).otherwise(lit(0.0))
            ).alias("revenue")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1h").alias("window_type"),
            lit("revenue_by_category").alias("metric_name"),
            col("revenue").cast("double").alias("metric_value"),
            lit("category").alias("dimension_key"),
            col("category").alias("dimension_value"),
        )
    )


def build_top_products_by_purchase(df: DataFrame) -> DataFrame:
    """
    F3.3.3 — Top 10 products by purchase count per 1-hour tumbling window.

    NOTE: Spark Structured Streaming does not support `orderBy` (full sort)
    within a streaming query. So we compute purchase_count per
    product per window. The actual Top-10 ranking will be performed
    downstream in the sink (foreachBatch) or by the API/Dashboard query.

    Returns DataFrame with columns:
        window_start, window_end, sku, purchase_count
    """
    purchase_df = df.filter(col("event_type") == "purchase")

    return (
        purchase_df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION),
            col("sku"),
        )
        .agg(count("*").alias("purchase_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1h").alias("window_type"),
            lit("product_purchase_count").alias("metric_name"),
            col("purchase_count").cast("double").alias("metric_value"),
            lit("sku").alias("dimension_key"),
            col("sku").alias("dimension_value"),
        )
    )


def build_user_segment_distribution(df: DataFrame) -> DataFrame:
    """
    F3.3.3 — User segment distribution per 1-hour tumbling window.
    Counts number of events per user_segment.

    Returns DataFrame with columns:
        window_start, window_end, user_segment, segment_count
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION),
            col("user_segment"),
        )
        .agg(count("*").alias("segment_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1h").alias("window_type"),
            lit("segment_distribution").alias("metric_name"),
            col("segment_count").cast("double").alias("metric_value"),
            lit("user_segment").alias("dimension_key"),
            col("user_segment").alias("dimension_value"),
        )
    )
