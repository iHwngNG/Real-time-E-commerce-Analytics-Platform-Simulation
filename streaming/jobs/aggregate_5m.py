import os
import sys

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    when,
    window,
    lit,
)

# =============================================================================
# Windowed Aggregation — F3.3.2 (Sliding Window 5 Minutes, Slide 1 Minute)
# Purpose: Calculate sliding-window ratio metrics for conversion analysis.
# Metrics:
#   1. Conversion rate: purchase_count / click_count
#   2. Add-to-cart rate: add_to_cart_count / product_view_count
#   3. Avg time_on_page_sec per category
# =============================================================================

WATERMARK_DURATION = os.environ.get("SPARK_WATERMARK", "2 minutes")
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION = "1 minute"


def build_conversion_rate(df: DataFrame) -> DataFrame:
    """
    F3.3.2 — Conversion rate: purchase_count / click_count
    per 5-minute sliding window (slide every 1 minute).

    Returns DataFrame with columns:
        window_start, window_end, metric_value (conversion_rate)
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION))
        .agg(
            count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
            count(when(col("event_type") == "click", 1)).alias("click_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("5m").alias("window_type"),
            lit("conversion_rate").alias("metric_name"),
            # Avoid division by zero
            when(
                col("click_count") > 0,
                (col("purchase_count") / col("click_count")),
            )
            .otherwise(lit(0.0))
            .cast("double")
            .alias("metric_value"),
            lit(None).cast("string").alias("dimension_key"),
            lit(None).cast("string").alias("dimension_value"),
        )
    )


def build_add_to_cart_rate(df: DataFrame) -> DataFrame:
    """
    F3.3.2 — Add-to-cart rate: add_to_cart_count / product_view_count
    per 5-minute sliding window (slide every 1 minute).

    Returns DataFrame with columns:
        window_start, window_end, metric_value (add_to_cart_rate)
    """
    return (
        df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION))
        .agg(
            count(when(col("event_type") == "add_to_cart", 1)).alias("cart_count"),
            count(when(col("event_type") == "product_view", 1)).alias("view_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("5m").alias("window_type"),
            lit("add_to_cart_rate").alias("metric_name"),
            when(
                col("view_count") > 0,
                (col("cart_count") / col("view_count")),
            )
            .otherwise(lit(0.0))
            .cast("double")
            .alias("metric_value"),
            lit(None).cast("string").alias("dimension_key"),
            lit(None).cast("string").alias("dimension_value"),
        )
    )


def build_avg_time_on_page_per_category(df: DataFrame) -> DataFrame:
    """
    F3.3.2 — Average time_on_page_sec per category
    per 5-minute sliding window (slide every 1 minute).
    Only product_view and click events have time_on_page_sec.

    Returns DataFrame with columns:
        window_start, window_end, category, avg_time_on_page
    """
    # Filter events that have time_on_page_sec data
    browsing_df = df.filter(
        col("event_type").isin("product_view", "click")
        & col("time_on_page_sec").isNotNull()
    )

    return (
        browsing_df.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("category"),
        )
        .agg(avg(col("time_on_page_sec")).alias("avg_time_on_page"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("5m").alias("window_type"),
            lit("avg_time_on_page").alias("metric_name"),
            col("avg_time_on_page").cast("double").alias("metric_value"),
            lit("category").alias("dimension_key"),
            col("category").alias("dimension_value"),
        )
    )
