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
# Windowed Aggregation — F3.3.1 (Tumbling Window 1 Minute)
# Purpose: Calculate per-minute real-time KPIs from clean event stream.
# Metrics:
#   1. Event count per event_type per minute
#   2. Revenue: SUM(sale_price * quantity) WHERE event_type = 'purchase'
#   3. Active users: COUNT(DISTINCT user_id) per minute
# =============================================================================

WATERMARK_DURATION = os.environ.get("SPARK_WATERMARK", "2 minutes")
WINDOW_DURATION = "1 minute"


def build_full_summary_1m(df: DataFrame) -> DataFrame:
    """
    F3.3 — Consolidated 1-minute summary aggregator.
    Performs all high-frequency KPI calculations in a single shuffle to save resources.
    """
    from pyspark.sql.functions import (
        sum as spark_sum,
        count,
        count_distinct,
        when,
        col,
        expr,
        lit,
        window,
    )

    return (
        df.groupBy(window(col("timestamp"), WINDOW_DURATION))
        .agg(
            count("*").alias("events"),
            count_distinct("user_id").alias("users"),
            spark_sum(
                when(
                    col("event_type") == "purchase", col("sale_price") * col("quantity")
                ).otherwise(0.0)
            ).alias("revenue"),
            count(when(col("event_type") == "click", 1)).alias("clicks"),
            count(when(col("event_type") == "add_to_cart", 1)).alias("carts"),
            count(when(col("event_type") == "purchase", 1)).alias("purchases"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit("1m").alias("window_type"),
            # Flatten metrics into standard format for sinks
            expr(
                "stack(6, "
                "'events_1m', cast(events as double), 'events', '', "
                "'active_users_1m', cast(users as double), 'users', '', "
                "'revenue_1m', cast(revenue as double), 'revenue', '', "
                "'click_1m', cast(clicks as double), 'click', '', "
                "'cart_1m', cast(carts as double), 'add_to_cart', '', "
                "'purchase_1m', cast(purchases as double), 'purchase', '') "
                "as (metric_name, metric_value, dimension_key, dimension_value)"
            ),
        )
    )
