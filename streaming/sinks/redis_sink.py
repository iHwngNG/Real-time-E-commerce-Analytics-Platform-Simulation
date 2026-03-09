import os
import sys
import json
import logging

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import redis
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")

# =============================================================================
# Redis Sink — F3.4.2
# Purpose: Push real-time counters, sorted-set leaderboards, and pub/sub
#          notifications so the API/Dashboard layer can serve live data.
# Key patterns follow storage/redis_schema.md
# =============================================================================

# Metrics that map to HSET fields in "metric:summary"
SUMMARY_METRIC_MAP = {
    "click": "click_1m",
    "add_to_cart": "cart_1m",
    "purchase": "purchase_1m",
}


def _get_redis_client():
    """Create a Redis client from the REDIS_URL environment variable."""
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _write_to_redis_batch(batch_df: DataFrame, batch_id: int):
    """
    F3.4.2 — Process each micro-batch and push data to Redis.
    Operations:
      1. HSET metric:summary -> update event_count counters per event_type
      2. HSET metric:summary -> update revenue_1m
      3. ZADD top:products:purchase -> sorted set for product leaderboard
      4. PUBLISH metrics-update -> notify API layer
    """
    if batch_df.isEmpty():
        return

    r = _get_redis_client()
    pipe = r.pipeline()

    rows = batch_df.collect()

    for row in rows:
        metric_name = row["metric_name"]
        metric_value = float(row["metric_value"]) if row["metric_value"] else 0.0
        dimension_key = row["dimension_key"]
        dimension_value = row["dimension_value"]
        window_type = row["window_type"]

        # 1. Update metric:summary HASH for 1-minute event counts (F3.3.1)
        if metric_name == "event_count" and window_type == "1m":
            redis_field = SUMMARY_METRIC_MAP.get(dimension_value)
            if redis_field:
                pipe.hset("metric:summary", redis_field, metric_value)

        # 2. Update revenue counter in metric:summary
        if metric_name == "revenue" and window_type == "1m":
            pipe.hset("metric:summary", "revenue_1m", round(metric_value, 2))

        # 3. Update active users counter
        if metric_name == "active_users" and window_type == "1m":
            pipe.hset("metric:summary", "active_users_1m", int(metric_value))

        # 4. ZADD for product leaderboard (1-hour purchase count)
        if metric_name == "product_purchase_count" and dimension_key == "sku":
            pipe.zadd(
                "top:products:purchase",
                {dimension_value: metric_value},
            )

        # 5. ZADD for click leaderboard (1-minute event count for clicks)
        if (
            metric_name == "event_count"
            and dimension_value == "click"
            and window_type == "1m"
        ):
            # Use the click count as overall click score
            pipe.zadd("top:products:click", {"total_clicks": metric_value})

    # Execute all batched Redis commands
    pipe.execute()

    # 6. PUBLISH notification to API layer (F3.4.2)
    notification = json.dumps(
        {
            "event": "refresh",
            "batch_id": batch_id,
            "metrics_count": len(rows),
        }
    )
    r.publish("metrics-update", notification)

    logger.info("Batch %d: Pushed %d metrics to Redis", batch_id, len(rows))
    r.close()


def start_redis_sink(agg_df: DataFrame, checkpoint_path: str):
    """
    Start the streaming query that pushes aggregated metrics to Redis.
    Output mode: update. Trigger: 30 seconds.
    """
    return (
        agg_df.writeStream.outputMode("update")
        .foreachBatch(_write_to_redis_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .start()
    )
