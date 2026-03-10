"""
F3 — Streaming Module Main Entry Point
Robust Multi-Sink Version
"""

import os
import sys
import logging
import time

# Ensure streaming root is on path
sys.path.insert(0, os.path.dirname(__file__))

from spark_session import create_spark_session, get_checkpoint_path
from jobs.ingest_stream import read_raw_events, process_ingestion

# Aggregation jobs
from jobs.aggregate_1m import build_full_summary_1m
from jobs.aggregate_5m import (
    build_conversion_rate,
    build_add_to_cart_rate,
    build_avg_time_on_page_per_category,
)
from jobs.aggregate_1h import (
    build_revenue_per_category,
    build_top_products_by_purchase,
    build_user_segment_distribution,
)

# Sinks
from sinks.postgres_sink import start_raw_events_sink, start_agg_metrics_sink
from sinks.redis_sink import start_redis_sink
from sinks.kafka_sink import start_kafka_sink

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("StreamingMain")


def main():
    """
    F3 — Main pipeline orchestrator.
    Starts multiple streaming sinks independently for maximum stability.
    """

    logger.info("=== STREAMING PIPELINE STARTING ===")

    # STep 1: Create Spark session (F3.1)
    spark = create_spark_session("EcommerceStreaming")
    logger.info("SparkSession created")

    # Step 2: Ingest from Kafka (F3.2)
    logger.info("Connecting to Kafka and reading [raw-events] topic...")
    df_raw = read_raw_events(spark)

    # Parse JSON, flatten, validate → clean_df + dlq_df
    clean_df, dlq_df = process_ingestion(df_raw)
    logger.info("Ingestion pipeline configured")

    # F3.3 — Apply watermark ONCE (F3.3.1)
    watermark_duration = os.environ.get("SPARK_WATERMARK", "5 minutes")
    clean_df_wm = clean_df.withWatermark("timestamp", watermark_duration)
    logger.info("Applied watermark of %s to clean events stream", watermark_duration)

    active_queries = []

    # 1. PostgreSQL Raw Events (Append)
    logger.info("Starting Sink: PostgreSQL Raw")
    active_queries.append(
        start_raw_events_sink(clean_df_wm, get_checkpoint_path("pg_raw_v1"))
    )

    # 2. Redis Sink (for Live Dashboard) — Consolidated F3.4.2
    logger.info("Starting Sink: Redis Live Metrics")
    from jobs.aggregate_1m import build_full_summary_1m
    full_metrics_1m = build_full_summary_1m(clean_df_wm)
    
    active_queries.append(
        start_redis_sink(full_metrics_1m, get_checkpoint_path("redis_v1"))
    )

    # (Skipping other sinks for stability/memory reduction)
    logger.info("=== ESSENTIAL STREAMING QUERIES RUNNING (RAW + REDIS) ===")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import traceback

    try:
        main()
    except Exception as e:
        logger.error("FATAL ERROR in streaming pipeline: %s", e)
        traceback.print_exc()
        sys.exit(1)
