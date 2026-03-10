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
from jobs.aggregate_1m import (
    build_event_count_per_type,
    build_revenue_per_minute,
    build_active_users_per_minute,
)
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

    # F3.3 — Apply watermark ONCE to prevent SameContext Analyzer errors
    watermark_duration = os.environ.get("SPARK_WATERMARK", "5 minutes")
    clean_df_wm = clean_df.withWatermark("timestamp", watermark_duration)
    logger.info("Applied watermark of %s to clean events stream", watermark_duration)

    active_queries = []

    # 1. PostgreSQL Raw Events (Append)
    logger.info("Starting Sink: PostgreSQL Raw")
    active_queries.append(
        start_raw_events_sink(clean_df, get_checkpoint_path("pg_raw_v1"))
    )

    # 2. Sequential Aggregation Sinks (to Postgres)
    # This prevents the giant Union analyzer error and allows staggered starts.
    aggregations = [
        ("count_1m", build_event_count_per_type(clean_df_wm)),
        ("rev_1m", build_revenue_per_minute(clean_df_wm)),
        ("users_1m", build_active_users_per_minute(clean_df_wm)),
        ("conv_5m", build_conversion_rate(clean_df_wm)),
        ("cart_5m", build_add_to_cart_rate(clean_df_wm)),
        ("page_5m", build_avg_time_on_page_per_category(clean_df_wm)),
        ("rev_cat_1h", build_revenue_per_category(clean_df_wm)),
        ("tops_1h", build_top_products_by_purchase(clean_df_wm)),
        ("segments_1h", build_user_segment_distribution(clean_df_wm)),
    ]

    for name, agg_df in aggregations:
        suffix = name
        logger.info(f"Starting Postgres Agg Sink: {suffix}")
        active_queries.append(
            start_agg_metrics_sink(agg_df, get_checkpoint_path(f"pg_agg_{suffix}"))
        )
        time.sleep(1)  # Breath between query starts

    # 3. Redis Sink (for Live Dashboard)
    logger.info("Starting Redis Sink")
    active_queries.append(
        start_redis_sink(
            build_event_count_per_type(clean_df_wm), get_checkpoint_path("redis_v1")
        )
    )

    # 4. Kafka Aggregations Sink (for downstream consumers)
    logger.info("Starting Kafka Agg Sink")
    active_queries.append(
        start_kafka_sink(
            build_revenue_per_minute(clean_df_wm), get_checkpoint_path("kafka_agg_v1")
        )
    )

    # 5. DLQ Sink
    logger.info("Starting DLQ Sink")
    from pyspark.sql.functions import to_json, struct, col

    dlq_kafka_df = dlq_df.select(
        to_json(struct([col(c) for c in dlq_df.columns])).alias("value")
    )
    q_dlq = (
        dlq_kafka_df.writeStream.outputMode("append")
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        )
        .option("topic", "dead-letter-queue")
        .option("checkpointLocation", get_checkpoint_path("dlq_v1"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    active_queries.append(q_dlq)

    logger.info("=== ALL %d STREAMING QUERIES RUNNING ===", len(active_queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    import traceback

    try:
        main()
    except Exception as e:
        logger.error("FATAL ERROR in streaming pipeline: %s", e)
        traceback.print_exc()
        sys.exit(1)
