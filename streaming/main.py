"""
F3 — Streaming Module Main Entry Point
Entrypoint: python main.py

Purpose: Orchestrate the full real-time pipeline:
  1. Ingest raw events from Kafka topic [raw-events]
  2. Parse, flatten, validate (route invalid → DLQ)
  3. Compute windowed aggregations (1m, 5m, 1h)
  4. Sink results to PostgreSQL, Redis, and Kafka [aggregated-metrics]
  5. Keep running (awaitTermination) until stopped

Architecture:
  Kafka [raw-events]
       │
       ▼
  Ingest & Validate ──► DLQ (invalid events)
       │
       ├──► Raw Events Sink      → PostgreSQL.raw_events (append)
       │
       ├──► 1m Aggregation ──┐
       ├──► 5m Aggregation ──┼──► Union all metrics
       ├──► 1h Aggregation ──┘         │
       │                               ├──► PostgreSQL.aggregated_metrics (upsert)
       │                               ├──► Redis (counters + leaderboards + pub/sub)
       │                               └──► Kafka [aggregated-metrics]
       │
       └──► Console (debug, optional)
"""

import os
import sys
import logging

# Ensure streaming root is on path
sys.path.insert(0, os.path.dirname(__file__))

from spark_session import create_spark_session, get_checkpoint_path

# Ingestion
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
    Starts all streaming queries and blocks until termination.
    """

    logger.info("=== STREAMING PIPELINE STARTING ===")

    # =========================================================================
    # Step 1: Create Spark session (F3.1)
    # =========================================================================
    spark = create_spark_session("EcommerceStreaming")
    logger.info("SparkSession created: %s", spark.sparkContext.appName)

    # =========================================================================
    # Step 2: Ingest from Kafka (F3.2)
    # =========================================================================
    logger.info("Connecting to Kafka and reading [raw-events] topic...")
    df_raw = read_raw_events(spark)

    # Parse JSON, flatten, validate → clean_df + dlq_df
    clean_df, dlq_df = process_ingestion(df_raw)
    logger.info("Ingestion pipeline configured (parse → flatten → validate)")

    # =========================================================================
    # Step 3: Build aggregation DataFrames (F3.3)
    # Each function returns a DataFrame with unified schema:
    #   window_start, window_end, window_type, metric_name,
    #   metric_value, dimension_key, dimension_value
    # =========================================================================
    logger.info("Building windowed aggregation queries...")

    # F3.3.1 — 1-minute tumbling window
    agg_event_count = build_event_count_per_type(clean_df)
    agg_revenue_1m = build_revenue_per_minute(clean_df)
    agg_active_users = build_active_users_per_minute(clean_df)

    # F3.3.2 — 5-minute sliding window (slide 1 min)
    agg_conversion = build_conversion_rate(clean_df)
    agg_cart_rate = build_add_to_cart_rate(clean_df)
    agg_time_on_page = build_avg_time_on_page_per_category(clean_df)

    # F3.3.3 — 1-hour tumbling window
    agg_revenue_cat = build_revenue_per_category(clean_df)
    agg_top_products = build_top_products_by_purchase(clean_df)
    agg_segments = build_user_segment_distribution(clean_df)

    # Union all 1-minute metrics for Redis + Kafka sinks
    agg_1m_union = agg_event_count.unionByName(agg_revenue_1m).unionByName(
        agg_active_users
    )

    # Union all metrics for PostgreSQL aggregated_metrics table
    agg_all_union = (
        agg_1m_union.unionByName(agg_conversion)
        .unionByName(agg_cart_rate)
        .unionByName(agg_time_on_page)
        .unionByName(agg_revenue_cat)
        .unionByName(agg_top_products)
        .unionByName(agg_segments)
    )

    logger.info("All aggregation queries built successfully")

    # =========================================================================
    # Step 4: Start all streaming sinks (F3.4)
    # =========================================================================
    active_queries = []

    # F3.4.1 — PostgreSQL: raw events (append)
    logger.info("Starting sink: PostgreSQL raw_events (append)...")
    q_pg_raw = start_raw_events_sink(
        clean_df,
        get_checkpoint_path("pg_raw_events"),
    )
    active_queries.append(q_pg_raw)

    # F3.4.1 — PostgreSQL: aggregated metrics (upsert)
    logger.info("Starting sink: PostgreSQL aggregated_metrics (upsert)...")
    q_pg_agg = start_agg_metrics_sink(
        agg_all_union,
        get_checkpoint_path("pg_agg_metrics"),
    )
    active_queries.append(q_pg_agg)

    # F3.4.2 — Redis: real-time counters + leaderboards + pub/sub
    logger.info("Starting sink: Redis (counters + leaderboards)...")
    q_redis = start_redis_sink(
        agg_1m_union.unionByName(agg_top_products),
        get_checkpoint_path("redis_sink"),
    )
    active_queries.append(q_redis)

    # F3.4.3 — Kafka: aggregated-metrics topic
    logger.info("Starting sink: Kafka [aggregated-metrics] topic...")
    q_kafka = start_kafka_sink(
        agg_1m_union,
        get_checkpoint_path("kafka_agg_sink"),
    )
    active_queries.append(q_kafka)

    # F3.5.1 — DLQ: malformed events → Kafka [dead-letter-queue]
    logger.info("Starting sink: Kafka [dead-letter-queue] for invalid events...")
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
        .option("checkpointLocation", get_checkpoint_path("dlq_sink"))
        .trigger(processingTime="30 seconds")
        .start()
    )
    active_queries.append(q_dlq)

    # =========================================================================
    # Step 5: Report and await termination (F3.5.3)
    # =========================================================================
    logger.info("=== STREAMING PIPELINE RUNNING ===")
    logger.info("Active streaming queries: %d", len(active_queries))
    for i, q in enumerate(active_queries):
        logger.info("  [%d] %s (id=%s)", i, q.name or "unnamed", q.id)

    logger.info(
        "Pipeline is now processing events in real-time. "
        "Press Ctrl+C to stop gracefully."
    )

    # Block until any query terminates (or manual stop)
    spark.streams.awaitAnyTermination()

    logger.info("=== STREAMING PIPELINE STOPPED ===")


if __name__ == "__main__":
    main()
