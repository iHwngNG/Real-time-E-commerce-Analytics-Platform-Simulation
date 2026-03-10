import os
import sys

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct

# =============================================================================
# Kafka Sink — F3.4.3
# Purpose: Write aggregated results back to Kafka topic "aggregated-metrics"
#          for downstream consumers (monitoring, external systems).
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
AGGREGATED_TOPIC = "aggregated-metrics"


def start_kafka_sink(agg_df: DataFrame, checkpoint_path: str):
    """
    F3.4.3 — Write aggregated metrics to Kafka topic 'aggregated-metrics'.

    Serializes the DataFrame rows as JSON and publishes them.
    Output mode: update. Trigger: 30 seconds.
    """
    # Serialize all columns into a single JSON "value" column
    kafka_df = agg_df.select(
        to_json(
            struct(
                col("window_start").cast("string").alias("window_start"),
                col("window_end").cast("string").alias("window_end"),
                col("window_type"),
                col("metric_name"),
                col("metric_value").cast("string").alias("metric_value"),
                col("dimension_key"),
                col("dimension_value"),
            )
        ).alias("value")
    )

    return (
        kafka_df.writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", AGGREGATED_TOPIC)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
