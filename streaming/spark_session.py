import os
from pyspark.sql import SparkSession

# =============================================================================
# Spark Session Factory — F3.1
# Purpose: Create and configure a reusable SparkSession for all streaming jobs.
# =============================================================================

# Connector packages (F3.1.1)
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SPARK_POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.3"

SPARK_JARS_PACKAGES = f"{SPARK_KAFKA_PACKAGE},{SPARK_POSTGRES_PACKAGE}"

# Checkpoint base directory (F3.1.2)
CHECKPOINT_BASE = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")


def create_spark_session(app_name: str = "EcommerceStreaming") -> SparkSession:
    """
    Build a SparkSession configured for Kafka Structured Streaming.

    F3.1.1 — Includes spark-sql-kafka connector package.
    F3.1.2 — Sets checkpoint location base path.

    Args:
        app_name: Spark application name shown in Spark UI.
    Returns:
        SparkSession: Configured session ready for streaming queries.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # F3.1.1 — Kafka and PostgreSQL connector packages
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        # F3.1.2 — Default checkpoint location for streaming queries
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        # Performance tuning for streaming workloads
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

    # Reduce noisy Spark logs to WARN level
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_checkpoint_path(job_name: str) -> str:
    """
    Generate checkpoint path for a specific job (F3.1.2).
    Each job gets its own checkpoint directory to track progress independently.

    Args:
        job_name: Unique identifier for the streaming job.
    Returns:
        str: Full checkpoint path, e.g. /tmp/spark-checkpoints/aggregate_1m
    """
    return os.path.join(CHECKPOINT_BASE, job_name)
