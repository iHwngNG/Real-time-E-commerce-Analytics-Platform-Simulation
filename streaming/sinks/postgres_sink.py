import os
import sys
import logging
import time

# Add streaming root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import DataFrame
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://admin:admin123@postgres:5432/ecommerce",
)

# Parse DATABASE_URL — F3.4.1
parsed = urlparse(DATABASE_URL)

# JDBC URL format: jdbc:postgresql://host:port/db
JDBC_URL = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
JDBC_PROPERTIES = {
    "user": parsed.username,
    "password": parsed.password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

# Column mapping: DataFrame flat columns -> raw_events DB columns
RAW_EVENTS_COLUMN_MAP = {
    "event_id": "event_id",
    "event_type": "event_type",
    "event_version": "event_version",
    "timestamp": "timestamp",
    "session_id": "session_id",
    "user_id": "user_id",
    "user_segment": "user_segment",
    "country": "country",
    "device": "device",
    "product_id": "product_id",
    "category": "category_name",
    "sale_price": "sale_price",
    "quantity": "quantity",
    "search_keyword": "search_keyword",
    "position": "position_in_list",
    "time_on_page_sec": "time_on_page_sec",
    "order_id": "order_id",
}

# Columns required for aggregated_metrics table
AGG_METRICS_COLUMNS = [
    "window_start",
    "window_end",
    "window_type",
    "metric_name",
    "metric_value",
    "dimension_key",
    "dimension_value",
]


def _execute_raw_sql_via_jvm(spark_session, sql):
    """
    Utility to execute raw SQL on the Postgres DB using Spark Driver's JDBC connection.
    Avoids needing psycopg2 or other python drivers.
    """
    try:
        # Access the underlying JVM bridge
        jvm = spark_session._jvm
        # Load the driver class (ensure it's on classpath)
        jvm.java.lang.Class.forName("org.postgresql.Driver")
        # Get connection
        conn = jvm.java.sql.DriverManager.getConnection(
            JDBC_URL, JDBC_PROPERTIES["user"], JDBC_PROPERTIES["password"]
        )
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error executing raw SQL: {e}")
        raise e


def _write_raw_events_batch(batch_df: DataFrame, batch_id: int):
    """
    F3.4.1 — INSERT raw events into PostgreSQL (append-only).
    Uses a simple JDBC append. Duplicate event_id will be rejected by DB.
    """
    if batch_df.isEmpty():
        return

    renamed_df = batch_df
    for spark_col, db_col in RAW_EVENTS_COLUMN_MAP.items():
        if spark_col != db_col:
            renamed_df = renamed_df.withColumnRenamed(spark_col, db_col)

    db_columns = list(RAW_EVENTS_COLUMN_MAP.values())
    write_df = renamed_df.select(*db_columns)

    try:
        write_df.write.jdbc(
            url=JDBC_URL,
            table="raw_events",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
        logger.info(f"Batch {batch_id}: Inserted {write_df.count()} raw events")
    except Exception as e:
        # Ignore individual batch duplicates for raw events if they happen on retries
        logger.warning(
            f"Batch {batch_id} raw events write issue (possible retry duplicate): {e}"
        )


def _write_agg_metrics_batch(batch_df: DataFrame, batch_id: int):
    """
    F3.4.1 — TRUE UPSERT for aggregated metrics into PostgreSQL.
    Spark JDBC doesn't support ON CONFLICT, so we staging -> upsert -> drop.
    """
    if batch_df.isEmpty():
        return

    # Use a unique temporary table name to avoid multi-query clashes
    temp_table = f"stg_agg_{batch_id}_{int(time.time() % 1000)}"

    # 1. Write the micro-batch to a temporary staging table
    select_df = batch_df.select(*AGG_METRICS_COLUMNS)
    select_df.write.jdbc(
        url=JDBC_URL,
        table=temp_table,
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # 2. Execute the UPSERT SQL
    sql = f"""
    INSERT INTO aggregated_metrics (window_start, window_end, window_type, metric_name, metric_value, dimension_key, dimension_value)
    SELECT window_start, window_end, window_type, metric_name, metric_value, dimension_key, dimension_value FROM {temp_table}
    ON CONFLICT (window_start, window_type, metric_name, dimension_key, dimension_value)
    DO UPDATE SET 
        metric_value = EXCLUDED.metric_value,
        window_end = EXCLUDED.window_end,
        computed_at = CURRENT_TIMESTAMP;
    
    DROP TABLE {temp_table};
    """

    try:
        _execute_raw_sql_via_jvm(batch_df.sparkSession, sql)
        logger.info(f"Batch {batch_id}: Upserted {select_df.count()} metrics")
    except Exception as e:
        logger.error(f"Batch {batch_id} UPSERT failed: {e}")


def start_raw_events_sink(clean_df: DataFrame, checkpoint_path: str):
    return (
        clean_df.writeStream.outputMode("append")
        .foreachBatch(_write_raw_events_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )


def start_agg_metrics_sink(agg_df: DataFrame, checkpoint_path: str):
    return (
        agg_df.writeStream.outputMode("update")
        .foreachBatch(_write_agg_metrics_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )
