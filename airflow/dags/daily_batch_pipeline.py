"""
F5.1 — DAG: daily_batch_pipeline
Schedule: 0 1 * * * (1:00 AM daily)
Purpose: Run full dbt pipeline after previous day's data is available.
Flow:
  check_data_availability >> run_dbt_staging >> run_dbt_marts >> run_dbt_tests
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor


# =============================================================================
# DAG Configuration — F5.1
# =============================================================================

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"

default_args = {
    "owner": "analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="daily_batch_pipeline",
    description="F5.1 — Full daily dbt pipeline: staging → marts → tests",
    schedule="0 1 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "daily", "batch"],
) as dag:

    # =========================================================================
    # F5.1.1 — Sensor: check raw_events has data for yesterday
    # =========================================================================
    check_data_availability = SqlSensor(
        task_id="check_data_availability",
        conn_id="postgres_default",
        sql="""
            SELECT COUNT(*) FROM raw_events
            WHERE "timestamp" >= CURRENT_DATE - INTERVAL '1 day'
              AND "timestamp" < CURRENT_DATE
        """,
        mode="poke",
        poke_interval=300,  # 5 minutes
        timeout=7200,  # 2 hours
    )

    # =========================================================================
    # F5.1.2 — Run staging models (views)
    # =========================================================================
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"{DBT_CMD_PREFIX} run --select staging --profiles-dir {DBT_PROJECT_DIR}",
    )

    # =========================================================================
    # F5.1.3 — Run mart models (incremental tables)
    # =========================================================================
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"{DBT_CMD_PREFIX} run --select marts --profiles-dir {DBT_PROJECT_DIR}",
    )

    # =========================================================================
    # F5.1.4 — Run dbt tests
    # Warn severity → task succeed; Error severity → task fail
    # =========================================================================
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"{DBT_CMD_PREFIX} test --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Pipeline dependency chain
    check_data_availability >> run_dbt_staging >> run_dbt_marts >> run_dbt_tests
