"""
F5.2 — DAG: hourly_refresh
Schedule: 0 * * * * (every hour)
Purpose: Refresh hourly mart tables and notify API to invalidate cache.
Flow:
  run_dbt_hourly_models >> notify_api_cache_refresh
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import requests
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# DAG Configuration — F5.2
# =============================================================================

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"

# API internal URL (Docker network)
API_BASE_URL = "http://api:8000"

default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _notify_api_cache_refresh():
    """
    F5.2.2 — Call POST /internal/cache/invalidate on API.
    API will clear stale cache and re-query from mart tables.
    """
    url = f"{API_BASE_URL}/internal/cache/invalidate"
    try:
        resp = requests.post(url, timeout=30)
        resp.raise_for_status()
        logger.info("Cache invalidation successful: %s", resp.json())
    except requests.exceptions.ConnectionError:
        # API may not be running yet; log warning but don't fail the DAG
        logger.warning(
            "Could not connect to API at %s. "
            "Cache will be refreshed on next API query.",
            url,
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("Cache invalidation request failed: %s", exc)


with DAG(
    dag_id="hourly_refresh",
    description="F5.2 — Refresh hourly dbt models and notify API",
    schedule="0 * * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "hourly"],
) as dag:

    # =========================================================================
    # F5.2.1 — Run hourly-tagged dbt models
    # =========================================================================
    run_dbt_hourly_models = BashOperator(
        task_id="run_dbt_hourly_models",
        bash_command=f"{DBT_CMD_PREFIX} run --select tag:hourly --profiles-dir {DBT_PROJECT_DIR}",
    )

    # =========================================================================
    # F5.2.2 — Notify API to invalidate cache
    # =========================================================================
    notify_api_cache_refresh = PythonOperator(
        task_id="notify_api_cache_refresh",
        python_callable=_notify_api_cache_refresh,
    )

    # Pipeline dependency chain
    run_dbt_hourly_models >> notify_api_cache_refresh
