"""
F5.3 — DAG: seed_and_init (manual trigger only)
Schedule: None
Purpose: Bootstrap — run migrations, seed data, compile dbt models.
Flow:
  run_migrations >> [seed_users, seed_products] >> run_dbt_compile
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# =============================================================================
# DAG Configuration — F5.3
# =============================================================================

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_CMD_PREFIX = f"cd {DBT_PROJECT_DIR} && dbt"

default_args = {
    "owner": "analytics",
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="seed_and_init",
    description="F5.3 — Bootstrap: migrations → seed data → dbt compile",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["init", "seed", "manual"],
) as dag:

    # =========================================================================
    # F5.3.1 — Run database migrations
    # =========================================================================
    run_migrations = BashOperator(
        task_id="run_migrations",
        bash_command="python /opt/airflow/scripts/run_migrations.py",
    )

    # =========================================================================
    # F5.3.2 — Seed users
    # =========================================================================
    seed_users = BashOperator(
        task_id="seed_users",
        bash_command="python /opt/airflow/scripts/seed_users.py",
    )

    # =========================================================================
    # F5.3.3 — Seed products
    # =========================================================================
    seed_products = BashOperator(
        task_id="seed_products",
        bash_command="python /opt/airflow/scripts/seed_products.py",
    )

    # =========================================================================
    # F5.3.4 — Compile dbt models to verify correctness
    # =========================================================================
    run_dbt_compile = BashOperator(
        task_id="run_dbt_compile",
        bash_command=f"{DBT_CMD_PREFIX} compile --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Pipeline dependency chain
    # Migrations first, then seeds in parallel, then dbt compile
    run_migrations >> [seed_users, seed_products] >> run_dbt_compile
