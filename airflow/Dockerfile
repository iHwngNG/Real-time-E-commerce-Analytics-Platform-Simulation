# =============================================================================
# Airflow Dockerfile — F5.4
# Purpose: Extend official Airflow image with dbt-postgres for batch pipeline.
# =============================================================================
FROM apache/airflow:2.9.0

USER airflow

# Install dbt-postgres into Airflow container
RUN pip install --no-cache-dir dbt-postgres==1.8.1 requests
