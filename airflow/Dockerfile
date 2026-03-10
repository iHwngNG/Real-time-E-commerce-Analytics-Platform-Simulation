# =============================================================================
# Airflow Dockerfile — F5.4
# Purpose: Extend official Airflow image with dbt-postgres for batch pipeline.
# =============================================================================
FROM apache/airflow:2.9.0

USER root
# Cài đặt build tools và libpq-dev để build dbt-postgres
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Không dùng --user vì Airflow container đã chạy trong một Python virtualenv
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root
# Sau khi compile và cài dbt (thường lưu ở ~/.local), xoá bỏ các gói build tools
RUN apt-get purge -y build-essential libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
