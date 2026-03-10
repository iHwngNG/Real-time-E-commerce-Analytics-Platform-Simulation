FROM python:3.10-slim

# Optimize Python execution & set Java Home
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Install Java and procps for PySpark
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create appuser and ensure checkpoints directory exists
RUN adduser --uid 1000 --disabled-password --gecos "" --home /home/appuser appuser \
    && mkdir -p /app/checkpoints \
    && chown -R appuser:appuser /app /home/appuser

USER appuser
ENV HOME=/home/appuser

# Pre-download Spark Jars into Ivy cache
RUN python -c "from pyspark.sql import SparkSession; \
    SparkSession.builder.config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3').getOrCreate().stop()"

# Copy source code
COPY --chown=appuser:appuser . .

# Entrypoint — F3
CMD ["python", "main.py"]
