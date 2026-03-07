#!/bin/bash
# =============================================================================
# Kafka Topic Initialization Script — F2.2.3
# =============================================================================
# Purpose : Create all required Kafka topics idempotently on startup
# Topics  : raw-events · aggregated-metrics · dead-letter-queue
# Run     : Executed automatically by docker-compose kafka-init service
# Idempotent: --if-not-exists flag prevents errors on re-run
# =============================================================================

set -e  # Exit immediately on any error

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
KAFKA_BIN="/opt/kafka/bin/kafka-topics.sh"

echo "================================================================"
echo " Kafka Topic Initialization — F2"
echo " Bootstrap server: ${BOOTSTRAP_SERVER}"
echo "================================================================"

# Wait for Kafka to be fully ready
echo "[INFO] Waiting for Kafka broker to be ready..."
until ${KAFKA_BIN} --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; do
  echo "[INFO] Kafka not ready yet, retrying in 3s..."
  sleep 3
done
echo "[INFO] Kafka is ready."
echo ""

# ─── F2.1 Topic: raw-events ──────────────────────────────────────────────────
# Partitions : 6   (high parallelism for PySpark consumers)
# Retention  : 24h (86400000 ms)
# Producer   : Simulator (F1)
# Consumer   : PySpark Streaming (F3) — groups: pyspark-streaming, raw-ingester
echo "[INFO] Creating topic: raw-events (partitions=6, retention=24h)..."
${KAFKA_BIN} \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic raw-events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete
echo "[OK]   raw-events created."

# ─── F2.1 Topic: aggregated-metrics ──────────────────────────────────────────
# Partitions : 3   (moderate, downstream API/dbt consumers)
# Retention  : 48h (172800000 ms)
# Producer   : PySpark Streaming (F3)
# Consumer   : FastAPI (F7)
echo "[INFO] Creating topic: aggregated-metrics (partitions=3, retention=48h)..."
${KAFKA_BIN} \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic aggregated-metrics \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=172800000 \
  --config cleanup.policy=delete
echo "[OK]   aggregated-metrics created."

# ─── F2.1 Topic: dead-letter-queue ───────────────────────────────────────────
# Partitions : 1   (single, manual inspection only)
# Retention  : 72h (259200000 ms)
# Producer   : PySpark Streaming (F3) — malformed events
# Consumer   : Manual inspection / debugging
echo "[INFO] Creating topic: dead-letter-queue (partitions=1, retention=72h)..."
${KAFKA_BIN} \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create --if-not-exists \
  --topic dead-letter-queue \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete
echo "[OK]   dead-letter-queue created."

# ─── Verify: list all topics ─────────────────────────────────────────────────
echo ""
echo "================================================================"
echo " Topics successfully initialized:"
${KAFKA_BIN} --bootstrap-server "${BOOTSTRAP_SERVER}" --list
echo "================================================================"
echo " Consumer groups defined (F2.3):"
echo "   pyspark-streaming → raw-events"
echo "   raw-ingester      → raw-events"
echo "================================================================"
