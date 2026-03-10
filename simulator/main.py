"""
F1 — Simulator Main Entry Point
Entrypoint: python main.py --mode [seed|simulate|both]

Modes:
  seed     — Seed users & products into PostgreSQL (run once).
  simulate — Start continuous event streaming to Kafka.
  both     — Seed first, then start simulation.
"""

import os
import sys
import time
import signal
import logging
import argparse

import psycopg2

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

from generators.event_generator import EventGenerator, load_config
from producer.kafka_producer import EventProducer
from seeder.seed_users import seed_users
from seeder.seed_products import seed_products

# =============================================================================
# Logging setup
# =============================================================================
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("Simulator")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce"
)

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global _shutdown
    logger.info("Received shutdown signal (%s). Finishing current batch...", signum)
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# =============================================================================
# F1.5 — Seed mode
# =============================================================================
def run_seed():
    """
    F1.5 — Seed users and products into PostgreSQL.
    Idempotent: ON CONFLICT DO NOTHING prevents duplicates on re-run.
    """
    logger.info("=== SEED MODE ===")
    logger.info("Step 1/2: Seeding users...")
    user_count = seed_users()
    logger.info("Step 2/2: Seeding products...")
    product_count = seed_products()
    logger.info("Seeding complete — users: %d, products: %d", user_count, product_count)


# =============================================================================
# F1.3 + F1.4 — Simulate mode
# =============================================================================
def _load_users_from_db() -> list:
    """Load active user records from PostgreSQL for event generation."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, user_segment, country, city, "
        "preferred_device, os, preferred_language, is_active "
        "FROM users WHERE is_active = true"
    )
    columns = [desc[0] for desc in cur.description]
    users = [dict(zip(columns, row)) for row in cur.fetchall()]

    # Convert UUID to string for JSON serialization
    for u in users:
        u["user_id"] = str(u["user_id"])

    cur.close()
    conn.close()
    logger.info("Loaded %d active users from database", len(users))
    return users


def _load_products_from_db() -> list:
    """Load product records from PostgreSQL for event generation."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute(
        "SELECT product_id, sku, product_name, brand, category_name, "
        "subcategory, sale_price, currency, popularity_score "
        "FROM products WHERE is_available = true"
    )
    columns = [desc[0] for desc in cur.description]
    products = [dict(zip(columns, row)) for row in cur.fetchall()]

    # Convert UUID and Decimal to serializable types
    for p in products:
        p["product_id"] = str(p["product_id"])
        p["sale_price"] = float(p["sale_price"]) if p["sale_price"] else 0.0
        p["popularity_score"] = int(p["popularity_score"] or 1)

    cur.close()
    conn.close()
    logger.info("Loaded %d available products from database", len(products))
    return products


def run_simulate():
    """
    F1.3 + F1.4 — Continuous event simulation loop.
    Generates events at TARGET_EVENTS_PER_SEC and publishes to Kafka.
    Supports peak hour multiplier and timed/continuous run modes.
    """
    global _shutdown

    logger.info("=== SIMULATE MODE ===")

    # Load seed data from database
    users = _load_users_from_db()
    products = _load_products_from_db()

    if not users:
        logger.error("No users in database. Run with --mode seed first.")
        sys.exit(1)
    if not products:
        logger.error("No products in database. Run with --mode seed first.")
        sys.exit(1)

    # Initialize components
    config = load_config()
    generator = EventGenerator(users=users, products=products, config=config)
    producer = EventProducer()

    # Determine run duration
    run_duration = generator.get_run_duration()
    mode_label = f"timed:{run_duration}s" if run_duration > 0 else "continuous"

    logger.info(
        "Starting simulation | mode=%s | active_users=%d | products=%d | "
        "target_rate=%d events/s | peak_hour=%s",
        mode_label,
        len(generator.active_users),
        len(products),
        config["target_events_per_sec"],
        config["peak_hour_enabled"],
    )

    # =========================================================================
    # Main simulation loop
    # =========================================================================
    start_time = time.time()
    total_events = 0
    log_interval = 2  # Frequent stats for visual feedback
    last_log_time = start_time

    # Accurate rate limiting
    # target_start_time = start_time

    try:
        while not _shutdown:
            # Check timed mode
            now = time.time()
            elapsed = now - start_time
            if run_duration > 0 and elapsed >= run_duration:
                logger.info("Timed mode expired after %ds. Stopping.", run_duration)
                break

            # Determine current rate (may vary with peak hours)
            current_rate = generator.get_current_rate()
            if current_rate <= 0:
                time.sleep(1)
                continue

            # Calculate how many events we SHOULD have sent by now
            target_total = int((now - start_time) * current_rate)
            to_send = target_total - total_events

            if to_send > 0:
                # Cap batch size to avoid long processing spikes
                batch_to_send = min(to_send, 100)
                events = generator.generate_batch(batch_to_send)
                producer.publish_batch(events)
                total_events += len(events)

            # Periodic stats logging
            if now - last_log_time >= log_interval:
                actual_rate = (
                    total_events / (now - start_time) if (now - start_time) > 0 else 0
                )
                stats = producer.get_stats()
                logger.info(
                    "STREAMING: Sent: %d | Rate: %.1f/s (Target: %d/s) | "
                    "Kafka OK: %d | Err: %d | Peak: %s",
                    total_events,
                    actual_rate,
                    current_rate,
                    stats["sent"],
                    stats["errors"],
                    generator.is_peak_hour(),
                )
                last_log_time = now

            # Sleep a tiny bit to avoid busy-waiting
            time.sleep(0.01)

    except Exception as exc:
        logger.exception("Simulation error: %s", exc)
    finally:
        # Graceful shutdown
        producer.close()
        elapsed = time.time() - start_time
        logger.info(
            "=== SIMULATION STOPPED ===\n"
            "  Total events : %d\n"
            "  Duration     : %.1f seconds\n"
            "  Avg rate     : %.0f events/s",
            total_events,
            elapsed,
            total_events / elapsed if elapsed > 0 else 0,
        )


# =============================================================================
# CLI entry point
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="E-commerce Simulator — F1")
    parser.add_argument(
        "--mode",
        choices=["seed", "simulate", "both"],
        default="simulate",
        help="seed: populate DB | simulate: stream events | both: seed then simulate",
    )
    args = parser.parse_args()

    logger.info("Simulator starting | mode=%s", args.mode)

    if args.mode == "seed":
        run_seed()
    elif args.mode == "simulate":
        run_simulate()
    elif args.mode == "both":
        run_seed()
        run_simulate()

    logger.info("Simulator finished.")


if __name__ == "__main__":
    main()
