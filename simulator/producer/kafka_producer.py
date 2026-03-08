import os
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# =============================================================================
# Kafka Producer Module — F1.4
# Purpose: Serialize and publish events to Kafka with reliability guarantees.
# Output : Messages in Kafka topic [raw-events]
# =============================================================================

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("KafkaEventProducer")


class EventProducer:
    """
    Reliable Kafka producer for publishing e-commerce events.
    Implements:
        F1.4.1 — JSON serialization
        F1.4.2 — Partition key = user_id (ordering per user)
        F1.4.3 — Batch publish (max 100 msgs or 50ms linger)
        F1.4.4 — Retry 3x with exponential backoff
    """

    def __init__(self, bootstrap_servers: str = None, topic: str = "raw-events"):
        """
        Initialize KafkaProducer with PRD-compliant settings.
        Args:
            bootstrap_servers: Kafka broker address(es).
            topic: Target Kafka topic name.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
        )

        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            # F1.4.1 — Serialize event dict -> JSON bytes (UTF-8)
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            # F1.4.3 — Batch publish: buffer up to 100 messages or 50ms
            batch_size=100 * 1024,  # ~100 messages (~1KB each)
            linger_ms=50,  # Wait max 50ms to fill batch
            # F1.4.4 — Retry 3 times with exponential backoff
            retries=3,
            retry_backoff_ms=200,  # 200ms -> 400ms -> 800ms
            # Reliability settings
            acks="all",  # Wait for all replicas to confirm
            max_in_flight_requests_per_connection=5,
            compression_type="snappy",  # Compress batches for throughput
        )

        logger.info(
            f"Producer initialized | brokers={self.bootstrap_servers} "
            f"| topic={self.topic} | batch_linger=50ms | retries=3"
        )

        # Internal counters for monitoring
        self._sent_count = 0
        self._error_count = 0

    def _on_send_success(self, record_metadata):
        """Callback when message is successfully delivered."""
        self._sent_count += 1

    def _on_send_error(self, excp):
        """Callback when message delivery fails after all retries."""
        self._error_count += 1
        logger.error(f"Message delivery failed: {excp}")

    def publish_event(self, event: dict) -> None:
        """
        Publish a single event to Kafka asynchronously.
        Partition key = user_id to guarantee per-user ordering (F1.4.2).
        Args:
            event: Event dictionary from EventGenerator.
        """
        # F1.4.2 — Use user_id as partition key for ordering
        partition_key = event.get("user", {}).get("user_id", "")

        try:
            future = self._producer.send(self.topic, key=partition_key, value=event)
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

        except KafkaError as e:
            self._error_count += 1
            logger.error(f"Failed to enqueue message: {e}")

    def publish_batch(self, events: list) -> None:
        """
        Publish a batch of events to Kafka.
        Each event is sent asynchronously; flush() ensures delivery.
        Args:
            events: List of event dictionaries.
        """
        for event in events:
            self.publish_event(event)

        # Force send all buffered messages
        self._producer.flush()

    def get_stats(self) -> dict:
        """Return delivery statistics."""
        return {
            "sent": self._sent_count,
            "errors": self._error_count,
            "total_attempted": self._sent_count + self._error_count,
        }

    def close(self):
        """Flush remaining messages and close producer gracefully."""
        logger.info("Flushing and closing producer...")
        self._producer.flush(timeout=10)
        self._producer.close(timeout=10)
        stats = self.get_stats()
        logger.info(
            f"Producer closed | sent={stats['sent']} " f"| errors={stats['errors']}"
        )


# For local testing (requires running Kafka broker)
if __name__ == "__main__":
    import sys

    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from generators.user_generator import UserGenerator
    from generators.product_generator import ProductGenerator
    from generators.event_generator import EventGenerator, load_config

    print("=== Kafka Producer Test ===")
    print("Generating sample data...")

    user_gen = UserGenerator()
    prod_gen = ProductGenerator()
    users = user_gen.generate_batch(50)
    products = prod_gen.generate_batch(20)

    cfg = load_config()
    event_gen = EventGenerator(users=users, products=products, config=cfg)

    # Generate a small batch of events
    events = event_gen.generate_batch(10)

    print(
        f"Connecting to Kafka at {os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')}..."
    )

    producer = EventProducer()

    try:
        producer.publish_batch(events)
        stats = producer.get_stats()
        print(f"\n📊 Results: Sent={stats['sent']}, Errors={stats['errors']}")
    finally:
        producer.close()
