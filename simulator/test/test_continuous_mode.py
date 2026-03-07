import os
import sys
import time
from datetime import datetime

# Add simulator path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generators.user_generator import UserGenerator
from generators.product_generator import ProductGenerator
from generators.event_generator import EventGenerator, load_config

# =============================================================================
# High-Throughput Continuous Mode Test
# Simulates realistic load: thousands of events/sec using batch generation.
# =============================================================================


def main():
    # Force continuous mode, target 500 events/sec (adjustable)
    os.environ["RUN_MODE"] = "continuous"
    os.environ["TARGET_EVENTS_PER_SEC"] = "500"

    print("================================================================")
    print(" CONTINUOUS MODE — HIGH THROUGHPUT TEST")
    print("================================================================")
    print(" Preparing seed data...")

    # Prepare realistic-sized seed data
    user_gen = UserGenerator()
    prod_gen = ProductGenerator()
    users = user_gen.generate_batch(1000)  # 1000 users
    products = prod_gen.generate_batch(500)  # 500 products

    cfg = load_config()
    event_gen = EventGenerator(users=users, products=products, config=cfg)

    target_rate = event_gen.get_current_rate()
    # Generate events in chunks per second instead of one-by-one sleep
    batch_size = int(target_rate)  # Events to generate per cycle (1 second)

    print(f" Users loaded       : {len(event_gen.active_users)}")
    print(f" Products loaded    : {len(products)}")
    print(f" Target rate        : {target_rate} events/sec")
    print(f" Peak hour active   : {event_gen.is_peak_hour()}")
    print(f" Batch size / cycle : {batch_size}")
    print("================================================================")
    print(" Press Ctrl+C to stop\n")

    total_events = 0
    start_time = time.time()

    # Track event type distribution for analytics
    event_type_counts = {}

    try:
        while True:
            cycle_start = time.time()

            # Generate a full batch at once (much faster than 1-by-1)
            batch = event_gen.generate_batch(batch_size)

            # Count event types for distribution report
            for event in batch:
                e_type = event["event_type"]
                event_type_counts[e_type] = event_type_counts.get(e_type, 0) + 1

            total_events += len(batch)
            elapsed_total = time.time() - start_time
            actual_rate = total_events / elapsed_total if elapsed_total > 0 else 0

            # Print 1-line summary per second (not per event)
            ts = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts}] "
                f"Batch: {len(batch):>5} events | "
                f"Total: {total_events:>10,} | "
                f"Rate: {actual_rate:>8,.1f} evt/s | "
                f"Elapsed: {elapsed_total:>6.1f}s"
            )

            # Sleep to maintain target rate (subtract generation time)
            cycle_elapsed = time.time() - cycle_start
            sleep_remaining = max(0, 1.0 - cycle_elapsed)
            if sleep_remaining > 0:
                time.sleep(sleep_remaining)

    except KeyboardInterrupt:
        print("\n\n🛑 STOPPED BY USER (Ctrl+C)")

    finally:
        elapsed = time.time() - start_time
        print("\n================================================================")
        print(" 📊 PERFORMANCE REPORT")
        print("================================================================")
        print(f" Total events generated : {total_events:>12,}")
        print(f" Total time             : {elapsed:>12.2f} seconds")
        if elapsed > 0:
            print(
                f" Average throughput     : {total_events / elapsed:>12,.1f} events/sec"
            )
        print()
        print(" Event Type Distribution:")
        print(" " + "-" * 40)
        for e_type, count in sorted(
            event_type_counts.items(), key=lambda x: x[1], reverse=True
        ):
            pct = (count / total_events * 100) if total_events > 0 else 0
            bar = "█" * int(pct)
            print(f"   {e_type:<20} {count:>8,}  ({pct:>5.1f}%) {bar}")
        print("================================================================\n")


if __name__ == "__main__":
    main()
