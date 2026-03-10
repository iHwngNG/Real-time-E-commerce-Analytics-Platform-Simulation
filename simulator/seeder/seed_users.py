import os
import sys
import psycopg2
from psycopg2.extras import execute_values

# Add simulator root to path for generator imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generators.user_generator import UserGenerator

# =============================================================================
# Seed Users — F1.5.1
# Purpose: Generate users and batch INSERT into PostgreSQL [users] table.
# Idempotent: Uses ON CONFLICT DO NOTHING to avoid duplicates on re-run.
# =============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce"
)
NUM_USERS = int(os.environ.get("NUM_USERS", 10000))
BATCH_SIZE = 500  # Rows per INSERT for optimal throughput


def seed_users():
    """Generate and insert users into PostgreSQL in batches."""
    print(f"[seed_users] Generating {NUM_USERS:,} users...")
    generator = UserGenerator()
    users = generator.generate_batch(NUM_USERS)

    print("[seed_users] Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # Column order must match the VALUES tuple below
    insert_sql = """
        INSERT INTO users (
            user_id, username, email, full_name, gender, date_of_birth,
            country, city, timezone, preferred_device, os, preferred_language,
            user_segment, purchase_frequency, price_sensitivity,
            registered_at, is_active, created_at
        ) VALUES %s
        ON CONFLICT (user_id) DO NOTHING
    """

    inserted = 0
    skipped = 0

    # Process in batches for memory efficiency and speed
    for i in range(0, len(users), BATCH_SIZE):
        batch = users[i : i + BATCH_SIZE]

        # Convert each user dict to a tuple matching column order
        values = [
            (
                u["user_id"],
                u["username"],
                u["email"],
                u["full_name"],
                u["gender"],
                u["date_of_birth"],
                u["country"],
                u["city"],
                u["timezone"],
                u["preferred_device"],
                u["os"],
                u["preferred_language"],
                u["user_segment"],
                u["purchase_frequency"],
                u["price_sensitivity"],
                u["registered_at"],
                u["is_active"],
                u["created_at"],
            )
            for u in batch
        ]

        try:
            execute_values(cur, insert_sql, values, page_size=BATCH_SIZE)
            batch_inserted = cur.rowcount
            inserted += batch_inserted
            skipped += len(batch) - batch_inserted
            conn.commit()
        except Exception as e:
            conn.rollback()
            error_msg = str(e).lower()
            if "already exists" in error_msg or "unique constraint" in error_msg:
                # If batch fails due to uniqueness (like username or user_id)
                skipped += len(batch)
                print(
                    f"[seed_users] Skipping batch {i // BATCH_SIZE + 1} due to duplicate data"
                )
            else:
                print(f"[seed_users] ERROR on batch {i // BATCH_SIZE + 1}: {e}")
                raise

        progress = min(i + BATCH_SIZE, len(users))
        print(
            f"[seed_users] Progress: {progress:,}/{NUM_USERS:,} "
            f"(inserted={inserted:,}, skipped={skipped:,})"
        )

    cur.close()
    conn.close()

    print(f"[seed_users] DONE — Inserted: {inserted:,}, Skipped: {skipped:,}")
    return inserted


if __name__ == "__main__":
    seed_users()
