import os
import sys
import json
import psycopg2
from psycopg2.extras import execute_values

# Add simulator root to path for generator imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from generators.product_generator import ProductGenerator

# =============================================================================
# Seed Products — F1.5.2
# Purpose: Generate products and batch INSERT into PostgreSQL [products] table.
# Idempotent: Uses ON CONFLICT DO NOTHING to avoid duplicates on re-run.
# =============================================================================

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce"
)
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", 5000))
BATCH_SIZE = 500  # Rows per INSERT for optimal throughput


def seed_products():
    """Generate and insert products into PostgreSQL in batches."""
    print(f"[seed_products] Generating {NUM_PRODUCTS:,} products...")
    generator = ProductGenerator()
    products = generator.generate_batch(NUM_PRODUCTS)

    print(f"[seed_products] Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO products (
            product_id, sku, product_name, brand, description, image_url,
            category_name, subcategory, tags, original_price, sale_price,
            discount_percent, stock_quantity, currency, rating_avg,
            rating_count, is_available, popularity_score,
            created_at, updated_at
        ) VALUES %s
        ON CONFLICT (product_id) DO NOTHING
    """

    inserted = 0
    skipped = 0

    for i in range(0, len(products), BATCH_SIZE):
        batch = products[i : i + BATCH_SIZE]

        # Convert each product dict to a tuple matching column order
        # tags must be serialized to JSON string for JSONB column
        values = [
            (
                p["product_id"],
                p["sku"],
                p["product_name"],
                p["brand"],
                p["description"],
                p["image_url"],
                p["category_name"],
                p["subcategory"],
                json.dumps(p["tags"]),
                p["original_price"],
                p["sale_price"],
                p["discount_percent"],
                p["stock_quantity"],
                p["currency"],
                p["rating_avg"],
                p["rating_count"],
                p["is_available"],
                p["popularity_score"],
                p["created_at"],
                p["updated_at"],
            )
            for p in batch
        ]

        try:
            execute_values(cur, insert_sql, values, page_size=BATCH_SIZE)
            batch_inserted = cur.rowcount
            inserted += batch_inserted
            skipped += len(batch) - batch_inserted
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[seed_products] ERROR on batch {i // BATCH_SIZE + 1}: {e}")
            raise

        progress = min(i + BATCH_SIZE, len(products))
        print(
            f"[seed_products] Progress: {progress:,}/{NUM_PRODUCTS:,} "
            f"(inserted={inserted:,}, skipped={skipped:,})"
        )

    cur.close()
    conn.close()

    print(f"[seed_products] DONE — Inserted: {inserted:,}, Skipped: {skipped:,}")
    return inserted


if __name__ == "__main__":
    seed_products()
