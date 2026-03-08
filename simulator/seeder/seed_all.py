import os
import sys
import time

# Add simulator root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from seeder.seed_users import seed_users
from seeder.seed_products import seed_products

# =============================================================================
# Seed All — F1.5.3
# Purpose: Run seed_users then seed_products sequentially.
# Idempotent: Safe to re-run — ON CONFLICT skips existing records.
# =============================================================================


def seed_all():
    """Execute full database seeding pipeline."""
    print("=" * 60)
    print(" DATA SEEDER — F1.5")
    print("=" * 60)
    print(f" NUM_USERS   : {os.environ.get('NUM_USERS', '10000')}")
    print(f" NUM_PRODUCTS : {os.environ.get('NUM_PRODUCTS', '5000')}")
    print("=" * 60)

    overall_start = time.time()

    # F1.5.1 — Seed Users first
    print("\n[STEP 1/2] Seeding users...\n")
    start = time.time()
    users_inserted = seed_users()
    user_time = time.time() - start

    # F1.5.2 — Seed Products
    print("\n[STEP 2/2] Seeding products...\n")
    start = time.time()
    products_inserted = seed_products()
    product_time = time.time() - start

    overall_time = time.time() - overall_start

    # Summary report
    print("\n" + "=" * 60)
    print(" SEEDING COMPLETE")
    print("=" * 60)
    print(f" Users inserted    : {users_inserted:,}  ({user_time:.2f}s)")
    print(f" Products inserted : {products_inserted:,}  ({product_time:.2f}s)")
    print(f" Total time        : {overall_time:.2f}s")
    print("=" * 60)
    print(" Idempotency: Re-running will skip existing records (0 inserts).")
    print("=" * 60)


if __name__ == "__main__":
    seed_all()
