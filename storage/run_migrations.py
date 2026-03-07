# =============================================================================
# PostgreSQL Migration Runner — F4.4.2
# =============================================================================
# Purpose   : Apply SQL files sequentially (idempotent, using schema_migrations)
# Executed  : Automatically by storage-init container
# =============================================================================

import os
import psycopg2
from psycopg2 import sql

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce"
)
MIGRATIONS_DIR = "/app/migrations"


def run_migrations():
    print(f"Connecting to Postgres: {DATABASE_URL.split('@')[-1]}")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Ensure migrations table exists
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR(255) PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
    """
    )

    # 2. Get list of files
    files = sorted([f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")])
    if not files:
        print("No migration files found.")
        return

    # 3. Read applied migrations
    cur.execute("SELECT version FROM schema_migrations")
    applied = set(row[0] for row in cur.fetchall())

    # 4. Apply new migrations sequentially
    for file in files:
        if file in applied:
            print(f"[SKIP] {file} already applied.")
            continue

        filepath = os.path.join(MIGRATIONS_DIR, file)
        print(f"[APPLY] Running {file} ...")

        with open(filepath, "r", encoding="utf-8") as f:
            sql_script = f.read()

        try:
            cur.execute(sql_script)
            cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (file,))
            print(f"  -> SUCCESS")
        except Exception as e:
            print(f"  -> ERROR applying {file}: {e}")
            conn.close()
            exit(1)

    print("All migrations are up-to-date!")
    conn.close()


if __name__ == "__main__":
    run_migrations()
