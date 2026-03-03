"""
AutoQuant ETL — Database Migration Runner.

Simple migration runner that:
1. Tracks applied migrations in a _migrations table
2. Applies .sql files from /migrations/ in order
3. Applies /seeds/ after schema migrations

Usage:
    python -m src.utils.migrate           # Run all pending migrations
    python -m src.utils.migrate --seed    # Run migrations + seed data
    python -m src.utils.migrate --reset   # Drop schema and re-run everything
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

import asyncpg

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config import get_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"
SEEDS_DIR = Path(__file__).resolve().parent.parent.parent / "seeds"
STEP1_DIR = Path(__file__).resolve().parent.parent.parent.parent / "step1"


async def get_connection() -> asyncpg.Connection:
    """Get a direct database connection (not pooled) for migrations."""
    settings = get_settings()
    dsn = settings.db.url_direct or settings.db.url
    conn = await asyncpg.connect(dsn)
    return conn


async def ensure_migration_table(conn: asyncpg.Connection, schema: str) -> None:
    """Create the migrations tracking table if it doesn't exist."""
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    await conn.execute(f"SET search_path TO {schema}, public")
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(300) NOT NULL UNIQUE,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            checksum VARCHAR(64)
        )
    """)


async def get_applied_migrations(conn: asyncpg.Connection) -> set[str]:
    """Get the set of already-applied migration filenames."""
    rows = await conn.fetch("SELECT filename FROM _migrations ORDER BY id")
    return {row["filename"] for row in rows}


async def apply_sql_file(conn: asyncpg.Connection, filepath: Path, track: bool = True) -> None:
    """Execute a SQL file and record it in _migrations."""
    logger.info("Applying: %s", filepath.name)
    sql = filepath.read_text(encoding="utf-8")

    if not sql.strip():
        logger.warning("Empty file, skipping: %s", filepath.name)
        return

    try:
        await conn.execute(sql)
        if track:
            await conn.execute(
                "INSERT INTO _migrations (filename) VALUES ($1) ON CONFLICT DO NOTHING",
                filepath.name,
            )
        logger.info("Applied: %s", filepath.name)
    except Exception as e:
        logger.error("FAILED: %s — %s", filepath.name, e)
        raise


async def run_migrations(seed: bool = False, reset: bool = False) -> None:
    """Run all pending migrations (and optionally seeds)."""
    settings = get_settings()
    schema = settings.db.schema_name
    conn = await get_connection()

    try:
        if reset:
            logger.warning("RESETTING schema '%s' — all data will be lost!", schema)
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            logger.info("Schema dropped.")

        await ensure_migration_table(conn, schema)
        applied = await get_applied_migrations(conn)

        # 1. Run DDL from step1/ (the canonical full schema)
        ddl_file = STEP1_DIR / "001_ddl_full_schema.sql"
        if ddl_file.exists() and ddl_file.name not in applied:
            await apply_sql_file(conn, ddl_file)

        # 2. Run numbered migrations from /migrations/
        if MIGRATIONS_DIR.exists():
            migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
            for f in migration_files:
                if f.name not in applied:
                    await apply_sql_file(conn, f)

        # 3. Run seeds
        if seed or reset:
            seed_file = STEP1_DIR / "002_seed_dimensions.sql"
            if seed_file.exists() and seed_file.name not in applied:
                await apply_sql_file(conn, seed_file)

            if SEEDS_DIR.exists():
                seed_files = sorted(SEEDS_DIR.glob("*.sql"))
                for f in seed_files:
                    if f.name not in applied:
                        await apply_sql_file(conn, f)

        # Verify
        counts = await conn.fetch(f"""
            SELECT 'dim_date' AS tbl, COUNT(*) AS cnt FROM {schema}.dim_date
            UNION ALL SELECT 'dim_oem', COUNT(*) FROM {schema}.dim_oem
            UNION ALL SELECT 'dim_oem_alias', COUNT(*) FROM {schema}.dim_oem_alias
            UNION ALL SELECT 'dim_segment', COUNT(*) FROM {schema}.dim_segment
            UNION ALL SELECT 'dim_fuel', COUNT(*) FROM {schema}.dim_fuel
            UNION ALL SELECT 'dim_vehicle_class_map', COUNT(*) FROM {schema}.dim_vehicle_class_map
            UNION ALL SELECT 'dim_geo', COUNT(*) FROM {schema}.dim_geo
        """)
        logger.info("=== Table Counts ===")
        for row in counts:
            logger.info("  %s: %d", row["tbl"], row["cnt"])

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoQuant DB Migration Runner")
    parser.add_argument("--seed", action="store_true", help="Run seed data after migrations")
    parser.add_argument("--reset", action="store_true", help="Drop schema and re-run everything")
    args = parser.parse_args()

    asyncio.run(run_migrations(seed=args.seed, reset=args.reset))


if __name__ == "__main__":
    main()
