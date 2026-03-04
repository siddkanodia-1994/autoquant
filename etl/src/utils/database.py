"""
AutoQuant ETL — Async Database Connection Manager.

Uses asyncpg with INDIVIDUAL connection parameters (not a DSN string)
to avoid URL-parsing issues with special characters in passwords.

Connection strategy:
  - Passes host, port, user, password, database separately to asyncpg
  - Uses ssl="require" for Supabase
  - For Supavisor transaction mode (port 6543): disables prepared statements
"""

import asyncio
import logging
import ssl as _ssl
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import asyncpg
from asyncpg import Pool, Connection

from config import get_settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Async database connection pool manager.

    Usage:
        db = DatabaseManager()
        await db.initialize()
        rows = await db.fetch("SELECT * FROM dim_oem WHERE is_listed = $1", True)
        await db.close()
    """

    def __init__(self) -> None:
        self._pool: Optional[Pool] = None
        self._settings = get_settings().db

    async def initialize(self) -> None:
        """Create the connection pool using individual parameters."""
        if self._pool is not None:
            return

        s = self._settings
        logger.info(
            "Connecting to %s:%d as %s (db=%s, schema=%s)",
            s.host, s.port, s.user, s.name, s.schema_name,
        )

        async def _init_connection(conn: Connection) -> None:
            """Set schema search path on every new connection."""
            await conn.execute(
                f"SET search_path TO {s.schema_name}, public"
            )
            await conn.execute(
                f"SET statement_timeout = '{s.statement_timeout_ms}'"
            )

        pool_kwargs: dict[str, Any] = {
            "host": s.host,
            "port": s.port,
            "user": s.user,
            "password": s.password,
            "database": s.name,
            "min_size": s.pool_min_size,
            "max_size": s.pool_max_size,
            "init": _init_connection,
            "command_timeout": 60,
            "ssl": "require",  # Supabase always requires SSL
        }

        # Supavisor transaction mode doesn't support prepared statements
        if s.port == 6543:
            pool_kwargs["statement_cache_size"] = 0
            logger.info("Supavisor transaction mode detected (port 6543)")

        try:
            self._pool = await asyncio.wait_for(
                asyncpg.create_pool(**pool_kwargs),
                timeout=15.0,
            )
            logger.info(
                "Database pool created (host=%s, min=%d, max=%d)",
                s.host, s.pool_min_size, s.pool_max_size,
            )
        except Exception as e:
            logger.error("Database connection failed: %s", e)
            raise

    async def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database pool closed.")

    def _ensure_pool(self) -> Pool:
        if self._pool is None:
            raise RuntimeError("Database pool not initialized. Call initialize() first.")
        return self._pool

    # ── Query Methods ──

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """Fetch multiple rows."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        """Fetch a single row."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Fetch a single value."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args: Any) -> str:
        """Execute a query (INSERT/UPDATE/DELETE). Returns status string."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args: list[tuple]) -> None:
        """Execute a query with many parameter sets (batch insert)."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            await conn.executemany(query, args)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Connection, None]:
        """Acquire a connection and start a transaction."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection, None]:
        """Acquire a raw connection from the pool."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            yield conn

    # ── Utility Methods ──

    async def table_count(self, table: str) -> int:
        """Quick row count for a table."""
        if not table.replace("_", "").isalnum():
            raise ValueError(f"Invalid table name: {table}")
        result = await self.fetchval(f"SELECT COUNT(*) FROM {table}")
        return result or 0

    async def refresh_materialized_view(self, view_name: str) -> None:
        """Refresh a materialized view concurrently."""
        logger.info("Refreshing materialized view: %s", view_name)
        await self.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
        logger.info("Materialized view refreshed: %s", view_name)

    async def health_check(self) -> bool:
        """Verify the database connection is alive."""
        try:
            result = await self.fetchval("SELECT 1")
            return result == 1
        except Exception as e:
            logger.error("Database health check failed: %s", e)
            return False


# ── Singleton ──
_db: Optional[DatabaseManager] = None


async def get_db() -> DatabaseManager:
    """Get or create the database manager singleton."""
    global _db
    if _db is None:
        _db = DatabaseManager()
        await _db.initialize()
    return _db


async def close_db() -> None:
    """Close the database singleton."""
    global _db
    if _db is not None:
        await _db.close()
        _db = None
