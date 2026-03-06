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
        self._schema = self._settings.schema_name  # e.g. "autoquant"

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
            """Set schema search path on every new connection.
            Note: In Supavisor transaction mode, SET commands may not persist
            between queries, so all table references should also be
            schema-qualified as a safety net."""
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

        self._search_path_sql = f"SET LOCAL search_path TO {s.schema_name}, public"

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
    # In Supavisor transaction mode, each auto-committed statement may route to
    # a different backend Postgres connection. To ensure search_path is set for
    # the actual query, we wrap SET LOCAL + query in an explicit transaction.
    # SET LOCAL only applies within the current transaction and is auto-reverted.

    _search_path_sql: str = ""  # populated in initialize()

    async def _with_search_path(self, conn: Connection, coro_factory):
        """Run a query inside a transaction with SET LOCAL search_path."""
        async with conn.transaction():
            await conn.execute(self._search_path_sql)
            return await coro_factory()

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        """Fetch multiple rows."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await self._with_search_path(
                conn, lambda: conn.fetch(query, *args)
            )

    async def fetchrow(self, query: str, *args: Any) -> Optional[asyncpg.Record]:
        """Fetch a single row."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await self._with_search_path(
                conn, lambda: conn.fetchrow(query, *args)
            )

    async def fetchval(self, query: str, *args: Any) -> Any:
        """Fetch a single value."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await self._with_search_path(
                conn, lambda: conn.fetchval(query, *args)
            )

    async def execute(self, query: str, *args: Any) -> str:
        """Execute a query (INSERT/UPDATE/DELETE). Returns status string."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            return await self._with_search_path(
                conn, lambda: conn.execute(query, *args)
            )

    async def executemany(self, query: str, args: list[tuple]) -> None:
        """Execute a query with many parameter sets (batch insert)."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(self._search_path_sql)
                await conn.executemany(query, args)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Connection, None]:
        """Acquire a connection and start a transaction with search_path set."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(self._search_path_sql)
                yield conn

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[Connection, None]:
        """Acquire a raw connection from the pool with search_path set."""
        pool = self._ensure_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(self._search_path_sql)
                yield conn

    # ── Utility Methods ──

    def _qualified(self, name: str) -> str:
        """Return schema-qualified table/view name if not already qualified."""
        if "." in name:
            return name
        return f"{self._schema}.{name}"

    async def table_count(self, table: str) -> int:
        """Quick row count for a table (auto schema-qualified)."""
        if not table.replace("_", "").replace(".", "").isalnum():
            raise ValueError(f"Invalid table name: {table}")
        qualified = self._qualified(table)
        result = await self.fetchval(f"SELECT COUNT(*) FROM {qualified}")
        return result or 0

    async def refresh_materialized_view(self, view_name: str) -> None:
        """Refresh a materialized view concurrently (auto schema-qualified)."""
        qualified = self._qualified(view_name)
        logger.info("Refreshing materialized view: %s", qualified)
        await self.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified}")
        logger.info("Materialized view refreshed: %s", qualified)

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
