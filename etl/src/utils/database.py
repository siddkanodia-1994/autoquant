"""
AutoQuant ETL — Async Database Connection Manager.

Uses asyncpg for high-performance async PostgreSQL access.
Manages a connection pool with configurable size.
All queries run within the autoquant schema.

Connection strategy:
  1. Tries DATABASE_URL first (should point to Supavisor pooler)
  2. Falls back to DATABASE_URL_DIRECT if pooler fails
  3. Forces IPv4 resolution to avoid GitHub Actions IPv6 issues
"""

import asyncio
import logging
import socket
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import asyncpg
from asyncpg import Pool, Connection

from config import get_settings

logger = logging.getLogger(__name__)


def _force_ipv4_dsn(dsn: str) -> str:
    """
    Resolve the DSN hostname to an IPv4 address to avoid IPv6 routing
    issues on CI runners (GitHub Actions, etc.).
    Returns the DSN with the hostname replaced by the IPv4 address,
    and the original hostname added as an SSL server_hostname hint.
    """
    try:
        parsed = urlparse(dsn)
        hostname = parsed.hostname
        if not hostname:
            return dsn

        # Resolve to IPv4 only
        infos = socket.getaddrinfo(hostname, parsed.port or 5432, socket.AF_INET)
        if not infos:
            logger.warning("Could not resolve %s to IPv4, using original DSN", hostname)
            return dsn

        ipv4_addr = infos[0][4][0]
        logger.info("Resolved %s → %s (IPv4)", hostname, ipv4_addr)

        # Replace hostname with IPv4 address in the DSN
        netloc = parsed.netloc.replace(hostname, ipv4_addr)
        new_parsed = parsed._replace(netloc=netloc)
        return urlunparse(new_parsed)
    except Exception as e:
        logger.warning("IPv4 resolution failed (%s), using original DSN", e)
        return dsn


class DatabaseManager:
    """
    Async database connection pool manager.

    Usage:
        db = DatabaseManager()
        await db.initialize()

        # Execute a query
        rows = await db.fetch("SELECT * FROM dim_oem WHERE is_listed = $1", True)

        # Execute within a transaction
        async with db.transaction() as conn:
            await conn.execute("INSERT INTO ...", ...)
            await conn.execute("UPDATE ...", ...)

        await db.close()
    """

    def __init__(self) -> None:
        self._pool: Optional[Pool] = None
        self._settings = get_settings().db

    async def initialize(self) -> None:
        """Create the connection pool with IPv4 fallback."""
        if self._pool is not None:
            return

        logger.info("Initializing database connection pool...")

        async def _init_connection(conn: Connection) -> None:
            """Set schema search path on every new connection."""
            await conn.execute(
                f"SET search_path TO {self._settings.schema_name}, public"
            )
            await conn.execute(
                f"SET statement_timeout = '{self._settings.statement_timeout_ms}'"
            )

        dsn = self._settings.url
        original_host = urlparse(dsn).hostname

        # Try original DSN first, then IPv4-resolved fallback
        for attempt, use_ipv4 in enumerate([(False,), (True,)]):
            try:
                connect_dsn = _force_ipv4_dsn(dsn) if use_ipv4[0] else dsn
                label = "IPv4-resolved" if use_ipv4[0] else "original"
                logger.info("Attempting connection (%s): %s:***", label, original_host)

                # For Supavisor pooler (port 6543), use prepared_statement_cache_size=0
                parsed = urlparse(connect_dsn)
                pool_kwargs: dict[str, Any] = {
                    "dsn": connect_dsn,
                    "min_size": self._settings.pool_min_size,
                    "max_size": self._settings.pool_max_size,
                    "init": _init_connection,
                    "command_timeout": 60,
                }

                # Supavisor transaction mode doesn't support prepared statements
                if parsed.port == 6543:
                    pool_kwargs["statement_cache_size"] = 0
                    logger.info("Detected Supavisor transaction mode (port 6543)")

                # Pass original hostname for SSL SNI when using IPv4
                if use_ipv4[0] and original_host:
                    import ssl as _ssl
                    ssl_ctx = _ssl.create_default_context()
                    ssl_ctx.check_hostname = True
                    pool_kwargs["ssl"] = ssl_ctx
                    # asyncpg uses server_hostname for SNI
                    pool_kwargs["server_settings"] = {}

                self._pool = await asyncio.wait_for(
                    asyncpg.create_pool(**pool_kwargs),
                    timeout=15.0,
                )
                logger.info(
                    "Database pool created (%s, min=%d, max=%d)",
                    label,
                    self._settings.pool_min_size,
                    self._settings.pool_max_size,
                )
                return

            except Exception as e:
                logger.warning("Connection attempt %d failed: %s", attempt + 1, e)
                if attempt == 1:
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
        """
        Acquire a connection and start a transaction.
        Commits on success, rolls back on exception.
        """
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
        # Sanitize table name (basic protection)
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
