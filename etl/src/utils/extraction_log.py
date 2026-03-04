"""
AutoQuant ETL — Extraction Log Manager.

Every ETL run gets a unique run_id from raw_extraction_log.
This module handles creating, updating, and querying run records.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from src.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class ExtractionLogManager:
    """Manages raw_extraction_log entries for ETL audit trail."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    async def start_run(self, source: str, notes: Optional[str] = None) -> int:
        """
        Create a new extraction run entry. Returns the run_id.
        """
        run_id = await self._db.fetchval(
            """
            INSERT INTO raw_extraction_log (source, started_at, status, notes)
            VALUES ($1, $2, 'RUNNING', $3)
            RETURNING run_id
            """,
            source,
            datetime.now(timezone.utc),
            notes,
        )
        logger.info("Started extraction run %d for source %s", run_id, source)
        return run_id

    async def complete_run(
        self,
        run_id: int,
        status: str,
        records_extracted: int,
        error_message: Optional[str] = None,
    ) -> None:
        """Update run entry on completion."""
        await self._db.execute(
            """
            UPDATE raw_extraction_log
            SET completed_at = $1, status = $2, records_extracted = $3, error_message = $4
            WHERE run_id = $5
            """,
            datetime.now(timezone.utc),
            status,
            records_extracted,
            error_message,
            run_id,
        )
        logger.info(
            "Completed run %d: status=%s, records=%d", run_id, status, records_extracted
        )

    async def get_last_successful_run(self, source: str) -> Optional[dict]:
        """Get the most recent successful run for a source."""
        row = await self._db.fetchrow(
            """
            SELECT run_id, started_at, completed_at, records_extracted
            FROM raw_extraction_log
            WHERE source = $1 AND status = 'SUCCESS'
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            source,
        )
        if row:
            return dict(row)
        return None
