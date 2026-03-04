"""
AutoQuant ETL — Connector/Adapter Base Class.

All data source connectors (VAHAN, FADA, BSE, NAPIX) implement this interface.
This allows the VAHAN scraper to be swapped for the official NAPIX API feed
in the future without changing the warehouse, transforms, or frontend.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class ConnectorSource(str, Enum):
    VAHAN = "VAHAN"
    FADA = "FADA"
    BSE_WHOLESALE = "BSE_WHOLESALE"
    NAPIX = "NAPIX"
    SIAM_HISTORICAL = "SIAM_HISTORICAL"


class ExtractionStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    VALIDATION_FAILED = "VALIDATION_FAILED"


@dataclass
class ExtractionResult:
    """Standardized result from any connector extraction."""
    source: ConnectorSource
    status: ExtractionStatus
    records: list[dict[str, Any]] = field(default_factory=list)
    records_count: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def mark_complete(self, status: Optional[ExtractionStatus] = None) -> None:
        self.completed_at = datetime.now(timezone.utc)
        self.records_count = len(self.records)
        if status:
            self.status = status

    @property
    def duration_seconds(self) -> float:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    @property
    def is_success(self) -> bool:
        return self.status in (ExtractionStatus.SUCCESS, ExtractionStatus.PARTIAL)


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.

    Every connector must implement:
      - extract(): Pull data from the source and return ExtractionResult
      - health_check(): Verify connectivity to the data source

    The connector writes raw data to bronze tables.
    Transform pipeline reads from bronze and writes to silver/gold.
    """

    source: ConnectorSource

    @abstractmethod
    async def extract(
        self,
        period: str,
        segments: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Extract data from the source for a given period.

        Args:
            period: Time period to extract (e.g., '2026-03' for March 2026)
            segments: Optional filter for segments (e.g., ['PV', 'CV', '2W'])
            **kwargs: Source-specific parameters

        Returns:
            ExtractionResult with raw records
        """
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Verify the data source is reachable and responding.

        Returns:
            True if source is healthy, False otherwise
        """
        ...

    async def setup(self) -> None:
        """Optional setup/initialization (e.g., browser launch for Playwright)."""
        pass

    async def teardown(self) -> None:
        """Optional cleanup (e.g., browser close)."""
        pass

    async def __aenter__(self) -> "BaseConnector":
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.teardown()
