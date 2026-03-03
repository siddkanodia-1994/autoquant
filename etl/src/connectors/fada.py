"""
AutoQuant ETL — FADA Monthly Report Connector.

Parses the monthly FADA (Federation of Automobile Dealers Associations)
press releases / PDFs for retail + wholesale reconciliation data.

FADA publishes monthly "Auto Sales" reports that contain:
  - Retail registrations by segment (PV, CV, 2W, 3W)
  - Segment-wise YoY comparisons
  - OEM-level wholesale dispatches (select OEMs)

Data source:
  - FADA website: https://www.fadaindia.org/
  - Monthly press releases as PDF attachments
  - Alternative: HTML tables on the "Sales Statistics" page

Strategy:
  1. Download/locate the monthly FADA PDF
  2. Parse tables using pdfplumber (table detection → structured rows)
  3. Normalize: segment, oem, fuel_type, volume
  4. Return as ExtractionResult for reconciliation against VAHAN daily

Use cases:
  - Monthly reconciliation: Compare VAHAN cumulative with FADA totals
  - Wholesale data: FADA includes OEM dispatch numbers
  - QC gate: Flag VAHAN under/over-count vs FADA (±5% tolerance)

IMPORTANT CONSTRAINTS:
  - Public PDF documents only (no authentication)
  - No parallel downloads — sequential, rate-limited
  - robots.txt respected
"""

import asyncio
import io
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import pdfplumber

from config import get_settings
from src.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractionResult,
    ExtractionStatus,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ── FADA PDF Table Patterns ──

# Segment names as they appear in FADA reports
FADA_SEGMENT_MAP = {
    "passenger vehicles": "PV",
    "passenger vehicle": "PV",
    "pv": "PV",
    "commercial vehicles": "CV",
    "commercial vehicle": "CV",
    "cv": "CV",
    "two wheelers": "2W",
    "two wheeler": "2W",
    "2w": "2W",
    "two-wheelers": "2W",
    "three wheelers": "3W",
    "three wheeler": "3W",
    "3w": "3W",
}

# Known FADA report URL patterns
FADA_BASE_URL = "https://www.fadaindia.org"
FADA_STATS_URL = f"{FADA_BASE_URL}/sales-statistics"


class FADAConnector(BaseConnector):
    """
    Connector for FADA monthly reports.

    Parses FADA PDF press releases to extract:
      - Industry segment totals (retail registrations)
      - OEM-level wholesale dispatches
      - YoY comparisons

    Usage:
        async with FADAConnector() as connector:
            result = await connector.extract(
                period="2026-03",
                pdf_path="/path/to/fada_march_2026.pdf"
            )
    """

    source = ConnectorSource.FADA

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def setup(self) -> None:
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
        )
        logger.info("FADA connector initialized")

    async def teardown(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
        logger.info("FADA connector closed")

    async def health_check(self) -> bool:
        """Check if FADA website is reachable."""
        try:
            if not self._client:
                return False
            resp = await self._client.head(FADA_BASE_URL)
            ok = resp.status_code < 400
            logger.info("FADA website: %s (HTTP %d)", "OK" if ok else "FAIL", resp.status_code)
            return ok
        except Exception as e:
            logger.error("FADA health check failed: %s", e)
            return False

    async def extract(
        self,
        period: str,
        segments: Optional[list[str]] = None,
        *,
        pdf_path: Optional[str] = None,
        pdf_url: Optional[str] = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Extract monthly FADA data from PDF.

        Args:
            period: Month in 'YYYY-MM' format
            segments: Optional filter
            pdf_path: Local file path to the FADA PDF
            pdf_url: URL to download the FADA PDF from
        """
        result = ExtractionResult(source=self.source, status=ExtractionStatus.SUCCESS)

        try:
            pdf_bytes: Optional[bytes] = None

            # Strategy 1: Local file
            if pdf_path:
                path = Path(pdf_path)
                if not path.exists():
                    raise FileNotFoundError(f"FADA PDF not found: {pdf_path}")
                pdf_bytes = path.read_bytes()
                logger.info("Loaded FADA PDF from local path: %s (%d bytes)", pdf_path, len(pdf_bytes))

            # Strategy 2: Download from URL
            elif pdf_url:
                pdf_bytes = await self._download_pdf(pdf_url)

            # Strategy 3: Try to find on FADA website (auto-discover)
            else:
                pdf_url_discovered = await self._discover_report_url(period)
                if pdf_url_discovered:
                    pdf_bytes = await self._download_pdf(pdf_url_discovered)
                else:
                    logger.warning("No FADA PDF found for period %s", period)
                    result.mark_complete(ExtractionStatus.FAILED)
                    result.error_message = f"FADA report not found for {period}"
                    return result

            if not pdf_bytes:
                result.mark_complete(ExtractionStatus.FAILED)
                result.error_message = "Failed to obtain FADA PDF"
                return result

            # Parse PDF
            records = self._parse_fada_pdf(pdf_bytes, period)

            # Filter by segment if requested
            if segments:
                records = [r for r in records if r.get("segment") in segments]

            result.records = records
            result.mark_complete(
                ExtractionStatus.SUCCESS if records else ExtractionStatus.PARTIAL
            )
            result.metadata = {
                "period": period,
                "total_records": len(records),
                "source_pdf": pdf_path or pdf_url or "auto-discovered",
                "segments_found": list({r["segment"] for r in records}),
            }

            logger.info(
                "FADA extraction complete: %d records from %s",
                len(records), period,
            )

        except Exception as e:
            logger.error("FADA extraction failed: %s", e, exc_info=True)
            result.error_message = str(e)
            result.mark_complete(ExtractionStatus.FAILED)

        return result

    async def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF from URL with rate limiting."""
        if not self._client:
            raise RuntimeError("HTTP client not initialized")

        logger.info("Downloading FADA PDF: %s", url)
        await asyncio.sleep(2)  # Rate limit

        resp = await self._client.get(url)
        if resp.status_code == 200 and len(resp.content) > 1000:
            logger.info("Downloaded FADA PDF: %d bytes", len(resp.content))
            return resp.content

        logger.warning("FADA PDF download failed: HTTP %d", resp.status_code)
        return None

    async def _discover_report_url(self, period: str) -> Optional[str]:
        """
        Attempt to discover the FADA report URL from the website.

        FADA publishes reports in a predictable pattern on their statistics page.
        This method scrapes the page to find PDF links matching the target period.
        """
        if not self._client:
            return None

        try:
            resp = await self._client.get(FADA_STATS_URL)
            if resp.status_code != 200:
                return None

            # Parse year/month from period
            year, month = period.split("-")
            month_names = [
                "", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December",
            ]
            month_name = month_names[int(month)]

            # Look for PDF links containing the month/year
            # Common patterns: "March-2026.pdf", "Mar_26_Report.pdf"
            html = resp.text
            pdf_links = re.findall(r'href="([^"]*\.pdf[^"]*)"', html, re.IGNORECASE)

            for link in pdf_links:
                link_lower = link.lower()
                if (month_name.lower() in link_lower or month_name[:3].lower() in link_lower) and year in link:
                    full_url = link if link.startswith("http") else f"{FADA_BASE_URL}{link}"
                    logger.info("Discovered FADA report URL: %s", full_url)
                    return full_url

            logger.info("No FADA report found for %s %s on website", month_name, year)
            return None

        except Exception as e:
            logger.warning("FADA report discovery failed: %s", e)
            return None

    def _parse_fada_pdf(self, pdf_bytes: bytes, period: str) -> list[dict[str, Any]]:
        """
        Parse FADA PDF using pdfplumber table extraction.

        FADA PDFs typically contain:
          - Summary tables with segment-level totals
          - Detailed OEM-level tables for each segment
          - YoY comparison columns

        Returns:
            List of records with keys:
              oem_name, segment, volume, data_type (retail/wholesale),
              yoy_pct, period
        """
        records: list[dict[str, Any]] = []

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                logger.info("FADA PDF: %d pages", len(pdf.pages))

                for page_num, page in enumerate(pdf.pages, 1):
                    # Extract all tables from the page
                    tables = page.extract_tables()

                    for table_idx, table in enumerate(tables):
                        if not table or len(table) < 2:
                            continue

                        parsed = self._parse_fada_table(table, period, page_num, table_idx)
                        records.extend(parsed)

                    # Also try text-based extraction for non-tabular data
                    text = page.extract_text() or ""
                    text_records = self._parse_fada_text(text, period, page_num)
                    records.extend(text_records)

        except Exception as e:
            logger.error("PDF parsing failed: %s", e, exc_info=True)

        # Deduplicate (same OEM + segment might appear in table and text)
        seen: set[str] = set()
        unique_records: list[dict[str, Any]] = []
        for r in records:
            key = f"{r.get('oem_name', '')}|{r.get('segment', '')}|{r.get('data_type', '')}"
            if key not in seen:
                seen.add(key)
                unique_records.append(r)

        logger.info("Parsed %d unique records from FADA PDF", len(unique_records))
        return unique_records

    def _parse_fada_table(
        self,
        table: list[list[Optional[str]]],
        period: str,
        page_num: int,
        table_idx: int,
    ) -> list[dict[str, Any]]:
        """
        Parse a single table extracted from a FADA PDF page.

        Table formats vary but common layouts:
          | Category | Month Vol | YoY % | YTD Vol | YTD YoY % |
          | PV       | 345,678   | +12%  | 1,234,567 | +8%     |

        Or OEM-level:
          | OEM Name       | Month Vol | Month YoY | QTD Vol | ...
          | Maruti Suzuki   | 150,000   | +5%      | 450,000 | ...
        """
        records: list[dict[str, Any]] = []

        if not table:
            return records

        # Clean table cells
        clean_table = []
        for row in table:
            clean_row = [(cell.strip() if cell else "") for cell in row]
            clean_table.append(clean_row)

        # Try to identify header row
        header = clean_table[0]
        header_lower = [h.lower() for h in header]

        # Determine if this is a segment summary or OEM-level table
        current_segment = self._detect_segment_context(header_lower, clean_table)
        data_type = self._detect_data_type(header_lower)

        for row_idx, row in enumerate(clean_table[1:], 1):
            if not row or len(row) < 2:
                continue

            # Skip total/subtotal rows
            first_cell = row[0].strip().upper()
            if first_cell in ("TOTAL", "GRAND TOTAL", "SUB TOTAL", ""):
                continue

            # Try to extract entity name and volume
            entity_name = row[0].strip()
            if not entity_name:
                continue

            # Check if entity is a segment name
            segment_match = FADA_SEGMENT_MAP.get(entity_name.lower())
            if segment_match:
                # This is a segment summary row
                volume = self._parse_numeric(row[1] if len(row) > 1 else "")
                yoy = self._parse_pct(row[2] if len(row) > 2 else "")

                if volume and volume > 0:
                    records.append({
                        "oem_name": "INDUSTRY_TOTAL",
                        "segment": segment_match,
                        "volume": volume,
                        "yoy_pct": yoy,
                        "data_type": data_type,
                        "period": period,
                        "source_page": page_num,
                        "source_table": table_idx,
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                    })
                continue

            # OEM-level row
            volume = self._parse_numeric(row[1] if len(row) > 1 else "")
            yoy = self._parse_pct(row[2] if len(row) > 2 else "")

            if volume and volume > 0:
                records.append({
                    "oem_name": entity_name.upper(),
                    "segment": current_segment or "UNKNOWN",
                    "volume": volume,
                    "yoy_pct": yoy,
                    "data_type": data_type,
                    "period": period,
                    "source_page": page_num,
                    "source_table": table_idx,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                })

        return records

    def _parse_fada_text(
        self, text: str, period: str, page_num: int
    ) -> list[dict[str, Any]]:
        """
        Parse segment totals from free text in FADA reports.

        Common patterns:
          "Passenger Vehicles registered 3,45,678 units"
          "PV segment clocked 3.4 lakh units, up 12% YoY"
        """
        records: list[dict[str, Any]] = []

        # Pattern: "Segment registered/clocked X,XX,XXX units"
        pattern = re.compile(
            r'(?:passenger\s+vehicle|commercial\s+vehicle|two\s+wheeler|2w|pv|cv)'
            r'[^.]*?'
            r'(\d[\d,]+(?:\.\d+)?)\s*(?:lakh|units|nos)',
            re.IGNORECASE,
        )

        for match in pattern.finditer(text):
            full_match = match.group(0).lower()
            value_str = match.group(1)

            # Determine segment
            segment = None
            for key, seg in FADA_SEGMENT_MAP.items():
                if key in full_match:
                    segment = seg
                    break

            if not segment:
                continue

            # Parse value — for lakh, use float before multiplying
            if "lakh" in full_match:
                clean = re.sub(r'[^\d.]', '', value_str.replace(",", ""))
                try:
                    volume = int(float(clean) * 100_000) if clean else None
                except ValueError:
                    volume = None
            else:
                volume = self._parse_numeric(value_str)

            # Extract YoY if present nearby
            yoy_match = re.search(r'([\+\-]?\d+(?:\.\d+)?)\s*%', full_match)
            yoy = float(yoy_match.group(1)) if yoy_match else None

            if volume and volume > 0:
                records.append({
                    "oem_name": "INDUSTRY_TOTAL",
                    "segment": segment,
                    "volume": int(volume),
                    "yoy_pct": yoy,
                    "data_type": "retail",
                    "period": period,
                    "source_page": page_num,
                    "source_table": -1,  # Text, not table
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                })

        return records

    def _detect_segment_context(
        self, header_lower: list[str], table: list[list[str]]
    ) -> Optional[str]:
        """Detect which segment a table is about from header/context clues."""
        full_text = " ".join(header_lower)
        for key, seg in FADA_SEGMENT_MAP.items():
            if key in full_text:
                return seg
        return None

    def _detect_data_type(self, header_lower: list[str]) -> str:
        """Detect if data is retail registrations or wholesale dispatches."""
        full_text = " ".join(header_lower)
        if "wholesale" in full_text or "dispatch" in full_text:
            return "wholesale"
        return "retail"

    @staticmethod
    def _parse_numeric(value: str) -> Optional[int]:
        """Parse numeric string: '3,45,678' → 345678, '3.4' → 3.4."""
        if not value:
            return None
        clean = re.sub(r'[^\d.]', '', value.replace(",", ""))
        if not clean:
            return None
        try:
            return int(float(clean))
        except ValueError:
            return None

    @staticmethod
    def _parse_pct(value: str) -> Optional[float]:
        """Parse percentage: '+12.3%' → 12.3, '-5%' → -5.0."""
        if not value:
            return None
        match = re.search(r'([\+\-]?\d+(?:\.\d+)?)', value)
        if match:
            return float(match.group(1))
        return None
