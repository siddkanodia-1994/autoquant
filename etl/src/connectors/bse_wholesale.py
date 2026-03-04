"""
AutoQuant ETL — BSE Wholesale Dispatch Connector.

Scrapes BSE (Bombay Stock Exchange) corporate filings for OEM-published
monthly wholesale/dispatch numbers.

Listed auto OEMs are required to publish monthly sales/dispatch data
as corporate announcements on BSE. These filings are publicly accessible at:
  https://www.bseindia.com/corporates/ann.html

Data flow:
  1. For each in-scope OEM (by BSE code), query BSE announcements API
  2. Filter for "Sales Data" / "Auto Sales" category filings
  3. Download PDF/HTML attachment
  4. Parse wholesale dispatch numbers by segment
  5. Return as ExtractionResult

OEMs typically report:
  - Domestic wholesale dispatches
  - Export dispatches
  - Total production (some OEMs)
  - Segment breakdown (PV/UV/CV/2W)
  - ICE vs EV split (increasingly common post-2024)

Use cases:
  - Cross-reference retail (VAHAN) vs wholesale (BSE) for inventory pipeline analysis
  - Gold layer: compute retail-to-wholesale ratio
  - Revenue estimation: wholesale × ASP as proxy

CONSTRAINTS:
  - Public data only (BSE announcements are freely available)
  - Rate-limited: 2-3s between API calls
  - No authentication required for public filings
"""

import asyncio
import re
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from config import get_settings
from src.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractionResult,
    ExtractionStatus,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# BSE API endpoints
BSE_API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_ANNOUNCEMENTS_URL = f"{BSE_API_BASE}/AnnGetData/w"

# In-scope OEMs with BSE codes (from dim_oem)
# These need to match the BSE codes in the database
BSE_AUTO_OEMS: dict[str, dict[str, str]] = {
    "MARUTI": {"bse_code": "532500", "name": "Maruti Suzuki India Ltd"},
    "TATAMOTORS_PV": {"bse_code": "500570", "name": "Tata Motors Ltd (PV)"},
    "TATAMOTORS_CV": {"bse_code": "500570", "name": "Tata Motors Ltd (CV)"},
    "M&M": {"bse_code": "500520", "name": "Mahindra & Mahindra Ltd"},
    "HYUNDAI": {"bse_code": "544274", "name": "Hyundai Motor India Ltd"},
    "BAJAJ-AUTO": {"bse_code": "532977", "name": "Bajaj Auto Ltd"},
    "HEROMOTOCO": {"bse_code": "500182", "name": "Hero MotoCorp Ltd"},
    "EICHERMOT": {"bse_code": "505200", "name": "Eicher Motors Ltd"},
    "TVSMOTOR": {"bse_code": "532343", "name": "TVS Motor Company Ltd"},
    "ASHOKLEY": {"bse_code": "500477", "name": "Ashok Leyland Ltd"},
    "ESCORTS": {"bse_code": "500495", "name": "Escorts Kubota Ltd"},
    "FORCEMOT": {"bse_code": "500033", "name": "Force Motors Ltd"},
    "SONACOMS": {"bse_code": "543300", "name": "Sona BLW Precision Forgings Ltd"},
    "OLECTRA": {"bse_code": "532439", "name": "Olectra Greentech Ltd"},
    "ATgl": {"bse_code": "543568", "name": "Adani Total Gas Ltd"},
}

# Month labels for matching BSE announcements
MONTH_LABELS = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december",
}


class BSEWholesaleConnector(BaseConnector):
    """
    Connector for BSE corporate filings — wholesale dispatch data.

    Queries BSE announcements API to find and parse auto sales/dispatch
    filings from listed OEMs.

    Usage:
        async with BSEWholesaleConnector() as connector:
            result = await connector.extract(
                period="2026-03",
                segments=["PV", "CV"],
            )
    """

    source = ConnectorSource.BSE_WHOLESALE

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def setup(self) -> None:
        """Initialize HTTP client with BSE-appropriate headers."""
        self._client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Referer": "https://www.bseindia.com/",
                "Origin": "https://www.bseindia.com",
            },
        )
        logger.info("BSE Wholesale connector initialized")

    async def teardown(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
        logger.info("BSE Wholesale connector closed")

    async def health_check(self) -> bool:
        """Check if BSE API is reachable."""
        try:
            if not self._client:
                return False
            resp = await self._client.get("https://www.bseindia.com/")
            ok = resp.status_code < 400
            logger.info("BSE India: %s (HTTP %d)", "OK" if ok else "FAIL", resp.status_code)
            return ok
        except Exception as e:
            logger.error("BSE health check failed: %s", e)
            return False

    async def extract(
        self,
        period: str,
        segments: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Extract wholesale dispatch data from BSE filings for a given month.

        Args:
            period: Month in 'YYYY-MM' format
            segments: Optional filter (currently applied post-extraction)
        """
        result = ExtractionResult(source=self.source, status=ExtractionStatus.SUCCESS)
        all_records: list[dict[str, Any]] = []

        try:
            year, month = period.split("-")
            month_int = int(month)
            month_name = MONTH_LABELS.get(month_int, "")

            # Query each OEM's BSE announcements
            for ticker, oem_info in BSE_AUTO_OEMS.items():
                try:
                    oem_records = await self._extract_oem_filings(
                        bse_code=oem_info["bse_code"],
                        oem_name=oem_info["name"],
                        ticker=ticker,
                        year=year,
                        month_int=month_int,
                        month_name=month_name,
                    )
                    all_records.extend(oem_records)
                except Exception as e:
                    logger.warning("Error extracting BSE filings for %s: %s", ticker, e)

                # Rate limit between OEMs
                await asyncio.sleep(2.5)

            result.records = all_records
            result.mark_complete(
                ExtractionStatus.SUCCESS if all_records else ExtractionStatus.PARTIAL
            )
            result.metadata = {
                "period": period,
                "oems_queried": len(BSE_AUTO_OEMS),
                "oems_with_data": len({r["ticker"] for r in all_records}),
                "total_records": len(all_records),
            }

            logger.info(
                "BSE wholesale extraction: %d records from %d OEMs",
                len(all_records),
                len({r["ticker"] for r in all_records}),
            )

        except Exception as e:
            logger.error("BSE extraction failed: %s", e, exc_info=True)
            result.error_message = str(e)
            result.mark_complete(ExtractionStatus.FAILED)

        return result

    async def _extract_oem_filings(
        self,
        bse_code: str,
        oem_name: str,
        ticker: str,
        year: str,
        month_int: int,
        month_name: str,
    ) -> list[dict[str, Any]]:
        """
        Query BSE announcements for a specific OEM and parse dispatch data.

        BSE Announcements API parameters:
          - scrip_cd: BSE code (e.g., '532500' for Maruti)
          -Ession: Subject filter keyword
          - from_dt/to_dt: Date range filter
        """
        if not self._client:
            raise RuntimeError("HTTP client not initialized")

        records: list[dict[str, Any]] = []

        # Query announcements API
        # BSE API format: from_dt and to_dt in dd/mm/yyyy
        # We search for announcements in the period + 5 days after month end
        from_dt = f"01/{month_int:02d}/{year}"
        # End of month + 5 days for late filings
        if month_int == 12:
            to_dt = f"05/01/{int(year) + 1}"
        else:
            to_dt = f"05/{month_int + 1:02d}/{year}"

        params = {
            "strCat": "-1",
            "strPrevDate": from_dt,
            "strScrip": bse_code,
            "strSearch": "P",
            "strToDate": to_dt,
            "strType": "C",
        }

        try:
            resp = await self._client.get(BSE_ANNOUNCEMENTS_URL, params=params)

            if resp.status_code != 200:
                logger.warning(
                    "BSE API returned %d for %s (%s)", resp.status_code, ticker, bse_code
                )
                return records

            announcements = resp.json()
            if not isinstance(announcements, dict):
                return records

            # BSE returns {"Table": [...]} with announcement entries
            table = announcements.get("Table", [])
            if not table:
                logger.debug("No announcements found for %s in %s %s", ticker, month_name, year)
                return records

            # Filter for sales/dispatch related announcements
            sales_filings = self._filter_sales_announcements(table, month_name, year)

            for filing in sales_filings:
                # Parse the announcement text/attachment for dispatch numbers
                parsed = self._parse_filing(filing, ticker, oem_name, f"{year}-{month_int:02d}")
                records.extend(parsed)

            logger.info(
                "BSE %s: %d sales filings found, %d records parsed",
                ticker, len(sales_filings), len(records),
            )

        except httpx.HTTPError as e:
            logger.warning("BSE HTTP error for %s: %s", ticker, e)
        except Exception as e:
            logger.warning("BSE parsing error for %s: %s", ticker, e)

        return records

    def _filter_sales_announcements(
        self, announcements: list[dict], month_name: str, year: str
    ) -> list[dict]:
        """
        Filter BSE announcements for monthly sales/dispatch filings.

        Auto OEMs typically file these with subject lines like:
          "Auto Sales data for March 2026"
          "Monthly Sales Update - March 2026"
          "Sales/Dispatch Numbers for Mar 2026"
        """
        keywords = [
            "sales data", "auto sales", "sales update",
            "dispatch", "wholesale", "monthly sales",
            "production and sales", "sales and production",
        ]

        filtered = []
        for ann in announcements:
            subject = (ann.get("NEWSSUB", "") or "").lower()
            headline = (ann.get("HEADLINE", "") or "").lower()
            combined = f"{subject} {headline}"

            # Must contain sales-related keyword
            has_keyword = any(kw in combined for kw in keywords)
            # Must reference the target month
            has_month = month_name[:3] in combined or month_name in combined

            if has_keyword and has_month:
                filtered.append(ann)

        return filtered

    def _parse_filing(
        self,
        filing: dict,
        ticker: str,
        oem_name: str,
        period: str,
    ) -> list[dict[str, Any]]:
        """
        Parse a BSE sales filing for dispatch numbers.

        Strategy:
          1. Check if filing has inline text with numbers
          2. Look for attachment PDF URL (would need PDF parsing)
          3. Extract headline numbers if available

        For V1, we extract numbers from the announcement text.
        PDF attachment parsing can be added later as enhancement.
        """
        records: list[dict[str, Any]] = []

        text = filing.get("NEWS_DT_BODY", "") or ""
        subject = filing.get("NEWSSUB", "") or ""
        headline = filing.get("HEADLINE", "") or ""
        filing_date = filing.get("NEWS_DT", "")
        attachment = filing.get("ATTACHMENT", "")

        combined_text = f"{subject}\n{headline}\n{text}"

        # Try to extract total dispatch number
        total_match = re.search(
            r'(?:total|overall|aggregate)\s+(?:sales|dispatches?|wholesale)\s*'
            r'(?:of|at|were|stood at)?\s*'
            r'(\d[\d,]+)\s*(?:units?|vehicles?|nos?)?',
            combined_text,
            re.IGNORECASE,
        )

        # Extract segment-specific numbers
        segment_patterns = {
            "PV": r'(?:passenger\s+vehicle|pv|car)\s*(?:sales|dispatches?|wholesale)?\s*(?:of|at|were|:)?\s*(\d[\d,]+)',
            "CV": r'(?:commercial\s+vehicle|cv|truck|bus)\s*(?:sales|dispatches?|wholesale)?\s*(?:of|at|were|:)?\s*(\d[\d,]+)',
            "2W": r'(?:two\s+wheeler|2w|motorcycle|scooter)\s*(?:sales|dispatches?|wholesale)?\s*(?:of|at|were|:)?\s*(\d[\d,]+)',
        }

        # Extract EV-specific numbers
        ev_match = re.search(
            r'(?:ev|electric|electric\s+vehicle)\s*(?:sales|dispatches?)?\s*(?:of|at|were|:)?\s*(\d[\d,]+)',
            combined_text,
            re.IGNORECASE,
        )

        base_record = {
            "ticker": ticker,
            "oem_name": oem_name,
            "data_type": "wholesale",
            "period": period,
            "filing_date": filing_date,
            "attachment_url": attachment if attachment else None,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        # Add total record
        if total_match:
            vol = self._clean_number(total_match.group(1))
            if vol and vol > 0:
                records.append({
                    **base_record,
                    "segment": "TOTAL",
                    "volume": vol,
                    "powertrain": "ALL",
                })

        # Add segment records
        for seg, pattern in segment_patterns.items():
            match = re.search(pattern, combined_text, re.IGNORECASE)
            if match:
                vol = self._clean_number(match.group(1))
                if vol and vol > 0:
                    records.append({
                        **base_record,
                        "segment": seg,
                        "volume": vol,
                        "powertrain": "ALL",
                    })

        # Add EV record
        if ev_match:
            vol = self._clean_number(ev_match.group(1))
            if vol and vol > 0:
                records.append({
                    **base_record,
                    "segment": "ALL",
                    "volume": vol,
                    "powertrain": "EV",
                })

        # If no structured data found, log the filing for manual review
        if not records:
            logger.debug(
                "No dispatch numbers parsed from BSE filing: %s - %s",
                ticker, subject[:80],
            )

        return records

    @staticmethod
    def _clean_number(value: str) -> Optional[int]:
        """Clean numeric string: '1,50,000' → 150000."""
        clean = value.replace(",", "").replace(" ", "").strip()
        try:
            return int(clean)
        except ValueError:
            return None
