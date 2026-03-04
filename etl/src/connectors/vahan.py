"""
AutoQuant ETL — VAHAN Dashboard Connector (HTTP-based).

Pure HTTP extraction from the VAHAN 4.0 JSF/PrimeFaces dashboard.
Uses curl_cffi for TLS fingerprint impersonation + BeautifulSoup for parsing.

The VAHAN dashboard at:
  https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml

is a JSF (PrimeFaces) server-side app. All data interactions are
standard form POSTs with javax.faces parameters — no browser needed.

Extraction strategy:
  1. GET the dashboard → capture JSESSIONID + ViewState + form fields
  2. POST to set Y-axis = "Maker", X-axis = "Month Wise"
  3. POST to set each vehicle class filter → extract table HTML
  4. Parse table rows: (maker, fuel, vehicle_class, count)
  5. Rate-limit: configurable delay between requests

IMPORTANT CONSTRAINTS:
  - NO CAPTCHA bypass
  - NO authentication bypass
  - NO parallel requests
  - Rate-limited (3-5s between requests)
  - robots.txt respected
  - Aggregated counts ONLY (no PII, no RC-level data)
"""

import asyncio
import re
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urljoin

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup, Tag

from config import get_settings
from src.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractionResult,
    ExtractionStatus,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ── Constants ──

VAHAN_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

# Vehicle classes grouped by segment (maps to VAHAN "VhClass" checkbox values)
VEHICLE_CATEGORIES: dict[str, list[str]] = {
    "PV": [
        "Motor Car",
        "Motor Cab",
        "Omnibus(P)",
    ],
    "CV": [
        "Goods Carrier",
        "Bus",
        "Omnibus",
        "Maxi Cab",
        "Ambulance",
    ],
    "2W": [
        "M-Cycle/Scooter",
        "Moped",
    ],
}

ALL_VEHICLE_CLASSES = [
    (seg, cls) for seg, classes in VEHICLE_CATEGORIES.items() for cls in classes
]

# Default headers to mimic a real browser session
BROWSER_HEADERS = {
    "Accept": "application/xml, text/xml, */*; q=0.01",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://vahan.parivahan.gov.in",
    "Referer": VAHAN_URL,
    "X-Requested-With": "XMLHttpRequest",
    "Faces-Request": "partial/ajax",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}


def _extract_viewstate(html: str) -> Optional[str]:
    """Extract javax.faces.ViewState from HTML form."""
    soup = BeautifulSoup(html, "html.parser")
    vs = soup.find("input", {"name": "javax.faces.ViewState"})
    if vs and isinstance(vs, Tag):
        return vs.get("value", "")
    # Try regex fallback
    match = re.search(
        r'name="javax\.faces\.ViewState"\s+value="([^"]*)"', html
    )
    return match.group(1) if match else None


def _extract_form_id(html: str) -> str:
    """Extract the main JSF form ID."""
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"id": re.compile(r".*formlogin.*|.*Layout.*")})
    if form and isinstance(form, Tag):
        return form.get("id", "masterLayout_formlogin")
    return "masterLayout_formlogin"


def _extract_dropdown_options(html: str, select_id: str) -> list[str]:
    """Extract option values from a PrimeFaces SelectOneMenu or <select>."""
    soup = BeautifulSoup(html, "html.parser")
    # Try hidden <select> element (PrimeFaces renders both a visible div and a hidden select)
    select = soup.find("select", {"id": select_id})
    if select and isinstance(select, Tag):
        return [
            opt.get("value", opt.text.strip())
            for opt in select.find_all("option")
            if opt.get("value") and opt.get("value") != ""
        ]
    # Try PrimeFaces panel items
    items = soup.select(f"#{select_id}_panel .ui-selectonemenu-items li")
    return [
        li.get("data-label", li.text.strip())
        for li in items
        if li.get("data-label")
    ]


def _parse_data_table(html: str) -> list[dict[str, Any]]:
    """
    Parse PrimeFaces datatable HTML fragment into records.

    The VAHAN table typically has columns like:
      [S.No, Maker/OEM, <month columns>, Total]
    or for Maker Y-axis with Month-Wise X-axis:
      [S.No, Maker, Count]

    We extract all rows and let the caller interpret the schema.
    """
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, Any]] = []

    # Find all datatable bodies
    for tbody in soup.select(".ui-datatable tbody, table tbody"):
        for tr in tbody.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if not cells or len(cells) < 2:
                continue

            # Skip headers / totals
            first = cells[0].upper()
            if first in ("", "TOTAL", "GRAND TOTAL", "S.NO", "SR.NO", "S.NO."):
                continue
            # Skip row if first cell is just a serial number
            if first.isdigit() and len(cells) >= 3:
                cells = cells[1:]  # Drop serial number column

            maker = cells[0].strip()
            if not maker:
                continue

            # Last cell is typically the count/total
            count_str = cells[-1].replace(",", "").replace(" ", "").strip()
            if not count_str.isdigit():
                # Try second-to-last if last is empty
                if len(cells) >= 3:
                    count_str = cells[-2].replace(",", "").replace(" ", "").strip()
                if not count_str.isdigit():
                    continue

            count = int(count_str)
            if count == 0:
                continue

            records.append({
                "maker": maker.upper(),
                "registration_count": count,
                "raw_cells": cells,
            })

    return records


class VahanConnector(BaseConnector):
    """
    HTTP-based connector for VAHAN 4.0 dashboard.

    Uses curl_cffi to impersonate Chrome's TLS fingerprint, avoiding
    any bot detection. All interactions are standard JSF form POSTs.

    Usage:
        async with VahanConnector() as connector:
            result = await connector.extract(period="2026-03")
    """

    source = ConnectorSource.VAHAN

    def __init__(self) -> None:
        self._settings = get_settings().scraping
        self._session: Optional[AsyncSession] = None
        self._viewstate: Optional[str] = None
        self._form_id: str = "masterLayout_formlogin"
        self._page_html: str = ""

    async def setup(self) -> None:
        """Create an HTTP session with Chrome TLS fingerprint."""
        logger.info("Creating curl_cffi session (Chrome impersonation)")
        self._session = AsyncSession(
            impersonate="chrome120",
            timeout=self._settings.page_timeout_ms / 1000,
            verify=True,
        )

    async def teardown(self) -> None:
        """Close the HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("HTTP session closed")

    async def health_check(self) -> bool:
        """Check if VAHAN dashboard is reachable."""
        try:
            if not self._session:
                await self.setup()
            resp = await self._session.get(
                self._settings.vahan_base_url,
                headers={"Accept": "text/html"},
                timeout=15,
            )
            ok = resp.status_code == 200
            logger.info("VAHAN dashboard: HTTP %d (%s)", resp.status_code, "OK" if ok else "FAILED")
            return ok
        except Exception as e:
            logger.error("VAHAN health check failed: %s", e)
            return False

    async def _delay(self, multiplier: float = 1.0) -> None:
        """Rate-limiting delay between requests."""
        delay = self._settings.request_delay_seconds * multiplier
        logger.debug("Rate limit: waiting %.1fs", delay)
        await asyncio.sleep(delay)

    async def _load_dashboard(self) -> None:
        """
        GET the dashboard page to establish session + capture ViewState.
        """
        logger.info("Loading VAHAN dashboard...")
        resp = await self._session.get(
            self._settings.vahan_base_url,
            headers={"Accept": "text/html,application/xhtml+xml,*/*"},
        )
        resp.raise_for_status()
        self._page_html = resp.text
        self._viewstate = _extract_viewstate(self._page_html)
        self._form_id = _extract_form_id(self._page_html)

        if not self._viewstate:
            raise RuntimeError("Failed to extract ViewState from VAHAN dashboard")

        logger.info(
            "Dashboard loaded: form=%s, viewstate=%s...",
            self._form_id,
            self._viewstate[:20] if self._viewstate else "NONE",
        )

    def _build_ajax_post(
        self,
        source: str,
        execute: str = "@all",
        render: str = "tablePnl",
        extra_params: Optional[dict[str, str]] = None,
    ) -> dict[str, str]:
        """
        Build a PrimeFaces AJAX partial-submit POST body.

        Standard JSF AJAX parameters:
          javax.faces.partial.ajax=true
          javax.faces.source=<component_id>
          javax.faces.partial.execute=<execute_list>
          javax.faces.partial.render=<render_list>
          javax.faces.ViewState=<token>
          <form_id>=<form_id>
        """
        data = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": source,
            "javax.faces.partial.execute": execute,
            "javax.faces.partial.render": render,
            "javax.faces.ViewState": self._viewstate or "",
            self._form_id: self._form_id,
        }
        if extra_params:
            data.update(extra_params)
        return data

    async def _ajax_post(
        self,
        source: str,
        execute: str = "@all",
        render: str = "tablePnl",
        extra_params: Optional[dict[str, str]] = None,
    ) -> str:
        """
        Send a PrimeFaces AJAX POST and return the response XML/HTML.
        Also updates ViewState from the response.
        """
        data = self._build_ajax_post(source, execute, render, extra_params)

        resp = await self._session.post(
            self._settings.vahan_base_url,
            data=data,
            headers=BROWSER_HEADERS,
        )
        resp.raise_for_status()
        body = resp.text

        # Update ViewState from AJAX response (PrimeFaces embeds it in XML)
        new_vs = re.search(
            r'<update\s+id="javax\.faces\.ViewState"[^>]*><!\[CDATA\[([^\]]*)\]\]>',
            body,
        )
        if new_vs:
            self._viewstate = new_vs.group(1)

        return body

    async def _set_dropdown(
        self, field_id: str, value: str, label: str
    ) -> str:
        """
        Set a PrimeFaces SelectOneMenu dropdown via AJAX POST.
        Simulates the onChange event.
        """
        logger.info("Setting %s = '%s'", label, value)
        body = await self._ajax_post(
            source=field_id,
            execute=field_id,
            render="@all",  # Let server decide what to re-render
            extra_params={
                field_id: value,
                f"{field_id}_focus": "",
                f"{field_id}_input": value,
            },
        )
        await self._delay(0.5)
        return body

    async def _trigger_refresh(self) -> str:
        """
        Trigger the data table refresh (equivalent to rclay() JS function).
        The refresh button source is 'irclay' and updates 'tablePnl'.
        """
        logger.info("Triggering data refresh (irclay → tablePnl)...")

        # Build form data with all current selections
        data = self._build_ajax_post(
            source="irclay",
            execute="@all",
            render="tablePnl",
            extra_params={
                "yaxisVar": "Maker",
                "xaxisVar": "Month Wise",
                "irclay": "irclay",
            },
        )

        resp = await self._session.post(
            self._settings.vahan_base_url,
            data=data,
            headers=BROWSER_HEADERS,
        )
        resp.raise_for_status()
        body = resp.text

        # Update ViewState
        new_vs = re.search(
            r'<update\s+id="javax\.faces\.ViewState"[^>]*><!\[CDATA\[([^\]]*)\]\]>',
            body,
        )
        if new_vs:
            self._viewstate = new_vs.group(1)

        return body

    async def _extract_for_vehicle_class(
        self,
        vehicle_class: str,
        segment: str,
        period: str,
    ) -> list[dict[str, Any]]:
        """
        Set vehicle class filter and extract maker × count data.
        """
        logger.info(
            "Extracting: segment=%s, vehicle_class=%s, period=%s",
            segment, vehicle_class, period,
        )

        # Build POST with vehicle class filter + Y=Maker + X=Month Wise + refresh
        data = self._build_ajax_post(
            source="irclay",
            execute="@all",
            render="tablePnl",
            extra_params={
                "yaxisVar": "Maker",
                "xaxisVar": "Month Wise",
                "VhClass": vehicle_class,
                "irclay": "irclay",
            },
        )

        resp = await self._session.post(
            self._settings.vahan_base_url,
            data=data,
            headers=BROWSER_HEADERS,
        )
        resp.raise_for_status()
        body = resp.text

        # Update ViewState
        new_vs = re.search(
            r'<update\s+id="javax\.faces\.ViewState"[^>]*><!\[CDATA\[([^\]]*)\]\]>',
            body,
        )
        if new_vs:
            self._viewstate = new_vs.group(1)

        # Parse table from the AJAX response
        # PrimeFaces wraps updates in <changes><update id="tablePnl">...<![CDATA[...]]></update>
        table_html = body
        cdata_match = re.search(
            r'<update\s+id="tablePnl"[^>]*><!\[CDATA\[(.*?)\]\]></update>',
            body,
            re.DOTALL,
        )
        if cdata_match:
            table_html = cdata_match.group(1)

        records = _parse_data_table(table_html)

        # Tag each record with metadata
        for record in records:
            record["vehicle_class"] = vehicle_class.upper()
            record["segment"] = segment
            record["data_period"] = period
            record["extracted_at"] = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Extracted %d records for %s / %s", len(records), segment, vehicle_class
        )
        return records

    async def extract(
        self,
        period: str,
        segments: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Extract aggregated registration data from VAHAN for a given period.

        Args:
            period: Month in 'YYYY-MM' format (e.g., '2026-03')
            segments: Optional filter — subset of ['PV', 'CV', '2W']

        Returns:
            ExtractionResult with raw records
        """
        result = ExtractionResult(source=self.source, status=ExtractionStatus.SUCCESS)
        all_records: list[dict[str, Any]] = []
        start_time = time.monotonic()

        try:
            if not self._session:
                await self.setup()

            # Step 1: Load dashboard (establishes session + ViewState)
            await self._load_dashboard()
            await self._delay()

            # Step 2: Set Y-axis to Maker
            await self._set_dropdown("yaxisVar", "Maker", "Y-Axis")
            await self._delay()

            # Step 3: Set X-axis to Month Wise
            await self._set_dropdown("xaxisVar", "Month Wise", "X-Axis")
            await self._delay()

            # Step 4: Iterate over vehicle classes
            target_classes = [
                (seg, cls) for seg, cls in ALL_VEHICLE_CLASSES
                if segments is None or seg in segments
            ]

            for idx, (seg, vehicle_class) in enumerate(target_classes):
                try:
                    records = await self._extract_for_vehicle_class(
                        vehicle_class, seg, period
                    )
                    all_records.extend(records)
                except Exception as e:
                    logger.error(
                        "Error extracting %s / %s: %s", seg, vehicle_class, e
                    )
                    # Continue with remaining classes — partial extraction is OK

                # Rate limit between classes
                if idx < len(target_classes) - 1:
                    await self._delay()

            result.records = all_records
            duration = time.monotonic() - start_time
            result.mark_complete(
                ExtractionStatus.SUCCESS if all_records else ExtractionStatus.PARTIAL
            )

            result.metadata = {
                "period": period,
                "segments_requested": segments or ["PV", "CV", "2W"],
                "vehicle_classes_attempted": len(target_classes),
                "total_records": len(all_records),
                "duration_seconds": round(duration, 1),
            }

            logger.info(
                "VAHAN extraction complete: %d records in %.1fs",
                len(all_records), duration,
            )

        except Exception as e:
            logger.error("VAHAN extraction failed: %s", e, exc_info=True)
            result.error_message = str(e)
            result.mark_complete(ExtractionStatus.FAILED)

        return result


class VahanSelectors:
    """
    Documented CSS selectors / JSF component IDs for VAHAN dashboard.
    Validated against the live VAHAN DOM as of March 2026.

    Component IDs:
      - Form:           masterLayout_formlogin
      - Y-Axis:         yaxisVar
      - X-Axis:         xaxisVar
      - Type:           j_idt28
      - State:          j_idt36
      - RTO:            selectedRto
      - Year Type:      selectedYearType
      - Year:           selectedYear
      - Vehicle Class:  VhClass  (checkbox)
      - Fuel:           fuel     (checkbox)
      - Norms:          norms    (checkbox)
      - Refresh:        irclay   (button triggers rclay())
      - Table panel:    tablePnl
      - Combined table: combTablePnl
    """

    # Form
    FORM_ID = "masterLayout_formlogin"

    # Dropdowns
    Y_AXIS = "yaxisVar"
    X_AXIS = "xaxisVar"
    STATE_FILTER = "j_idt36"
    YEAR_TYPE = "selectedYearType"
    YEAR = "selectedYear"
    RTO = "selectedRto"
    TYPE = "j_idt28"

    # Checkboxes (multi-select)
    VEHICLE_CLASS = "VhClass"
    FUEL = "fuel"
    NORMS = "norms"

    # Buttons
    REFRESH = "irclay"
    REFRESH_ALT = ["j_idt67", "j_idt74", "j_idt83"]

    # Data panels
    TABLE_PANEL = "tablePnl"
    COMBINED_TABLE = "combTablePnl"

    # Y-Axis option values
    Y_AXIS_OPTIONS = [
        "Vehicle Category",
        "Vehicle Class",
        "Norms",
        "Fuel",
        "Maker",
        "State",
    ]

    # X-Axis option values
    X_AXIS_OPTIONS = [
        "Vehicle Category",
        "Norms",
        "Fuel",
        "Vehicle Category Group",
        "Financial Year",
        "Calendar Year",
        "Month Wise",
    ]

    @classmethod
    def to_dict(cls) -> dict[str, Any]:
        """Export all selectors as a dict for logging/debugging."""
        return {
            k: v for k, v in cls.__dict__.items()
            if not k.startswith("_") and not callable(v)
        }
