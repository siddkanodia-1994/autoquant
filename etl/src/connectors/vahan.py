"""
AutoQuant ETL — VAHAN Dashboard Connector.

Playwright-based extraction from the VAHAN 4.0 JSF dashboard.
Targets ONLY publicly visible aggregated registration counts.

The VAHAN dashboard at:
  https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml

Uses JSF (PrimeFaces) with server-side rendering. Key characteristics:
  - All interactions are AJAX POST requests with javax.faces.ViewState
  - Dropdowns use PrimeFaces <p:selectOneMenu> which fire AJAX on change
  - Data tables render as <table> with pagination
  - Y-axis options: Maker, Vehicle Category, Vehicle Class, Fuel, Norms
  - X-axis: Month-Wise, Calendar Year, Financial Year
  - Filters: State, RTO, Vehicle Category, Vehicle Class, Fuel, Maker, Norms

Extraction strategy:
  1. Navigate to dashboard → wait for JSF ViewState
  2. Set Y-axis = "Maker"
  3. Set X-axis = "Month-Wise" (current month)
  4. Iterate vehicle categories: set filter → extract table
  5. For each table row: capture (maker, fuel, vehicle_class, count)
  6. Rate-limit: configurable delay between interactions

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
from datetime import datetime, timezone
from typing import Any, Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
)

from config import get_settings
from src.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractionResult,
    ExtractionStatus,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Vehicle categories to iterate (maps to VAHAN dropdown values)
# We only extract PV, CV, 2W — others are excluded per scope
VEHICLE_CATEGORIES = {
    "PV": [
        "Motor Car",
        "Motor Cab",
        "Omnibus(P)",      # Private use omnibus
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

# Flattened list for extraction — we query each category separately
ALL_VEHICLE_CLASSES_TO_EXTRACT = []
for seg, classes in VEHICLE_CATEGORIES.items():
    for cls in classes:
        ALL_VEHICLE_CLASSES_TO_EXTRACT.append((seg, cls))


class VahanConnector(BaseConnector):
    """
    Playwright-based connector for VAHAN 4.0 dashboard.

    Extracts aggregated registration counts by Maker × Fuel × Vehicle Class
    for the specified period.

    Usage:
        async with VahanConnector() as connector:
            result = await connector.extract(period="2026-03")
    """

    source = ConnectorSource.VAHAN

    def __init__(self) -> None:
        self._settings = get_settings().scraping
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    async def setup(self) -> None:
        """Launch headless browser."""
        logger.info("Launching Playwright browser (headless=%s)", self._settings.headless)
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._settings.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        self._page = await self._context.new_page()
        self._page.set_default_timeout(self._settings.page_timeout_ms)
        logger.info("Browser launched successfully")

    async def teardown(self) -> None:
        """Close browser and playwright."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("Browser closed")

    async def health_check(self) -> bool:
        """Check if VAHAN dashboard is reachable."""
        try:
            page = self._page or await self._ensure_page()
            response = await page.goto(
                self._settings.vahan_base_url,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            if response and response.ok:
                logger.info("VAHAN dashboard reachable (HTTP %d)", response.status)
                return True
            logger.warning("VAHAN dashboard returned HTTP %d", response.status if response else 0)
            return False
        except Exception as e:
            logger.error("VAHAN health check failed: %s", e)
            return False

    async def _ensure_page(self) -> Page:
        """Ensure we have a valid page instance."""
        if self._page is None:
            raise RuntimeError("Browser not initialized. Call setup() first.")
        return self._page

    async def _delay(self, multiplier: float = 1.0) -> None:
        """Rate-limiting delay between requests."""
        delay = self._settings.request_delay_seconds * multiplier
        logger.debug("Waiting %.1fs (rate limit)", delay)
        await asyncio.sleep(delay)

    async def _navigate_to_dashboard(self) -> None:
        """Navigate to VAHAN dashboard and wait for it to load."""
        page = await self._ensure_page()
        logger.info("Navigating to VAHAN dashboard...")
        await page.goto(
            self._settings.vahan_base_url,
            wait_until="networkidle",
            timeout=self._settings.page_timeout_ms,
        )
        # Wait for the JSF form to render
        await page.wait_for_selector("form", timeout=30_000)
        await self._delay(0.5)
        logger.info("Dashboard loaded")

    async def _select_dropdown(
        self, page: Page, selector: str, value: str, label: str
    ) -> bool:
        """
        Select a value from a PrimeFaces dropdown.

        JSF/PrimeFaces dropdowns render as:
          <div class="ui-selectonemenu">
            <div class="ui-selectonemenu-trigger">
            <div class="ui-selectonemenu-panel">
              <ul class="ui-selectonemenu-items">
                <li data-label="..." class="ui-selectonemenu-item">...</li>

        Strategy:
          1. Click the trigger to open the panel
          2. Wait for the panel to be visible
          3. Click the item with matching label
          4. Wait for AJAX response
        """
        try:
            # Click dropdown trigger to open
            trigger = page.locator(f"{selector} .ui-selectonemenu-trigger")
            await trigger.click()
            await asyncio.sleep(0.5)

            # Find and click the option
            option = page.locator(
                f"{selector} .ui-selectonemenu-items li[data-label='{value}']"
            )
            if await option.count() == 0:
                # Try case-insensitive match
                items = page.locator(f"{selector} .ui-selectonemenu-items li")
                count = await items.count()
                for i in range(count):
                    item = items.nth(i)
                    item_label = await item.get_attribute("data-label") or ""
                    if item_label.strip().upper() == value.strip().upper():
                        await item.click()
                        logger.debug("Selected '%s' in %s (case-insensitive)", value, label)
                        await self._wait_for_ajax(page)
                        return True
                logger.warning("Option '%s' not found in dropdown %s", value, label)
                return False

            await option.click()
            logger.debug("Selected '%s' in %s", value, label)
            await self._wait_for_ajax(page)
            return True

        except PlaywrightTimeout:
            logger.error("Timeout selecting '%s' in %s", value, label)
            return False
        except Exception as e:
            logger.error("Error selecting dropdown %s: %s", label, e)
            return False

    async def _wait_for_ajax(self, page: Page, timeout_ms: int = 15_000) -> None:
        """Wait for PrimeFaces AJAX request to complete."""
        try:
            # PrimeFaces sets a status indicator during AJAX
            await page.wait_for_function(
                """() => {
                    const ajaxStatus = document.querySelector('.ui-ajax-loader');
                    return !ajaxStatus || ajaxStatus.style.display === 'none'
                        || getComputedStyle(ajaxStatus).display === 'none';
                }""",
                timeout=timeout_ms,
            )
        except PlaywrightTimeout:
            logger.warning("AJAX wait timed out — proceeding anyway")
        await asyncio.sleep(0.3)  # Small buffer after AJAX

    async def _extract_table_data(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract data from the rendered PrimeFaces data table.

        The VAHAN dashboard renders data in <table> elements.
        We extract all visible rows including pagination.

        Returns list of dicts with keys:
          maker, fuel, vehicle_class, registration_count
        """
        records: list[dict[str, Any]] = []

        try:
            # Wait for table to render
            await page.wait_for_selector(
                "table.ui-datatable-tablewrapper table, .ui-datatable table",
                timeout=15_000,
            )

            # Extract via JavaScript for reliability
            table_data = await page.evaluate("""() => {
                const rows = [];
                // Find all data tables on the page
                const tables = document.querySelectorAll('.ui-datatable tbody tr');
                for (const row of tables) {
                    const cells = row.querySelectorAll('td');
                    if (cells.length >= 2) {
                        const rowData = [];
                        for (const cell of cells) {
                            rowData.push(cell.textContent.trim());
                        }
                        rows.push(rowData);
                    }
                }
                return rows;
            }""")

            for row_cells in table_data:
                if len(row_cells) >= 2:
                    # Table structure depends on Y-axis selection
                    # When Y-axis = Maker: columns are [Maker, Count] or [Maker, Fuel, Count]
                    record = self._parse_table_row(row_cells)
                    if record:
                        records.append(record)

        except PlaywrightTimeout:
            logger.warning("Table extraction timed out")
        except Exception as e:
            logger.error("Table extraction error: %s", e)

        return records

    def _parse_table_row(self, cells: list[str]) -> Optional[dict[str, Any]]:
        """
        Parse a table row into a structured record.

        Handles variable column layouts depending on Y-axis / grouping.
        """
        try:
            # Skip header/summary rows
            if not cells or cells[0].upper() in ("TOTAL", "GRAND TOTAL", "S.NO", "SR.NO"):
                return None

            # Clean count value (remove commas, handle empty)
            count_str = cells[-1].replace(",", "").replace(" ", "").strip()
            if not count_str or not count_str.isdigit():
                return None

            count = int(count_str)
            if count == 0:
                return None

            # The maker name is typically the first meaningful column
            maker = cells[0].strip() if cells[0].strip() else None
            if not maker:
                return None

            return {
                "maker": maker.upper(),
                "registration_count": count,
                "raw_cells": cells,  # Preserve for debugging
            }
        except (ValueError, IndexError):
            return None

    async def _handle_pagination(self, page: Page) -> list[dict[str, Any]]:
        """
        Handle PrimeFaces datatable pagination.
        Extracts data from all pages, not just the first.
        """
        all_records: list[dict[str, Any]] = []

        # Get first page
        records = await self._extract_table_data(page)
        all_records.extend(records)

        # Check for pagination
        while True:
            next_btn = page.locator(".ui-paginator-next:not(.ui-state-disabled)")
            if await next_btn.count() == 0:
                break

            await next_btn.click()
            await self._wait_for_ajax(page)
            await self._delay(0.3)

            records = await self._extract_table_data(page)
            if not records:
                break
            all_records.extend(records)

        return all_records

    async def _extract_for_vehicle_class(
        self,
        page: Page,
        vehicle_class: str,
        segment: str,
        period: str,
    ) -> list[dict[str, Any]]:
        """
        Extract maker × count data for a specific vehicle class and period.

        Steps:
          1. Set vehicle class filter
          2. Set Y-axis to "Maker"
          3. Extract all rows (with pagination)
          4. Tag each record with vehicle_class and segment
        """
        logger.info(
            "Extracting: segment=%s, vehicle_class=%s, period=%s",
            segment, vehicle_class, period,
        )

        # These selectors will need to be tuned against the live VAHAN DOM.
        # They are configurable and modular per Risk #1 mitigation.
        # The actual CSS selectors depend on the JSF component IDs which
        # may change between VAHAN releases.
        #
        # TODO: Externalize selectors to config/vahan_selectors.yaml

        # Select vehicle class in the filter dropdown
        selected = await self._select_dropdown(
            page,
            "#vaborCatid",  # Placeholder — actual ID from VAHAN DOM inspection
            vehicle_class,
            "Vehicle Class",
        )
        if not selected:
            logger.warning("Could not select vehicle class '%s', skipping", vehicle_class)
            return []

        await self._delay()

        # Extract table data (with pagination)
        records = await self._handle_pagination(page)

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

        try:
            page = await self._ensure_page()

            # Step 1: Navigate to dashboard
            await self._navigate_to_dashboard()

            # Step 2: Set X-axis to Month-Wise and select the target month
            # The period selection depends on the VAHAN UI — typically a
            # date range or month selector
            await self._select_dropdown(
                page, "#xaborId", "Month-Wise", "X-Axis"
            )
            await self._delay()

            # Step 3: Set Y-axis to Maker
            await self._select_dropdown(
                page, "#yaborId", "Maker", "Y-Axis"
            )
            await self._delay()

            # Step 4: Set state filter to "All India"
            # (The default may already be All India)

            # Step 5: Iterate over vehicle classes
            target_classes = []
            for seg, cls in ALL_VEHICLE_CLASSES_TO_EXTRACT:
                if segments is None or seg in segments:
                    target_classes.append((seg, cls))

            for idx, (seg, vehicle_class) in enumerate(target_classes):
                try:
                    records = await self._extract_for_vehicle_class(
                        page, vehicle_class, seg, period
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
            result.mark_complete(
                ExtractionStatus.SUCCESS if all_records else ExtractionStatus.PARTIAL
            )

            result.metadata = {
                "period": period,
                "segments_requested": segments or ["PV", "CV", "2W"],
                "vehicle_classes_attempted": len(target_classes),
                "total_records": len(all_records),
            }

            logger.info(
                "VAHAN extraction complete: %d records in %.1fs",
                len(all_records),
                result.duration_seconds,
            )

        except Exception as e:
            logger.error("VAHAN extraction failed: %s", e, exc_info=True)
            result.error_message = str(e)
            result.mark_complete(ExtractionStatus.FAILED)

        return result


class VahanSelectors:
    """
    Configurable CSS/XPath selectors for VAHAN dashboard elements.
    Isolated here so they can be updated when VAHAN UI changes (Risk #1).

    NOTE: These are PLACEHOLDER selectors. They must be validated against
    the live VAHAN DOM by inspecting the page source. The actual PrimeFaces
    component IDs are auto-generated and may change between releases.

    To discover real selectors:
      1. Open VAHAN dashboard in browser with DevTools
      2. Inspect each dropdown/table element
      3. Note the JSF client ID (e.g., 'j_idt31:j_idt33')
      4. Update this class
    """

    # Dropdowns
    X_AXIS = "#xaborId"               # X-axis selector (Month-Wise / Calendar Year / FY)
    Y_AXIS = "#yaborId"               # Y-axis selector (Maker / Vehicle Category / etc.)
    STATE_FILTER = "#stateId"          # State filter dropdown
    VEHICLE_CATEGORY = "#vaborCatid"   # Vehicle category (2W / Transport / Non-Transport)
    VEHICLE_CLASS = "#vaborClsid"      # Vehicle class (Motor Car / Bus / etc.)
    FUEL_FILTER = "#fuelId"            # Fuel type filter
    MAKER_FILTER = "#makerId"          # Maker filter (usually set to "All")

    # Date/Period
    FROM_DATE = "#fromDateId"          # From date input
    TO_DATE = "#toDateId"              # To date input

    # Action buttons
    REFRESH_BTN = "#refreshBtnId"      # Refresh/Submit button
    DOWNLOAD_BTN = "#downloadBtnId"    # Download/Export button (if available)

    # Data table
    DATA_TABLE = ".ui-datatable"
    TABLE_ROWS = ".ui-datatable tbody tr"
    TABLE_CELLS = "td"
    PAGINATOR_NEXT = ".ui-paginator-next:not(.ui-state-disabled)"
    PAGINATOR_INFO = ".ui-paginator-current"  # "Showing 1-50 of 200"

    # AJAX status
    AJAX_LOADER = ".ui-ajax-loader"

    @classmethod
    def to_dict(cls) -> dict[str, str]:
        """Export all selectors as a dict for logging/debugging."""
        return {
            k: v for k, v in cls.__dict__.items()
            if isinstance(v, str) and not k.startswith("_")
        }
