"""
AutoQuant ETL — SIAM Historical CSV Loader.

Loads historical vehicle registration/production data from SIAM
(Society of Indian Automobile Manufacturers) CSV exports.

SIAM publishes historical production and domestic sales data via:
  - Annual statistical profiles
  - Category-wise domestic sales (FY-wise)
  - Segment summaries (PV/CV/2W/3W)

This connector loads pre-downloaded CSV files that follow the AutoQuant
CSV template defined in the project spec:

  CSV Template (historical_registrations.csv):
    data_date,oem_name,vehicle_class,fuel_type,registration_count,source
    2016-01-15,MARUTI SUZUKI INDIA LTD,MOTOR CAR,PETROL,45000,SIAM
    2016-01-15,TATA MOTORS LTD,GOODS CARRIER,DIESEL,12000,SIAM

Historical data coverage: Jan 2016 → Dec 2024 (pre-VAHAN live feed).

Usage in pipeline:
  - STEP 7 historical backfill loads these CSVs
  - Records get the same bronze→silver→gold treatment as VAHAN data
  - Confidence flag: "SIAM_HISTORICAL" (lower than "VAHAN_DAILY")

IMPORTANT:
  - CSV files must be manually curated from SIAM publications
  - No web scraping — SIAM data requires manual download
  - Data is FY-aggregated; monthly estimates are interpolated
"""

import csv
import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.connectors.base import (
    BaseConnector,
    ConnectorSource,
    ExtractionResult,
    ExtractionStatus,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Expected CSV columns
REQUIRED_COLUMNS = {"data_date", "oem_name", "vehicle_class", "registration_count"}
OPTIONAL_COLUMNS = {"fuel_type", "source", "segment", "powertrain", "notes"}

# Valid segment codes
VALID_SEGMENTS = {"PV", "CV", "2W", "3W"}


class SIAMHistoricalConnector(BaseConnector):
    """
    Loads historical auto data from curated SIAM CSV files.

    Supports multiple CSV formats:
      1. AutoQuant standard template (preferred)
      2. SIAM annual profile export
      3. Simple segment-level aggregates

    Usage:
        async with SIAMHistoricalConnector() as connector:
            result = await connector.extract(
                period="2016-2024",
                csv_path="/data/historical_registrations.csv",
            )
    """

    source = ConnectorSource.SIAM_HISTORICAL

    async def setup(self) -> None:
        """No setup needed for CSV loader."""
        logger.info("SIAM Historical connector initialized")

    async def teardown(self) -> None:
        """No teardown needed."""
        pass

    async def health_check(self) -> bool:
        """Always returns True — CSV is local file based."""
        return True

    async def extract(
        self,
        period: str,
        segments: Optional[list[str]] = None,
        *,
        csv_path: Optional[str] = None,
        csv_dir: Optional[str] = None,
        csv_content: Optional[str] = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Load historical data from CSV file(s).

        Args:
            period: Date range filter ('YYYY-MM' for single month, 'YYYY-YYYY' for range)
            segments: Optional segment filter
            csv_path: Path to a single CSV file
            csv_dir: Path to directory of CSV files (loads all .csv files)
            csv_content: Raw CSV content as string (for testing)
        """
        result = ExtractionResult(source=self.source, status=ExtractionStatus.SUCCESS)
        all_records: list[dict[str, Any]] = []

        try:
            if csv_content:
                # Direct CSV content (for testing)
                records = self._parse_csv_content(csv_content)
                all_records.extend(records)

            elif csv_path:
                # Single file
                path = Path(csv_path)
                if not path.exists():
                    raise FileNotFoundError(f"CSV file not found: {csv_path}")
                content = path.read_text(encoding="utf-8")
                records = self._parse_csv_content(content)
                all_records.extend(records)
                logger.info("Loaded %d records from %s", len(records), csv_path)

            elif csv_dir:
                # Directory of CSVs
                dir_path = Path(csv_dir)
                if not dir_path.is_dir():
                    raise NotADirectoryError(f"Not a directory: {csv_dir}")

                csv_files = sorted(dir_path.glob("*.csv"))
                logger.info("Found %d CSV files in %s", len(csv_files), csv_dir)

                for csv_file in csv_files:
                    content = csv_file.read_text(encoding="utf-8")
                    records = self._parse_csv_content(content)
                    all_records.extend(records)
                    logger.info("Loaded %d records from %s", len(records), csv_file.name)

            else:
                result.mark_complete(ExtractionStatus.FAILED)
                result.error_message = "No CSV source specified (csv_path, csv_dir, or csv_content)"
                return result

            # Apply period filter
            all_records = self._filter_by_period(all_records, period)

            # Apply segment filter
            if segments:
                all_records = [r for r in all_records if r.get("segment") in segments]

            # Validate records
            valid_records, invalid_count = self._validate_records(all_records)

            result.records = valid_records
            result.mark_complete(
                ExtractionStatus.SUCCESS if valid_records else ExtractionStatus.PARTIAL
            )
            result.metadata = {
                "period_filter": period,
                "total_parsed": len(all_records),
                "valid_records": len(valid_records),
                "invalid_records": invalid_count,
                "date_range": self._get_date_range(valid_records),
                "segments_found": list({r.get("segment", "UNKNOWN") for r in valid_records}),
                "oems_found": len({r.get("oem_name", "") for r in valid_records}),
            }

            logger.info(
                "SIAM historical load: %d valid records (%d invalid, %d total)",
                len(valid_records), invalid_count, len(all_records),
            )

        except Exception as e:
            logger.error("SIAM CSV load failed: %s", e, exc_info=True)
            result.error_message = str(e)
            result.mark_complete(ExtractionStatus.FAILED)

        return result

    def _parse_csv_content(self, content: str) -> list[dict[str, Any]]:
        """
        Parse CSV content into records.

        Supports headers with various naming conventions:
          - data_date, oem_name, vehicle_class, fuel_type, registration_count
          - date, maker, category, fuel, count
          - Date, OEM, Vehicle Class, Fuel Type, Registrations
        """
        records: list[dict[str, Any]] = []

        reader = csv.DictReader(io.StringIO(content))
        if not reader.fieldnames:
            logger.warning("CSV has no header row")
            return records

        # Normalize column names
        col_map = self._build_column_map(reader.fieldnames)

        for row_num, row in enumerate(reader, 2):  # row 2 = first data row
            try:
                record = self._normalize_row(row, col_map, row_num)
                if record:
                    records.append(record)
            except Exception as e:
                logger.debug("Row %d parse error: %s", row_num, e)

        return records

    def _build_column_map(self, fieldnames: list[str]) -> dict[str, str]:
        """
        Map CSV column names to canonical names.

        Returns dict: canonical_name → actual_csv_column_name
        """
        col_map: dict[str, str] = {}
        normalized = {f.strip().lower().replace(" ", "_"): f for f in fieldnames}

        # data_date
        for alias in ["data_date", "date", "month", "period", "report_date"]:
            if alias in normalized:
                col_map["data_date"] = normalized[alias]
                break

        # oem_name
        for alias in ["oem_name", "oem", "maker", "manufacturer", "company"]:
            if alias in normalized:
                col_map["oem_name"] = normalized[alias]
                break

        # vehicle_class
        for alias in ["vehicle_class", "vehicle_category", "category", "class", "type"]:
            if alias in normalized:
                col_map["vehicle_class"] = normalized[alias]
                break

        # fuel_type
        for alias in ["fuel_type", "fuel", "powertrain_type"]:
            if alias in normalized:
                col_map["fuel_type"] = normalized[alias]
                break

        # registration_count
        for alias in [
            "registration_count", "registrations", "count", "volume",
            "units", "sales", "dispatches", "total",
        ]:
            if alias in normalized:
                col_map["registration_count"] = normalized[alias]
                break

        # segment (optional)
        for alias in ["segment", "segment_code", "seg"]:
            if alias in normalized:
                col_map["segment"] = normalized[alias]
                break

        # source (optional)
        for alias in ["source", "data_source"]:
            if alias in normalized:
                col_map["source"] = normalized[alias]
                break

        return col_map

    def _normalize_row(
        self, row: dict[str, str], col_map: dict[str, str], row_num: int
    ) -> Optional[dict[str, Any]]:
        """Normalize a CSV row into a canonical record."""
        # Required: registration_count
        count_col = col_map.get("registration_count")
        if not count_col or not row.get(count_col):
            return None

        count_str = row[count_col].replace(",", "").replace(" ", "").strip()
        if not count_str:
            return None
        try:
            count = int(float(count_str))
        except ValueError:
            return None

        if count <= 0:
            return None

        # data_date
        date_col = col_map.get("data_date")
        data_date = row.get(date_col, "") if date_col else ""
        data_date = self._normalize_date(data_date)
        if not data_date:
            return None

        # oem_name
        oem_col = col_map.get("oem_name")
        oem_name = (row.get(oem_col, "") if oem_col else "").strip().upper()

        # vehicle_class
        vc_col = col_map.get("vehicle_class")
        vehicle_class = (row.get(vc_col, "") if vc_col else "").strip().upper()

        # fuel_type
        fuel_col = col_map.get("fuel_type")
        fuel_type = (row.get(fuel_col, "") if fuel_col else "").strip().upper()

        # segment
        seg_col = col_map.get("segment")
        segment = (row.get(seg_col, "") if seg_col else "").strip().upper()

        # source
        src_col = col_map.get("source")
        source = (row.get(src_col, "") if src_col else "SIAM").strip()

        return {
            "data_date": data_date,
            "oem_name": oem_name or "UNKNOWN",
            "vehicle_class": vehicle_class,
            "fuel_type": fuel_type,
            "segment": segment if segment in VALID_SEGMENTS else "",
            "registration_count": count,
            "source": source,
            "confidence": "SIAM_HISTORICAL",
            "row_number": row_num,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _normalize_date(date_str: str) -> Optional[str]:
        """
        Normalize various date formats to 'YYYY-MM-DD'.

        Handles:
          - 2016-01-15
          - 15/01/2016
          - Jan-2016 → 2016-01-15 (mid-month)
          - FY2016-17 → 2016-04-01 (FY start)
          - 2016 → 2016-01-01
        """
        date_str = date_str.strip()
        if not date_str:
            return None

        # ISO format: 2016-01-15
        if len(date_str) == 10 and date_str[4] == "-":
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                pass

        # DD/MM/YYYY
        try:
            d = datetime.strptime(date_str, "%d/%m/%Y")
            return d.strftime("%Y-%m-%d")
        except ValueError:
            pass

        # Mon-YYYY (e.g., Jan-2016)
        try:
            d = datetime.strptime(date_str, "%b-%Y")
            return d.strftime("%Y-%m-15")  # Mid-month estimate
        except ValueError:
            pass

        # YYYY-MM (e.g., 2016-01)
        if len(date_str) == 7 and date_str[4] == "-":
            try:
                d = datetime.strptime(date_str, "%Y-%m")
                return d.strftime("%Y-%m-15")
            except ValueError:
                pass

        # FY format: FY2016-17 → April 2016
        import re
        fy_match = re.match(r'FY\s*(\d{4})', date_str, re.IGNORECASE)
        if fy_match:
            fy_start_year = int(fy_match.group(1))
            return f"{fy_start_year}-04-01"

        # Plain year: 2016 → Jan 2016
        if len(date_str) == 4 and date_str.isdigit():
            return f"{date_str}-01-01"

        return None

    @staticmethod
    def _filter_by_period(records: list[dict], period: str) -> list[dict]:
        """
        Filter records by period specification.

        Supports:
          - 'YYYY-MM' → single month
          - 'YYYY-YYYY' → year range
          - 'all' → no filter
        """
        if period.lower() == "all":
            return records

        if "-" in period and len(period) == 9 and period[4] == "-":
            # Year range: 2016-2024
            start_year = int(period[:4])
            end_year = int(period[5:])
            return [
                r for r in records
                if r.get("data_date") and start_year <= int(r["data_date"][:4]) <= end_year
            ]

        if "-" in period and len(period) == 7:
            # Single month: 2026-03
            return [
                r for r in records
                if r.get("data_date") and r["data_date"][:7] == period
            ]

        return records

    @staticmethod
    def _validate_records(
        records: list[dict],
    ) -> tuple[list[dict], int]:
        """
        Validate records — remove invalid/incomplete ones.

        Returns: (valid_records, invalid_count)
        """
        valid: list[dict] = []
        invalid = 0

        for r in records:
            # Must have date and count
            if not r.get("data_date") or not r.get("registration_count"):
                invalid += 1
                continue

            # Date must be valid format
            try:
                datetime.strptime(r["data_date"], "%Y-%m-%d")
            except ValueError:
                invalid += 1
                continue

            # Count must be positive
            if r["registration_count"] <= 0:
                invalid += 1
                continue

            valid.append(r)

        return valid, invalid

    @staticmethod
    def _get_date_range(records: list[dict]) -> dict[str, str]:
        """Get min/max dates from records."""
        if not records:
            return {"min_date": "", "max_date": ""}

        dates = [r["data_date"] for r in records if r.get("data_date")]
        if not dates:
            return {"min_date": "", "max_date": ""}

        return {"min_date": min(dates), "max_date": max(dates)}
