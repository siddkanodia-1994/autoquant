"""
AutoQuant ETL — Historical Backfill Orchestrator.

Manages the full 2016-2025 historical data load:
  1. Generate sample CSV (dev/test) or accept curated CSVs
  2. Batch-load through SIAM Historical connector
  3. Map via DimensionMapper (with Tata PV/CV split)
  4. Load into fact_daily_registrations
  5. Refresh mv_oem_monthly_summary
  6. Compute est_quarterly_revenue via GoldLayerRefresh

Key features:
  - Batch processing with configurable chunk size
  - Progress tracking per year/quarter
  - Duplicate detection (ON CONFLICT DO NOTHING)
  - Comprehensive summary with per-OEM stats

Usage:
    python main.py backfill --csv-dir /data/historical/ --period 2016-2025
    python main.py backfill --generate-sample --period 2016-2025
"""

import csv
import io
import random
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from src.utils.database import DatabaseManager
from src.utils.logging_config import get_logger
from src.transforms.gold_refresh import GoldLayerRefresh, calendar_to_fy, fy_quarter_label

logger = get_logger(__name__)

# ── OEM profiles for sample data generation ──
# Maps OEM name → (primary segment, monthly base volume, EV share %)
SAMPLE_OEM_PROFILES: dict[str, dict[str, Any]] = {
    "MARUTI SUZUKI INDIA LTD": {
        "segment": "PV", "vehicle_class": "MOTOR CAR",
        "base_monthly": 130_000, "ev_share": 0.0, "growth_rate": 0.06,
    },
    "TATA MOTORS LTD": {
        "segment": "PV", "vehicle_class": "MOTOR CAR",
        "base_monthly": 42_000, "ev_share": 0.12, "growth_rate": 0.10,
    },
    "TATA MOTORS LTD (CV)": {
        "segment": "CV", "vehicle_class": "GOODS CARRIER",
        "base_monthly": 30_000, "ev_share": 0.02, "growth_rate": 0.04,
    },
    "HYUNDAI MOTOR INDIA LTD": {
        "segment": "PV", "vehicle_class": "MOTOR CAR",
        "base_monthly": 48_000, "ev_share": 0.05, "growth_rate": 0.07,
    },
    "MAHINDRA & MAHINDRA LTD": {
        "segment": "PV", "vehicle_class": "MOTOR CAR",
        "base_monthly": 30_000, "ev_share": 0.08, "growth_rate": 0.12,
    },
    "BAJAJ AUTO LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 200_000, "ev_share": 0.03, "growth_rate": 0.05,
    },
    "HERO MOTOCORP LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 450_000, "ev_share": 0.02, "growth_rate": 0.03,
    },
    "TVS MOTOR COMPANY LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 250_000, "ev_share": 0.06, "growth_rate": 0.08,
    },
    "EICHER MOTORS LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 65_000, "ev_share": 0.0, "growth_rate": 0.09,
    },
    "ASHOK LEYLAND LTD": {
        "segment": "CV", "vehicle_class": "GOODS CARRIER",
        "base_monthly": 14_000, "ev_share": 0.01, "growth_rate": 0.05,
    },
    "OLA ELECTRIC TECHNOLOGIES LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 0, "ev_share": 1.0, "growth_rate": 0.0,
        "start_year": 2022,  # OLA started in 2022
    },
    "ATHER ENERGY PVT LTD": {
        "segment": "2W", "vehicle_class": "M-CYCLE/SCOOTER",
        "base_monthly": 0, "ev_share": 1.0, "growth_rate": 0.0,
        "start_year": 2020,
    },
    "BYD INDIA PVT LTD": {
        "segment": "PV", "vehicle_class": "MOTOR CAR",
        "base_monthly": 0, "ev_share": 1.0, "growth_rate": 0.0,
        "start_year": 2023,
    },
    "FORCE MOTORS LTD": {
        "segment": "CV", "vehicle_class": "GOODS CARRIER",
        "base_monthly": 2_500, "ev_share": 0.0, "growth_rate": 0.03,
    },
    "SML ISUZU LTD": {
        "segment": "CV", "vehicle_class": "GOODS CARRIER",
        "base_monthly": 1_200, "ev_share": 0.0, "growth_rate": 0.02,
    },
    "OLECTRA GREENTECH LTD": {
        "segment": "CV", "vehicle_class": "OMNIBUS",
        "base_monthly": 0, "ev_share": 1.0, "growth_rate": 0.0,
        "start_year": 2021,
    },
}

# EV-only OEM volume ramp curves (units/month at year mark)
EV_RAMP_CURVES: dict[str, dict[int, int]] = {
    "OLA ELECTRIC TECHNOLOGIES LTD": {
        2022: 10_000, 2023: 30_000, 2024: 35_000, 2025: 40_000,
    },
    "ATHER ENERGY PVT LTD": {
        2020: 1_000, 2021: 3_000, 2022: 8_000, 2023: 12_000,
        2024: 15_000, 2025: 18_000,
    },
    "BYD INDIA PVT LTD": {
        2023: 800, 2024: 1_500, 2025: 2_500,
    },
    "OLECTRA GREENTECH LTD": {
        2021: 50, 2022: 100, 2023: 200, 2024: 350, 2025: 500,
    },
}

# Seasonal factors by month (India auto market)
# Festive season boost Oct-Nov, lean period Jun-Jul
SEASONAL_FACTORS = {
    1: 0.95, 2: 0.90, 3: 1.05, 4: 0.88, 5: 0.85, 6: 0.82,
    7: 0.85, 8: 0.92, 9: 0.98, 10: 1.18, 11: 1.15, 12: 1.02,
}


def generate_sample_csv(
    start_year: int = 2016,
    end_year: int = 2025,
    output_path: Optional[str] = None,
) -> str:
    """
    Generate realistic sample historical CSV data.

    Creates monthly registration data for all OEMs in SAMPLE_OEM_PROFILES
    with seasonal variation, year-over-year growth, and EV ramp-up curves.

    Args:
        start_year: First year of data (default: 2016)
        end_year: Last year of data (default: 2025)
        output_path: If provided, write to file; else return as string

    Returns:
        CSV content as string
    """
    random.seed(42)  # Reproducible output

    rows: list[dict[str, Any]] = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Skip future months
            if year == end_year and month > 12:
                break

            data_date = f"{year}-{month:02d}-15"
            seasonal = SEASONAL_FACTORS.get(month, 1.0)

            for oem_name, profile in SAMPLE_OEM_PROFILES.items():
                start_yr = profile.get("start_year", 2016)
                if year < start_yr:
                    continue

                years_elapsed = year - 2016
                growth_mult = (1 + profile["growth_rate"]) ** years_elapsed

                if profile["ev_share"] == 1.0:
                    # Pure EV OEM — use ramp curve
                    ramp = EV_RAMP_CURVES.get(oem_name, {})
                    base = ramp.get(year, 0)
                    if base == 0:
                        continue

                    # Add monthly variance
                    volume = int(base * seasonal * random.uniform(0.85, 1.15))

                    rows.append({
                        "data_date": data_date,
                        "oem_name": oem_name.replace(" (CV)", ""),
                        "vehicle_class": profile["vehicle_class"],
                        "fuel_type": "ELECTRIC(BOV)",
                        "registration_count": max(1, volume),
                        "segment": profile["segment"],
                        "source": "SIAM",
                    })
                else:
                    # ICE + EV mix OEM
                    total_base = int(profile["base_monthly"] * growth_mult * seasonal)
                    total_base = int(total_base * random.uniform(0.90, 1.10))

                    ev_share = profile["ev_share"]
                    # EV adoption ramps up over time
                    if year >= 2020 and ev_share > 0:
                        ev_share = min(
                            ev_share * (1.3 ** (year - 2020)),
                            0.35,  # Cap at 35%
                        )

                    ev_volume = int(total_base * ev_share)
                    ice_volume = total_base - ev_volume

                    # Determine fuel split for ICE
                    if profile["segment"] == "2W":
                        fuel = "PETROL"
                    elif profile["segment"] == "CV":
                        fuel = "DIESEL"
                    else:
                        # PV: ~60% petrol, ~25% diesel, ~15% CNG (recent years)
                        fuel = "PETROL"

                    if ice_volume > 0:
                        rows.append({
                            "data_date": data_date,
                            "oem_name": oem_name.replace(" (CV)", ""),
                            "vehicle_class": profile["vehicle_class"],
                            "fuel_type": fuel,
                            "registration_count": ice_volume,
                            "segment": profile["segment"],
                            "source": "SIAM",
                        })

                    if ev_volume > 0:
                        rows.append({
                            "data_date": data_date,
                            "oem_name": oem_name.replace(" (CV)", ""),
                            "vehicle_class": profile["vehicle_class"],
                            "fuel_type": "ELECTRIC(BOV)",
                            "registration_count": ev_volume,
                            "segment": profile["segment"],
                            "source": "SIAM",
                        })

    # Sort by date, OEM
    rows.sort(key=lambda r: (r["data_date"], r["oem_name"]))

    # Write CSV
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["data_date", "oem_name", "vehicle_class", "fuel_type",
                     "registration_count", "segment", "source"],
    )
    writer.writeheader()
    writer.writerows(rows)
    csv_content = output.getvalue()

    if output_path:
        Path(output_path).write_text(csv_content, encoding="utf-8")
        logger.info(
            "Generated sample CSV: %d rows → %s",
            len(rows), output_path,
        )

    logger.info(
        "Sample CSV: %d rows, %d OEMs, %d-%d",
        len(rows),
        len(set(r["oem_name"] for r in rows)),
        start_year, end_year,
    )

    return csv_content


class BackfillOrchestrator:
    """
    Manages the full historical backfill pipeline.

    Orchestrates:
      1. CSV ingestion (via SIAMHistoricalConnector)
      2. Dimension mapping (via DimensionMapper)
      3. Silver-layer loading (fact_daily_registrations)
      4. MV refresh (mv_oem_monthly_summary)
      5. Gold-layer computation (est_quarterly_revenue)
    """

    def __init__(
        self,
        db: DatabaseManager,
        batch_size: int = 5000,
    ) -> None:
        self._db = db
        self._batch_size = batch_size
        self._gold = GoldLayerRefresh(db)

    async def run_backfill(
        self,
        *,
        csv_path: Optional[str] = None,
        csv_dir: Optional[str] = None,
        csv_content: Optional[str] = None,
        period: str = "2016-2025",
        generate_sample: bool = False,
        skip_gold: bool = False,
    ) -> dict[str, Any]:
        """
        Execute the full backfill pipeline.

        Args:
            csv_path: Single CSV file path
            csv_dir: Directory of CSV files
            csv_content: Raw CSV string (testing)
            period: Period filter (e.g. '2016-2025')
            generate_sample: If True, generate sample data
            skip_gold: If True, skip gold-layer computation

        Returns:
            Comprehensive summary dict
        """
        from src.connectors.siam_historical import SIAMHistoricalConnector
        from src.transforms.mapper import DimensionMapper

        start_time = datetime.now(timezone.utc)
        logger.info("=== Historical Backfill: %s ===", period)

        summary: dict[str, Any] = {
            "period": period,
            "status": "running",
            "silver_loaded": 0,
            "silver_skipped_dup": 0,
            "unmapped_makers": set(),
            "unmapped_fuels": set(),
            "oem_stats": {},
            "year_stats": {},
            "gold_summary": None,
        }

        # Step 0: Generate sample if requested
        if generate_sample:
            start_y, end_y = (int(x) for x in period.split("-"))
            csv_content = generate_sample_csv(start_y, end_y)
            logger.info("Using generated sample data")

        # Step 1: Load dimension mappings
        mapper = DimensionMapper(self._db)
        await mapper.load_all()
        logger.info("Dimension mappings loaded")

        # Step 2: Extract from CSV
        async with SIAMHistoricalConnector() as connector:
            result = await connector.extract(
                period=period,
                csv_path=csv_path,
                csv_dir=csv_dir,
                csv_content=csv_content,
            )

        if not result.is_success:
            summary["status"] = "failed"
            summary["error"] = result.error_message
            return summary

        records = result.records
        logger.info("Extracted %d records from CSV", len(records))

        # Step 3: Map + Load in batches
        total = len(records)
        loaded = 0
        skipped = 0

        for batch_start in range(0, total, self._batch_size):
            batch = records[batch_start:batch_start + self._batch_size]
            batch_loaded, batch_skipped = await self._process_batch(
                batch, mapper, summary
            )
            loaded += batch_loaded
            skipped += batch_skipped

            if (batch_start + self._batch_size) % 10_000 == 0 or batch_start + self._batch_size >= total:
                pct = min(100, (batch_start + self._batch_size) / total * 100)
                logger.info(
                    "Progress: %.0f%% (%d/%d loaded, %d skipped)",
                    pct, loaded, total, skipped,
                )

        summary["silver_loaded"] = loaded
        summary["silver_skipped_dup"] = skipped

        # Step 4: Refresh materialized view
        try:
            await self._db.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY autoquant.mv_oem_monthly_summary"
            )
            logger.info("Materialized view refreshed")
        except Exception as e:
            # Fall back to non-concurrent refresh
            logger.warning("Concurrent refresh failed, trying regular: %s", e)
            try:
                await self._db.execute(
                    "REFRESH MATERIALIZED VIEW autoquant.mv_oem_monthly_summary"
                )
            except Exception as e2:
                logger.error("MV refresh failed: %s", e2)

        # Step 5: Gold-layer computation
        if not skip_gold:
            try:
                gold_summary = await self._gold.refresh_all_quarters()
                summary["gold_summary"] = gold_summary
                logger.info("Gold-layer refresh: %s", gold_summary.get("status"))
            except Exception as e:
                logger.warning("Gold-layer refresh failed (non-blocking): %s", e)
                summary["gold_summary"] = {"status": "failed", "error": str(e)}

        # Finalize
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        summary["status"] = "success"
        summary["duration_seconds"] = round(duration, 1)
        summary["unmapped_makers"] = sorted(summary["unmapped_makers"])
        summary["unmapped_fuels"] = sorted(summary["unmapped_fuels"])

        logger.info(
            "Backfill complete: %d loaded, %d skipped, %.1fs, "
            "%d unmapped makers, %d unmapped fuels",
            loaded, skipped, duration,
            len(summary["unmapped_makers"]),
            len(summary["unmapped_fuels"]),
        )

        return summary

    async def _process_batch(
        self,
        records: list[dict],
        mapper: Any,
        summary: dict[str, Any],
    ) -> tuple[int, int]:
        """
        Map and load a batch of records into fact_daily_registrations.

        Returns: (loaded_count, skipped_count)
        """
        loaded = 0
        skipped = 0

        rows_to_insert: list[tuple] = []

        for record in records:
            oem_name = record.get("oem_name", "")
            fuel_type = record.get("fuel_type", "")
            vehicle_class = record.get("vehicle_class", "")
            segment_hint = record.get("segment", "")
            data_date_str = record.get("data_date", "")
            count = record.get("registration_count", 0)

            if not data_date_str or count <= 0:
                continue

            # Parse date
            try:
                data_date = datetime.strptime(data_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            # Resolve vehicle class → segment
            vc_result = mapper.resolve_vehicle_class(vehicle_class)
            if vc_result:
                segment_id, is_excluded, class_mapped = vc_result
                if is_excluded:
                    continue
            elif segment_hint:
                # Fallback: use segment hint to get segment_id
                segment_id = mapper.get_segment_id_by_code(segment_hint)
                if segment_id is None:
                    continue
            else:
                continue

            # Resolve fuel
            fuel_id, fuel_mapped = mapper.resolve_fuel(fuel_type)
            if not fuel_mapped:
                # Default to PETROL for historical data where fuel isn't specified
                if not fuel_type:
                    fuel_id = mapper.get_default_fuel_id()
                else:
                    summary["unmapped_fuels"].add(fuel_type)
                    fuel_id = mapper.get_default_fuel_id()

            if fuel_id is None:
                continue

            # Get segment code for Tata split
            segment_code = mapper._segment_id_to_code.get(segment_id)

            # Resolve OEM (with Tata split)
            oem_id, oem_mapped = mapper.resolve_oem(
                "SIAM_HISTORICAL", oem_name, segment_code
            )
            if not oem_mapped:
                # Try VAHAN aliases as fallback
                oem_id, oem_mapped = mapper.resolve_oem(
                    "VAHAN", oem_name, segment_code
                )

            if not oem_mapped:
                summary["unmapped_makers"].add(oem_name)
                oem_id = mapper.others_oem_id
                if oem_id is None:
                    continue

            rows_to_insert.append((
                data_date,
                1,  # geo_id = All India
                oem_id,
                segment_id,
                fuel_id,
                count,
                "SIAM_HISTORICAL",
                "LOW",  # confidence
                1,  # revision_num
            ))

            # Track OEM stats
            oem_key = oem_name or "UNKNOWN"
            if oem_key not in summary["oem_stats"]:
                summary["oem_stats"][oem_key] = {
                    "records": 0, "volume": 0, "mapped": oem_mapped,
                }
            summary["oem_stats"][oem_key]["records"] += 1
            summary["oem_stats"][oem_key]["volume"] += count

            # Track year stats
            year = data_date.year
            if year not in summary["year_stats"]:
                summary["year_stats"][year] = {"records": 0, "volume": 0}
            summary["year_stats"][year]["records"] += 1
            summary["year_stats"][year]["volume"] += count

        # Batch insert with ON CONFLICT DO NOTHING
        if rows_to_insert:
            for row in rows_to_insert:
                try:
                    result = await self._db.execute(
                        """
                        INSERT INTO autoquant.fact_daily_registrations
                            (data_date, geo_id, oem_id, segment_id, fuel_id,
                             registrations, source, confidence, revision_num)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (data_date, oem_id, segment_id, fuel_id, revision_num)
                        DO NOTHING
                        """,
                        *row,
                    )
                    # Check if row was actually inserted
                    loaded += 1
                except Exception as e:
                    logger.debug("Insert error (continuing): %s", e)
                    skipped += 1

        return loaded, skipped
