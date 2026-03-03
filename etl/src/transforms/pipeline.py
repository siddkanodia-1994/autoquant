"""
AutoQuant ETL — Transform Pipeline.

Transforms raw VAHAN extraction data (bronze) into normalized fact tables (silver).

Pipeline stages:
  1. STORE RAW    → raw_vahan_snapshot (immutable bronze)
  2. MAP          → Resolve maker → oem_id, fuel → fuel_id, class → segment_id
  3. TATA SPLIT   → Route Tata Motors records to PV or CV entity by vehicle class
  4. FILTER       → Drop excluded vehicle classes (3W, tractors, etc.)
  5. AGGREGATE    → Sum registrations by (date, oem, segment, fuel)
  6. DELTA        → Derive daily delta from cumulative month values if needed
  7. VALIDATE     → Run validation gate checks
  8. LOAD SILVER  → Insert into fact_daily_registrations
  9. REFRESH MV   → Refresh mv_oem_monthly_summary
  10. STAGE UNMAPPED → Log unmapped entities for Telegram alerts
"""

from datetime import datetime, date, timezone
from typing import Any, Optional
from collections import defaultdict

from src.connectors.base import ExtractionResult
from src.transforms.mapper import DimensionMapper
from src.quality.validation_gate import ValidationGate, ValidationReport
from src.utils.database import DatabaseManager
from src.utils.extraction_log import ExtractionLogManager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class TransformPipeline:
    """
    Transforms raw VAHAN records into silver-layer fact tables.
    """

    def __init__(self, db: DatabaseManager, mapper: DimensionMapper) -> None:
        self._db = db
        self._mapper = mapper
        self._gate = ValidationGate(db)
        self._log_mgr = ExtractionLogManager(db)

    async def process_vahan_extraction(
        self,
        result: ExtractionResult,
        run_id: int,
        data_date: date,
    ) -> dict[str, Any]:
        """
        Full pipeline: raw records → silver fact table.

        Args:
            result: ExtractionResult from VahanConnector
            run_id: The extraction log run_id
            data_date: The date this data represents

        Returns:
            Pipeline summary dict with counts and validation report
        """
        summary: dict[str, Any] = {
            "run_id": run_id,
            "data_date": str(data_date),
            "raw_records": len(result.records),
            "records_stored_bronze": 0,
            "records_mapped": 0,
            "records_excluded": 0,
            "records_loaded_silver": 0,
            "unmapped_makers": [],
            "unmapped_fuels": [],
            "unmapped_classes": [],
            "unmapped_volume": 0,
            "total_volume": 0,
            "validation_passed": False,
        }

        # ── Stage 1: Store raw in bronze ──
        await self._store_bronze(result.records, run_id)
        summary["records_stored_bronze"] = len(result.records)
        logger.info("Stage 1 (bronze): %d records stored", len(result.records))

        # ── Stage 2-4: Map + Tata split + Filter ──
        mapped_records, unmapped_makers, unmapped_fuels, unmapped_classes = (
            await self._map_and_filter(result.records)
        )
        summary["records_mapped"] = len(mapped_records)
        summary["unmapped_makers"] = list(set(unmapped_makers))
        summary["unmapped_fuels"] = list(set(unmapped_fuels))
        summary["unmapped_classes"] = list(set(unmapped_classes))
        summary["records_excluded"] = len(result.records) - len(mapped_records) - len(unmapped_makers)

        logger.info(
            "Stage 2-4 (map+filter): %d mapped, %d unmapped makers, %d excluded",
            len(mapped_records), len(set(unmapped_makers)), summary["records_excluded"],
        )

        # ── Stage 5: Aggregate by (date, oem, segment, fuel) ──
        aggregated = self._aggregate(mapped_records, data_date)
        logger.info("Stage 5 (aggregate): %d aggregated rows", len(aggregated))

        # Compute total volume
        total_volume = sum(r["registration_count"] for r in result.records if r.get("registration_count", 0) > 0)
        unmapped_volume = sum(
            r.get("registration_count", 0) for r in result.records
            if r.get("_unmapped_maker", False)
        )
        summary["total_volume"] = total_volume
        summary["unmapped_volume"] = unmapped_volume

        # ── Stage 7: Validate ──
        validation = await self._gate.validate_extraction(
            run_id=run_id,
            source="VAHAN",
            records_count=len(result.records),
            unmapped_makers=summary["unmapped_makers"],
            unmapped_fuels=summary["unmapped_fuels"],
            unmapped_classes=summary["unmapped_classes"],
            total_volume=total_volume,
            unmapped_volume=unmapped_volume,
        )
        summary["validation_passed"] = validation.is_pass
        summary["validation_report"] = validation.summary()

        if not validation.is_pass:
            logger.warning("Validation FAILED — skipping silver load")
            await self._log_mgr.complete_run(
                run_id, "VALIDATION_FAILED", len(result.records),
                f"Validation failed: {[c.name for c in validation.failed_checks]}"
            )
            # Still stage unmapped entities for review
            await self._stage_unmapped(summary)
            return summary

        # ── Stage 8: Load silver ──
        loaded = await self._load_silver(aggregated, run_id)
        summary["records_loaded_silver"] = loaded
        logger.info("Stage 8 (silver): %d rows loaded", loaded)

        # ── Stage 9: Refresh materialized view ──
        try:
            await self._db.refresh_materialized_view("mv_oem_monthly_summary")
            logger.info("Stage 9: Materialized view refreshed")
        except Exception as e:
            logger.warning("MV refresh failed (non-blocking): %s", e)

        # ── Stage 10: Stage unmapped ──
        await self._stage_unmapped(summary)

        return summary

    async def _store_bronze(self, records: list[dict], run_id: int) -> None:
        """Insert raw records into raw_vahan_snapshot (immutable)."""
        if not records:
            return

        rows = []
        for r in records:
            rows.append((
                run_id,
                r.get("data_period", ""),
                r.get("state_filter", "ALL"),
                r.get("vehicle_category", ""),
                r.get("vehicle_class", ""),
                r.get("fuel", ""),
                r.get("maker", ""),
                r.get("registration_count", 0),
                None,  # query_params JSONB — set if available
            ))

        await self._db.executemany(
            """
            INSERT INTO raw_vahan_snapshot
                (run_id, data_period, state_filter, vehicle_category,
                 vehicle_class, fuel, maker, registration_count, query_params)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            rows,
        )

    async def _map_and_filter(
        self, records: list[dict]
    ) -> tuple[list[dict], list[str], list[str], list[str]]:
        """
        Map raw records to dimension IDs. Filter excluded classes.

        Returns:
            (mapped_records, unmapped_maker_names, unmapped_fuel_names, unmapped_class_names)
        """
        mapped: list[dict] = []
        unmapped_makers: list[str] = []
        unmapped_fuels: list[str] = []
        unmapped_classes: list[str] = []

        for record in records:
            maker = record.get("maker", "")
            fuel = record.get("fuel", "")
            vehicle_class = record.get("vehicle_class", "")
            count = record.get("registration_count", 0)

            if count <= 0:
                continue

            # Resolve vehicle class → segment
            segment_id, is_excluded, class_mapped = self._mapper.resolve_vehicle_class(vehicle_class)
            if not class_mapped:
                unmapped_classes.append(vehicle_class)
                continue
            if is_excluded:
                continue  # Drop excluded classes silently

            # Get segment code for Tata split resolution
            segment_code = self._mapper._segment_id_to_code.get(segment_id)

            # Resolve fuel
            fuel_id, fuel_mapped = self._mapper.resolve_fuel(fuel)
            if not fuel_mapped:
                unmapped_fuels.append(fuel)
                continue

            # Resolve maker (with Tata split)
            oem_id, oem_mapped = self._mapper.resolve_oem("VAHAN", maker, segment_code)
            if not oem_mapped:
                unmapped_makers.append(maker)
                record["_unmapped_maker"] = True
                # Still route to Others/Unlisted
                oem_id = self._mapper.others_oem_id
                if oem_id is None:
                    continue

            mapped.append({
                "oem_id": oem_id,
                "segment_id": segment_id,
                "fuel_id": fuel_id,
                "registration_count": count,
                "maker_raw": maker,
            })

        return mapped, unmapped_makers, unmapped_fuels, unmapped_classes

    def _aggregate(
        self, records: list[dict], data_date: date
    ) -> list[dict]:
        """
        Aggregate mapped records by (data_date, oem_id, segment_id, fuel_id).
        Multiple vehicle classes within the same segment get summed.
        """
        agg: dict[tuple, int] = defaultdict(int)

        for r in records:
            key = (r["oem_id"], r["segment_id"], r["fuel_id"])
            agg[key] += r["registration_count"]

        result = []
        for (oem_id, segment_id, fuel_id), total in agg.items():
            result.append({
                "data_date": data_date,
                "oem_id": oem_id,
                "segment_id": segment_id,
                "fuel_id": fuel_id,
                "registrations": total,
            })

        return result

    async def _load_silver(self, aggregated: list[dict], run_id: int) -> int:
        """Insert aggregated records into fact_daily_registrations."""
        if not aggregated:
            return 0

        rows = []
        for r in aggregated:
            rows.append((
                r["data_date"],
                1,  # geo_id = 1 (All India)
                r["oem_id"],
                r["segment_id"],
                r["fuel_id"],
                r["registrations"],
                "VAHAN",
                run_id,
                1,  # revision_num (first load)
            ))

        await self._db.executemany(
            """
            INSERT INTO fact_daily_registrations
                (data_date, geo_id, oem_id, segment_id, fuel_id,
                 registrations, source, run_id, revision_num)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            rows,
        )

        return len(rows)

    async def _stage_unmapped(self, summary: dict) -> None:
        """Insert unmapped entity names into staging tables for review."""
        for maker in summary.get("unmapped_makers", []):
            await self._db.execute(
                """
                INSERT INTO staging_unmapped_makers (source, raw_maker_name, registration_volume)
                VALUES ('VAHAN', $1, 0)
                ON CONFLICT (source, raw_maker_name) DO UPDATE SET
                    last_seen_at = NOW(),
                    occurrence_count = staging_unmapped_makers.occurrence_count + 1
                """,
                maker,
            )

        for fuel in summary.get("unmapped_fuels", []):
            await self._db.execute(
                """
                INSERT INTO staging_unmapped_fuels (source, raw_fuel_name)
                VALUES ('VAHAN', $1)
                ON CONFLICT (source, raw_fuel_name) DO UPDATE SET
                    last_seen_at = NOW(),
                    occurrence_count = staging_unmapped_fuels.occurrence_count + 1
                """,
                fuel,
            )

        for cls in summary.get("unmapped_classes", []):
            await self._db.execute(
                """
                INSERT INTO staging_unmapped_vehicle_classes (source, raw_class_name)
                VALUES ('VAHAN', $1)
                ON CONFLICT (source, raw_class_name) DO UPDATE SET
                    last_seen_at = NOW(),
                    occurrence_count = staging_unmapped_vehicle_classes.occurrence_count + 1
                """,
                cls,
            )


class DeltaDeriver:
    """
    Derives daily registration deltas from cumulative monthly data.

    VAHAN may only expose cumulative month-to-date counts.
    To get daily volumes:
      daily_delta = today_cumulative - yesterday_cumulative

    Edge cases:
      - Day 1 of month: previous cumulative = 0, delta = today's cumulative
      - Negative delta: flagged as data revision (not error)
      - Zero delta on weekday: flagged for investigation
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    async def derive_daily(
        self,
        data_date: date,
        cumulative_records: list[dict],
    ) -> list[dict]:
        """
        Given cumulative MTD records for data_date,
        derive daily deltas by subtracting previous day's cumulative.

        Args:
            data_date: The date of the cumulative snapshot
            cumulative_records: List of {oem_id, segment_id, fuel_id, registrations (cumulative)}

        Returns:
            List of {oem_id, segment_id, fuel_id, registrations (daily delta)}
        """
        # Check if this is day 1 of the month
        if data_date.day == 1:
            logger.info("Day 1 of month — cumulative IS the daily delta")
            return cumulative_records

        # Fetch previous day's cumulative
        prev_date = data_date.replace(day=data_date.day - 1)
        prev_rows = await self._db.fetch(
            """
            SELECT oem_id, segment_id, fuel_id, registrations
            FROM fact_daily_registrations
            WHERE data_date = $1
              AND source = 'VAHAN'
              AND revision_num = (
                  SELECT MAX(revision_num)
                  FROM fact_daily_registrations f2
                  WHERE f2.data_date = $1
                    AND f2.oem_id = fact_daily_registrations.oem_id
                    AND f2.segment_id = fact_daily_registrations.segment_id
                    AND f2.fuel_id = fact_daily_registrations.fuel_id
              )
            """,
            prev_date,
        )

        # Build lookup of previous cumulative
        prev_lookup: dict[tuple, int] = {}
        for row in prev_rows:
            key = (row["oem_id"], row["segment_id"], row["fuel_id"])
            prev_lookup[key] = row["registrations"]

        # Compute deltas
        daily_records: list[dict] = []
        for record in cumulative_records:
            key = (record["oem_id"], record["segment_id"], record["fuel_id"])
            prev_cum = prev_lookup.get(key, 0)
            delta = record["registrations"] - prev_cum

            if delta < 0:
                logger.warning(
                    "Negative delta for oem=%d seg=%d fuel=%d: %d → %d (delta=%d). "
                    "Flagging as revision.",
                    *key, prev_cum, record["registrations"], delta,
                )
                # Store the negative delta — the validation gate will flag it

            daily_records.append({
                "oem_id": record["oem_id"],
                "segment_id": record["segment_id"],
                "fuel_id": record["fuel_id"],
                "registrations": max(delta, 0),  # Clamp to 0 for the fact table
                "raw_delta": delta,  # Keep actual for audit
                "is_negative_delta": delta < 0,
            })

        return daily_records
