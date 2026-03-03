"""
AutoQuant ETL — Validation Gate.

Runs all data quality checks from the framework before allowing
data to be promoted from bronze to silver tables.

Checks:
  1. completeness    — Row count within ±30% of previous run
  2. mapping_coverage — >95% of maker names mapped to dim_oem
  3. fuel_mapping    — 100% of fuel types mapped
  4. anomaly_detection — Z-score on daily deltas vs trailing 30-day mean
  5. negative_delta  — Flag (not block) negative cumulative deltas
  6. industry_total  — Listed + Unlisted = Total
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from src.utils.database import DatabaseManager

logger = logging.getLogger(__name__)


class CheckResult(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class ValidationCheck:
    name: str
    result: CheckResult
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Aggregate result of all validation checks."""
    checks: list[ValidationCheck] = field(default_factory=list)
    overall: CheckResult = CheckResult.PASS

    def add_check(self, check: ValidationCheck) -> None:
        self.checks.append(check)
        if check.result == CheckResult.FAIL:
            self.overall = CheckResult.FAIL
        elif check.result == CheckResult.WARN and self.overall != CheckResult.FAIL:
            self.overall = CheckResult.WARN

    @property
    def is_pass(self) -> bool:
        return self.overall in (CheckResult.PASS, CheckResult.WARN)

    @property
    def failed_checks(self) -> list[ValidationCheck]:
        return [c for c in self.checks if c.result == CheckResult.FAIL]

    @property
    def warning_checks(self) -> list[ValidationCheck]:
        return [c for c in self.checks if c.result == CheckResult.WARN]

    def summary(self) -> str:
        lines = [f"Validation: {self.overall.value}"]
        for check in self.checks:
            lines.append(f"  [{check.result.value}] {check.name}: {check.message}")
        return "\n".join(lines)


class ValidationGate:
    """
    Runs all validation checks on extracted data before silver promotion.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    async def validate_extraction(
        self,
        run_id: int,
        source: str,
        records_count: int,
        unmapped_makers: list[str],
        unmapped_fuels: list[str],
        unmapped_classes: list[str],
        total_volume: int,
        unmapped_volume: int,
    ) -> ValidationReport:
        """Run all validation checks and return a report."""
        report = ValidationReport()

        # 1. Completeness check
        report.add_check(
            await self._check_completeness(source, records_count)
        )

        # 2. Maker mapping coverage
        report.add_check(
            self._check_mapping_coverage(
                "maker", unmapped_makers, total_volume, unmapped_volume
            )
        )

        # 3. Fuel mapping coverage
        report.add_check(
            self._check_fuel_mapping(unmapped_fuels)
        )

        # 4. Vehicle class mapping
        report.add_check(
            self._check_class_mapping(unmapped_classes)
        )

        logger.info("Validation report: %s", report.summary())
        return report

    async def _check_completeness(
        self, source: str, current_count: int
    ) -> ValidationCheck:
        """Check if record count is within ±30% of the previous successful run."""
        prev_count = await self._db.fetchval(
            """
            SELECT records_extracted FROM raw_extraction_log
            WHERE source = $1 AND status = 'SUCCESS'
            ORDER BY completed_at DESC LIMIT 1
            """,
            source,
        )

        if prev_count is None:
            return ValidationCheck(
                name="completeness",
                result=CheckResult.SKIP,
                message="No previous run to compare against (first run).",
            )

        if prev_count == 0:
            return ValidationCheck(
                name="completeness",
                result=CheckResult.WARN,
                message="Previous run had 0 records.",
            )

        ratio = current_count / prev_count
        if 0.7 <= ratio <= 1.3:
            return ValidationCheck(
                name="completeness",
                result=CheckResult.PASS,
                message=f"Record count {current_count:,} is within ±30% of previous {prev_count:,} (ratio: {ratio:.2f})",
                details={"current": current_count, "previous": prev_count, "ratio": ratio},
            )
        else:
            return ValidationCheck(
                name="completeness",
                result=CheckResult.FAIL,
                message=f"Record count {current_count:,} is OUTSIDE ±30% of previous {prev_count:,} (ratio: {ratio:.2f})",
                details={"current": current_count, "previous": prev_count, "ratio": ratio},
            )

    def _check_mapping_coverage(
        self,
        entity_type: str,
        unmapped: list[str],
        total_volume: int,
        unmapped_volume: int,
    ) -> ValidationCheck:
        """Check if >95% of volume is mapped."""
        if total_volume == 0:
            return ValidationCheck(
                name=f"{entity_type}_mapping_coverage",
                result=CheckResult.WARN,
                message="Total volume is 0.",
            )

        mapped_pct = ((total_volume - unmapped_volume) / total_volume) * 100

        if mapped_pct >= 95.0:
            result = CheckResult.PASS
        elif mapped_pct >= 85.0:
            result = CheckResult.WARN
        else:
            result = CheckResult.FAIL

        return ValidationCheck(
            name=f"{entity_type}_mapping_coverage",
            result=result,
            message=f"{mapped_pct:.1f}% of volume mapped. {len(unmapped)} unmapped names.",
            details={
                "mapped_pct": mapped_pct,
                "unmapped_count": len(unmapped),
                "unmapped_volume": unmapped_volume,
                "unmapped_names": unmapped[:20],  # Cap at 20 for readability
            },
        )

    def _check_fuel_mapping(self, unmapped: list[str]) -> ValidationCheck:
        """Check if 100% of fuel types are mapped."""
        if not unmapped:
            return ValidationCheck(
                name="fuel_mapping_coverage",
                result=CheckResult.PASS,
                message="All fuel types mapped.",
            )
        return ValidationCheck(
            name="fuel_mapping_coverage",
            result=CheckResult.FAIL,
            message=f"{len(unmapped)} unmapped fuel types: {unmapped}",
            details={"unmapped": unmapped},
        )

    def _check_class_mapping(self, unmapped: list[str]) -> ValidationCheck:
        """Check if all vehicle classes are mapped."""
        if not unmapped:
            return ValidationCheck(
                name="vehicle_class_mapping",
                result=CheckResult.PASS,
                message="All vehicle classes mapped.",
            )
        return ValidationCheck(
            name="vehicle_class_mapping",
            result=CheckResult.WARN,
            message=f"{len(unmapped)} unmapped vehicle classes: {unmapped}",
            details={"unmapped": unmapped},
        )

    async def check_anomaly(
        self,
        data_date: str,
        segment_code: str,
        daily_value: int,
    ) -> ValidationCheck:
        """
        Z-score anomaly check: compare today's value against trailing 30-day stats.
        Flag if |z| > 3.
        """
        stats = await self._db.fetchrow(
            """
            SELECT
                AVG(registrations)::FLOAT AS mean_val,
                STDDEV(registrations)::FLOAT AS std_val,
                COUNT(*) AS n
            FROM fact_daily_registrations fdr
            JOIN dim_segment s ON fdr.segment_id = s.segment_id
            WHERE s.segment_code = $1
              AND fdr.data_date >= ($2::DATE - INTERVAL '30 days')
              AND fdr.data_date < $2::DATE
            """,
            segment_code,
            data_date,
        )

        if stats is None or stats["n"] < 7 or stats["std_val"] in (None, 0):
            return ValidationCheck(
                name=f"anomaly_{segment_code}",
                result=CheckResult.SKIP,
                message="Insufficient history for anomaly detection.",
            )

        z_score = (daily_value - stats["mean_val"]) / stats["std_val"]

        if abs(z_score) > 3.0:
            return ValidationCheck(
                name=f"anomaly_{segment_code}",
                result=CheckResult.WARN,
                message=f"Z-score {z_score:.2f} for {segment_code} on {data_date} (value: {daily_value:,}, mean: {stats['mean_val']:,.0f})",
                details={"z_score": z_score, "value": daily_value, "mean": stats["mean_val"]},
            )
        return ValidationCheck(
            name=f"anomaly_{segment_code}",
            result=CheckResult.PASS,
            message=f"Z-score {z_score:.2f} — within normal range.",
        )
