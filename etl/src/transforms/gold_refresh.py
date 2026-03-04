"""
AutoQuant ETL — Gold-Layer Refresh.

Computes est_quarterly_revenue from:
  - mv_oem_monthly_summary (aggregated registrations)
  - fact_asp_master (segment × fuel ASP assumptions)

Revenue proxy formula:
  est_domestic_rev_cr = SUM(registrations × ASP_lakhs) / 100
  (÷100 converts from lakhs to crores)

Sensitivity bands:
  est_rev_low_cr  = est_domestic_rev_cr × 0.85  (−15%)
  est_rev_high_cr = est_domestic_rev_cr × 1.15  (+15%)

FY Calendar (India):
  Q1 = Apr–Jun, Q2 = Jul–Sep, Q3 = Oct–Dec, Q4 = Jan–Mar
  FY26 = Apr 2025 → Mar 2026
"""

from datetime import date, datetime, timezone
from typing import Any, Optional

from src.utils.database import DatabaseManager
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# FY quarter → calendar months mapping
FY_QUARTER_MONTHS: dict[str, list[int]] = {
    "Q1": [4, 5, 6],
    "Q2": [7, 8, 9],
    "Q3": [10, 11, 12],
    "Q4": [1, 2, 3],
}


def calendar_to_fy(year: int, month: int) -> tuple[str, str]:
    """
    Convert calendar year+month to FY quarter label.

    Examples:
        (2025, 4) → ('FY26', 'Q1')  # Apr 2025 = Q1FY26
        (2026, 1) → ('FY26', 'Q4')  # Jan 2026 = Q4FY26
        (2016, 7) → ('FY17', 'Q2')  # Jul 2016 = Q2FY17
    """
    if month >= 4:
        fy_year = year + 1
    else:
        fy_year = year

    fy_label = f"FY{fy_year % 100:02d}"

    if month in (4, 5, 6):
        quarter = "Q1"
    elif month in (7, 8, 9):
        quarter = "Q2"
    elif month in (10, 11, 12):
        quarter = "Q3"
    else:  # 1, 2, 3
        quarter = "Q4"

    return fy_label, quarter


def fy_quarter_label(fy: str, quarter: str) -> str:
    """Build 'Q3FY26' style label."""
    return f"{quarter}{fy}"


def quarter_day_count(fy: str, quarter: str) -> int:
    """
    Return total calendar days in a given FY quarter.

    For data completeness calculation.
    """
    fy_num = int(fy[2:])
    full_year = 2000 + fy_num  # FY26 → 2026

    if quarter == "Q1":
        # Apr–Jun of previous calendar year
        start = date(full_year - 1, 4, 1)
        end = date(full_year - 1, 7, 1)
    elif quarter == "Q2":
        start = date(full_year - 1, 7, 1)
        end = date(full_year - 1, 10, 1)
    elif quarter == "Q3":
        start = date(full_year - 1, 10, 1)
        end = date(full_year, 1, 1)
    else:  # Q4
        start = date(full_year, 1, 1)
        end = date(full_year, 4, 1)

    return (end - start).days


class GoldLayerRefresh:
    """
    Computes demand-based implied revenue proxy and populates
    the est_quarterly_revenue gold table.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    async def refresh_all_quarters(
        self,
        *,
        fy_from: Optional[str] = None,
        fy_to: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Recompute est_quarterly_revenue for all quarters with data.

        Args:
            fy_from: Starting FY (e.g. 'FY17'). Default: earliest data.
            fy_to: Ending FY (e.g. 'FY26'). Default: latest data.

        Returns:
            Summary dict with counts and totals.
        """
        logger.info("Gold-layer refresh: computing quarterly revenue estimates")

        # Load ASP assumptions
        asp_map = await self._load_asp_map()
        if not asp_map:
            logger.warning("No ASP assumptions found in fact_asp_master")
            return {"status": "skipped", "reason": "no ASP data"}

        # Get all OEM+quarter combinations from MV
        aggregates = await self._fetch_quarterly_aggregates(fy_from, fy_to)
        if not aggregates:
            logger.warning("No data in mv_oem_monthly_summary for gold refresh")
            return {"status": "skipped", "reason": "no MV data"}

        # Compute revenue for each OEM+quarter
        estimates: list[dict[str, Any]] = []
        for agg in aggregates:
            estimate = self._compute_revenue(agg, asp_map)
            if estimate:
                estimates.append(estimate)

        # Upsert into est_quarterly_revenue
        loaded = await self._upsert_estimates(estimates)

        summary = {
            "status": "success",
            "quarters_processed": len(set(e["fy_quarter"] for e in estimates)),
            "oem_quarter_rows": loaded,
            "total_est_rev_cr": sum(
                float(e.get("est_domestic_rev_cr", 0)) for e in estimates
            ),
        }

        logger.info(
            "Gold refresh complete: %d OEM-quarter rows, ₹%.0f Cr total estimated",
            loaded, summary["total_est_rev_cr"],
        )

        return summary

    async def refresh_single_quarter(
        self, fy: str, quarter: str
    ) -> dict[str, Any]:
        """Refresh a single quarter (e.g. 'FY26', 'Q3')."""
        label = fy_quarter_label(fy, quarter)
        logger.info("Gold refresh: single quarter %s", label)

        asp_map = await self._load_asp_map()
        aggregates = await self._fetch_quarterly_aggregates(
            fy_from=fy, fy_to=fy
        )

        # Filter to just this quarter
        aggregates = [
            a for a in aggregates
            if a["fy_quarter"] == label
        ]

        estimates = []
        for agg in aggregates:
            estimate = self._compute_revenue(agg, asp_map)
            if estimate:
                estimates.append(estimate)

        loaded = await self._upsert_estimates(estimates)
        return {"quarter": label, "rows": loaded}

    async def _load_asp_map(self) -> dict[tuple[int, int], dict]:
        """
        Load ASP assumptions keyed by (segment_id, fuel_id).

        Uses the currently-active assumption (effective_to IS NULL or >= today).
        """
        rows = await self._db.fetch(
            """
            SELECT segment_id, fuel_id, asp_lakhs, asp_low_lakhs, asp_high_lakhs,
                   confidence, effective_from
            FROM autoquant.fact_asp_master
            WHERE effective_to IS NULL
               OR effective_to >= CURRENT_DATE
            ORDER BY segment_id, fuel_id, effective_from DESC
            """
        )

        asp_map: dict[tuple[int, int], dict] = {}
        for row in rows:
            key = (row["segment_id"], row["fuel_id"])
            if key not in asp_map:  # Take most recent
                asp_map[key] = {
                    "asp_lakhs": float(row["asp_lakhs"]),
                    "asp_low_lakhs": float(row.get("asp_low_lakhs") or row["asp_lakhs"]),
                    "asp_high_lakhs": float(row.get("asp_high_lakhs") or row["asp_lakhs"]),
                    "confidence": row.get("confidence", "MEDIUM"),
                }

        logger.info("Loaded %d ASP assumptions", len(asp_map))
        return asp_map

    async def _fetch_quarterly_aggregates(
        self,
        fy_from: Optional[str] = None,
        fy_to: Optional[str] = None,
    ) -> list[dict]:
        """
        Aggregate MV data by OEM + FY quarter.

        Returns list of dicts with:
            oem_id, oem_name, nse_ticker, fy_year, fy_quarter,
            segment_id, fuel_group_id, total_registrations, months_with_data
        """
        # Build FY filter
        conditions = []
        params: list[Any] = []

        if fy_from:
            conditions.append(f"fy_year >= ${len(params) + 1}")
            params.append(fy_from)
        if fy_to:
            conditions.append(f"fy_year <= ${len(params) + 1}")
            params.append(fy_to)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT
                m.oem_id,
                m.oem_name,
                m.nse_ticker,
                m.fy_year,
                m.fy_quarter,
                m.segment_id,
                m.fuel_id,
                SUM(m.total_registrations) as total_registrations,
                COUNT(DISTINCT m.calendar_month) as months_with_data
            FROM (
                SELECT
                    o.oem_id,
                    o.oem_name,
                    o.nse_ticker,
                    d.fy_year,
                    d.fy_quarter,
                    f.segment_id,
                    f.fuel_id,
                    d.calendar_month,
                    SUM(f.registrations) as total_registrations
                FROM autoquant.fact_daily_registrations f
                JOIN autoquant.dim_oem o ON f.oem_id = o.oem_id
                JOIN autoquant.dim_date d ON f.data_date = d.date_key
                {where_clause}
                GROUP BY o.oem_id, o.oem_name, o.nse_ticker,
                         d.fy_year, d.fy_quarter, f.segment_id,
                         f.fuel_id, d.calendar_month
            ) m
            GROUP BY m.oem_id, m.oem_name, m.nse_ticker,
                     m.fy_year, m.fy_quarter, m.segment_id, m.fuel_id
            ORDER BY m.fy_year, m.fy_quarter, m.oem_id
        """

        rows = await self._db.fetch(query, *params)
        logger.info("Fetched %d OEM-quarter-segment-fuel aggregates", len(rows))

        # Reshape: group by (oem_id, fy_quarter) → combine segments/fuels
        oem_quarter_map: dict[tuple[int, str], dict] = {}

        for row in rows:
            label = fy_quarter_label(row["fy_year"], row["fy_quarter"])
            key = (row["oem_id"], label)

            if key not in oem_quarter_map:
                oem_quarter_map[key] = {
                    "oem_id": row["oem_id"],
                    "oem_name": row["oem_name"],
                    "nse_ticker": row["nse_ticker"],
                    "fy_year": row["fy_year"],
                    "fy_quarter": label,
                    "breakdowns": [],
                    "total_registrations": 0,
                    "months_with_data": 0,
                }

            entry = oem_quarter_map[key]
            regs = int(row["total_registrations"])
            entry["breakdowns"].append({
                "segment_id": row["segment_id"],
                "fuel_id": row["fuel_id"],
                "registrations": regs,
            })
            entry["total_registrations"] += regs
            entry["months_with_data"] = max(
                entry["months_with_data"], int(row["months_with_data"])
            )

        return list(oem_quarter_map.values())

    def _compute_revenue(
        self,
        agg: dict[str, Any],
        asp_map: dict[tuple[int, int], dict],
    ) -> Optional[dict[str, Any]]:
        """
        Compute revenue estimate for one OEM-quarter.

        Revenue = SUM over (segment, fuel):
            registrations(seg, fuel) × ASP_lakhs(seg, fuel) / 100

        Converts lakhs → crores (÷100).
        """
        est_rev = 0.0
        est_rev_low = 0.0
        est_rev_high = 0.0
        matched_volume = 0

        for bd in agg["breakdowns"]:
            seg_id = bd["segment_id"]
            fuel_id = bd["fuel_id"]
            regs = bd["registrations"]

            asp = asp_map.get((seg_id, fuel_id))
            if asp:
                est_rev += regs * asp["asp_lakhs"] / 100
                est_rev_low += regs * asp["asp_low_lakhs"] / 100
                est_rev_high += regs * asp["asp_high_lakhs"] / 100
                matched_volume += regs
            else:
                # Try segment-level fallback (fuel_id=0 or segment default)
                # For now, skip unmatched — they'll show in completeness
                pass

        if est_rev == 0 and agg["total_registrations"] == 0:
            return None

        # Data completeness: months_with_data / 3 (quarter has 3 months)
        completeness = min(100.0, (agg["months_with_data"] / 3) * 100)

        # Parse FY+quarter for day count
        label = agg["fy_quarter"]
        quarter = label[:2]
        fy = label[2:]

        return {
            "oem_id": agg["oem_id"],
            "fy_quarter": label,
            "estimate_date": date.today(),
            "reg_volume": agg["total_registrations"],
            "wholesale_volume": None,  # From BSE — populated separately
            "export_volume": None,
            "est_domestic_rev_cr": round(est_rev, 2),
            "est_total_rev_cr": round(est_rev, 2),  # Domestic only for now
            "est_rev_low_cr": round(est_rev_low, 2),
            "est_rev_high_cr": round(est_rev_high, 2),
            "data_completeness_pct": round(completeness, 2),
        }

    async def _upsert_estimates(self, estimates: list[dict]) -> int:
        """Insert/update est_quarterly_revenue rows."""
        if not estimates:
            return 0

        count = 0
        for e in estimates:
            await self._db.execute(
                """
                INSERT INTO autoquant.est_quarterly_revenue
                    (oem_id, fy_quarter, estimate_date, reg_volume,
                     wholesale_volume, export_volume,
                     est_domestic_rev_cr, est_total_rev_cr,
                     est_rev_low_cr, est_rev_high_cr,
                     data_completeness_pct)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (oem_id, fy_quarter, estimate_date)
                DO UPDATE SET
                    reg_volume = EXCLUDED.reg_volume,
                    est_domestic_rev_cr = EXCLUDED.est_domestic_rev_cr,
                    est_total_rev_cr = EXCLUDED.est_total_rev_cr,
                    est_rev_low_cr = EXCLUDED.est_rev_low_cr,
                    est_rev_high_cr = EXCLUDED.est_rev_high_cr,
                    data_completeness_pct = EXCLUDED.data_completeness_pct
                """,
                e["oem_id"], e["fy_quarter"], e["estimate_date"],
                e["reg_volume"], e["wholesale_volume"], e["export_volume"],
                e["est_domestic_rev_cr"], e["est_total_rev_cr"],
                e["est_rev_low_cr"], e["est_rev_high_cr"],
                e["data_completeness_pct"],
            )
            count += 1

        return count
