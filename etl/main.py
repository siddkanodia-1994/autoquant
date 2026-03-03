"""
AutoQuant ETL — Main Orchestrator.

Entry points for all ETL jobs:
  - daily_vahan_extract
  - weekly_backfill
  - monthly_reconciliation (FADA)
  - wholesale_bse (BSE filings)
  - historical_backfill (SIAM CSV)
  - quarterly_calibration

Usage:
    python main.py daily          # Run daily VAHAN extraction
    python main.py weekly         # Run weekly backfill
    python main.py monthly        # Run monthly FADA reconciliation
    python main.py wholesale      # Run BSE wholesale extraction
    python main.py historical     # Run SIAM historical backfill
    python main.py seed           # Seed dimension tables
    python main.py migrate        # Run database migrations
    python main.py health         # Check all connections
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import get_settings
from src.utils.logging_config import setup_logging, get_logger
from src.utils.database import get_db, close_db
from src.alerts.telegram import get_alert_manager, AlertSeverity

logger: logging.Logger = None  # type: ignore


async def cmd_health() -> None:
    """Check database and Telegram connectivity."""
    logger.info("Running health checks...")

    # Database
    db = await get_db()
    db_ok = await db.health_check()
    logger.info("Database: %s", "OK" if db_ok else "FAILED")

    if db_ok:
        # Check table counts
        for table in ["dim_date", "dim_oem", "dim_segment", "dim_fuel", "dim_oem_alias"]:
            count = await db.table_count(table)
            logger.info("  %s: %d rows", table, count)

    # Telegram
    alerts = await get_alert_manager()
    if alerts._settings.is_configured:
        sent = await alerts.send_message("🏥 AutoQuant health check — system online.")
        logger.info("Telegram: %s", "OK" if sent else "FAILED")
    else:
        logger.info("Telegram: NOT CONFIGURED")

    await close_db()


async def cmd_daily(dry_run: bool = False) -> None:
    """
    Run daily VAHAN extraction pipeline.

    Full flow:
      1. Launch Playwright → Extract aggregated counts from VAHAN
      2. Store raw in bronze (raw_vahan_snapshot)
      3. Map maker/fuel/class → dimension IDs (with Tata PV/CV split)
      4. Filter excluded vehicle classes
      5. Aggregate by (date, oem, segment, fuel)
      6. Run validation gate (completeness, mapping, anomaly)
      7. If PASS → load into fact_daily_registrations
      8. Refresh mv_oem_monthly_summary
      9. Trigger Vercel ISR revalidation
      10. Send Telegram summary
    """
    from src.connectors.vahan import VahanConnector
    from src.transforms.mapper import DimensionMapper
    from src.transforms.pipeline import TransformPipeline
    from src.utils.extraction_log import ExtractionLogManager
    from src.utils.vercel import trigger_revalidation

    logger.info("=== Daily VAHAN Extraction ===")
    start_time = datetime.now(timezone.utc)
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)
    mapper = DimensionMapper(db)
    pipeline = TransformPipeline(db, mapper)

    # Current month in YYYY-MM format
    now = datetime.now(timezone.utc)
    current_period = now.strftime("%Y-%m")
    data_date = now.date()

    run_id = await log_mgr.start_run("VAHAN", f"Daily extraction for {current_period}")

    try:
        # Load dimension mappings
        await mapper.load_all()
        logger.info("Dimension mappings loaded")

        if dry_run:
            logger.info("DRY RUN — skipping actual extraction")
            await log_mgr.complete_run(run_id, "SUCCESS", 0, "Dry run")
            return

        # ── Extract ──
        async with VahanConnector() as connector:
            # Health check first
            healthy = await connector.health_check()
            if not healthy:
                raise RuntimeError("VAHAN dashboard unreachable")

            result = await connector.extract(
                period=current_period,
                segments=["PV", "CV", "2W"],
            )

        if not result.is_success:
            raise RuntimeError(f"Extraction failed: {result.error_message}")

        logger.info(
            "Extraction complete: %d records in %.1fs",
            result.records_count, result.duration_seconds,
        )

        # ── Transform + Validate + Load ──
        summary = await pipeline.process_vahan_extraction(
            result=result,
            run_id=run_id,
            data_date=data_date,
        )

        # ── Post-pipeline ──
        if summary["validation_passed"]:
            await log_mgr.complete_run(
                run_id, "SUCCESS", result.records_count
            )

            # Trigger Vercel ISR revalidation
            await trigger_revalidation()

            # Send Telegram success summary
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            await alerts.alert_daily_summary(
                run_id=run_id,
                records_extracted=result.records_count,
                records_loaded=summary["records_loaded_silver"],
                unmapped_count=len(summary["unmapped_makers"]),
                duration_seconds=duration,
            )

            # Send individual alerts for unmapped entities
            for maker in summary["unmapped_makers"][:10]:
                await alerts.alert_unmapped_entity("maker", maker, "VAHAN")
            for fuel in summary["unmapped_fuels"]:
                await alerts.alert_unmapped_entity("fuel", fuel, "VAHAN")

        else:
            await alerts.alert_validation_failure(
                check_name="daily_gate",
                source="VAHAN",
                run_id=run_id,
                reason=summary.get("validation_report", "Unknown"),
            )

    except Exception as e:
        logger.error("Daily extraction failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "VAHAN", run_id, f"Daily extraction failed: {e}"
        )
    finally:
        await close_db()


async def cmd_weekly() -> None:
    """
    Weekly backfill: Re-extract last 30 days from VAHAN.
    Compare with stored daily series → identify revisions.
    """
    from src.connectors.vahan import VahanConnector
    from src.transforms.mapper import DimensionMapper
    from src.transforms.pipeline import TransformPipeline
    from src.utils.extraction_log import ExtractionLogManager

    logger.info("=== Weekly Backfill ===")
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)
    mapper = DimensionMapper(db)
    pipeline = TransformPipeline(db, mapper)

    run_id = await log_mgr.start_run("VAHAN", "Weekly backfill (last 30 days)")

    try:
        await mapper.load_all()

        now = datetime.now(timezone.utc)
        current_period = now.strftime("%Y-%m")
        prev_month = (now.replace(day=1) - timedelta(days=1))
        prev_period = prev_month.strftime("%Y-%m")

        async with VahanConnector() as connector:
            for period in [prev_period, current_period]:
                result = await connector.extract(period=period, segments=["PV", "CV", "2W"])
                if result.is_success:
                    await pipeline.process_vahan_extraction(
                        result=result,
                        run_id=run_id,
                        data_date=now.date(),
                    )

        await log_mgr.complete_run(run_id, "SUCCESS", 0, "Weekly backfill complete")
        logger.info("Weekly backfill complete")

    except Exception as e:
        logger.error("Weekly backfill failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "VAHAN", run_id, f"Weekly backfill failed: {e}"
        )
    finally:
        await close_db()


async def cmd_monthly(
    pdf_path: Optional[str] = None,
    pdf_url: Optional[str] = None,
) -> None:
    """
    Monthly FADA reconciliation.

    Parses the FADA monthly report to extract industry-level totals
    and OEM wholesale data. Compares with VAHAN cumulative to flag
    discrepancies > ±5%.
    """
    from src.connectors.fada import FADAConnector
    from src.utils.extraction_log import ExtractionLogManager

    logger.info("=== Monthly FADA Reconciliation ===")
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)

    now = datetime.now(timezone.utc)
    prev_month = (now.replace(day=1) - timedelta(days=1))
    period = prev_month.strftime("%Y-%m")

    run_id = await log_mgr.start_run("FADA", f"Monthly reconciliation for {period}")

    try:
        async with FADAConnector() as connector:
            result = await connector.extract(
                period=period,
                pdf_path=pdf_path,
                pdf_url=pdf_url,
            )

        if not result.is_success:
            logger.warning("FADA extraction failed: %s", result.error_message)
            await log_mgr.complete_run(run_id, "FAILED", 0, result.error_message)
            return

        logger.info("FADA extraction: %d records", result.records_count)

        # Store raw FADA data in bronze
        for record in result.records:
            await db.execute(
                """
                INSERT INTO autoquant.raw_fada_report
                    (report_period, oem_name, segment, volume, yoy_pct,
                     data_type, source_page, extracted_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (report_period, oem_name, segment, data_type)
                DO UPDATE SET volume = EXCLUDED.volume,
                              yoy_pct = EXCLUDED.yoy_pct,
                              extracted_at = NOW()
                """,
                period,
                record.get("oem_name", ""),
                record.get("segment", ""),
                record.get("volume", 0),
                record.get("yoy_pct"),
                record.get("data_type", "retail"),
                record.get("source_page", 0),
            )

        # ── Reconciliation: FADA vs VAHAN ──
        fada_totals: dict[str, int] = {}
        for r in result.records:
            if r.get("oem_name") == "INDUSTRY_TOTAL" and r.get("data_type") == "retail":
                seg = r.get("segment", "")
                fada_totals[seg] = r.get("volume", 0)

        if fada_totals:
            year, month = period.split("-")
            vahan_totals = await db.fetch(
                """
                SELECT segment_code, SUM(total_registrations)::bigint as total
                FROM autoquant.mv_oem_monthly_summary
                WHERE calendar_year = $1 AND calendar_month = $2
                GROUP BY segment_code
                """,
                int(year), int(month),
            )

            vahan_by_seg: dict[str, int] = {
                r["segment_code"]: int(r["total"]) for r in vahan_totals
            }

            for seg, fada_vol in fada_totals.items():
                vahan_vol = vahan_by_seg.get(seg, 0)
                if fada_vol > 0 and vahan_vol > 0:
                    diff_pct = abs(vahan_vol - fada_vol) / fada_vol * 100

                    logger.info(
                        "Reconciliation %s: FADA=%d, VAHAN=%d, diff=%.1f%%",
                        seg, fada_vol, vahan_vol, diff_pct,
                    )

                    if diff_pct > 5:
                        await alerts.alert_reconciliation(
                            segment=seg,
                            fada_total=fada_vol,
                            vahan_total=vahan_vol,
                            diff_pct=diff_pct,
                            period=period,
                        )

        await log_mgr.complete_run(run_id, "SUCCESS", result.records_count)
        logger.info("Monthly FADA reconciliation complete")

    except Exception as e:
        logger.error("Monthly reconciliation failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "FADA", run_id, f"Monthly reconciliation failed: {e}"
        )
    finally:
        await close_db()


async def cmd_wholesale(period: Optional[str] = None) -> None:
    """
    BSE wholesale dispatch extraction.

    Queries BSE filings API for auto OEMs and parses
    monthly dispatch numbers from corporate announcements.
    """
    from src.connectors.bse_wholesale import BSEWholesaleConnector
    from src.utils.extraction_log import ExtractionLogManager

    logger.info("=== BSE Wholesale Extraction ===")
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)

    now = datetime.now(timezone.utc)
    if not period:
        prev_month = (now.replace(day=1) - timedelta(days=1))
        period = prev_month.strftime("%Y-%m")

    run_id = await log_mgr.start_run("BSE_WHOLESALE", f"Wholesale extraction for {period}")

    try:
        async with BSEWholesaleConnector() as connector:
            healthy = await connector.health_check()
            if not healthy:
                logger.warning("BSE website unreachable — skipping")
                await log_mgr.complete_run(run_id, "FAILED", 0, "BSE unreachable")
                return

            result = await connector.extract(period=period)

        if not result.is_success:
            logger.warning("BSE extraction failed: %s", result.error_message)
            await log_mgr.complete_run(run_id, "FAILED", 0, result.error_message)
            return

        logger.info(
            "BSE extraction: %d records from %d OEMs",
            result.records_count,
            result.metadata.get("oems_with_data", 0),
        )

        # Store in bronze
        for record in result.records:
            await db.execute(
                """
                INSERT INTO autoquant.raw_bse_wholesale
                    (period, ticker, oem_name, segment, volume,
                     powertrain, data_type, filing_date, attachment_url, extracted_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
                ON CONFLICT (period, ticker, segment, powertrain)
                DO UPDATE SET volume = EXCLUDED.volume,
                              filing_date = EXCLUDED.filing_date,
                              extracted_at = NOW()
                """,
                period,
                record.get("ticker", ""),
                record.get("oem_name", ""),
                record.get("segment", ""),
                record.get("volume", 0),
                record.get("powertrain", "ALL"),
                record.get("data_type", "wholesale"),
                record.get("filing_date", ""),
                record.get("attachment_url"),
            )

        await log_mgr.complete_run(run_id, "SUCCESS", result.records_count)
        logger.info("BSE wholesale extraction complete: %d records", result.records_count)

    except Exception as e:
        logger.error("BSE wholesale extraction failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "BSE_WHOLESALE", run_id,
            f"BSE wholesale extraction failed: {e}",
        )
    finally:
        await close_db()


async def cmd_historical(
    csv_path: Optional[str] = None,
    csv_dir: Optional[str] = None,
    period: str = "2016-2024",
) -> None:
    """
    SIAM historical backfill from CSV files.

    Loads pre-curated CSVs containing historical vehicle
    registration/production data from SIAM publications.
    """
    from src.connectors.siam_historical import SIAMHistoricalConnector
    from src.transforms.mapper import DimensionMapper
    from src.utils.extraction_log import ExtractionLogManager

    logger.info("=== SIAM Historical Backfill ===")
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)
    mapper = DimensionMapper(db)

    run_id = await log_mgr.start_run("SIAM_HISTORICAL", f"Historical backfill {period}")

    try:
        await mapper.load_all()

        async with SIAMHistoricalConnector() as connector:
            result = await connector.extract(
                period=period,
                csv_path=csv_path,
                csv_dir=csv_dir,
            )

        if not result.is_success:
            logger.warning("SIAM load failed: %s", result.error_message)
            await log_mgr.complete_run(run_id, "FAILED", 0, result.error_message)
            return

        logger.info(
            "SIAM load: %d valid records, date range: %s → %s",
            result.records_count,
            result.metadata.get("date_range", {}).get("min_date", "?"),
            result.metadata.get("date_range", {}).get("max_date", "?"),
        )

        # Map and load records
        loaded_count = 0
        unmapped_makers: set[str] = set()

        for record in result.records:
            oem_id = mapper.resolve_oem(
                record.get("oem_name", ""),
                record.get("segment", ""),
            )

            fuel_id = mapper.resolve_fuel(record.get("fuel_type", ""))
            vc_result = mapper.resolve_vehicle_class(record.get("vehicle_class", ""))

            if oem_id is None:
                unmapped_makers.add(record.get("oem_name", "UNKNOWN"))

            segment_id = vc_result.get("segment_id") if vc_result else None

            await db.execute(
                """
                INSERT INTO autoquant.fact_daily_registrations
                    (data_date, oem_id, segment_id, fuel_id, registrations,
                     source, confidence, extraction_run_id, revision_num)
                VALUES ($1, $2, $3, $4, $5, 'SIAM_HISTORICAL', 'LOW', $6, 1)
                ON CONFLICT (data_date, oem_id, segment_id, fuel_id, revision_num)
                DO NOTHING
                """,
                record["data_date"],
                oem_id or mapper.get_others_oem_id(),
                segment_id,
                fuel_id,
                record["registration_count"],
                run_id,
            )
            loaded_count += 1

        # Refresh materialized view
        await db.execute("SELECT autoquant.refresh_oem_monthly_summary()")
        logger.info("Materialized view refreshed")

        if unmapped_makers:
            logger.warning(
                "Unmapped makers in historical data: %s",
                ", ".join(sorted(unmapped_makers)[:20]),
            )

        await log_mgr.complete_run(run_id, "SUCCESS", loaded_count)
        logger.info("Historical backfill complete: %d records loaded", loaded_count)

    except Exception as e:
        logger.error("Historical backfill failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "SIAM_HISTORICAL", run_id,
            f"Historical backfill failed: {e}",
        )
    finally:
        await close_db()


async def cmd_backfill(
    csv_path: Optional[str] = None,
    csv_dir: Optional[str] = None,
    period: str = "2016-2025",
    generate_sample: bool = False,
    skip_gold: bool = False,
) -> None:
    """
    Full historical backfill with gold-layer computation.

    Enhanced version of cmd_historical that:
      - Supports sample data generation for dev/testing
      - Runs in batches with progress tracking
      - Computes est_quarterly_revenue after loading
      - Provides comprehensive per-OEM/per-year stats

    Usage:
        python main.py backfill --generate-sample --period 2016-2025
        python main.py backfill --csv-dir /data/historical/ --period 2016-2024
    """
    from src.transforms.backfill import BackfillOrchestrator
    from src.utils.extraction_log import ExtractionLogManager

    logger.info("=== Full Historical Backfill ===")
    db = await get_db()
    alerts = await get_alert_manager()
    log_mgr = ExtractionLogManager(db)

    run_id = await log_mgr.start_run("BACKFILL", f"Full backfill {period}")

    try:
        orchestrator = BackfillOrchestrator(db, batch_size=5000)
        summary = await orchestrator.run_backfill(
            csv_path=csv_path,
            csv_dir=csv_dir,
            period=period,
            generate_sample=generate_sample,
            skip_gold=skip_gold,
        )

        if summary["status"] == "success":
            await log_mgr.complete_run(
                run_id, "SUCCESS", summary["silver_loaded"],
                f"Backfill {period}: {summary['silver_loaded']} loaded, "
                f"{summary['silver_skipped_dup']} duplicates skipped"
            )

            # Print summary
            logger.info("── Backfill Summary ──")
            logger.info("  Period: %s", period)
            logger.info("  Silver loaded: %d", summary["silver_loaded"])
            logger.info("  Duplicates skipped: %d", summary["silver_skipped_dup"])
            logger.info("  Duration: %.1fs", summary.get("duration_seconds", 0))

            if summary.get("year_stats"):
                logger.info("  Per-year breakdown:")
                for year in sorted(summary["year_stats"]):
                    ys = summary["year_stats"][year]
                    logger.info("    %d: %d records, %d total volume",
                                year, ys["records"], ys["volume"])

            if summary.get("unmapped_makers"):
                logger.warning("  Unmapped makers: %s",
                               ", ".join(summary["unmapped_makers"][:20]))

            gold = summary.get("gold_summary")
            if gold and gold.get("status") == "success":
                logger.info("  Gold-layer: %d OEM-quarter rows, ₹%.0f Cr est. total",
                            gold.get("oem_quarter_rows", 0),
                            gold.get("total_est_rev_cr", 0))

        else:
            await log_mgr.complete_run(run_id, "FAILED", 0,
                                       summary.get("error", "Unknown error"))

    except Exception as e:
        logger.error("Backfill failed: %s", e, exc_info=True)
        await log_mgr.complete_run(run_id, "FAILED", 0, str(e))
        await alerts.alert_etl_run(
            AlertSeverity.ERROR, "BACKFILL", run_id, f"Backfill failed: {e}"
        )
    finally:
        await close_db()


async def cmd_gold_refresh(
    fy_from: Optional[str] = None,
    fy_to: Optional[str] = None,
) -> None:
    """
    Refresh the gold-layer est_quarterly_revenue table.

    Recomputes demand-based implied revenue proxy from the
    materialized view + ASP assumptions.

    Usage:
        python main.py gold-refresh                     # All quarters
        python main.py gold-refresh --fy-from FY20      # From FY20 onwards
        python main.py gold-refresh --fy-from FY26 --fy-to FY26  # Single FY
    """
    from src.transforms.gold_refresh import GoldLayerRefresh

    logger.info("=== Gold-Layer Refresh ===")
    db = await get_db()

    try:
        gold = GoldLayerRefresh(db)
        summary = await gold.refresh_all_quarters(
            fy_from=fy_from,
            fy_to=fy_to,
        )

        logger.info("Gold refresh result: %s", summary)

    except Exception as e:
        logger.error("Gold refresh failed: %s", e, exc_info=True)
    finally:
        await close_db()


async def cmd_migrate(seed: bool = False, reset: bool = False) -> None:
    """Run database migrations."""
    from src.utils.migrate import run_migrations
    await run_migrations(seed=seed, reset=reset)


def main() -> None:
    global logger
    setup_logging()
    logger = get_logger("autoquant.main")

    parser = argparse.ArgumentParser(description="AutoQuant ETL Orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("health", help="Run health checks")

    daily_cmd = sub.add_parser("daily", help="Run daily VAHAN extraction")
    daily_cmd.add_argument("--dry-run", action="store_true", help="Skip actual extraction")

    sub.add_parser("weekly", help="Run weekly backfill")

    monthly_cmd = sub.add_parser("monthly", help="Run monthly FADA reconciliation")
    monthly_cmd.add_argument("--pdf-path", help="Path to FADA PDF file")
    monthly_cmd.add_argument("--pdf-url", help="URL to download FADA PDF")

    wholesale_cmd = sub.add_parser("wholesale", help="Run BSE wholesale extraction")
    wholesale_cmd.add_argument("--period", help="Period in YYYY-MM format (default: prev month)")

    hist_cmd = sub.add_parser("historical", help="Run SIAM historical backfill")
    hist_cmd.add_argument("--csv-path", help="Path to CSV file")
    hist_cmd.add_argument("--csv-dir", help="Path to directory of CSV files")
    hist_cmd.add_argument("--period", default="2016-2024", help="Period filter (default: 2016-2024)")

    backfill_cmd = sub.add_parser("backfill", help="Full historical backfill with gold-layer")
    backfill_cmd.add_argument("--csv-path", help="Path to CSV file")
    backfill_cmd.add_argument("--csv-dir", help="Path to directory of CSV files")
    backfill_cmd.add_argument("--period", default="2016-2025", help="Period filter (default: 2016-2025)")
    backfill_cmd.add_argument("--generate-sample", action="store_true",
                              help="Generate sample data instead of loading CSV")
    backfill_cmd.add_argument("--skip-gold", action="store_true",
                              help="Skip gold-layer revenue computation")

    gold_cmd = sub.add_parser("gold-refresh", help="Refresh gold-layer revenue estimates")
    gold_cmd.add_argument("--fy-from", help="Starting FY (e.g. FY17)")
    gold_cmd.add_argument("--fy-to", help="Ending FY (e.g. FY26)")

    migrate_cmd = sub.add_parser("migrate", help="Run database migrations")
    migrate_cmd.add_argument("--seed", action="store_true")
    migrate_cmd.add_argument("--reset", action="store_true")

    sub.add_parser("seed", help="Seed dimension tables")

    args = parser.parse_args()

    if args.command == "health":
        asyncio.run(cmd_health())
    elif args.command == "daily":
        asyncio.run(cmd_daily(dry_run=getattr(args, "dry_run", False)))
    elif args.command == "weekly":
        asyncio.run(cmd_weekly())
    elif args.command == "monthly":
        asyncio.run(cmd_monthly(
            pdf_path=getattr(args, "pdf_path", None),
            pdf_url=getattr(args, "pdf_url", None),
        ))
    elif args.command == "wholesale":
        asyncio.run(cmd_wholesale(period=getattr(args, "period", None)))
    elif args.command == "historical":
        asyncio.run(cmd_historical(
            csv_path=getattr(args, "csv_path", None),
            csv_dir=getattr(args, "csv_dir", None),
            period=getattr(args, "period", "2016-2024"),
        ))
    elif args.command == "backfill":
        asyncio.run(cmd_backfill(
            csv_path=getattr(args, "csv_path", None),
            csv_dir=getattr(args, "csv_dir", None),
            period=getattr(args, "period", "2016-2025"),
            generate_sample=getattr(args, "generate_sample", False),
            skip_gold=getattr(args, "skip_gold", False),
        ))
    elif args.command == "gold-refresh":
        asyncio.run(cmd_gold_refresh(
            fy_from=getattr(args, "fy_from", None),
            fy_to=getattr(args, "fy_to", None),
        ))
    elif args.command == "migrate":
        asyncio.run(cmd_migrate(seed=args.seed, reset=args.reset))
    elif args.command == "seed":
        asyncio.run(cmd_migrate(seed=True))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
