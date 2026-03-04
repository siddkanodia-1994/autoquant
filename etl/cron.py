"""
AutoQuant ETL — Cron Scheduler.

Schedules all recurring ETL jobs:
  - Daily VAHAN extraction:   6:00 AM IST (00:30 UTC) every day
  - Weekly backfill:          2:00 AM IST (20:30 UTC Sat) every Sunday
  - Monthly reconciliation:   10:00 AM IST (04:30 UTC) on the 5th

For Railway: Use this as the entrypoint.
For GitHub Actions: Use the workflow file in .github/workflows/etl.yml instead.

Usage:
    python cron.py              # Start scheduler (blocks forever)
    python cron.py --run-once daily   # Run one job immediately then exit
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import get_settings
from src.utils.logging_config import setup_logging, get_logger

logger = None


async def run_daily() -> None:
    """Wrapper for daily VAHAN extraction."""
    from main import cmd_daily
    logger.info("CRON: Starting daily VAHAN extraction")
    try:
        await cmd_daily()
    except Exception as e:
        logger.error("CRON: Daily extraction failed: %s", e, exc_info=True)


async def run_weekly() -> None:
    """Wrapper for weekly backfill."""
    from main import cmd_weekly
    logger.info("CRON: Starting weekly backfill")
    try:
        await cmd_weekly()
    except Exception as e:
        logger.error("CRON: Weekly backfill failed: %s", e, exc_info=True)


async def run_monthly() -> None:
    """Wrapper for monthly reconciliation."""
    logger.info("CRON: Monthly reconciliation — not yet implemented (STEP 5)")


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

    # Daily VAHAN extraction — 6:00 AM IST every day
    scheduler.add_job(
        run_daily,
        CronTrigger(hour=6, minute=0, timezone="Asia/Kolkata"),
        id="daily_vahan_extract",
        name="Daily VAHAN Extraction",
        misfire_grace_time=3600,  # 1 hour grace
        max_instances=1,
    )

    # Weekly backfill — 2:00 AM IST every Sunday
    scheduler.add_job(
        run_weekly,
        CronTrigger(day_of_week="sun", hour=2, minute=0, timezone="Asia/Kolkata"),
        id="weekly_backfill",
        name="Weekly Backfill (Last 30 Days)",
        misfire_grace_time=7200,
        max_instances=1,
    )

    # Monthly reconciliation — 10:00 AM IST on the 5th
    scheduler.add_job(
        run_monthly,
        CronTrigger(day=5, hour=10, minute=0, timezone="Asia/Kolkata"),
        id="monthly_reconciliation",
        name="Monthly FADA Reconciliation",
        misfire_grace_time=14400,
        max_instances=1,
    )

    return scheduler


def main() -> None:
    global logger
    setup_logging()
    logger = get_logger("autoquant.cron")

    parser = argparse.ArgumentParser(description="AutoQuant ETL Scheduler")
    parser.add_argument(
        "--run-once",
        choices=["daily", "weekly", "monthly"],
        help="Run a single job immediately and exit",
    )
    args = parser.parse_args()

    if args.run_once:
        job_map = {
            "daily": run_daily,
            "weekly": run_weekly,
            "monthly": run_monthly,
        }
        logger.info("Running %s job once...", args.run_once)
        asyncio.run(job_map[args.run_once]())
        return

    # Start the persistent scheduler
    scheduler = create_scheduler()
    scheduler.start()

    logger.info("AutoQuant ETL Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        logger.info("  [%s] %s — next run: %s", job.id, job.name, job.next_run_time)

    # Keep running until interrupted
    loop = asyncio.new_event_loop()

    def shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping scheduler...")
        scheduler.shutdown(wait=False)
        loop.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_forever()
    finally:
        loop.close()
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
