#!/usr/bin/env python3
"""
AutoQuant — Local Daily Runner
================================
Run this from your terminal or VS Code to execute the daily VAHAN extraction.
VAHAN blocks cloud IPs (GitHub Actions), so this must run from a local machine.

Usage:
    python run_daily.py              # Full daily extraction
    python run_daily.py --health     # Health check only (DB + VAHAN reachability)
    python run_daily.py --dry-run    # Run pipeline without actual extraction

Setup (first time only):
    cd autoquant/etl
    pip install -r requirements.txt
"""

import os
import sys
import subprocess
import time
from pathlib import Path

# ── Configuration ──
ETL_DIR = Path(__file__).resolve().parent / "etl"
PYTHON = sys.executable  # Use the same Python that runs this script


def run_command(args: list[str], label: str) -> int:
    """Run a command and print status."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}\n")

    start = time.time()
    result = subprocess.run(
        args,
        cwd=str(ETL_DIR),
        env={**os.environ, "PYTHONPATH": str(ETL_DIR)},
    )
    elapsed = time.time() - start

    status = "OK" if result.returncode == 0 else "FAILED"
    print(f"\n  [{status}] {label} — {elapsed:.1f}s\n")
    return result.returncode


def check_dependencies():
    """Verify required packages are installed."""
    try:
        import asyncpg       # noqa: F401
        import curl_cffi     # noqa: F401
        import bs4           # noqa: F401
        import pydantic_settings  # noqa: F401
        return True
    except ImportError as e:
        print(f"\nMissing dependency: {e}")
        print(f"Run:  pip install -r {ETL_DIR / 'requirements.txt'}")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AutoQuant Local Daily Runner")
    parser.add_argument("--health", action="store_true", help="Run health check only")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no extraction)")
    args = parser.parse_args()

    # Verify .env exists
    env_file = ETL_DIR / ".env"
    if not env_file.exists():
        print(f"ERROR: {env_file} not found. Copy .env.example and configure it.")
        sys.exit(1)

    # Check dependencies
    if not check_dependencies():
        sys.exit(1)

    print("\n" + "="*60)
    print("  AutoQuant — Daily VAHAN Extraction (Local)")
    print("="*60)

    if args.health:
        # Health check: DB + VAHAN connectivity
        rc = run_command([PYTHON, "main.py", "health"], "Health Check")
        sys.exit(rc)

    if args.dry_run:
        rc = run_command([PYTHON, "main.py", "daily", "--dry-run"], "Daily Extraction (Dry Run)")
        sys.exit(rc)

    # Full daily extraction
    # Step 1: Quick health check
    rc = run_command([PYTHON, "main.py", "health"], "Step 1/2 — Health Check")
    if rc != 0:
        print("\nHealth check failed. Fix connection issues before running extraction.")
        sys.exit(rc)

    # Step 2: Run extraction
    rc = run_command([PYTHON, "main.py", "daily"], "Step 2/2 — Daily VAHAN Extraction")

    if rc == 0:
        print("\n" + "="*60)
        print("  ALL DONE — Daily extraction completed successfully!")
        print("  Data pipeline: VAHAN → Bronze → Silver → Gold → Dashboard")
        print("="*60 + "\n")
    else:
        print("\n" + "="*60)
        print("  FAILED — Check logs above for details.")
        print("  Common issues:")
        print("    - VAHAN site temporarily down → retry in 30 min")
        print("    - Network timeout → check internet connection")
        print("    - DB connection error → verify .env credentials")
        print("="*60 + "\n")

    sys.exit(rc)


if __name__ == "__main__":
    main()
