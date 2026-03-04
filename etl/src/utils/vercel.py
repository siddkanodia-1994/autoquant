"""
AutoQuant ETL — Vercel ISR Revalidation Trigger.

After a successful ETL run, triggers Vercel's on-demand ISR revalidation
so dashboard pages refresh with new data without waiting for the
revalidate interval.

Includes retry with exponential backoff for transient failures.
"""

import asyncio
import logging
from typing import Optional

import httpx

from config import get_settings

logger = logging.getLogger(__name__)

# Default paths to revalidate after each ETL run
DEFAULT_REVALIDATE_PATHS = [
    "/dashboard",
    "/scorecard",
    "/revenue",
    "/history",
]


async def trigger_revalidation(
    paths: Optional[list[str]] = None,
    max_retries: int = 2,
) -> dict[str, bool]:
    """
    Trigger Vercel ISR revalidation for specified paths.

    Args:
        paths: List of paths to revalidate (e.g., ['/dashboard', '/oem/MARUTI']).
               If None, revalidates all default dashboard pages.
        max_retries: Max retry attempts per path for transient errors.

    Returns:
        Dict mapping path → success boolean.
    """
    settings = get_settings().vercel
    if not settings.is_configured:
        logger.debug("Vercel revalidation not configured, skipping.")
        return {}

    if paths is None:
        paths = DEFAULT_REVALIDATE_PATHS

    results: dict[str, bool] = {}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for path in paths:
            success = await _revalidate_single(
                client, settings, path, max_retries
            )
            results[path] = success

    succeeded = sum(1 for v in results.values() if v)
    total = len(results)
    if succeeded < total:
        logger.warning(
            "Revalidation partial: %d/%d paths succeeded", succeeded, total
        )
    else:
        logger.info("Revalidation complete: %d/%d paths", succeeded, total)

    return results


async def _revalidate_single(
    client: httpx.AsyncClient,
    settings: object,
    path: str,
    max_retries: int,
) -> bool:
    """Revalidate a single path with retry logic."""
    delay = 1.0

    for attempt in range(1, max_retries + 2):
        try:
            response = await client.post(
                settings.revalidation_url,
                json={"path": path},
                headers={
                    "Authorization": f"Bearer {settings.revalidation_secret}",
                },
            )

            if response.status_code == 200:
                logger.info("Revalidated: %s", path)
                return True

            # Auth/config errors — don't retry
            if response.status_code in (401, 403, 404):
                logger.error(
                    "Revalidation auth/config error for %s: HTTP %d",
                    path, response.status_code,
                )
                return False

            # Server errors — retry
            if response.status_code >= 500:
                logger.warning(
                    "Revalidation server error for %s: HTTP %d (attempt %d/%d)",
                    path, response.status_code, attempt, max_retries + 1,
                )
            else:
                logger.warning(
                    "Revalidation unexpected status for %s: HTTP %d",
                    path, response.status_code,
                )
                return False

        except httpx.TimeoutException:
            logger.warning(
                "Revalidation timeout for %s (attempt %d/%d)",
                path, attempt, max_retries + 1,
            )
        except httpx.ConnectError as e:
            logger.warning(
                "Revalidation connection error for %s (attempt %d/%d): %s",
                path, attempt, max_retries + 1, e,
            )
        except Exception as e:
            logger.error("Revalidation error for %s: %s", path, e)
            return False

        # Backoff before retry
        if attempt <= max_retries:
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)

    logger.error("Revalidation failed for %s after %d attempts", path, max_retries + 1)
    return False
