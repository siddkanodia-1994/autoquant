"""
AutoQuant ETL — Telegram Alert Utility.

Sends structured alerts to a Telegram chat for:
  - ETL run failures
  - Unmapped maker/fuel/vehicle class names
  - Data quality anomalies
  - Validation gate failures
  - Successful run summaries
"""

import asyncio
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import httpx

from config import get_settings
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    @property
    def emoji(self) -> str:
        return {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "critical": "🚨",
        }[self.value]


class TelegramAlertManager:
    """
    Sends formatted alert messages to Telegram.

    Messages are formatted in Markdown for readability.
    Rate-limited to avoid flooding (max 20 messages/minute per Telegram API).
    """

    TELEGRAM_API_BASE = "https://api.telegram.org"

    def __init__(self) -> None:
        self._settings = get_settings().telegram
        self._client: Optional[httpx.AsyncClient] = None
        self._message_count = 0
        self._minute_start = datetime.now(timezone.utc)

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _rate_limit(self) -> None:
        """Basic rate limiter: max 15 messages per minute."""
        now = datetime.now(timezone.utc)
        elapsed = (now - self._minute_start).total_seconds()
        if elapsed > 60:
            self._message_count = 0
            self._minute_start = now

        if self._message_count >= 15:
            wait = 60 - elapsed
            if wait > 0:
                logger.warning("Telegram rate limit hit, waiting %.1fs", wait)
                await asyncio.sleep(wait)
            self._message_count = 0
            self._minute_start = datetime.now(timezone.utc)

    async def send_message(
        self, text: str, parse_mode: str = "Markdown", max_retries: int = 2
    ) -> bool:
        """
        Send a message to the configured Telegram chat.

        Includes retry with exponential backoff for transient errors.
        Distinguishes auth errors (no retry) from network errors (retry).
        Truncates messages > 4000 chars (Telegram limit: 4096).
        """
        if not self._settings.is_configured:
            logger.warning("Telegram not configured, skipping alert")
            return False

        if not self._settings.enabled:
            logger.debug("Telegram alerts disabled, skipping")
            return False

        await self._rate_limit()

        # Truncate to Telegram max (4096 chars, with margin)
        if len(text) > 4000:
            text = text[:3990] + "\n\n_(truncated)_"

        delay = 1.0
        for attempt in range(1, max_retries + 2):
            try:
                client = await self._get_client()
                url = f"{self.TELEGRAM_API_BASE}/bot{self._settings.bot_token}/sendMessage"
                payload = {
                    "chat_id": self._settings.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                }
                response = await client.post(url, json=payload)

                # Auth errors — don't retry
                if response.status_code in (401, 403):
                    logger.error(
                        "Telegram auth error (HTTP %d). Check bot token.",
                        response.status_code,
                    )
                    return False

                # Rate limit — back off and retry
                if response.status_code == 429:
                    retry_after = float(
                        response.headers.get("Retry-After", str(delay * 2))
                    )
                    logger.warning("Telegram rate limit (429), waiting %.1fs", retry_after)
                    await asyncio.sleep(retry_after)
                    continue

                response.raise_for_status()
                self._message_count += 1
                return True

            except httpx.TimeoutException:
                logger.warning(
                    "Telegram timeout (attempt %d/%d)", attempt, max_retries + 1
                )
            except httpx.ConnectError as e:
                logger.warning(
                    "Telegram connection error (attempt %d/%d): %s",
                    attempt, max_retries + 1, e,
                )
            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt <= max_retries:
                    logger.warning("Telegram server error %d, retrying", e.response.status_code)
                else:
                    logger.error("Telegram HTTP error: %s", e)
                    return False
            except Exception as e:
                logger.error("Telegram send failed: %s (%s)", e, type(e).__name__)
                return False

            if attempt <= max_retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15.0)

        logger.error("Telegram send failed after %d attempts", max_retries + 1)
        return False

    # ── High-Level Alert Methods ──

    async def alert_etl_run(
        self,
        severity: AlertSeverity,
        source: str,
        run_id: int,
        message: str,
        details: Optional[dict] = None,
    ) -> bool:
        """Alert about an ETL run status."""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            f"{severity.emoji} *AutoQuant ETL Alert*",
            f"*Source:* `{source}`",
            f"*Run ID:* `{run_id}`",
            f"*Time:* {timestamp}",
            "",
            f"*Message:* {message}",
        ]
        if details:
            lines.append("")
            lines.append("*Details:*")
            for key, value in details.items():
                lines.append(f"  • {key}: `{value}`")

        return await self.send_message("\n".join(lines))

    async def alert_unmapped_entity(
        self,
        entity_type: str,  # 'maker', 'fuel', 'vehicle_class'
        raw_name: str,
        source: str,
        volume: Optional[int] = None,
    ) -> bool:
        """Alert about an unmapped entity found during extraction."""
        lines = [
            f"⚠️ *Unmapped {entity_type.title()}*",
            f"*Source:* `{source}`",
            f"*Raw Name:* `{raw_name}`",
        ]
        if volume is not None:
            lines.append(f"*Volume:* `{volume:,}` registrations")
        lines.extend([
            "",
            "Action needed: Add mapping to dimension table or staging\\_unmapped table.",
        ])
        return await self.send_message("\n".join(lines))

    async def alert_anomaly(
        self,
        metric: str,
        current_value: float,
        expected_value: float,
        z_score: float,
        date: str,
    ) -> bool:
        """Alert about a statistical anomaly in the data."""
        direction = "above" if current_value > expected_value else "below"
        lines = [
            f"📊 *Data Anomaly Detected*",
            f"*Metric:* {metric}",
            f"*Date:* {date}",
            f"*Value:* `{current_value:,.0f}` ({direction} expected `{expected_value:,.0f}`)",
            f"*Z-Score:* `{z_score:.2f}`",
            "",
            "Review required before data is published to dashboard.",
        ]
        return await self.send_message("\n".join(lines))

    async def alert_validation_failure(
        self,
        check_name: str,
        source: str,
        run_id: int,
        reason: str,
    ) -> bool:
        """Alert about a validation gate failure."""
        lines = [
            f"🛑 *Validation Gate FAILED*",
            f"*Check:* `{check_name}`",
            f"*Source:* `{source}`",
            f"*Run ID:* `{run_id}`",
            "",
            f"*Reason:* {reason}",
            "",
            "Dashboard refresh SKIPPED. Manual review required.",
        ]
        return await self.send_message("\n".join(lines))

    async def alert_reconciliation(
        self,
        segment: str,
        fada_total: int,
        vahan_total: int,
        diff_pct: float,
        period: str,
        *,
        # Legacy aliases
        month: str = "",
        variance_pct: float = 0.0,
    ) -> bool:
        """Alert about VAHAN vs FADA monthly reconciliation variance."""
        display_month = period or month
        pct = diff_pct or variance_pct
        status = "✅ PASS" if abs(pct) <= 5.0 else "❌ FAIL"
        lines = [
            f"📋 *Monthly Reconciliation — {display_month}*",
            f"*Segment:* `{segment}`",
            f"*VAHAN Total:* `{vahan_total:,}`",
            f"*FADA Total:* `{fada_total:,}`",
            f"*Variance:* `{pct:+.1f}%` {status}",
        ]
        if abs(pct) > 5.0:
            lines.extend(["", "Investigation required: variance exceeds ±5% threshold."])

        return await self.send_message("\n".join(lines))

    async def alert_daily_summary(
        self,
        run_id: int,
        records_extracted: int,
        records_loaded: int,
        unmapped_count: int,
        duration_seconds: float,
    ) -> bool:
        """Send a daily run summary."""
        lines = [
            f"✅ *Daily ETL Complete*",
            f"*Run ID:* `{run_id}`",
            f"*Records Extracted:* `{records_extracted:,}`",
            f"*Records Loaded:* `{records_loaded:,}`",
            f"*Unmapped Names:* `{unmapped_count}`",
            f"*Duration:* `{duration_seconds:.1f}s`",
        ]
        if unmapped_count > 0:
            lines.append("\n⚠️ Review unmapped entities in staging tables.")

        return await self.send_message("\n".join(lines))

    async def alert_backfill_summary(
        self,
        period: str,
        records_loaded: int,
        records_skipped: int,
        unmapped_count: int,
        duration_seconds: float,
        gold_status: str = "skipped",
    ) -> bool:
        """Send backfill completion summary."""
        lines = [
            f"📦 *Historical Backfill Complete*",
            f"*Period:* `{period}`",
            f"*Records Loaded:* `{records_loaded:,}`",
            f"*Duplicates Skipped:* `{records_skipped:,}`",
            f"*Unmapped Makers:* `{unmapped_count}`",
            f"*Duration:* `{duration_seconds:.1f}s`",
            f"*Gold Layer:* `{gold_status}`",
        ]
        return await self.send_message("\n".join(lines))

    async def alert_system_health(
        self,
        db_ok: bool,
        telegram_ok: bool,
        table_counts: Optional[dict[str, int]] = None,
    ) -> bool:
        """Send system health check report."""
        db_status = "✅" if db_ok else "❌"
        tg_status = "✅" if telegram_ok else "❌"
        lines = [
            f"🏥 *System Health Check*",
            f"*Database:* {db_status}",
            f"*Telegram:* {tg_status}",
        ]
        if table_counts:
            lines.append("")
            lines.append("*Table Counts:*")
            for table, count in table_counts.items():
                lines.append(f"  • `{table}`: {count:,}")

        return await self.send_message("\n".join(lines))


# Singleton
_alert_manager: Optional[TelegramAlertManager] = None


async def get_alert_manager() -> TelegramAlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = TelegramAlertManager()
    return _alert_manager
