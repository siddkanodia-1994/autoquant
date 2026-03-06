"""
AutoQuant ETL — Configuration via pydantic-settings.
Reads from environment variables / .env file.

IMPORTANT: In pydantic-settings v2, env var lookup uses
`validation_alias` (NOT `alias`). Using `alias` only affects
dict/JSON parsing, not environment variable binding.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class DatabaseSettings(BaseSettings):
    """PostgreSQL connection settings — uses individual params to avoid
    URL-parsing issues with special characters in passwords."""
    host: str = Field("db.jqxsgnvdnfyyeenqyqzs.supabase.co", validation_alias="DB_HOST")
    port: int = Field(5432, validation_alias="DB_PORT")
    user: str = Field("postgres.jqxsgnvdnfyyeenqyqzs", validation_alias="DB_USER")
    password: str = Field("", validation_alias="DB_PASSWORD")
    name: str = Field("postgres", validation_alias="DB_NAME")
    schema_name: str = Field("autoquant", validation_alias="DB_SCHEMA")
    pool_min_size: int = 2
    pool_max_size: int = 10
    statement_timeout_ms: int = 30_000

    model_config = {"env_prefix": "", "extra": "ignore"}


class TelegramSettings(BaseSettings):
    """Telegram bot alert settings."""
    bot_token: str = Field("", validation_alias="TELEGRAM_BOT_TOKEN")
    chat_id: str = Field("", validation_alias="TELEGRAM_CHAT_ID")
    enabled: bool = True

    @property
    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    model_config = {"env_prefix": "", "extra": "ignore"}


class ScrapingSettings(BaseSettings):
    """VAHAN scraper configuration."""
    vahan_base_url: str = Field(
        "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml",
        validation_alias="VAHAN_BASE_URL",
    )
    request_delay_seconds: float = Field(4.0, validation_alias="REQUEST_DELAY_SECONDS")
    headless: bool = Field(True, validation_alias="HEADLESS_BROWSER")
    page_timeout_ms: int = 60_000
    max_retries: int = 3
    retry_delay_seconds: float = 10.0

    model_config = {"env_prefix": "", "extra": "ignore"}


class VercelSettings(BaseSettings):
    """Vercel ISR revalidation webhook."""
    revalidation_url: str = Field("", validation_alias="VERCEL_REVALIDATION_URL")
    revalidation_secret: str = Field("", validation_alias="VERCEL_REVALIDATION_SECRET")

    @property
    def is_configured(self) -> bool:
        return bool(self.revalidation_url and self.revalidation_secret)

    model_config = {"env_prefix": "", "extra": "ignore"}


class Settings(BaseSettings):
    """Root settings — composes all sub-settings."""
    environment: str = Field("development", validation_alias="ENVIRONMENT")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    db: DatabaseSettings = DatabaseSettings()  # type: ignore[call-arg]
    telegram: TelegramSettings = TelegramSettings()  # type: ignore[call-arg]
    scraping: ScrapingSettings = ScrapingSettings()
    vercel: VercelSettings = VercelSettings()

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


# Singleton
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]
    return _settings
