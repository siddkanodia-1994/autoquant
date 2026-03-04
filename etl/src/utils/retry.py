"""
AutoQuant ETL — Retry & Resilience Utilities.

Provides:
  - Exponential backoff retry decorator for async functions
  - Circuit breaker for external service calls
  - Timeout wrapper for async operations

Usage:
    @retry(max_attempts=3, base_delay=1.0, retryable=(httpx.TimeoutException,))
    async def fetch_data():
        ...

    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
    async with breaker:
        await call_external_api()
"""

import asyncio
import functools
import logging
import time
from typing import Any, Callable, Sequence, Type

logger = logging.getLogger(__name__)

# Default transient error types that justify retry
TRANSIENT_EXCEPTIONS: tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)

# Try to include asyncpg transient errors
try:
    import asyncpg
    TRANSIENT_DB_EXCEPTIONS: tuple[Type[Exception], ...] = (
        asyncpg.PostgresConnectionError,
        asyncpg.InterfaceError,
        asyncpg.InternalServerError,
        asyncpg.ConnectionDoesNotExistError,
    )
except ImportError:
    TRANSIENT_DB_EXCEPTIONS = ()

# Try to include httpx transient errors
try:
    import httpx
    TRANSIENT_HTTP_EXCEPTIONS: tuple[Type[Exception], ...] = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
    )
except ImportError:
    TRANSIENT_HTTP_EXCEPTIONS = ()


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
    retryable: Sequence[Type[Exception]] | None = None,
    on_retry: Callable[..., Any] | None = None,
) -> Callable:
    """
    Async retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts (including first try)
        base_delay: Initial delay in seconds between retries
        max_delay: Maximum delay cap in seconds
        backoff_factor: Multiplier for delay on each retry
        retryable: Tuple of exception types that trigger retry.
                   Defaults to TRANSIENT_EXCEPTIONS.
        on_retry: Optional callback(attempt, exception, delay) called before each retry

    Example:
        @retry(max_attempts=3, base_delay=2.0)
        async def fetch_from_api():
            ...
    """
    if retryable is None:
        retryable_types = TRANSIENT_EXCEPTIONS + TRANSIENT_HTTP_EXCEPTIONS
    else:
        retryable_types = tuple(retryable)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None
            delay = base_delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable_types as e:
                    last_exception = e

                    if attempt >= max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_attempts, e,
                        )
                        raise

                    # Jittered delay
                    jittered_delay = delay * (0.5 + 0.5 * (hash(str(e)) % 100 / 100))
                    jittered_delay = min(jittered_delay, max_delay)

                    logger.warning(
                        "%s attempt %d/%d failed (%s), retrying in %.1fs",
                        func.__name__, attempt, max_attempts,
                        type(e).__name__, jittered_delay,
                    )

                    if on_retry:
                        on_retry(attempt, e, jittered_delay)

                    await asyncio.sleep(jittered_delay)
                    delay = min(delay * backoff_factor, max_delay)

            # Should not reach here, but safety
            if last_exception:
                raise last_exception
            raise RuntimeError(f"{func.__name__}: retry exhausted without exception")

        return wrapper
    return decorator


def retry_db(max_attempts: int = 3, base_delay: float = 0.5) -> Callable:
    """Shorthand retry for database operations."""
    return retry(
        max_attempts=max_attempts,
        base_delay=base_delay,
        retryable=TRANSIENT_EXCEPTIONS + TRANSIENT_DB_EXCEPTIONS,
    )


def retry_http(max_attempts: int = 3, base_delay: float = 1.0) -> Callable:
    """Shorthand retry for HTTP operations."""
    return retry(
        max_attempts=max_attempts,
        base_delay=base_delay,
        retryable=TRANSIENT_EXCEPTIONS + TRANSIENT_HTTP_EXCEPTIONS,
    )


class CircuitBreaker:
    """
    Circuit breaker for external service calls.

    States:
        CLOSED: Normal operation, calls go through
        OPEN: Service is down, calls fail immediately
        HALF_OPEN: Testing recovery, allow one call through

    Usage:
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

        try:
            async with breaker:
                await call_external_api()
        except CircuitOpenError:
            # Service is known to be down
            pass
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        name: str = "default",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name

        self._failure_count = 0
        self._last_failure_time: float = 0
        self._state = "CLOSED"  # CLOSED | OPEN | HALF_OPEN

    @property
    def state(self) -> str:
        if self._state == "OPEN":
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                self._state = "HALF_OPEN"
        return self._state

    def record_success(self) -> None:
        """Record a successful call — reset failure count."""
        self._failure_count = 0
        self._state = "CLOSED"

    def record_failure(self) -> None:
        """Record a failed call — possibly trip the breaker."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._failure_count >= self.failure_threshold:
            self._state = "OPEN"
            logger.warning(
                "Circuit breaker '%s' OPENED after %d failures. "
                "Recovery in %.0fs.",
                self.name, self._failure_count, self.recovery_timeout,
            )

    async def __aenter__(self) -> "CircuitBreaker":
        current = self.state
        if current == "OPEN":
            raise CircuitOpenError(
                f"Circuit breaker '{self.name}' is OPEN. "
                f"Service unavailable."
            )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            self.record_success()
        else:
            self.record_failure()
        return False  # Don't suppress exceptions


class CircuitOpenError(Exception):
    """Raised when circuit breaker is in OPEN state."""
    pass


async def with_timeout(coro: Any, timeout_seconds: float, label: str = "") -> Any:
    """
    Run an async coroutine with a timeout.

    Args:
        coro: The coroutine to run
        timeout_seconds: Maximum seconds to wait
        label: Optional label for error messages

    Raises:
        TimeoutError: If the coroutine doesn't complete in time
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        msg = f"Operation timed out after {timeout_seconds}s"
        if label:
            msg = f"{label}: {msg}"
        logger.error(msg)
        raise TimeoutError(msg) from None
