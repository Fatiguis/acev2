"""
Global Rate Limiter Module.
Handles 429 rate-limit backoff across all HTTP clients.
"""

import logging
import asyncio
import time
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RateLimitState:
    """Track rate limit state globally."""
    is_limited: bool = False
    backoff_until: float = 0.0
    consecutive_429s: int = 0
    total_429s: int = 0


class GlobalRateLimiter:
    """
    Global rate limiter with exponential backoff for 429 responses.

    Shared across all HTTP clients (orderbook, market_discovery, execution).
    When any client hits 429, all clients back off.
    """

    def __init__(
        self,
        base_backoff: float = 2.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0
    ):
        """
        Initialize the rate limiter.

        Args:
            base_backoff: Base backoff time in seconds.
            max_backoff: Maximum backoff time in seconds.
            backoff_multiplier: Multiplier for exponential backoff.
        """
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.backoff_multiplier = backoff_multiplier

        self._state = RateLimitState()
        self._lock = asyncio.Lock()

    async def wait_if_limited(self) -> bool:
        """
        Wait if currently rate limited.

        Returns:
            True if had to wait, False if not limited.
        """
        async with self._lock:
            if not self._state.is_limited:
                return False

            now = time.time()
            if now >= self._state.backoff_until:
                # Backoff period expired
                self._state.is_limited = False
                logger.info("Rate limit backoff expired, resuming requests")
                return False

            wait_time = self._state.backoff_until - now
            logger.debug(f"Rate limited, waiting {wait_time:.1f}s...")

        # Wait outside the lock
        await asyncio.sleep(wait_time)
        return True

    async def record_429(self, endpoint: str = "") -> float:
        """
        Record a 429 response and calculate backoff.

        Args:
            endpoint: The endpoint that returned 429 (for logging).

        Returns:
            Backoff time in seconds.
        """
        async with self._lock:
            self._state.consecutive_429s += 1
            self._state.total_429s += 1
            self._state.is_limited = True

            # Exponential backoff
            backoff = min(
                self.base_backoff * (self.backoff_multiplier ** (self._state.consecutive_429s - 1)),
                self.max_backoff
            )
            self._state.backoff_until = time.time() + backoff

            logger.warning(
                f"429 RATE LIMITED on {endpoint}! "
                f"Consecutive: {self._state.consecutive_429s}, "
                f"Backing off {backoff:.1f}s"
            )

            return backoff

    async def record_success(self):
        """Record a successful request, reset consecutive counter."""
        async with self._lock:
            if self._state.consecutive_429s > 0:
                logger.debug(f"Rate limit cleared after {self._state.consecutive_429s} 429s")
            self._state.consecutive_429s = 0
            self._state.is_limited = False

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        return {
            "is_limited": self._state.is_limited,
            "consecutive_429s": self._state.consecutive_429s,
            "total_429s": self._state.total_429s,
            "backoff_remaining": max(0, self._state.backoff_until - time.time())
        }

    @property
    def is_limited(self) -> bool:
        """Check if currently rate limited."""
        return self._state.is_limited and time.time() < self._state.backoff_until


# Global singleton instance
_global_limiter: Optional[GlobalRateLimiter] = None


def get_global_limiter() -> GlobalRateLimiter:
    """Get or create the global rate limiter instance."""
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = GlobalRateLimiter()
    return _global_limiter


async def handle_response_status(status: int, endpoint: str = "") -> bool:
    """
    Handle HTTP response status with rate limit detection.

    Per Grok audit: Handle non-429 rate limit responses (some APIs return 503/5xx).

    Args:
        status: HTTP status code.
        endpoint: Endpoint name for logging.

    Returns:
        True if should retry, False otherwise.
    """
    limiter = get_global_limiter()

    # Per Grok audit: Handle various rate limit status codes
    # 429 = Rate limited (standard)
    # 503 = Service unavailable (often rate limit)
    # 520-529 = Cloudflare/proxy rate limits
    if status == 429 or status == 503 or (520 <= status <= 529):
        await limiter.record_429(endpoint)
        await limiter.wait_if_limited()
        return True  # Should retry

    if status == 200:
        await limiter.record_success()

    return False  # No retry needed


def is_rate_limit_error(exception: Exception) -> bool:
    """
    Check if an exception indicates a rate limit.

    Per Grok audit: py-clob-client exceptions vary - check string patterns.

    Args:
        exception: The exception to check.

    Returns:
        True if this looks like a rate limit error.
    """
    error_str = str(exception).lower()
    rate_limit_patterns = [
        '429', 'rate limit', 'too many', 'throttl',
        'exceeded', 'slow down', 'quota', 'limit exceeded'
    ]
    return any(pattern in error_str for pattern in rate_limit_patterns)
