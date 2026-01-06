"""
Rate Limiter Module.
Handles 429 rate-limit backoff with per-wallet isolation.

Per Grok Round 7: Added per-wallet rate limiting to prevent multi-wallet
burst hitting global limits.

Per Grok Round 20 CRITICAL FIX: Global backoff on any 429 was halting ALL wallets.
With per-key limits (3500/10s burst per Polymarket docs), multi-wallet should isolate.
One bad wallet was freezing entire bot → missed arbs during live events.
Fix: Backoff is now per-wallet only; global only for non-key-specific errors (5xx).
"""

import logging
import asyncio
import time
import random
from typing import Optional, Dict
from dataclasses import dataclass, field

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
    Global rate limiter for server-side errors (5xx) only.

    Per Grok Round 20: This is now for SERVER errors (503, 5xx) only.
    429 errors are handled per-wallet in PerWalletRateLimiter since
    Polymarket limits are per-API-key (3500/10s burst).

    Global backoff only triggers on:
    - 503 Service Unavailable (server overload)
    - 5xx errors (server-side issues)
    - Cloudflare blocks (520-529)

    NOT for 429 (per-key rate limit) - use per-wallet limiter instead.
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


@dataclass
class WalletRateLimitState:
    """
    Per Grok Round 7: Track rate limit state per wallet.

    Multi-wallet execution can burst-hit global limits if all wallets
    fire simultaneously. This tracks per-wallet request timing to
    stagger requests and avoid 429s.
    """
    wallet_address: str
    last_request_time: float = 0.0
    requests_in_window: int = 0
    window_start: float = 0.0
    consecutive_429s: int = 0


class PerWalletRateLimiter:
    """
    Per Grok Round 7: Per-wallet rate limiting for multi-wallet execution.

    Prevents multiple wallets from firing simultaneously and hitting
    global API limits. Each wallet gets its own bucket with configurable
    requests per second.

    Per Grok Round 8: Added random jitter (0.05-0.2s) to prevent synchronized
    bursts from hitting Cloudflare/API limits.
    """

    def __init__(
        self,
        requests_per_second: float = 2.0,
        window_size_seconds: float = 1.0,
        min_delay_between_requests: float = 0.1,
        jitter_min: float = 0.05,
        jitter_max: float = 0.2
    ):
        """
        Initialize per-wallet rate limiter.

        Args:
            requests_per_second: Max requests per wallet per second.
            window_size_seconds: Rolling window size for counting requests.
            min_delay_between_requests: Minimum delay between any two requests.
            jitter_min: Minimum random jitter in seconds (Per Grok Round 8).
            jitter_max: Maximum random jitter in seconds (Per Grok Round 8).
        """
        self.requests_per_second = requests_per_second
        self.window_size_seconds = window_size_seconds
        self.min_delay_between_requests = min_delay_between_requests
        self.jitter_min = jitter_min
        self.jitter_max = jitter_max

        self._wallet_states: Dict[str, WalletRateLimitState] = {}
        self._lock = asyncio.Lock()
        self._global_last_request = 0.0  # Global timing to stagger across wallets

    async def acquire(self, wallet_address: str) -> float:
        """
        Acquire permission to make a request for a wallet.

        Waits if necessary to stay within rate limits.

        Args:
            wallet_address: The wallet making the request.

        Returns:
            Time waited in seconds (0 if no wait needed).
        """
        async with self._lock:
            now = time.time()
            wait_time = 0.0

            # Get or create wallet state
            if wallet_address not in self._wallet_states:
                self._wallet_states[wallet_address] = WalletRateLimitState(
                    wallet_address=wallet_address,
                    window_start=now
                )

            state = self._wallet_states[wallet_address]

            # Reset window if expired
            if now - state.window_start >= self.window_size_seconds:
                state.window_start = now
                state.requests_in_window = 0

            # Check per-wallet limit
            max_requests = int(self.requests_per_second * self.window_size_seconds)
            if state.requests_in_window >= max_requests:
                # Wait until window resets
                wait_time = state.window_start + self.window_size_seconds - now
                if wait_time > 0:
                    logger.debug(
                        f"Per-wallet limit: {wallet_address[:8]}... waiting {wait_time:.2f}s"
                    )

            # Check minimum delay since last request (global staggering)
            time_since_global = now - self._global_last_request
            if time_since_global < self.min_delay_between_requests:
                global_wait = self.min_delay_between_requests - time_since_global
                wait_time = max(wait_time, global_wait)

            # Check minimum delay since wallet's last request
            time_since_wallet = now - state.last_request_time
            if time_since_wallet < self.min_delay_between_requests:
                wallet_wait = self.min_delay_between_requests - time_since_wallet
                wait_time = max(wait_time, wallet_wait)

        # Per Grok Round 8: Add random jitter to prevent synchronized bursts
        # Multiple wallets or reconnects can synchronize their request timing,
        # causing burst patterns that trigger Cloudflare/API rate limits.
        # Random jitter (0.05-0.2s default) breaks this synchronization.
        jitter = random.uniform(self.jitter_min, self.jitter_max)
        wait_time += jitter

        # Wait outside lock if needed
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        # Update state after wait
        async with self._lock:
            now = time.time()
            state = self._wallet_states[wallet_address]
            state.last_request_time = now
            state.requests_in_window += 1
            self._global_last_request = now

        return wait_time

    async def record_429(self, wallet_address: str):
        """
        Record a 429 for a specific wallet.

        Args:
            wallet_address: The wallet that got rate limited.
        """
        async with self._lock:
            if wallet_address in self._wallet_states:
                self._wallet_states[wallet_address].consecutive_429s += 1
                logger.warning(
                    f"Wallet {wallet_address[:8]}... got 429, "
                    f"consecutive: {self._wallet_states[wallet_address].consecutive_429s}"
                )

    async def record_success(self, wallet_address: str):
        """
        Record a successful request for a wallet.

        Args:
            wallet_address: The wallet that succeeded.
        """
        async with self._lock:
            if wallet_address in self._wallet_states:
                self._wallet_states[wallet_address].consecutive_429s = 0

    def get_stats(self) -> Dict[str, dict]:
        """Get per-wallet rate limiter statistics."""
        return {
            addr: {
                "requests_in_window": state.requests_in_window,
                "consecutive_429s": state.consecutive_429s,
                "last_request": state.last_request_time,
            }
            for addr, state in self._wallet_states.items()
        }


# Global singleton instance
_global_limiter: Optional[GlobalRateLimiter] = None
_wallet_limiter: Optional[PerWalletRateLimiter] = None


def get_global_limiter() -> GlobalRateLimiter:
    """Get or create the global rate limiter instance."""
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = GlobalRateLimiter()
    return _global_limiter


def get_wallet_limiter() -> PerWalletRateLimiter:
    """
    Per Grok Round 7: Get or create the per-wallet rate limiter instance.

    Use this in multi-wallet execution to prevent burst-hitting API limits.
    """
    global _wallet_limiter
    if _wallet_limiter is None:
        _wallet_limiter = PerWalletRateLimiter()
    return _wallet_limiter


async def handle_response_status(
    status: int,
    endpoint: str = "",
    wallet_address: Optional[str] = None
) -> bool:
    """
    Handle HTTP response status with rate limit detection.

    Per Grok Round 20: 429s route to per-wallet limiter (per-API-key limits).
    Server errors (503, 5xx) route to global limiter.

    Args:
        status: HTTP status code.
        endpoint: Endpoint name for logging.
        wallet_address: If provided, 429s apply to this wallet only (per-key isolation).

    Returns:
        True if should retry, False otherwise.

    Raises:
        GeoBlockError: If 451/403 indicates geographic restriction.
    """
    global_limiter = get_global_limiter()

    # Per Grok Round 17: Handle geoblocking (451/403)
    # Polymarket blocks US/restricted IPs - fatal error, no retry
    if status == 451:
        logger.critical(
            f"451 GEOBLOCKED: Polymarket restricts access from this region! "
            f"Endpoint: {endpoint}. Move VPS to allowed region (EU/Asia)."
        )
        raise GeoBlockError(f"451 Unavailable For Legal Reasons on {endpoint}")

    if status == 403:
        logger.error(
            f"403 FORBIDDEN on {endpoint} - may be geoblocking or auth issue. "
            f"Check IP region and API credentials."
        )
        # Don't raise - could be auth issue, let caller handle

    # Per Grok Round 20: Route 429 to per-wallet limiter (per-API-key isolation)
    # Polymarket limits are per-API-key (3500/10s burst), not global
    # One wallet hitting limit should NOT freeze other wallets
    if status == 429:
        if wallet_address:
            wallet_limiter = get_wallet_limiter()
            await wallet_limiter.record_429(wallet_address)
            logger.warning(
                f"429 on {endpoint} for wallet {wallet_address[:10]}... - per-wallet backoff only"
            )
            # Wait using wallet-specific delay (not global)
            await wallet_limiter.acquire(wallet_address)
        else:
            # No wallet specified - fall back to conservative global backoff
            logger.warning(f"429 on {endpoint} - no wallet specified, using global backoff")
            await global_limiter.record_429(endpoint)
            await global_limiter.wait_if_limited()
        return True  # Should retry

    # Per Grok audit: Server-side errors → global backoff (server is overloaded)
    # 503 = Service unavailable
    # 520-529 = Cloudflare/proxy errors
    if status == 503 or (520 <= status <= 529):
        logger.warning(f"Server error {status} on {endpoint} - global backoff (server overload)")
        await global_limiter.record_429(endpoint)
        await global_limiter.wait_if_limited()
        return True  # Should retry

    if status == 200:
        await global_limiter.record_success()
        if wallet_address:
            wallet_limiter = get_wallet_limiter()
            await wallet_limiter.record_success(wallet_address)

    return False  # No retry needed


class GeoBlockError(Exception):
    """Raised when Polymarket returns 451 (geographic restriction)."""
    pass


def is_rate_limit_error(exception: Exception) -> bool:
    """
    Check if an exception indicates a rate limit.

    Per Grok audit Round 3: py-clob-client raises custom exceptions with
    varying messages. Added patterns for allowance/approval failures that
    may indicate transient issues.

    Args:
        exception: The exception to check.

    Returns:
        True if this looks like a rate limit error.
    """
    error_str = str(exception).lower()
    rate_limit_patterns = [
        '429', 'rate limit', 'too many', 'throttl',
        'exceeded', 'slow down', 'quota', 'limit exceeded',
        # Per Grok Round 3: py-clob-client specific patterns
        'invalid allowance',  # Can occur during high-frequency trading
        'nonce too low',      # Often from burst requests
        'replacement transaction',  # Gas price race condition
        'already known',      # Duplicate tx submission
    ]
    return any(pattern in error_str for pattern in rate_limit_patterns)


def is_approval_error(exception: Exception) -> bool:
    """
    Check if an exception indicates an approval/allowance failure.

    Per Grok Round 3: py-clob-client raises on insufficient approvals.
    These should trigger re-approval flow, not just retry.

    Args:
        exception: The exception to check.

    Returns:
        True if this is an approval-related error.
    """
    error_str = str(exception).lower()
    approval_patterns = [
        'allowance', 'approval', 'not approved',
        'insufficient allowance', 'erc20: insufficient allowance',
        'transfer amount exceeds allowance'
    ]
    return any(pattern in error_str for pattern in approval_patterns)
