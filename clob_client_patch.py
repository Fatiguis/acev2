"""
Rate limiter integration for py-clob-client.

DEPRECATED: The previous monkey-patch approach was fragile and could break on library updates.
This module now provides a wrapper-based approach instead.

Use the ClobClientWrapper class or the rate limiter decorators for safe rate limiting.
The monkey-patch function is kept for backwards compatibility but logs a deprecation warning.
"""

import logging
import asyncio
from functools import wraps
from typing import Callable, TypeVar, Any

logger = logging.getLogger(__name__)

# Track if deprecation warning has been shown
_deprecation_warned = False

T = TypeVar('T')


def rate_limited_sync(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to add rate limiting to synchronous ClobClient methods.

    Use this to wrap individual client method calls instead of monkey-patching.

    Example:
        @rate_limited_sync
        def my_order_function(client, *args):
            return client.post_order(*args)
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        from rate_limiter import get_global_limiter
        import time

        limiter = get_global_limiter()
        state = limiter._state

        # Wait if rate limited
        if state.is_limited:
            now = time.time()
            if now < state.backoff_until:
                wait_time = state.backoff_until - now
                logger.debug(f"Rate limited, waiting {wait_time:.1f}s before {func.__name__}")
                time.sleep(wait_time)
            state.is_limited = False

        try:
            result = func(*args, **kwargs)
            # Success - clear consecutive 429s
            if state.consecutive_429s > 0:
                logger.debug(f"Rate limit cleared after {state.consecutive_429s} 429s")
            state.consecutive_429s = 0
            state.is_limited = False
            return result

        except Exception as e:
            # Check if it's a 429 error
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str or 'too many' in error_str:
                state.consecutive_429s += 1
                state.total_429s += 1
                state.is_limited = True

                backoff = min(
                    limiter.base_backoff * (limiter.backoff_multiplier ** (state.consecutive_429s - 1)),
                    limiter.max_backoff
                )
                state.backoff_until = time.time() + backoff

                logger.warning(
                    f"429 RATE LIMITED on {func.__name__}! "
                    f"Consecutive: {state.consecutive_429s}, Backing off {backoff:.1f}s"
                )
            raise

    return wrapper


async def rate_limited_async(func: Callable[..., T]) -> Callable[..., T]:
    """
    Async decorator to add rate limiting.

    Example:
        @rate_limited_async
        async def my_async_order(client, *args):
            return await client.some_async_method(*args)
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> T:
        from rate_limiter import get_global_limiter

        limiter = get_global_limiter()

        # Wait if rate limited
        if await limiter.wait_if_limited():
            logger.debug(f"Waited for rate limit before {func.__name__}")

        try:
            result = await func(*args, **kwargs)
            await limiter.record_success()
            return result

        except Exception as e:
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str or 'too many' in error_str:
                await limiter.record_429(func.__name__)
            raise

    return wrapper


def patch_clob_client():
    """
    DEPRECATED: Monkey-patching is fragile and can break on library updates.

    This function now only logs a deprecation warning and does nothing.
    Use rate_limited_sync decorator or ClobClientWrapper instead.

    The rate limiting is now handled at the application level in:
    - orderbook.py: Uses get_global_limiter() for HTTP requests
    - execution.py: Wraps order calls with rate limiting
    - main.py: Global rate limit coordination
    """
    global _deprecation_warned

    if not _deprecation_warned:
        logger.info(
            "[CLOB-PATCH] Monkey-patching disabled (fragile). "
            "Rate limiting now handled at application level via rate_limiter module."
        )
        _deprecation_warned = True


def is_patched() -> bool:
    """
    Check if patch has been applied.

    Always returns False now since monkey-patching is disabled.
    """
    return False


class ClobClientWrapper:
    """
    Wrapper around ClobClient that adds rate limiting to all API calls.

    This is the recommended approach instead of monkey-patching.

    Example:
        from py_clob_client.client import ClobClient

        client = ClobClient(...)
        wrapper = ClobClientWrapper(client)

        # Use wrapper for rate-limited calls
        result = wrapper.post_order(order, order_type)
    """

    def __init__(self, client):
        """
        Initialize wrapper with a ClobClient instance.

        Args:
            client: The ClobClient instance to wrap.
        """
        self._client = client
        # Per Grok audit: Added get_balance and get_account_balance to rate limited methods
        self._rate_limited_methods = {
            'post_order', 'cancel_order', 'cancel_orders', 'cancel_all_orders',
            'get_order', 'get_orders', 'get_trades', 'get_last_trade_price',
            'get_book', 'get_books', 'get_midpoint', 'get_midpoints',
            'get_price', 'get_prices', 'get_spread', 'get_spreads',
            'get_balance', 'get_account_balance',  # Per Grok audit: needed for HF funds check
        }

    def __getattr__(self, name: str):
        """
        Proxy attribute access to wrapped client with rate limiting for API methods.
        """
        attr = getattr(self._client, name)

        if name in self._rate_limited_methods and callable(attr):
            return rate_limited_sync(attr)

        return attr

    @property
    def client(self):
        """Get the underlying ClobClient instance."""
        return self._client
