"""
Rate limiter integration for py-clob-client.

DEPRECATED: The previous monkey-patch approach was fragile and could break on library updates.
This module now provides a wrapper-based approach instead.

Use the ClobClientWrapper class or the rate limiter decorators for safe rate limiting.
The monkey-patch function is kept for backwards compatibility but logs a deprecation warning.

Per Grok Round 6: py-clob-client is synchronous (requests-based). Direct calls in async
contexts block the event loop, reducing HF fill probability. Use AsyncClobClientWrapper
or run_sync_in_thread() for non-blocking execution.
"""

import logging
import asyncio
from functools import wraps
from typing import Callable, TypeVar, Any
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Track if deprecation warning has been shown
_deprecation_warned = False

T = TypeVar('T')

# Per Grok Round 6: Thread pool for running sync py-clob-client calls without blocking
# Size 4 balances parallelism with API rate limits
_executor: ThreadPoolExecutor = None


def _get_executor() -> ThreadPoolExecutor:
    """Get or create the shared thread pool executor."""
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="clob_")
    return _executor


async def run_sync_in_thread(func: Callable[..., T], *args, **kwargs) -> T:
    """
    Run a synchronous function in a thread pool without blocking the event loop.

    Per Grok Round 6: py-clob-client is synchronous (GitHub: requests-based).
    Direct calls block asyncio event loop, delaying WS updates and reducing
    fill probability in <1s rn1-style edges.

    This wrapper runs sync calls in a thread pool, allowing the event loop
    to continue processing WebSocket updates and other async tasks.

    Args:
        func: Synchronous function to call.
        *args: Positional arguments for func.
        **kwargs: Keyword arguments for func.

    Returns:
        Result of func(*args, **kwargs).

    Example:
        # Instead of blocking:
        result = client.post_order(order, order_type)

        # Use non-blocking:
        result = await run_sync_in_thread(client.post_order, order, order_type)
    """
    loop = asyncio.get_running_loop()
    executor = _get_executor()

    # Run sync function in thread pool
    return await loop.run_in_executor(
        executor,
        lambda: func(*args, **kwargs)
    )


def rate_limited_sync(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to add rate limiting to synchronous ClobClient methods.

    Per Grok Round 5: Uses time.sleep() which blocks.
    This is acceptable for:
    - CLI scripts (cancel_orders.py, close_positions.py)
    - Initialization code (runs before event loop)
    - ClobClientWrapper methods (py-clob-client is sync anyway)

    For pure async contexts, wrap in run_in_executor or use rate_limited_async.

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

        # Wait if rate limited (blocking - see docstring for appropriate use)
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
    NOTE: This wrapper is SYNCHRONOUS - use AsyncClobClientWrapper for async contexts.

    Example:
        from py_clob_client.client import ClobClient

        client = ClobClient(...)
        wrapper = ClobClientWrapper(client)

        # Use wrapper for rate-limited calls (BLOCKING)
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


class AsyncClobClientWrapper:
    """
    Async wrapper around ClobClient that runs sync calls in a thread pool.

    Per Grok Round 6: py-clob-client is synchronous (requests-based). Direct calls
    in async contexts block the event loop, delaying WS updates and reducing fill
    probability in <1s rn1-style edges.

    This wrapper runs all API calls in a thread pool via asyncio.to_thread,
    allowing the event loop to continue processing while waiting for API responses.

    Example:
        from py_clob_client.client import ClobClient

        client = ClobClient(...)
        async_wrapper = AsyncClobClientWrapper(client)

        # Use wrapper for NON-BLOCKING async calls
        result = await async_wrapper.post_order(order, order_type)
    """

    def __init__(self, client):
        """
        Initialize async wrapper with a ClobClient instance.

        Args:
            client: The ClobClient instance to wrap.
        """
        self._client = client
        self._rate_limited_methods = {
            'post_order', 'cancel_order', 'cancel_orders', 'cancel_all_orders',
            'get_order', 'get_orders', 'get_trades', 'get_last_trade_price',
            'get_book', 'get_books', 'get_midpoint', 'get_midpoints',
            'get_price', 'get_prices', 'get_spread', 'get_spreads',
            'get_balance', 'get_account_balance',
            'create_order', 'create_market_order',  # Order building
        }

    def __getattr__(self, name: str):
        """
        Proxy attribute access with async wrapping for API methods.
        """
        attr = getattr(self._client, name)

        if name in self._rate_limited_methods and callable(attr):
            # Return an async wrapper function
            async def async_method(*args, **kwargs):
                from rate_limiter import get_global_limiter
                import time

                limiter = get_global_limiter()

                # Check rate limit before call
                if await limiter.wait_if_limited():
                    logger.debug(f"Waited for rate limit before async {name}")

                try:
                    # Run sync method in thread pool
                    result = await run_sync_in_thread(attr, *args, **kwargs)
                    await limiter.record_success()
                    return result

                except Exception as e:
                    error_str = str(e).lower()
                    if '429' in error_str or 'rate limit' in error_str or 'too many' in error_str:
                        await limiter.record_429(name)
                    raise

            return async_method

        return attr

    @property
    def client(self):
        """Get the underlying ClobClient instance."""
        return self._client

    def get_sync_wrapper(self) -> ClobClientWrapper:
        """Get a synchronous wrapper for CLI/initialization contexts."""
        return ClobClientWrapper(self._client)
