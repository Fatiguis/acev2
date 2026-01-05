"""
Monkey-patch py-clob-client's internal httpx calls with global rate limiter.

This hooks into py_clob_client.http_helpers.helpers.request() to:
1. Wait if globally rate limited before making requests
2. Record 429 responses to trigger global backoff
3. Record success to clear consecutive 429 counter

Per audit: This catches the rare edge case where py-clob-client's internal
HTTP calls (post_order, get_order, etc.) hit 429s that bypass our async code.
"""

import logging
import time
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)

# Track if patch has been applied
_patched = False


def _sync_wait_if_limited() -> bool:
    """
    Synchronous version of rate limit wait.

    py-clob-client uses sync httpx, so we need sync wait.
    """
    from rate_limiter import get_global_limiter

    limiter = get_global_limiter()
    state = limiter._state

    if not state.is_limited:
        return False

    now = time.time()
    if now >= state.backoff_until:
        state.is_limited = False
        return False

    wait_time = state.backoff_until - now
    logger.debug(f"[CLOB-PATCH] Rate limited, waiting {wait_time:.1f}s...")
    time.sleep(wait_time)
    return True


def _sync_record_429(endpoint: str = "") -> float:
    """
    Synchronous version of 429 recording.
    """
    from rate_limiter import get_global_limiter

    limiter = get_global_limiter()
    state = limiter._state

    state.consecutive_429s += 1
    state.total_429s += 1
    state.is_limited = True

    backoff = min(
        limiter.base_backoff * (limiter.backoff_multiplier ** (state.consecutive_429s - 1)),
        limiter.max_backoff
    )
    state.backoff_until = time.time() + backoff

    logger.warning(
        f"[CLOB-PATCH] 429 RATE LIMITED on {endpoint}! "
        f"Consecutive: {state.consecutive_429s}, Backing off {backoff:.1f}s"
    )

    return backoff


def _sync_record_success():
    """Synchronous version of success recording."""
    from rate_limiter import get_global_limiter

    limiter = get_global_limiter()
    state = limiter._state

    if state.consecutive_429s > 0:
        logger.debug(f"[CLOB-PATCH] Rate limit cleared after {state.consecutive_429s} 429s")
    state.consecutive_429s = 0
    state.is_limited = False


def patch_clob_client():
    """
    Apply monkey-patch to py-clob-client HTTP helpers.

    This wraps the request() function to integrate with our global rate limiter.
    Safe to call multiple times (will only patch once).
    """
    global _patched

    if _patched:
        logger.debug("[CLOB-PATCH] Already patched, skipping")
        return

    try:
        from py_clob_client.http_helpers import helpers
        from py_clob_client.exceptions import PolyApiException

        # Save original request function
        original_request = helpers.request

        @wraps(original_request)
        def patched_request(endpoint: str, method: str, headers=None, data=None):
            """
            Wrapped request function with rate limit integration.
            """
            # Wait if globally rate limited
            _sync_wait_if_limited()

            try:
                result = original_request(endpoint, method, headers, data)
                # Success - clear consecutive 429s
                _sync_record_success()
                return result

            except PolyApiException as e:
                # Check if it's a 429
                if hasattr(e, 'response') and e.response is not None:
                    if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                        _sync_record_429(endpoint)
                        # Wait and retry once
                        _sync_wait_if_limited()
                        try:
                            result = original_request(endpoint, method, headers, data)
                            _sync_record_success()
                            return result
                        except Exception:
                            pass  # Fall through to re-raise original
                raise

        # Apply patch
        helpers.request = patched_request
        _patched = True

        logger.info("[CLOB-PATCH] Successfully patched py-clob-client HTTP helpers with global rate limiter")

    except ImportError as e:
        logger.warning(f"[CLOB-PATCH] Could not patch py-clob-client: {e}")
    except Exception as e:
        logger.error(f"[CLOB-PATCH] Failed to patch py-clob-client: {e}")


def is_patched() -> bool:
    """Check if patch has been applied."""
    return _patched
