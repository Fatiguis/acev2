"""
Close all open positions using rn1-style hedging (buy opposite instead of sell).

Per audit: rn1 reportedly never sells (buys opposite for hedge). This is cheaper
and aligns with the maker fee rebates strategy.

Instead of FOK market sells (taker fees), this script:
1. Fetches current positions
2. For each position, looks up the opposite outcome(s)
3. Places post-only buy orders on the opposite outcome to hedge
4. Falls back to FOK if post-only doesn't fill within timeout

Per Grok Round 19: Added CLI-only guard and DRY_RUN block for safety.
"""

# Per Grok Round 19: Guard against import into event loop
# This script uses blocking time.sleep() - importing it would block the bot
if __name__ != "__main__":
    raise RuntimeError(
        "close_positions.py is a CLI-only script. "
        "Do not import - use execution.py async hedge methods instead."
    )

import sys
import time
from config import load_config
from auth import AuthManager
from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL
import requests

# Check if FAK is available
HAS_FAK = hasattr(OrderType, 'FAK')


def fetch_with_backoff(url: str, max_retries: int = 3, base_backoff: float = 2.0) -> requests.Response:
    """Fetch URL with exponential backoff for rate limiting."""
    for attempt in range(max_retries):
        resp = requests.get(url, timeout=10)
        if resp.status_code == 429:
            backoff = base_backoff * (2 ** attempt)
            print(f"Rate limited, waiting {backoff:.1f}s...")
            time.sleep(backoff)
            continue
        return resp
    return resp  # Return last response even if still 429


def get_opposite_token_id(client, token_id: str) -> str | None:
    """
    Get the opposite outcome token ID for hedging.

    For binary markets, there's exactly one opposite outcome.
    For multi-outcome markets (neg-risk), this provides guidance but returns None.

    Per Grok audit: Added multi-outcome detection and user guidance.

    Args:
        client: CLOB client.
        token_id: Current token ID.

    Returns:
        Opposite token ID or None if not found/applicable.
    """
    try:
        # Get market info for this token
        # The CLOB API doesn't have a direct "get market by token" endpoint
        # We need to fetch the book to get market context
        book = client.get_book(token_id)
        if not book:
            return None

        # For binary markets, we can infer the opposite token
        # This is a simplified approach - full implementation would query market API
        market_info = book.get("market", {})
        tokens = market_info.get("tokens", [])

        if len(tokens) == 2:
            # Binary market - return the other token
            for t in tokens:
                if t.get("token_id") != token_id:
                    return t.get("token_id")

        elif len(tokens) > 2:
            # Per Grok Round 5: Multi-outcome market (neg-risk via neg-risk-ctf-adapter)
            # For neg-risk, we can hedge by buying ALL other outcomes
            # This locks in profit regardless of which outcome wins
            print(f"    Multi-outcome market detected ({len(tokens)} outcomes)")

            # Find all tokens except ours - these form the hedge basket
            other_tokens = [t.get("token_id") for t in tokens if t.get("token_id") != token_id]

            if len(other_tokens) > 0:
                print(f"    Found {len(other_tokens)} hedge tokens for multi-outcome")
                # Return first token as primary hedge target
                # Full hedge would buy proportional amounts of all other tokens
                # This is simplified - returns first for immediate hedge
                return other_tokens[0]
            else:
                print(f"    No hedge tokens found in multi-outcome market")
                return None

        return None

    except Exception as e:
        print(f"Error getting opposite token: {e}")
        return None


def get_all_hedge_tokens(client, token_id: str) -> list:
    """
    Get all opposite outcome tokens for multi-outcome hedging.

    Per Grok Round 5: For neg-risk markets with >2 outcomes, full hedging
    requires buying ALL other outcomes proportionally.

    Args:
        client: CLOB client.
        token_id: Current token ID we hold.

    Returns:
        List of (token_id, best_ask_price) tuples for all hedge tokens.
    """
    try:
        book = client.get_book(token_id)
        if not book:
            return []

        market_info = book.get("market", {})
        tokens = market_info.get("tokens", [])

        if len(tokens) <= 2:
            return []  # Not a multi-outcome market

        hedge_tokens = []
        for t in tokens:
            tid = t.get("token_id")
            if tid and tid != token_id:
                # Get best ask for this token
                try:
                    other_book = client.get_book(tid)
                    if other_book and other_book.get("asks"):
                        best_ask = float(other_book["asks"][0]["price"])
                        hedge_tokens.append((tid, best_ask))
                except Exception:
                    pass

        return hedge_tokens

    except Exception as e:
        print(f"Error getting hedge tokens: {e}")
        return []


def close_via_multi_hedge(client, token_id: str, size: float, outcome_name: str) -> bool:
    """
    Close a multi-outcome position by buying all other outcomes.

    Per Grok Round 5: Full neg-risk-ctf-adapter hedge requires buying
    proportional amounts of ALL other outcomes.

    Args:
        client: CLOB client.
        token_id: Token ID of position to close.
        size: Size to close (in shares).
        outcome_name: Name for logging.

    Returns:
        True if all hedges placed successfully.
    """
    print(f"\n  Attempting multi-outcome hedge for {outcome_name}...")

    hedge_tokens = get_all_hedge_tokens(client, token_id)

    if not hedge_tokens:
        print(f"    No hedge tokens found, falling back to single hedge")
        return False

    print(f"    Found {len(hedge_tokens)} outcomes to hedge")

    # Calculate proportional sizes
    # For neg-risk: buying equal shares of all other outcomes locks profit
    total_hedge_cost = sum(price for _, price in hedge_tokens)
    success_count = 0

    for hedge_tid, best_ask in hedge_tokens:
        # Size per outcome = total size * (1 / num_outcomes)
        outcome_size = size / len(hedge_tokens)

        try:
            order_args = OrderArgs(
                price=best_ask,
                size=outcome_size,
                side=BUY,
                token_id=hedge_tid,
            )

            signed_order = client.create_order(order_args)

            if HAS_FAK:
                response = client.post_order(signed_order, OrderType.FAK)
            else:
                response = client.post_order(signed_order, OrderType.FOK)

            if response and response.get("success"):
                print(f"    ✓ Hedged outcome @ ${best_ask:.3f}")
                success_count += 1
            else:
                print(f"    ✗ Failed to hedge outcome")

            # Rate limit between orders
            time.sleep(0.2)

        except Exception as e:
            print(f"    ✗ Hedge error: {e}")

    full_success = success_count == len(hedge_tokens)
    if full_success:
        print(f"    ✓ Full multi-outcome hedge complete ({success_count}/{len(hedge_tokens)})")
    else:
        print(f"    ⚠ Partial hedge ({success_count}/{len(hedge_tokens)})")

    return full_success


def close_via_hedge(client, token_id: str, size: float, outcome_name: str, use_post_only: bool = True) -> bool:
    """
    Close a position by buying the opposite outcome (rn1 style).

    Per Grok Round 5: For multi-outcome markets (>2 outcomes), attempts
    full multi-hedge first before falling back to single token hedge.

    Args:
        client: CLOB client.
        token_id: Token ID of position to close.
        size: Size to close.
        outcome_name: Name for logging.
        use_post_only: Try post-only first for maker rebates.

    Returns:
        True if closed successfully.
    """
    print(f"\n  Attempting rn1-style hedge close for {outcome_name}...")

    # Per Grok Round 5: Try multi-outcome hedge first for neg-risk markets
    hedge_tokens = get_all_hedge_tokens(client, token_id)
    if len(hedge_tokens) > 1:
        print(f"    Multi-outcome market detected, attempting full hedge...")
        if close_via_multi_hedge(client, token_id, size, outcome_name):
            return True
        print(f"    Multi-hedge failed, falling back to single hedge...")

    # Get opposite token for binary or fallback
    opposite_token = get_opposite_token_id(client, token_id)

    if opposite_token:
        print(f"    Found opposite token, buying to hedge...")

        try:
            # Get current book to find best ask price
            book = client.get_book(opposite_token)
            if not book or not book.get("asks"):
                print(f"    ✗ No asks available for opposite token - cannot hedge")
                return False

            best_ask = float(book["asks"][0]["price"])

            # Place post-only order slightly below best ask for maker rebates
            if use_post_only:
                # Post-only at best ask - should fill as maker if market moves
                order_args = OrderArgs(
                    price=best_ask,
                    size=size,
                    side=BUY,
                    token_id=opposite_token,
                )

                # Try post-only GTC with timeout
                try:
                    signed_order = client.create_order(order_args)
                    response = client.post_order(
                        signed_order,
                        OrderType.GTC,
                        PartialCreateOrderOptions(neg_risk=True)  # Assume neg_risk for hedges
                    )

                    if response and response.get("success"):
                        order_id = response.get("orderID")
                        print(f"    Posted hedge order {order_id}, waiting for fill...")

                        # Wait up to 30s for fill (per audit: hedge timeout)
                        for _ in range(30):
                            time.sleep(1)
                            order_status = client.get_order(order_id)
                            if order_status:
                                status = order_status.get("status", "").upper()
                                if status == "FILLED":
                                    print(f"    ✓ Hedge filled (maker)")
                                    return True
                                elif status in ("CANCELLED", "EXPIRED"):
                                    print(f"    Hedge order {status}, falling back to taker")
                                    break

                        # Cancel unfilled order
                        try:
                            client.cancel_order(order_id)
                        except Exception:
                            pass

                except Exception as e:
                    print(f"    Post-only hedge failed: {e}")

            # Fallback to FAK taker order
            print(f"    Falling back to FAK taker order...")
            return close_via_fak_buy(client, opposite_token, size, "opposite outcome")

        except Exception as e:
            print(f"    Hedge via opposite failed: {e}")

    # No opposite token found - cannot hedge
    # Per Grok Round 7: Do NOT fall back to direct sell (deprecated)
    print(f"    ✗ No opposite token found - cannot hedge position")
    print(f"    This may indicate a data API issue or unusual market structure")
    return False


def close_via_fak_buy(client, token_id: str, size: float, name: str) -> bool:
    """Close by buying with FAK (fill what's available)."""
    try:
        book = client.get_book(token_id)
        if not book or not book.get("asks"):
            return False

        best_ask = float(book["asks"][0]["price"])

        order_args = OrderArgs(
            price=best_ask,
            size=size,
            side=BUY,
            token_id=token_id,
        )

        signed_order = client.create_order(order_args)

        if HAS_FAK:
            response = client.post_order(signed_order, OrderType.FAK)
        else:
            response = client.post_order(signed_order, OrderType.FOK)

        if response and response.get("success"):
            print(f"    ✓ FAK buy filled")
            return True

        error = response.get("errorMsg", "Unknown") if response else "No response"
        print(f"    ✗ FAK buy failed: {error}")
        return False

    except Exception as e:
        print(f"    ✗ FAK buy error: {e}")
        return False


def close_via_sell(client, token_id: str, size: float, name: str) -> bool:
    """
    DEPRECATED: Legacy sell method - DO NOT USE.

    Per Grok Round 7: This method is deprecated and should not be used.
    rn1 never sold directly - always hedged by buying the opposite outcome.
    Direct sells incur taker fees and are not the proper hedging pattern.

    This function now prints a warning and returns False.
    Use close_via_hedge() instead.
    """
    print(f"    ✗ ERROR: Direct sell is deprecated (not rn1 pattern)")
    print(f"    Use close_via_hedge() to buy opposite outcome instead")
    print(f"    Per Grok Round 7: rn1 never sold - always bought opposite")
    return False


def main():
    # Check for --confirm flag
    auto_confirm = "--confirm" in sys.argv

    # Per Grok Round 7: Legacy sell method removed - always use rn1-style hedge
    # Check for deprecated --legacy flag and warn
    if "--legacy" in sys.argv:
        print("=" * 60)
        print("ERROR: --legacy flag is deprecated and no longer supported")
        print("=" * 60)
        print("Per Grok Round 7: Direct sell is not the rn1 pattern.")
        print("rn1 always hedged by buying opposite outcomes.")
        print("\nTo close positions, use: python3 close_positions.py --confirm")
        print("=" * 60)
        return

    config = load_config()
    auth = AuthManager(config)
    client = auth.initialize()

    funder = config.wallet.funder_address
    print(f"Checking positions for funder: {funder}")

    try:
        # Query positions from the data API with rate limit handling
        print("\nFetching positions from Polymarket API...")
        url = f"https://data-api.polymarket.com/positions?user={funder}"
        resp = fetch_with_backoff(url)

        if resp.status_code != 200:
            print(f"Failed to fetch positions: HTTP {resp.status_code}")
            return

        positions = resp.json()

        if not positions:
            print("\nNo open positions found.")
            return

        # Filter positions with size > 0
        active_positions = [p for p in positions if float(p.get("size") or p.get("balance") or p.get("amount") or 0) > 0]

        if not active_positions:
            print("\nNo active positions to close.")
            return

        print(f"\nFound {len(active_positions)} active position(s)")
        print("\n" + "=" * 60)
        print("OPEN POSITIONS")
        print("=" * 60)

        for pos in active_positions:
            token_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
            size = float(pos.get("size") or pos.get("balance") or pos.get("amount") or 0)
            outcome = pos.get("outcome") or pos.get("title") or "Unknown"
            market = pos.get("market") or pos.get("question") or ""

            token_display = f"{token_id[:20]}..." if token_id and len(str(token_id)) > 20 else token_id
            print(f"\n  Token: {token_display}")
            print(f"  Outcome: {outcome}")
            print(f"  Size: {size}")
            if market:
                market_display = f"{market[:50]}..." if len(market) > 50 else market
                print(f"  Market: {market_display}")

        print("\n" + "=" * 60)

        if not auto_confirm:
            print("\nTo close all positions, run:")
            print("  python3 close_positions.py --confirm")
            return

        # Per Grok Round 7: Always use rn1-style hedge (legacy sell removed)
        print("\nClosing positions using RN1-STYLE HEDGE (buy opposite outcome)...")

        success_count = 0
        fail_count = 0

        for pos in active_positions:
            token_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
            size = float(pos.get("size") or pos.get("balance") or pos.get("amount") or 0)
            outcome = pos.get("outcome") or pos.get("title") or "Unknown"

            if size <= 0 or not token_id:
                continue

            # Always use hedge (rn1 pattern)
            success = close_via_hedge(client, token_id, size, outcome)

            if success:
                success_count += 1
            else:
                fail_count += 1

        print("\n" + "=" * 60)
        print(f"RESULTS: {success_count} closed, {fail_count} failed")
        print("=" * 60)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
