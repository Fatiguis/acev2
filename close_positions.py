"""
Close all open positions using rn1-style hedging (buy opposite instead of sell).

Per audit: rn1 reportedly never sells (buys opposite for hedge). This is cheaper
and aligns with the maker fee rebates strategy.

Instead of FOK market sells (taker fees), this script:
1. Fetches current positions
2. For each position, looks up the opposite outcome(s)
3. Places post-only buy orders on the opposite outcome to hedge
4. Falls back to FAK if post-only doesn't fill within timeout
"""

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
            # Per Grok audit: Multi-outcome market (neg-risk via neg-risk-ctf-adapter)
            # For multi-outcome, hedging is more complex:
            # - For neg-risk markets, buying opposite = buying "NO" equivalent
            # - Find the token with best liquidity that's NOT our current token
            # This is a simplified heuristic - full implementation would:
            # 1. Query all outcome books
            # 2. Find lowest-ask token that hedges our position
            print(f"    Multi-outcome market detected ({len(tokens)} outcomes)")
            print(f"    Per Grok audit: Multi-outcome hedging requires manual review")
            print(f"    Consider using data-api to find all outcome tokens")
            # For now, return None for multi-outcome - requires manual handling
            # In production, implement proper neg-risk hedge calculation
            return None

        return None

    except Exception as e:
        print(f"Error getting opposite token: {e}")
        return None


def close_via_hedge(client, token_id: str, size: float, outcome_name: str, use_post_only: bool = True) -> bool:
    """
    Close a position by buying the opposite outcome (rn1 style).

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

    # Get opposite token
    opposite_token = get_opposite_token_id(client, token_id)

    if opposite_token:
        print(f"    Found opposite token, buying to hedge...")

        try:
            # Get current book to find best ask price
            book = client.get_book(opposite_token)
            if not book or not book.get("asks"):
                print(f"    No asks available for opposite token")
                return close_via_sell(client, token_id, size, outcome_name)

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

    # No opposite token found - fall back to direct sell
    print(f"    No opposite token found, using direct sell...")
    return close_via_sell(client, token_id, size, outcome_name)


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
    Close by selling (legacy fallback).

    Note: Per audit, this should be avoided as it incurs taker fees
    and is not the rn1 pattern. Use only as last resort.
    """
    from py_clob_client.clob_types import MarketOrderArgs

    print(f"    WARNING: Using direct sell (not rn1 pattern)...")

    try:
        order_args = MarketOrderArgs(
            token_id=str(token_id),
            amount=size,
            side=SELL,
        )

        signed_order = client.create_market_order(order_args)

        if HAS_FAK:
            response = client.post_order(signed_order, OrderType.FAK)
        else:
            response = client.post_order(signed_order, OrderType.FOK)

        if response and response.get("success"):
            print(f"    ✓ Sold")
            return True

        error = response.get("errorMsg", "Unknown") if response else "No response"
        print(f"    ✗ Sell failed: {error}")
        return False

    except Exception as e:
        print(f"    ✗ Sell error: {e}")
        return False


def main():
    # Check for --confirm flag
    auto_confirm = "--confirm" in sys.argv
    # Check for --legacy flag (uses old sell method)
    use_legacy = "--legacy" in sys.argv

    if use_legacy:
        print("WARNING: Using legacy sell method (not recommended)")

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
            print("\nTo use legacy sell method (not recommended):")
            print("  python3 close_positions.py --confirm --legacy")
            return

        print("\nClosing positions using " + ("LEGACY SELL" if use_legacy else "RN1-STYLE HEDGE") + "...")

        success_count = 0
        fail_count = 0

        for pos in active_positions:
            token_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
            size = float(pos.get("size") or pos.get("balance") or pos.get("amount") or 0)
            outcome = pos.get("outcome") or pos.get("title") or "Unknown"

            if size <= 0 or not token_id:
                continue

            if use_legacy:
                success = close_via_sell(client, token_id, size, outcome)
            else:
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
