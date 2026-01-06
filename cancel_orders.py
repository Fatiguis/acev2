"""
Cancel all open orders.

Per Grok audit fixes:
- Check dry_run flag properly (don't use private _dry_run)
- Handle KeyError on missing order fields
- Add rate limiting between operations

Per Grok Round 5: This is a standalone CLI script (not async integration).
time.sleep is acceptable here as this is not run inside the bot's event loop.
For bot integration, use the async cancel methods in execution.py instead.

Per Grok Round 7 CRITICAL FIX: Removed dangerous dry_run override that could
cancel real orders when DRY_RUN=True in config. Now requires explicit
--confirm flag to execute any real orders.
"""

import sys
import time
from config import load_config
from auth import AuthManager


def fetch_with_rate_limit(func, *args, delay: float = 0.5, **kwargs):
    """
    Execute function with rate limiting.

    Note: Uses time.sleep (blocking) - acceptable for CLI scripts.
    For async contexts, use execution.py's async cancel methods.
    """
    time.sleep(delay)  # Pre-request delay to avoid bursts
    return func(*args, **kwargs)


def main():
    # Per Grok Round 7 CRITICAL FIX: Require explicit --confirm flag
    # This prevents accidental capital loss from running the script
    has_confirm = "--confirm" in sys.argv

    config = load_config()

    # CRITICAL: Block execution if dry_run is set (safety first)
    if config.dry_run:
        print("=" * 60)
        print("BLOCKED: DRY_RUN is enabled in config")
        print("=" * 60)
        print("\nThis script cancels REAL orders on the live exchange.")
        print("Running with DRY_RUN=True is blocked for safety.")
        print("\nTo cancel orders:")
        print("  1. Set DRY_RUN=false in your .env file")
        print("  2. Run: python3 cancel_orders.py --confirm")
        print("=" * 60)
        return

    # Require --confirm flag for safety
    if not has_confirm:
        print("=" * 60)
        print("CANCEL ALL ORDERS")
        print("=" * 60)
        print("\nThis will cancel ALL open orders on your Polymarket account.")
        print("\nTo proceed, run:")
        print("  python3 cancel_orders.py --confirm")
        print("=" * 60)
        return

    auth = AuthManager(config)
    client = auth.initialize()

    print("Fetching open orders...")

    try:
        # Get all open orders with rate limiting
        open_orders = fetch_with_rate_limit(client.get_orders, delay=0.2)

        if not open_orders:
            print("No open orders found.")
            return

        print(f"Found {len(open_orders)} open order(s)")

        for order in open_orders:
            # Safe extraction with fallbacks (handles KeyError/missing fields)
            order_id = None
            for key in ["id", "order_id", "orderID", "orderId"]:
                try:
                    order_id = order.get(key)
                    if order_id:
                        break
                except (KeyError, AttributeError, TypeError):
                    continue

            if order_id:
                print(f"  - Order: {order_id}")
            else:
                print(f"  - Order: (unknown id) - raw: {str(order)[:80]}...")

        # Cancel all orders with rate limiting
        print("\nCancelling all orders...")
        time.sleep(0.5)  # Rate limit before cancel
        result = client.cancel_all()
        print(f"Cancel result: {result}")

        # Verify cancellation with rate limiting
        time.sleep(1.0)  # Wait for cancellation to propagate
        remaining = fetch_with_rate_limit(client.get_orders, delay=0.5)
        if remaining:
            print(f"\nWarning: {len(remaining)} orders still open")
            print("Retrying individual cancellation...")
            for order in remaining:
                order_id = None
                for key in ["id", "order_id", "orderID", "orderId"]:
                    try:
                        order_id = order.get(key)
                        if order_id:
                            break
                    except (KeyError, AttributeError, TypeError):
                        continue
                if order_id:
                    try:
                        time.sleep(0.3)  # Rate limit between cancels
                        client.cancel(order_id)
                        print(f"  Cancelled: {order_id}")
                    except Exception as e:
                        print(f"  Failed to cancel {order_id}: {e}")
        else:
            print("\nAll orders cancelled successfully!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
