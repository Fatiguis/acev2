"""Close all open positions by selling them at market price."""

import sys
from config import load_config
from auth import AuthManager
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import SELL
import requests

def main():
    # Check for --confirm flag
    auto_confirm = "--confirm" in sys.argv

    config = load_config()
    auth = AuthManager(config)
    client = auth.initialize()

    funder = config.wallet.funder_address
    print(f"Checking positions for funder: {funder}")

    try:
        # Query positions from the data API
        print("\nFetching positions from Polymarket API...")
        url = f"https://data-api.polymarket.com/positions?user={funder}"
        resp = requests.get(url, timeout=10)

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

        print("\nClosing positions...")

        success_count = 0
        fail_count = 0

        for pos in active_positions:
            token_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
            size = float(pos.get("size") or pos.get("balance") or pos.get("amount") or 0)
            outcome = pos.get("outcome") or pos.get("title") or "Unknown"

            if size <= 0 or not token_id:
                continue

            try:
                print(f"\n  Selling {size:.4f} of {outcome}...")

                # Create market sell order
                order_args = MarketOrderArgs(
                    token_id=str(token_id),
                    amount=size,
                    side=SELL,
                )

                signed_order = client.create_market_order(order_args)
                response = client.post_order(signed_order, OrderType.FOK)

                if response and response.get("success"):
                    print(f"    ✓ Sold successfully")
                    success_count += 1
                else:
                    error = response.get("errorMsg", "Unknown error") if response else "No response"
                    print(f"    ✗ Failed: {error}")
                    fail_count += 1

            except Exception as e:
                print(f"    ✗ Error: {e}")
                fail_count += 1

        print("\n" + "=" * 60)
        print(f"RESULTS: {success_count} sold, {fail_count} failed")
        print("=" * 60)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
