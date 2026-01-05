"""Cancel all open orders."""

import asyncio
from config import load_config
from auth import AuthManager

def main():
    config = load_config()

    # Override dry_run for this script
    config._dry_run = False

    auth = AuthManager(config)
    client = auth.initialize()

    print("Fetching open orders...")

    try:
        # Get all open orders
        open_orders = client.get_orders()

        if not open_orders:
            print("No open orders found.")
            return

        print(f"Found {len(open_orders)} open order(s)")

        for order in open_orders:
            order_id = order.get("id") or order.get("order_id") or order.get("orderID")
            if order_id:
                print(f"  - Order: {order_id}")

        # Cancel all orders
        print("\nCancelling all orders...")
        result = client.cancel_all()
        print(f"Cancel result: {result}")

        # Verify cancellation
        remaining = client.get_orders()
        if remaining:
            print(f"\nWarning: {len(remaining)} orders still open")
        else:
            print("\nAll orders cancelled successfully!")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
