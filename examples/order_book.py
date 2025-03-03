from coinbase.websocket import WSClient, WebsocketResponse
import asyncio
import json
import os
import heapq
from collections import defaultdict
from datetime import datetime, timezone

# Store order book data
order_book = {
    "bids": [],
    "asks": []
}

# Dictionary to maintain best 50 prices
best_prices = defaultdict(lambda: {"bids": [], "asks": []})

# Global timestamp for throttling updates
last_update_time = None

def update_order_book(product_id, side, price, quantity):
    """
    Updates the order book with new bid/ask prices, keeping only the top 50 levels.
    """
    book_side = "bids" if side == "bid" else "asks"
    
    # Convert price & quantity to float
    price, quantity = float(price), float(quantity)

    # Remove old price level if quantity is 0
    if quantity == 0:
        best_prices[product_id][book_side] = [
            (p, q) for p, q in best_prices[product_id][book_side] if p != price
        ]
    else:
        # Update or add new price level
        updated = False
        for i, (p, q) in enumerate(best_prices[product_id][book_side]):
            if p == price:
                best_prices[product_id][book_side][i] = (price, quantity)
                updated = True
                break
        if not updated:
            best_prices[product_id][book_side].append((price, quantity))

    # Keep only top 50 prices sorted correctly
    if book_side == "bids":
        best_prices[product_id][book_side] = sorted(
            best_prices[product_id][book_side], key=lambda x: -x[0]
        )[:50]  # Highest prices first
    else:
        best_prices[product_id][book_side] = sorted(
            best_prices[product_id][book_side], key=lambda x: x[0]
        )[:50]  # Lowest prices first

def on_message(msg):
    """
    Handles WebSocket messages and updates the order book.
    """
    global last_update_time

    try:
        data = json.loads(msg)

        if data["channel"] != "l2_data":
            return  # Ignore non-order book messages

        for event in data["events"]:
            if event["type"] == "snapshot" or event["type"] == "update":
                product_id = event["product_id"]
                for update in event["updates"]:
                    update_order_book(
                        product_id, update["side"], update["price_level"], update["new_quantity"]
                    )

        # Throttle updates to once every 30 seconds
        current_time = datetime.now(timezone.utc)
        if last_update_time is None or (current_time - last_update_time).total_seconds() >= 30:
            print(f"\n📊 Order Book for {product_id} (Top 50 levels)")
            print("📉 Best Bids:")
            for price, quantity in best_prices[product_id]["bids"]:
                print(f"  {price} -> {quantity}")

            print("\n📈 Best Asks:")
            for price, quantity in best_prices[product_id]["asks"]:
                print(f"  {price} -> {quantity}")

            last_update_time = current_time

    except Exception as e:
        print(f"Error processing message: {e}")

async def main():
    client = WSClient(on_message=on_message)
    client.open()
    await asyncio.sleep(1)  # Ensure connection is established before subscribing

    # Subscribe to Level2 order book for BTC-USD
    client.level2(product_ids=["BTC-USD"])
    
    try:
        client.run_forever_with_exception_check()
    except KeyboardInterrupt:
        print("Closing WebSocket...")
        client.level2_unsubscribe(product_ids="BTC-USD")
        client.close()

asyncio.run(main())
