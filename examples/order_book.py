from coinbase.websocket import WSClient, WebsocketResponse
from math import ceil
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Union, List, Tuple
from pathlib import Path

product_order_book: Dict[str, Dict] = {}


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MAX_DEPTH = 5

with open("extras/order_book_products.json", "r") as f:
    products = list(json.load(f).keys())


def split_updates(updates: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    for i, update in enumerate(updates):
        if update["side"] == "offer":
            return updates[:i], updates[i:]
    return updates, []  # all bids, no offers


def apply_update(
    side: str, updates: List[Dict], levels: List[Tuple[float, float]]
) -> List[Tuple[float, float]]:
    book = {price: quantity for price, quantity in levels}
    for update in updates:
        price = float(update["price_level"])
        quantity = float(update["new_quantity"])
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity
    sorted_book = sorted(
        book.items(), key=lambda x: -x[0] if side == "bid" else x[0]
    )
    return sorted_book[:MAX_DEPTH]


def to_ts(s: str):
    return int(datetime.fromisoformat(s[:26]).timestamp())


def on_message(msg):
    """
    Handles WebSocket messages and updates the order book.
    """
    try:
        data: Dict[Dict] = json.loads(msg)
        if "channel" not in data or data["channel"] != "l2_data":
            return  # Ignore non-order book messages

        events: List[Union[Dict, str]] = data.get("events", [])

        for event in events:
            event_type = event.get("type")
            if event_type in ("snapshot", "update"):
                product_id = event.get("product_id")
                updates = event.get("updates", [])
                if updates:
                    timestamp = updates[0]["event_time"]
                else:
                    continue

            if event_type == "snapshot":
                product_order_book[product_id] = {}
                bids, asks = [], []
                for update in updates:
                    price = float(update["price_level"])
                    quantity = float(update["new_quantity"])
                    update_type = update["side"]
                    if update_type == "bid" and len(bids) < MAX_DEPTH:
                        bids.append((price, quantity))
                    elif update_type == "offer" and len(asks) < MAX_DEPTH:
                        asks.append((price, quantity))
                    else:
                        break

            # !RULE:
            # Coinbase always sends sorted data, the first fragment of the updates list is the `bid`(s)
            # The second fragment is the `offer`(s)

            elif event.get("type") == "update":
                current = product_order_book.get(
                    product_id, {"timestamp": timestamp, "bids": [], "asks": []}
                )
                old_bids = current["bids"]
                old_asks = current["asks"]
                bid_updates, ask_updates = split_updates(updates)
                bids = apply_update("bid", bid_updates, old_bids)
                asks = apply_update("offer", ask_updates, old_asks)

            product_order_book[product_id] = {
                "timestamp": to_ts(timestamp),
                "bids": bids,
                "asks": asks,
            }

    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")


async def periodic_writer(interval: int = 5):
    print("🟢 periodic_writer started")
    while True:
        await asyncio.sleep(interval)
        if not product_order_book:
            print("⏳ Waiting for order book data...")
            continue

        for product_id, snapshot in product_order_book.items():
            try:
                path = Path("data", "coinbase", "order_book", product_id)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a") as f:
                    json.dump(snapshot, f)
                    f.write("\n")
                print(f"✅ Wrote snapshot for {product_id}")
            except Exception as e:
                print(f"❌ Error writing {product_id}: {e}")


async def main():
    golden_number = 30
    clients_num = ceil(len(products) / golden_number)

    clients = [WSClient(on_message=on_message) for _ in range(clients_num)]

    for client in clients:
        client.open()

    await asyncio.sleep(3)

    product_slice = lambda i: products[
        i * golden_number : (i + 1) * golden_number
    ]
    for i, client in enumerate(clients):
        section = product_slice(i)
        logging.info(f"📡 Subscribing to order books: {section}")
        client.level2(product_ids=section)

    writer_task = asyncio.create_task(periodic_writer(5))
    # Run WebSocket client in a separate task
    loop = asyncio.get_running_loop()
    ws_tasks = [
        loop.run_in_executor(None, client.run_forever_with_exception_check)
        for client in clients
    ]

    try:
        await asyncio.gather(writer_task, *ws_tasks)

    except KeyboardInterrupt:
        logging.warning("❌ Closing WebSocket...")
        for i, client in enumerate(clients):
            client.level2_unsubscribe(product_slice(i))
        client.close()


if __name__ == "__main__":
    asyncio.run(main())


#   "updates": [
#         {
#             "side": "bid",
#             "event_time": "2025-03-21T00:03:58.668950764Z",
#             "price_level": "2.4328",
#             "new_quantity": "1185.523621"
#         },

#         {
#     "side": "offer",
#     "event_time": "2025-03-24T02:34:13.294636653Z",
#     "price_level": "0.0795",
#     "new_quantity": "1833.05"
# }
