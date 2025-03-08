import utils.exchange as exchange
from typing import List, Dict, AsyncGenerator, Optional, Union
from datetime import datetime, timezone
from coinbase.websocket import WSClient
import asyncio
import heapq
import json

class OrderBook(exchange.OrderBook):
    def __init__(self, products: List[str], frequency: float | int, depth: int):
        """
        Base class for an order book.
        
        Args:
            product (str): The trading pair (e.g., "BTC-USD").
            frequency (float): Time interval (seconds) between snapshots.
            depth (int): Number of order book levels to maintain.
        """
        self._products = products
        self._frequency = frequency
        self._depth = depth
        self._client = None
        self._best_prices = {product: {"bids": [], "asks": []} for product in products}   
        self._last_update_time = None   
      
    async def __aenter__(self):
        """Async context manager entry: Initializes WebSocket client."""
        self._client = WSClient(on_message=self._on_message)
        self._client.open()
        await asyncio.sleep(1)  # Ensure connection is established
        self._client.level2(product_ids=self._products)
        return self
   
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Closes WebSocket client."""
        if self._client:
            self._client.level2_unsubscribe(product_ids=self._products) 
            self._client.close()
            self._client = None

 
    async def snapshots(self, until: Optional[Union[str, datetime]] = None) -> AsyncGenerator[Dict, None]:
        """
        Yields snapshots of the top N bids and asks for multiple products.

        Args:
            until (Optional[Union[str, datetime]]): Timestamp or ISO format string indicating when to stop.

        Yields:
            dict: Order book snapshot for all tracked products.
        """
       
        if until:
            until = datetime.fromisoformat(until) if isinstance(until, str) else until

        while until is None or datetime.now(timezone.utc) < until:
            await asyncio.sleep(self._frequency)  # Wait before collecting next snapshot

            yield {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "products": {
                    product: self._best_prices[product] for product in self._products
                },
            }

    def _on_message(self, msg: str):
        """Handles WebSocket messages and updates the order book."""
        try:
            data = json.loads(msg)
            if data["channel"] != "l2_data":
                return  # Ignore non-order book messages
    
            for event in data["events"]:
                if event["type"] in ["snapshot", "update"]:
                    product_id = event["product_id"]
                    if product_id in self._best_prices:
                        for update in event["updates"]:
                            self._update_order_book(
                                product_id, update["side"], update["price_level"], update["new_quantity"]
                            )

        except Exception as e:
            print(f"Error processing message: {e}")

    def _update_order_book(self, product_id: str, side: str, price: str, quantity: str):
        """
        Updates the order book for a specific product.
        Keeps only the top `self._depth` bids & asks.
        """
        #print(f"update: {product_id}\t{side}\t{price}\t{qua}")
        book_side = "bids" if side == "bid" else "asks"
        price, quantity = float(price), float(quantity)

        # Ensure the product exists in _best_prices
        if product_id not in self._best_prices:
            self._best_prices[product_id] = {"bids": [], "asks": []}

        heap = self._best_prices[product_id][book_side]  # ✅ Now heap is guaranteed to exist

        if quantity == 0:
            # ✅ Remove the price level if quantity is zero
            self._best_prices[product_id][book_side] = [(p, q) for p, q in heap if p != price]
        else:
            # ✅ Check if the price already exists in heap and update it
            for i, (p, q) in enumerate(heap):
                if p == price:
                    heap[i] = (price, quantity)  # ✅ Update quantity at the price level
                    break
            else:
                # ✅ Only add new prices when needed
                heapq.heappush(heap, (price, quantity))

            # ✅ Keep only the top `self._depth` levels
            if len(heap) > self._depth:
                heapq.heappop(heap)  # ✅ Ensure the heap size remains within the limit

        # ✅ Ensure proper heap sorting
        if book_side == "bids":
            self._best_prices[product_id][book_side] = heapq.nlargest(self._depth, heap, key=lambda x: x[0])
        else:
            self._best_prices[product_id][book_side] = heapq.nsmallest(self._depth, heap, key=lambda x: x[0])   