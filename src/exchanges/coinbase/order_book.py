"""
order_book.py

This module defines the OrderBook class, which manages real-time order book 
data for multiple cryptocurrency trading pairs using the Coinbase WebSocket API.

Features:
    - Maintains an in-memory order book with the top N bids and asks.
    - Handles real-time updates via WebSocket.
    - Provides an async generator (`snapshots()`) for periodic snapshots.
    - Implements an async context manager for automatic WebSocket management.

Dependencies:
    - utils.exchange
    - coinbase.websocket.WSClient
    - asyncio, heapq, json
"""


import utils.exchange as exchange
from typing import List, Dict, AsyncGenerator, Optional, Union, Tuple
from datetime import datetime, timezone
from coinbase.websocket import WSClient
import asyncio
import heapq
import json

class OrderBook(exchange.OrderBook):
    """
    Manages real-time order book data for multiple cryptocurrency trading pairs 
    using the Coinbase WebSocket API. Keeps track of the top N bid/ask levels.

    Attributes:
        _products (List[str]): List of trading pairs to track (e.g., ["BTC-USD", "ETH-USD"]).
        _frequency (float | int): Time interval (seconds) between snapshots.
        _depth (int): Number of order book levels to maintain.
        _client (Optional[WSClient]): WebSocket client instance for data streaming.
        _best_prices (Dict[str, Dict[str, List[Tuple[float, float]]]]]): 
            Stores the top N bid/ask levels per product.
        _last_update_time (Optional[datetime]): Timestamp of the last snapshot update.
    """

    def __init__(self, products: List[str], frequency: float | int, depth: int):
        """
        Initializes the order book for tracking multiple trading pairs.

        Args:
            products (List[str]): List of trading pairs to track (e.g., ["BTC-USD", "ETH-USD"]).
            frequency (float | int): Time interval (seconds) between snapshots.
            depth (int): Number of order book levels to maintain per side (bids/asks).
        """
        self._products = products
        self._frequency = frequency
        self._depth = depth
        self._client = None
        self._best_prices = {product: {"bids": [], "asks": []} for product in products}   
        self._last_update_time = None   
      
    async def __aenter__(self):
        """
        Async context manager entry. Initializes the WebSocket client and subscribes to Level 2 data.

        Returns:
            OrderBook: The initialized order book instance.
        """
        self._client = WSClient(on_message=self._on_message)
        self._client.open()
        await asyncio.sleep(1)  # Ensure connection is established
        self._client.level2(product_ids=self._products)
        return self
   
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit. Unsubscribes from WebSocket and closes the client.

        Args:
            exc_type (Optional[Type[BaseException]]): Exception type if an error occurred.
            exc_val (Optional[BaseException]): Exception instance if an error occurred.
            exc_tb (Optional[TracebackType]): Traceback information if an error occurred.
        """
        if self._client:
            self._client.level2_unsubscribe(product_ids=self._products) 
            self._client.close()
            self._client = None

 
    async def snapshots(self, until: Optional[Union[str, datetime]] = None) -> AsyncGenerator[Dict[str, Union[str, Dict[str, List[Tuple[float, float]]]]], None]:
        """
        Periodically yields snapshots of the top N bids and asks for tracked products.

        Args:
            until (Optional[Union[str, datetime]]): Timestamp or ISO format string indicating when to stop.
                If None, continues indefinitely.

        Yields:
            Dict[str, Union[str, Dict[str, List[Tuple[float, float]]]]]: 
                Snapshot of the order book with timestamp.
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

    def _on_message(self, message: str):
        """
        Handles WebSocket messages and updates the order book.

        Args:
            message (str): JSON-formatted message received from the WebSocket.
        """
        try:
            data = json.loads(message)
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
        Updates the order book for a specific product by adding, modifying, or removing price levels.

        Args:
            product_id (str): The trading pair being updated (e.g., "BTC-USD").
            side (str): The side of the order book ("bid" or "ask").
            price (str): The price level being updated.
            quantity (str): The new quantity at the given price level.

        Updates:
            - If quantity = 0, removes the price level.
            - If price exists, updates its quantity.
            - If price is new, adds it to the order book.
            - Maintains only the top `self._depth` levels.
        """
        book_side = "bids" if side == "bid" else "asks"
        price, quantity = float(price), float(quantity)
 
        if product_id not in self._best_prices:
            self._best_prices[product_id] = {"bids": [], "asks": []}

        heap = self._best_prices[product_id][book_side]  

        if quantity == 0:
            self._best_prices[product_id][book_side] = [(p, q) for p, q in heap if p != price]
        else:
            for i, (p, _) in enumerate(heap):
                if p == price:
                    heap[i] = (price, quantity) 
                    break
            else:
                heapq.heappush(heap, (price, quantity))

        
            if len(heap) > self._depth:
                heapq.heappop(heap)
 
        if book_side == "bids":
            self._best_prices[product_id][book_side] = heapq.nlargest(self._depth, heap, key=lambda x: x[0])
        else:
            self._best_prices[product_id][book_side] = heapq.nsmallest(self._depth, heap, key=lambda x: x[0])   