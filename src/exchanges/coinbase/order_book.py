import utils.exchange as exchange
from typing import List, Dict, AsyncGenerator, Optional, Union
from datetime import datetime, timezone
from coinbase.websocket import WSClient
import asyncio

class OrderBook(exchange.OrderBook):
    def __init__(self, products: List[str], frequency: float | int, depth: int):
        """
        Base class for an order book.
        
        Args:
            product (str): The trading pair (e.g., "BTC-USD").
            granularity (float): Time interval (seconds) between snapshots.
            depth (int): Number of order book levels to maintain.
        """
        self._products = products
        self._frequency = frequency
        self._depth = depth

      
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

 
    async def snapshots(self, untill: Optional[Union[str, datetime]]) -> AsyncGenerator[Dict, None]:
        """
        Yields snapshots of the top N bids and asks for multiple products.

        Args:
            until (Optional[Union[str, datetime]]): Timestamp or ISO format string indicating when to stop.

        Yields:
            dict: Order book snapshot for all tracked products.
        """
        until_time = None
        if until:
            until_time = datetime.fromisoformat(until) if isinstance(until, str) else until

        while until_time is None or datetime.now(timezone.utc) < until_time:
            await asyncio.sleep(self._frequency)  # ✅ Wait before collecting next snapshot

            yield {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "products": {
                    product: self._best_prices[product] for product in self._products
                },
            }