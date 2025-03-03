from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional, Union, Dict, List
from datetime import datetime

class OrderBook(ABC):
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

    @abstractmethod    
    async def __aenter__(self):
        """Async context manager entry: Initializes WebSocket client."""

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Closes WebSocket client."""

    @abstractmethod
    async def snapshots(untill: Optional[Union[str, datetime]]) -> AsyncGenerator[Dict, None]:
        """
        Abstract generator for yielding order book snapshots.

        Args:
            until (Optional[Union[str, datetime]]): Timestamp or ISO format string indicating when to stop collecting. If None given than it will not stop.

        Yields:
            dict: The current order book snapshot.
        """
        pass