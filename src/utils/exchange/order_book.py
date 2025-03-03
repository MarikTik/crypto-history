from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional, Union, Dict
from datetime import datetime

class OrderBook(ABC):
    def __init__(self, product: str, granularity: float | int, depth: int):
        """
        Base class for an order book.
        
        Args:
            product (str): The trading pair (e.g., "BTC-USD").
            granularity (float): Time interval (seconds) between snapshots.
            depth (int): Number of order book levels to maintain.
        """
        self._product = product
        self._granularity = granularity
        self._depth = depth
        

    @abstractmethod
    async def snapshots(untill: Optional[Union[str, datetime]]) -> AsyncGenerator[Dict, None]:
        """
        Abstract generator for yielding order book snapshots.

        Args:
            until (Optional[Union[str, datetime]]): Timestamp or ISO format string indicating when to stop collecting.

        Yields:
            dict: The current order book snapshot.
        """
        pass