from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union, Optional, Dict, TypeVar, Generic, Any, Type, AsyncGenerator, List

 
T = TypeVar("T", bound="OHLCV_History")
class OHLCV_History(ABC, Generic[T]):
    def __init__(self, exchange: str, product: str, granularity: int):
        self._exchange = exchange
        self._product = product
        self._granularity = granularity

    @abstractmethod
    async def __aenter__(self):
        """Async context manager entry: Create session."""
        return self
    
    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Close session properly."""
        pass

    @abstractmethod
    async def fetch_timeframe(
        self,
        start_time: datetime,
        end_time: Optional[datetime]) -> AsyncGenerator[List[int | float], None]:
        pass
    
    @abstractmethod
    async def fetch(
        self,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        default_start_date: str) -> AsyncGenerator[List[int | float], None]:
        pass

    @staticmethod
    async def fetch_many(cls: Type[T], products: Dict[str, Dict]) -> AsyncGenerator[Dict[str, List[int | float]], None]:
        """
        Fetches OHLCV data for multiple products asynchronously.
        This method allows selecting which subclass of OHLCV_History should be used.

        Args:
            cls (Type[T]): Subclass of OHLCV_History (e.g., CoinbaseOHLCV, BinanceOHLCV).
            products (Dict[str, Dict]): Dictionary with products and fetch parameters:
            {
                "BTC-USD": {
                    "start_date": datetime | str | None,  # If None, starts from the earliest record
                    "end_date": datetime | str | None,  # If None, fetches until now
                    "granularity": int | None  # If None, defaults to the subclass's default granularity
                },
                "ETH-USD": { ... }, ...
            }
         Yields:
            Dict[str, Any]: The fetched OHLCV data per product (untill finished).
        """

        for product, params in products.items():
            start_date, end_date, granularity = [params.get(key, None) for key in ["start_date", "end_date", "granularity"]]
            async with cls(product, granularity) as instance:   
                async for ohlcv_list in instance.fetch(start_date, end_date):
                    yield {"product" : product, "data" : ohlcv_list}
       
