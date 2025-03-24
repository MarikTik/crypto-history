"""
ohlcv_history.py

Defines the abstract base class `OHLCV_History` for fetching OHLCV (Open, High, Low, Close, Volume)
data from cryptocurrency exchanges. This class provides an interface and common structure for all
exchange-specific OHLCV implementations.

Classes:
    - OHLCV_History (ABC): Abstract base class for time-series candle data retrieval.

Intended Usage:
    Subclass this interface to implement exchange-specific behavior such as fetching data
    from APIs (e.g., Coinbase, Binance, Kraken). The fetch_timeframe and fetch methods must be
    overridden to define how candles are pulled and streamed.

Includes:
    - Logging support via `logger_manager`
    - Granularity settings
    - Support for async context management

Example:
    class BinanceOHLCV(OHLCV_History):
        ...
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Union,
    Optional,
    Dict,
    TypeVar,
    Generic,
    Type,
    AsyncGenerator,
    List,
    Literal,
)
from pathlib import Path
from loggers import logger_manager

T = TypeVar("T", bound="OHLCV_History")


class OHLCV_History(ABC, Generic[T]):
    """Abstract base class for retrieving OHLCV (Open, High, Low, Close, Volume) data.

    This class defines the interface and common attributes for exchange-specific
    OHLCV fetchers. Subclasses must implement async context management, as well as
    methods for fetching OHLCV data over timeframes and historical ranges.

    Attributes:
        _product (str): The product or trading pair to fetch (e.g., 'BTC-USD').
        _granularity (int): The granularity of the candles in seconds (e.g., 60 for 1 minute).
        _logger (Logger): The logger instance for recording fetch activity and errors.

    Methods:
        __aenter__(): Asynchronous context manager entry to set up resources.
        __aexit__(): Asynchronous context manager exit to clean up resources.
        fetch_timeframe(): Fetch a single time chunk of OHLCV data.
        fetch(): Sequentially fetch data from a range of timestamps.
        fetch_many(): Class method to fetch OHLCV data for multiple products in parallel.
    """

    def __init__(self, product: str, granularity: int, log_dir: Path):
        self._product = product
        self._granularity = granularity
        self._logger = logger_manager.get_logger(log_dir)

    @abstractmethod
    async def __aenter__(self) -> "OHLCV_History":
        """Async context manager entry: Create session."""
        return self

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Close session properly."""
        pass

    @abstractmethod
    async def fetch_timeframe(
        self, start_time: datetime, end_time: Optional[datetime] = None
    ) -> Union[
        List[List[int | float]],
        Literal[
            "not_found",
            "api_failure",
            "no_data",
            "timeout_error",
            "rate_limited",
            "server_error",
            "network_error",
        ],
    ]:
        """

        Fetches a specific time range of cryptocurrency candle data from Coinbase API.
        Args:

            start_time (datetime): The starting point for fetching data.
            end_time (datetime or None): The ending point for fetching data. Assigned start_time + MAX_CANDLES (minutes) if None provided.

        Returns:
            Union[List[List[int | float]], Literal(str)]: list containing fetched OHLCV data (timestamp: int, open, low, high, close, volume).
            str: `"not_found"` if the coin pair wasn't found in database (404 error).
                 `"api_failure"` if the response status was not 200 or returned invalid JSON.
                 `"no_data"` if the response was successful but no candle data present in it.
                 `"timeout_error"` if the request took longer than TIMEOUT seconds.
                 `"rate_limited"` if the request was blocked due to API rate limits (429).
                 `"server_error"` if Coinbase returns a 5xx server error.
                 `"network_error"` if a netwrok error occurred.
        """
        pass

    @abstractmethod
    async def fetch(
        self,
        start_date: Optional[Union[str, datetime]],
        end_date: Optional[Union[str, datetime]],
        default_start_date: str,
    ) -> AsyncGenerator[List[int | float], None]:
        """Fetches OHLCV data in sequential time chunks.

        This method is expected to yield historical and/or live OHLCV candle data
        between the specified time range.

        Args:
            start_date (Union[str, datetime] or None): The starting date or datetime.
                If None, defaults to `default_start_date`.
            end_date (Union[str, datetime] or None): The ending date or datetime.
                If None, defaults to the current UTC time.
            default_start_date (str): Fallback ISO-format string to use when `start_date` is None.

        Yields:
            List[int | float]: A batch of OHLCV data rows, where each row is a list
            representing [timestamp, open, low, high, close, volume].

        Raises:
            This method should be implemented by a subclass and will raise
            NotImplementedError if not overridden.
        """
        pass

    @classmethod
    async def fetch_many(
        cls: "Type[T]", products: Dict[str, Dict]
    ) -> AsyncGenerator[Dict[str, List[int | float]], None]:
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
            start_date, end_date, granularity = [
                params.get(key, None)
                for key in ["start_date", "end_date", "granularity"]
            ]
            async with cls(product, granularity) as instance:
                async for ohlcv_list in instance.fetch(start_date, end_date):
                    yield {"product": product, "data": ohlcv_list}
