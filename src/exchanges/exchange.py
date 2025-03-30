"""
File: exchange.py

This module defines the `Exchange` class, a base registry for specialized
OHLCV_History and OrderBook implementations.

Subclasses of `Exchange` (e.g., `Coinbase`, `Binance`) override these
class-level attributes with their own implementations:

    class Coinbase(Exchange):
        ohlcv = CoinbaseOHLCV
        order_book = CoinbaseOrderBook

Additionally, exchanges must implement the `fetch_products()` method to retrieve
the current list of actively traded and non-traded products:

    class Coinbase(Exchange):
        @staticmethod
        async def fetch_products():
            # Fetch trading pairs from Coinbase API

    class Kraken(Exchange):
        @staticmethod
        async def fetch_products():
            # Fetch trading pairs from Kraken API

Key Features:
- Stores references to exchange-specific OHLCV and OrderBook classes.
- Requires subclasses to implement `fetch_products()` for retrieving market pairs.
- Provides flexible scheduling of trading pair updates based on timeframes and intervals.
- Maintains sets of currently traded, non-traded, enlisted, and delisted products.

This design allows direct class-level access to exchange-specific methods
without needing an instantiated `Exchange` object.
"""

import asyncio
from typing import Type, Set, List, Tuple, Literal
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from database import *
from loggers import logger_manager, NullLogger
from .ohlcv_history import OHLCV_History
from .order_book import OrderBook


class Exchange(ABC):
    """
    Base class for exchange-specific OHLCV_History and OrderBook implementations.

    Each subclass must define:
    - `ohlcv` and `order_book` with their respective implementations.
    - `fetch_products()` to fetch the list of traded/non-traded products.

    It is not obligatory but recommended to:
    - Override `start_product_updates` method to schedule appropriate update times

    If needed, subclasses can also override database implementations.

    Example:
        class Coinbase(Exchange):
            ohlcv = CoinbaseOHLCV
            order_book = CoinbaseOrderBook

            @staticmethod
            async def fetch_products():
                # Fetch Coinbase trading pairs

            @staticmethod
            def start_product_updates():
                async def _schedule():
                    start_utc = time(18, 0, tzinfo=timezone.utc)
                    end_utc = time(22, 30, tzinfo=timezone.utc)
                    await Coinbase.schedule_updates(
                        [((start_utc, end_utc), timedelta(minutes=1))]
                    )
                asyncio.create_task(_schedule())

        class Kraken(Exchange):
            ohlcv = KrakenOHLCV
            order_book = KrakenOrderBook
            ohlcv_db = KrakenOHLCVDatabase
            order_book_db = KrakenOrderBookDatabase

            @staticmethod
            async def fetch_products():
                # Fetch Kraken trading pairs

    Usage Example:
            async def main():
                Coinbase.configure_logging("path/to/log")
                Coinbase.start_product_updates()

            if __name__ == "__main__"
                asyncio.run(main)
        ])

    Attributes:
        ohlcv (Type[OHLCV_History]): Reference to an exchange-specific OHLCV implementation.
        order_book (Type[OrderBook]): Reference to an exchange-specific OrderBook implementation.
        ohlcv_db (Type[WriteOnlyDatabase]): Database implementation for storing OHLCV data.
        order_book_db (Type[WriteOnlyDatabase]): Database implementation for storing order book data.
        _trading_products (Set[str]): Actively traded products on the exchange.
        _non_trading_products (Set[str]): Products not currently being traded.
        _enlisted_products (Set[str]): Most recently enlisted products.
        _delisted_products (Set[str]): Most recently delisted products.
        _product_update_callbacks: A dictionary to store callbacks for product updates.
                                      Keys are event types (e.g., "enlisted", "delisted").
                                      Values are lists of callable functions.
    """

    CallbackType = Callable[[Set[str]], None]
    ohlcv: Type[OHLCV_History]
    order_book: Type[OrderBook]

    ohlcv_db: Type[WriteOnlyDatabase] = OHLCV_Database
    order_book_db: Type[WriteOnlyDatabase] = OrderBookDatabase

    _trading_products: Set[str] = set()
    _non_trading_products: Set[str] = set()
    _enlisted_products: Set[str] = set()
    _delisted_products: Set[str] = set()
    _product_update_callbacks: Dict[str, CallbackType] = {
        "enlisted": set(),
        "delisted": set(),
    }
    _logger = NullLogger()

    @staticmethod
    def configure_logging(logs_path: Path):
        """Initializes the logger."""
        Exchange._logger = logger_manager.get_logger(logs_path)

    @staticmethod
    async def schedule_updates(
        schedule: List[Tuple[Tuple[time, time], timedelta]],
    ):
        """Schedules trading pair updates based on timeframes and intervals.

        Args:
            schedule (List[Tuple[Tuple[time, time], timedelta]]):
                A list of tuples, where each tuple contains:
                - A tuple of start and end times (time objects) for the update schedule.
                - A timedelta object representing the interval between updates.
        """
        for timeframe, interval in schedule:
            asyncio.create_task(Exchange._scheduled_update(timeframe, interval))

    @staticmethod
    def start_product_updates():
        """
        Starts the background task that updates the exchange's trading products.

        Each subclass **must implement** this method to schedule the product updates
        at the appropriate time(s).
        """
        pass

    @staticmethod
    @abstractmethod
    async def fetch_products() -> tuple[set[str], set[str]]:
        """
        Abstract method to fetch trading and non-trading products from the exchange API.

        Each subclass **must implement** this method to return the exchange's active
        and inactive trading pairs.

        Returns:
            Tuple[Set[str], Set[str]]:
                - The first set contains currently **traded** products.
                - The second set contains **non-traded** products (including delisted ones).

        Raises:
            `Exception` or its variations. `_update_trading_products` handles it gracefully.
        """
        pass

    @staticmethod
    def trading_products() -> Set[str]:
        """Returns a copy of the set of currently traded products."""
        return set(Exchange._trading_products)

    @staticmethod
    def non_trading_products() -> Set[str]:
        """Returns a copy of the set of currently non-traded products."""
        return set(Exchange._non_trading_products)

    @staticmethod
    def enlisted_products() -> Set[str]:
        """Returns a copy of the set of recently enlisted products."""
        return set(Exchange._enlisted_products)

    @staticmethod
    def delisted_products() -> Set[str]:
        """Returns a copy of the set of recently delisted products."""
        return set(Exchange._delisted_products)

    @staticmethod
    def set_trading_products(products: Set[str]):
        """Sets the set of currently traded products."""
        Exchange._trading_products = products

    @staticmethod
    def set_non_trading_products(products: Set[str]):
        """Sets the set of currently non-traded products."""
        Exchange._non_trading_products = products

    @staticmethod
    def set_enlisted_products(products: Set[str]):
        """Sets the set of recently enlisted products."""
        Exchange._enlisted_products = products

    @staticmethod
    def set_delisted_products(products: Set[str]):
        """Sets the set of recently delisted products."""
        Exchange._delisted_products = products

    @staticmethod
    def subscribe_to_product_updates(
        event_type: Literal["enlisted", "delisted"],
        *callbacks: CallbackType,
    ):
        """
        Subscribes a callback function to product update events.

        Args:
            event_type (str): The type of event to subscribe to (e.g., "enlisted", "delisted").
            callbacks (Callable[[Set[str]], None]): A tuple of callable functions that will be called
                                                  when the event occurs. It will receive a set
                                                  of product strings as an argument.

        Raises:
            ValueError: If the event_type is invalid.
        """
        if event_type not in Exchange._product_update_callbacks:
            raise ValueError(f"Invalid event type: {event_type}")
        for callback in callbacks:
            Exchange._product_update_callbacks[event_type].add(callback)

    @staticmethod
    def unsubscribe_from_product_updates(
        event_type: Literal["enlisted", "delisted"],
        *callbacks: CallbackType,
    ):
        """
        Unsubscribes a callback function from product update events.

        Args:
            event_type (str): The type of event to unsubscribe from (e.g., "enlisted", "delisted").
            callbacks (Callable[[Set[str]], None]): The callback functions to unsubscribe.

        Raises:
            ValueError: If the event_type is invalid.
        """
        logger = Exchange._logger
        if event_type not in Exchange._product_update_callbacks:
            raise ValueError(f"Invalid event type: {event_type}")
        try:
            for callback in callbacks:
                Exchange._product_update_callbacks[event_type].remove(callback)
        except ValueError:
            logger.debug(f"Callback not found for event type '{event_type}'.")

    @staticmethod
    async def _notify_product_updates(event_type: str, products: Set[str]):
        """
        Notifies all subscribed callbacks of a product update event.

        Args:
            event_type (str): The type of event (e.g., "enlisted", "delisted").
            products (Set[str]): The set of products affected by the event.
        """
        if event_type not in Exchange._product_update_callbacks:
            raise ValueError(f"Invalid event type: {event_type}")
        for callback in Exchange._product_update_callbacks[event_type]:
            callback(products)  # Call the callback function

    @staticmethod
    async def _update_trading_products():
        """
        Fetches the latest trading products and updates the internal state.

        This function updates the sets:
        - `_trading_products`: Active trading pairs.
        - `_non_trading_products`: Inactive trading pairs.
        - `_enlisted_products`: Most recently added trading pairs.
        - `_delisted_products`: Most recently removed trading pairs.

        Logs changes and ensures that only newly enlisted and delisted products
        are recorded in `_enlisted_products` and `_delisted_products`.

        Handles API failures gracefully, preventing overwrites if an error occurs.
        """
        logger = Exchange._logger
        try:
            new_trading, new_non_trading = await Exchange.fetch_products()

            old_trading = Exchange._trading_products

            enlisted_products = new_trading - old_trading  # New products
            delisted_products = (
                old_trading - new_trading
            )  # Pairs removed from trading

            # Update trading & non-trading sets
            Exchange._trading_products = new_trading
            Exchange._non_trading_products = new_non_trading

            # Track only **newly** enlisted and delisted pairs
            Exchange._enlisted_products = enlisted_products
            Exchange._delisted_products = delisted_products

            # Notify subscriber callbacks
            await Exchange._notify_product_updates(
                "enlisted", enlisted_products
            )
            await Exchange._notify_product_updates(
                "delisted", delisted_products
            )
            if enlisted_products:
                logger.info(f"Enlisted products: {enlisted_products}")
            if delisted_products:
                logger.info(f"Delisted products: {delisted_products}")

        except RuntimeError as e:
            logger.error(f"Trading pair update failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in update_trading_pairs: {e}")

    @staticmethod
    async def _scheduled_update(
        timeframe: Tuple[time, time], interval: timedelta
    ):
        """
        Runs `_update_trading_products()` within a specified timeframe and interval.

        Args:
            timeframe (Tuple[time, time]): Start and end times (UTC) for the schedule.
            interval (timedelta): The time interval between updates.
        """
        start_time, end_time = timeframe
        logger = Exchange._logger
        while True:
            now = datetime.now(timezone.utc).time()
            if start_time <= now <= end_time:
                await Exchange._update_trading_products()
                await asyncio.sleep(interval.total_seconds())
            else:
                # Wait until the next start time
                now_dt = datetime.now(timezone.utc)
                next_start = datetime.combine(
                    now_dt.date(), start_time, tzinfo=timezone.utc
                )
                if now > end_time:
                    next_start += timedelta(days=1)
                sleep_seconds = (next_start - now_dt).total_seconds()
                logger.info(
                    f"⏳ Next update scheduled for {next_start} UTC (in {sleep_seconds:.2f} seconds)"
                )
                await asyncio.sleep(sleep_seconds)
