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
        async def fetch_products(self):
            # Fetch trading pairs from Coinbase API

    class Kraken(Exchange):
        async def fetch_products(self):
            # Fetch trading pairs from Kraken API

Key Features:
- Stores references to exchange-specific OHLCV and OrderBook classes.
- Requires subclasses to implement `fetch_products()` for retrieving market pairs.
- Provides automatic daily updates of trading pairs at a scheduled UTC time.
- Maintains sets of currently traded, non-traded, enlisted, and delisted products.

This design allows direct class-level access to exchange-specific methods
without needing an instantiated `Exchange` object.
"""

import asyncio
from typing import Type, Set
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta, time

from .ohlcv_history import OHLCV_History
from .order_book import OrderBook
from database import *

from loggers import logger_manager


class Exchange(ABC):
    """
    Base class for exchange-specific OHLCV_History and OrderBook implementations.

    Each subclass must define:
    - `ohlcv` and `order_book` with their respective implementations.
    - `fetch_products()` to fetch the list of traded/non-traded products.

    If needed, subclasses can also override database implementations:

    Example:
        class Coinbase(Exchange):
            ohlcv = CoinbaseOHLCV
            order_book = CoinbaseOrderBook

            async def fetch_products(self):
                # Fetch Coinbase trading pairs

        class Kraken(Exchange):
            ohlcv = KrakenOHLCV
            order_book = KrakenOrderBook
            ohlcv_db = KrakenOHLCVDatabase
            order_book_db = KrakenOrderBookDatabase

            async def fetch_products(self):
                # Fetch Kraken trading pairs

    Attributes:
        ohlcv (Type[OHLCV_History]): Reference to an exchange-specific OHLCV implementation.
        order_book (Type[OrderBook]): Reference to an exchange-specific OrderBook implementation.
        ohlcv_db (Type[WriteOnlyDatabase]): Database implementation for storing OHLCV data.
        order_book_db (Type[WriteOnlyDatabase]): Database implementation for storing order book data.
        _trading_products (Set[str]): Actively traded products on the exchange.
        _non_trading_products (Set[str]): Products not currently being traded.
        _enlisted_products (Set[str]): Most recently enlisted products.
        _delisted_products (Set[str]): Most recently delisted products.
    """

    ohlcv: Type[OHLCV_History]
    order_book: Type[OrderBook]

    ohlcv_db: Type[WriteOnlyDatabase] = OHLCV_Database
    order_book_db: Type[WriteOnlyDatabase] = OrderBookDatabase

    def __init__(self, logs_path: Path):
        """
        Initializes the Exchange instance and schedules a daily trading pair update.

        Args:
            logs_path (Path): Path to the log file directory.

        This constructor:
        - Initializes sets for tracking trading, non-trading, enlisted, and delisted products.
        - Sets up logging for the exchange instance.
        - Automatically schedules `_daily_update()` to run at 18:00 UTC every day.

        The `_daily_update()` function will fetch trading pairs once per day
        and update the internal state asynchronously.
        """
        self._trading_products: Set[str] = []
        self._non_trading_products: Set[str] = []
        self._enlisted_products: Set[str] = []
        self._delisted_products: Set[str] = []
        self._logger = logger_manager.get_logger(logs_path)
        # scheduling update at 18 PM UTC every day
        asyncio.create_task(self._daily_update(time(18)))

    @abstractmethod
    async def fetch_products(self):
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

    @property
    def trading_products(self):
        return self._trading_products

    @property
    def non_trading_products(self):
        return self._non_trading_products

    @property
    def enlisted_products(self):
        return self._enlisted_products

    @property
    def delisted_products(self):
        return self._delisted_products

    async def _update_trading_products(self):
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
        logger = self._logger
        try:
            new_trading, new_non_trading = await self.fetch_products()

            old_trading = self._trading_products

            enlisted_products = new_trading - old_trading  # New products
            delisted_products = (
                old_trading - new_trading
            )  # Pairs removed from trading

            # Update trading & non-trading sets
            self._trading_products = new_trading
            self._non_trading_products = new_non_trading

            # Track only **newly** enlisted and delisted pairs
            self._enlisted_products = enlisted_products
            self._delisted_products = delisted_products

            if enlisted_products:
                logger.info(f"Enlisted products: {enlisted_products}")
            if delisted_products:
                logger.info(f"Delisted products: {delisted_products}")

        except RuntimeError as e:
            print(f"Trading pair update failed: {e}")
        except Exception as e:
            print(f"Unexpected error in update_trading_pairs: {e}")

    async def _daily_update(self, when: time):
        """
        Runs `_update_trading_products()` at a scheduled time every day.

        Args:
            when (time): The UTC time at which `_update_trading_products()` should run.

        This function:
        - Calculates the next execution time.
        - Waits asynchronously until that time.
        - Executes `_update_trading_products()` exactly at the scheduled time.
        - Reschedules itself for the next day.

        Ensures efficient scheduling without CPU-intensive polling.
        """
        while True:
            now = datetime.now(timezone.utc)

            # Schedule next execution at the given time today
            next_run = datetime.combine(now.date(), when, tzinfo=timezone.utc)

            # If it's already past the target time today, schedule for tomorrow
            if now >= next_run:
                next_run += timedelta(days=1)

            sleep_seconds = (next_run - now).total_seconds()
            self._logger.info(
                f"⏳ Next update scheduled for {next_run} UTC (in {sleep_seconds:.2f} seconds)"
            )

            await asyncio.sleep(sleep_seconds)

            await self._update_trading_products()
