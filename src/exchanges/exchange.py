"""
File: exchange.py

This module defines a lightweight `Exchange` class, which acts as a
registry for specialized OHLCV_History and OrderBook classes.

Subclasses of `Exchange` (e.g., `Coinbase`, `Binance`) override these
class-level attributes with their own implementations:

    class Coinbase(Exchange):
        ohlcv = CoinbaseOHLCV
        order_book = CoinbaseOrderBook

in case an exchange needs to deviate from a standard database implementation it may override the default types
`OHLCV_Database` for `ohlcv_db` and/or `OrderBookDatabase` for `order_book_db` as:

    class Kraken(Exchange):
        ohlcv = CoinbaseOHLCV
        order_book = CoinbaseOrderBook
        ohlcv_db = KrakenCustomOHLCV_DB
        order_book_db = KrakenCustomOrderBookDB

By storing types rather than instances, callers can directly invoke
class methods (like `fetch_many`) on `Exchange.ohlcv` without needing
to instantiate an `Exchange` object.

Assumptions & Usage:
- `ohlcv` is expected to reference a subclass of `OHLCV_History`,
  which defines methods for fetching historical candlestick data.
- `order_book` is expected to reference a subclass of `OrderBook`,
  which defines methods for real-time or historical order-book snapshots.
- This design is suitable when each exchange only needs to share its
  specialized class types, without needing dedicated state per instance.
"""

from .ohlcv_history import OHLCV_History
from .order_book import OrderBook
from database import *
from typing import Type, Set
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from loggers import logger_manager


class Exchange(ABC):
    """
    Base class for referencing specific OHLCV_History and OrderBook implementations.

    Subclasses should assign `ohlcv` and `order_book` to the appropriate
    derived classes. For example:

        class Coinbase(Exchange):
            ohlcv = CoinbaseOHLCV
            order_book = CoinbaseOrderBook

    in case an exchange needs to deviate from a standard database implementation it may override the default types
    `OHLCV_Database` for `ohlcv_db` and/or `OrderBookDatabase` for `order_book_db` as:

        class Kraken(Exchange):
          ohlcv = CoinbaseOHLCV
          order_book = CoinbaseOrderBook
          ohlcv_db = KrakenCustomOHLCV_DB
          order_book_db = KrakenCustomOrderBookDB

    Attributes:
        ohlcv (Type[OHLCV_History]): Reference to an exchange-specific OHLCV implementation.
        order_book (Type[OrderBook]): Reference to an exchange-specific OrderBook implementation.
        ohlcv_db (Type[WriteOnlyDatabase]): Database implementation for storing OHLCV data.
        order_book_db (Type[WriteOnlyDatabase]): Database implementation for storing order book data.
    """

    ohlcv: Type[OHLCV_History]
    order_book: Type[OrderBook]

    ohlcv_db: Type[WriteOnlyDatabase] = OHLCV_Database
    order_book_db: Type[WriteOnlyDatabase] = OrderBookDatabase

    def __init__(self, logs_path: Path):
        self._trading_products: Set[str] = []
        self._non_trading_products: Set[str] = []
        self._enlisted_products: Set[str] = []
        self._delisted_products: Set[str] = []
        self._logger = logger_manager.get_logger(logs_path)

    @abstractmethod
    async def fetch_products(self):
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

    async def _update_trading_pairs(self):
        """Fetches trading products and updates the internal state, tracking delisted pairs."""
        logger = self._logger
        try:
            # Fetch latest trading and non-trading products
            new_trading, new_non_trading = await self.fetch_products()

            old_trading = self._trading_products

            # Identify newly enlisted and delisted pairs
            enlisted_products = new_trading - old_trading  # New trading pairs
            delisted_products = (
                old_trading - new_trading
            )  # Pairs removed from trading

            # Update trading & non-trading sets
            self._trading_products = new_trading
            self._non_trading_products = new_non_trading

            # Track only **newly** enlisted and delisted pairs
            self._enlisted_products = enlisted_products
            self._delisted_products = delisted_products

            # Update timestamp
            self._last_updated = datetime.now(timezone.utc)

            if enlisted_products:
                logger.info(f"Enlisted products: {enlisted_products}")
            if delisted_products:
                logger.info(f"Delisted products: {delisted_products}")

        except RuntimeError as e:
            print(f"Trading pair update failed: {e}")
        except Exception as e:
            print(f"Unexpected error in update_trading_pairs: {e}")
