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
from typing import Type


class Exchange:
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
        ohlcv (Type[OHLCV_History]):
            A reference to a subclass of `OHLCV_History` that implements 
            exchange-specific logic for retrieving candlestick data.
        
        order_book (Type[OrderBook]):
            A reference to a subclass of `OrderBook` that implements
            exchange-specific logic for accessing the order book data.
        
        ohlcv_db(Type[WriteOnlyDatabase]):
            A reference to a subcalss of `WriteOnlyDatabase` that implements
            the `write` method as well as `__enter__` and `__exit__` for 
            storing ohlcv data

        order_book_db(Type[WriteOnlyDatabase]):
            A reference to a subcalss of `WriteOnlyDatabase` that implements
            the `write` method as well as `__enter__` and `__exit__` for 
            storing order book data
        
    """

    ohlcv: Type[OHLCV_History]
    order_book: Type [OrderBook]

    ohlcv_db: Type[WriteOnlyDatabase] = OHLCV_Database
    order_book_db: Type[WriteOnlyDatabase] = OrderBookDatabase