from ohlcv_history import OHLCV_History
from order_book import OrderBook
from typing import Type


class Exchange:
    ohlcv: Type[OHLCV_History]
    order_book: Type [OrderBook]