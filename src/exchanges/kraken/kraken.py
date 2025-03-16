from ..exchange import Exchange
from .ohlcv_history import OHLCV_History
from .order_book import OrderBook

class Kraken(Exchange):
    ohlcv = OHLCV_History
    order_book = OrderBook
