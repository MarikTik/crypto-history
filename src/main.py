# import asyncio
# from pathlib import Path
# from coin_db import CoinDB
# from parser import Parser
# from exchanges.coinbase import OHLCV_History
# from datetime import datetime

# from utils.exchange import *
# import exchanges.coinbase as coinbase
# import exchanges.binance as binance
# import exchanges.kraken as kraken
# import exchanges.robinhood as robinhood

# exchanges = {exchange.__name__.split(".")[-1] : exchange for exchange in [coinbase, binance, kraken, robinhood]}

# async def download_order_book(exchange, products, depth, frequency, end_date, directory):
#      db: Database = exchange.Database(directory=directory)
#      async with exchange.OrderBook(products=products.keys(), depth=depth, frequency=frequency) as order_book:
#           async for snapshot in order_book.snapshots(until=end_date):
#                await db.store_order_book_snapshot(snapshot)

# async def download_ohlcv(exchange, products, directory):
#      db: Database = exchange.Database(directory=directory)
#      gen = exchange.OHLCV_History.fetch_many(exchange.OLHCV_History, products)
#      async for candles in gen:
#           await db.store_ohlcv_candles(candles)

# async def main(*args):
#      download_type, exchange, products, extras, directory = args
#      if download_type == "ohlcv":
#           await download_ohlcv(exchange, products, directory)
#      elif download_type == "order_book":
#           await download_order_book(exchange, products, *extras, directory)

# if __name__ == "__main__":
#      parser = Parser()
#      args = parser.parse()
#      asyncio.run(main(*args))

from exchanges.coinbase import OHLCV_History
from datetime import datetime
import asyncio
import json
async def main():
     async with OHLCV_History("BTC-USD", 60) as f:
          async for batch in f.fetch():
               print(batch)
               print("\n\n")

asyncio.run(main())
