import aiohttp
import asyncio
from pathlib import Path

import pyarrow as pa

from parser import Parser
from coinbase_candle_history import CoinbaseCandleHistory
from parquet import Parquet

SCHEMA = pa.schema([
     ("time", pa.int64()),
     ("low", pa.float64()),
     ("high", pa.float64()),
     ("open", pa.float64()),
     ("close", pa.float64()),
     ("volume", pa.float64()),
     ("timestamp", pa.timestamp("s"))
])

COLUMNS = ["time", "low", "high", "open", "close", "volume"]

SORT_KEY = "timestamp"


async def main(symbol, start_date, end_date, granularity, dir):
     async with aiohttp.ClientSession() as session:
          gen = CoinbaseCandleHistory.fetch(session, symbol, start_date, end_date, granularity)
          await (
               Parquet
               .from_generator(gen=gen, columns=COLUMNS, schema=SCHEMA, sort_by=SORT_KEY)
               .to(Path(dir) / symbol)
          )
     

if __name__ == "__main__":
     parser = Parser()
     args = parser.parse()

     asyncio.run(
          main(
               args.name,
               args.start_date,
               args.end_date,
               args.granularity,
               args.dir
          )
     )
