import asyncio
from pathlib import Path
import pyarrow as pa
from coin_db import CoinDB
from parser import Parser
from coinbase_candle_history import CoinbaseCandleHistory
from parquet import Parquet
import logging

logging.basicConfig(filename='logs/log', level=logging.INFO, 
                        format='%(message)s')
# SCHEMA = pa.schema([
#      ("time", pa.int64()),
#      ("low", pa.float64()),
#      ("high", pa.float64()),
#      ("open", pa.float64()),
#      ("close", pa.float64()),
#      ("volume", pa.float64()),
#      ("timestamp", pa.timestamp("s"))
# ])

# COLUMNS = ["time", "low", "high", "open", "close", "volume"]

# SORT_KEY = "timestamp"
 

with Path("extras", "coin-pairs").open("r") as file:
     coins = file.readlines()
     
async def main(symbols, start_date, end_date, granularity, dir):
     
     gen = CoinbaseCandleHistory.fetch(
         symbols,  
         start_date,
         end_date,
         granularity
     )

     db = CoinDB(Path(dir))
     await db.store(gen)
 
if __name__ == "__main__":
     parser = Parser()
     args = parser.parse()
     asyncio.run(main(*args))