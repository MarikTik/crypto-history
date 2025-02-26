import asyncio
from pathlib import Path
from coin_db import CoinDB
from parser import Parser
from coinbase_candle_history import CoinbaseCandleHistory

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