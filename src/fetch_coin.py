import aiohttp
import asyncio
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time
from pathlib import Path
import os
from datetime import datetime, timezone, timedelta
from parser import Parser

COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{}/candles"
COINBASE_REQUEST_LIMIT_PER_SECOND = 5 # Increase up to 10 for your own risk

 
async def fetch_historical_data(session, symbol, start_date, end_date, granularity=60):
     """Fetch historical data from Coinbase API asynchronously in chunks (Oldest → Newest)."""
     url = COINBASE_CANDLES_URL.format(symbol)
     max_candles = 300  # Max allowed per request
     chunk_size = timedelta(minutes=max_candles)  # 300-minute chunks

     start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
     end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

     current_start = start_date  
     while current_start < end_date:
          current_end = min(current_start + chunk_size, end_date)  

          params = {
              "start": current_start.isoformat(),
              "end": current_end.isoformat(),
              "granularity": granularity
          }

          async with session.get(url, params=params) as response:
               if response.status != 200:
                    print(f"⚠️ Error fetching data: {await response.text()}")
                    await asyncio.sleep(2)  # Rate limiting fallback
                    continue

               data = await response.json()

                
               if data:
                   print(f"📊 Yielding {len(data)} candles for {current_start} → {current_end}")
                   yield data  
               else:
                   print(f"⚠️ No data for {current_start} → {current_end}, skipping.")

          print(f"📊 Downloaded candles for {current_start} → {current_end}")

          current_start = current_end  
          await asyncio.sleep(1 / COINBASE_REQUEST_LIMIT_PER_SECOND)


async def save_to_parquet(data_gen, filename):
     """Save streamed OHLC data into a Parquet file incrementally."""
     parquet_path = filename
     schema = pa.schema([
          ("time", pa.int64()),
          ("low", pa.float64()),
          ("high", pa.float64()),
          ("open", pa.float64()),
          ("close", pa.float64()),
          ("volume", pa.float64()),
          ("timestamp", pa.timestamp("s"))
     ])

     # Delete file if it exists to start fresh
     if os.path.exists(parquet_path):
          os.remove(parquet_path)

     async for chunk in data_gen:
          
          df = pd.DataFrame(chunk, columns=["time", "low", "high", "open", "close", "volume"])
     
          # Convert timestamp column
          df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

          df = df.sort_values(by="timestamp", ascending=True) # This was added because coinbase sends candles from most recent to oldest for some reason (like 15:59, 15:58, ...)
         

          table = pa.Table.from_pandas(df, schema=schema)

           
          with pq.ParquetWriter(parquet_path, schema=schema, compression="snappy", use_dictionary=True) as writer:
               writer.write_table(table)

          print(f"✅ Saved {len(df)} rows to {filename}")

async def main(symbol, start_date, end_date, granularity, dir):
     async with aiohttp.ClientSession() as session:
          data_gen = fetch_historical_data(session, symbol, start_date, end_date, granularity)
          save_path = Path(dir) / symbol
          await save_to_parquet(data_gen, f"{save_path}.parquet")

if __name__ == "__main__":
     parser = Parser()
     args = parser.parse()

     asyncio.run(main(args.name, args.start_date, args.end_date, args.granularity, args.dir))