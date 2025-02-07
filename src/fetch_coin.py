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
COINBASE_REQUEST_LIMIT_PER_SECOND = 7  # Increase up to 10 at your own risk

async def fetch_historical_data(session, symbol, start_date, end_date, granularity=60, temp_csv_path="data/temp.csv"):
     """Fetch historical data from Coinbase API asynchronously and append it to a CSV file."""
     url = COINBASE_CANDLES_URL.format(symbol)
     max_candles = 300  # Max allowed per request
     chunk_size = timedelta(minutes=max_candles)  # 300-minute chunks

     start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
     end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

     current_start = start_date  
     last_known_row = None  # Store last known row for filling missing gaps

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
                    print(f"📊 Fetched {len(data)} candles for {current_start} → {current_end}")

                    df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
                    df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

                    # Ensure chronological order (Coinbase returns most recent first)
                    df = df.sort_values(by="timestamp", ascending=True)

                    # Generate a complete minute-by-minute index
                    full_time_range = pd.date_range(
                    start=df["timestamp"].iloc[0],
                    end=df["timestamp"].iloc[-1],
                    freq="T"
                    )
                    df = df.set_index("timestamp").reindex(full_time_range)

                    # Forward fill missing values
                    df.ffill(inplace=True)

                    # Convert back to regular dataframe
                    df = df.reset_index()
                    df.rename(columns={"index": "timestamp"}, inplace=True)

                    # Convert back to integer timestamps for saving
                    df["time"] = df["timestamp"].astype(int) // 10**9  # Convert to Unix timestamp

                    # Save last row to fill future gaps
                    last_known_row = df.iloc[-1].copy()

                    # Append to CSV file
                    df.to_csv(temp_csv_path, mode="a", header=not os.path.exists(temp_csv_path), index=False)

               else:
                    print(f"⚠️ No data for {current_start} → {current_end}, skipping.")
                    # If no data, use last known values (if available)
                    if last_known_row is not None:
                        last_known_row["timestamp"] = current_start
                        last_known_row["time"] = int(current_start.timestamp())
                        pd.DataFrame([last_known_row]).to_csv(
                            temp_csv_path, mode="a", header=not os.path.exists(temp_csv_path), index=False
                        )

          current_start = current_end  
          await asyncio.sleep(1 / COINBASE_REQUEST_LIMIT_PER_SECOND)


async def convert_csv_to_parquet(temp_csv_path, parquet_path):
     """Convert the temporary CSV file to Parquet format."""
     if not os.path.exists(temp_csv_path):
         print("⚠️ No data file found, skipping conversion to Parquet.")
         return

     df = pd.read_csv(temp_csv_path)

     schema = pa.schema([
         ("time", pa.int64()),
         ("low", pa.float64()),
         ("high", pa.float64()),
         ("open", pa.float64()),
         ("close", pa.float64()),
         ("volume", pa.float64()),
         ("timestamp", pa.timestamp("s"))
     ])

     df = df.sort_values(by="timestamp", ascending=True)  # Ensure chronological order
     table = pa.Table.from_pandas(df, schema=schema)

     pq.write_table(table, parquet_path, compression="snappy")

     print(f"✅ Converted CSV to {parquet_path}")

     # Delete temporary CSV file
     os.remove(temp_csv_path)
     print("🗑️ Deleted temporary CSV file.")

async def main(symbol, start_date, end_date, granularity, dir):
     save_path = Path(dir) / f"{symbol}.parquet"
     temp_csv_path = Path(dir) / "temp.csv"

     async with aiohttp.ClientSession() as session:
          await fetch_historical_data(session, symbol, start_date, end_date, granularity, temp_csv_path)

     # Convert CSV to Parquet after fetching all data
     await convert_csv_to_parquet(temp_csv_path, save_path)

if __name__ == "__main__":
     parser = Parser()
     args = parser.parse()

     asyncio.run(main(args.name, args.start_date, args.end_date, args.granularity, args.dir))
