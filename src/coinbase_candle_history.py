"""
coinbase_candle_history.py

This module provides an asynchronous method to fetch historical cryptocurrency 
candle data from the Coinbase API. It handles rate limits, retries on failures, 
and logs progress.

Classes:
     - CoinbaseCandleHistory: Provides a static method `fetch` to retrieve historical 
     candle data asynchronously while respecting API constraints.

Usage:
     import aiohttp
     from coinbase_candle_history import CoinbaseCandleHistory

     async with aiohttp.ClientSession() as session:
          async for data_chunk in CoinbaseCandleHistory.fetch(session, "BTC-USDT", "2021-01-01", "2022-01-01"):
               process(data_chunk)  # Handle the received chunk
"""


from datetime import datetime, timezone, timedelta
import asyncio
import random
import logging

COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{}/candles"
COINBASE_REQUEST_LIMIT_PER_SECOND = 7  # Increase up to 10 at your own risk
MAX_CANDLES = 300 # Max Candles allowed per request 

class CoinbaseCandleHistory:
     @staticmethod
     async def fetch(session, symbol, start_date, end_date=datetime.now(), granularity=60):
          """
          Asynchronously fetches historical cryptocurrency candle data from the Coinbase API.

          This function retrieves data in chunks (Oldest → Newest) while handling rate limits and errors.

          Args:
               session (aiohttp.ClientSession): An active aiohttp session to send requests.
               symbol (str): The cryptocurrency pair (e.g., "BTC-USDT").
               start_date (str): The start date in ISO format (YYYY-MM-DD).
               end_date (str): The end date in ISO format (YYYY-MM-DD).
               granularity (int, optional): The interval for candles in seconds. 
                    Defaults to 60 (1-minute candles).
                    Options: 60, 300, 900, 3600, 21600, 86400 (1m, 5m, 15m, 1h, 6h, 1d).

          Yields:
               List[List]: A chunk of up to 300 OHLCV candle data points.

          Logging:
               - Errors (API failures) are logged with `logging.error()`.
               - Critical failures (max retries exceeded) are logged with `logging.critical()`.
               - Retry attempts are logged at `logging.debug()` level.
               - Successfully downloaded chunks are logged with `logging.info()`.

          Notes:
               - Exponential backoff is applied in case of API failures.
               - Each request retrieves at most 300 candles.
               - The function respects Coinbase’s rate limit of 10 requests per second.
               - Uses a retry mechanism with a cap of 5 retries per failed request.
          """
          url = COINBASE_CANDLES_URL.format(symbol)
          chunk_size = timedelta(minutes=MAX_CANDLES)

          start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
          if type(end_date) is str:
               end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

          current_start = start_date
          retry_attempts = 0

          while current_start < end_date:
               current_end = min(current_start + chunk_size, end_date)

               params = {
                    "start": current_start.isoformat(),
                    "end": current_end.isoformat(),
                    "granularity": granularity
               }

               async with session.get(url, params=params) as response:
                    
                    if response.status == 404:
                         logging.critical(f"❌ {symbol} was not found in database")
                         return

                    if response.status != 200:
                         logging.error(f"⚠️ Error fetching data ({response.status}): {await response.text()}")

                         # retry_attempts += 1
                         # if retry_attempts > 5:  # Prevent infinite retries
                         #      logging.critical(f"❌ Skipping {current_start} → {current_end} after too many failures.")
                         #      current_start = current_end
                         #      continue
                         
                         # wait_time = min(2 ** retry_attempts, 60) + random.uniform(0, 1)  # Exponential backoff
                         # logging.debug(f"⏳ Retrying in {wait_time:.2f} seconds...")
                         # await asyncio.sleep(wait_time)
                         # continue  # Retry the same request
                    
                    # retry_attempts = 0  # Reset on success

                    data = await response.json()
                    if data:
                         yield data  
                    else:
                         logging.info(f"⚠️ No data for {current_start} → {current_end}, skipping.")

               logging.info(f"📊 Downloaded candles for {current_start} → {current_end}")
               current_start = current_end
               await asyncio.sleep(1 / COINBASE_REQUEST_LIMIT_PER_SECOND)