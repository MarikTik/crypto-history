"""
coinbase_candle_history.py

This module provides an asynchronous method to fetch both historical and continuous 
real-time cryptocurrency candle data from the Coinbase API. It handles rate limits, 
retries on failures, and logs progress while ensuring efficient data retrieval.

Features:
    - Fetches historical OHLCV (Open, High, Low, Close, Volume) candlestick data in batches.
    - Supports fetching multiple cryptocurrency pairs simultaneously.
    - Allows continuous fetching when no end date is provided, ensuring up-to-date market data.
    - Implements rate limiting and exponential backoff for API stability.
    - Uses async generators to efficiently stream large datasets without excessive memory usage.

Classes:
    - CoinbaseCandleHistory: Provides two static methods:
        - `fetch_timeframe`: Fetches a specific range of historical data in chunks.
        - `fetch`: Manages multi-coin fetching, supporting both fixed and continuous retrieval.

Usage Example:
    import aiohttp
    from coinbase_candle_history import CoinbaseCandleHistory

    async with aiohttp.ClientSession() as session:
        async for data_chunk in CoinbaseCandleHistory.fetch(session, ["BTC-USDT", "ETH-USDT"], "2021-01-01"):
            process(data_chunk)  # Handle the received chunk (e.g., store in a database)

Notes:
    - The generator-based design ensures efficient handling of large datasets without consuming too much memory.
    - When `end_date` is not provided, the fetch function runs indefinitely, keeping the data updated in real time.
    - The function supports different granularity options (1m, 5m, 15m, 1h, 6h, 1d).
"""


from datetime import datetime, timezone, timedelta
from typing import Optional, List, AsyncGenerator, Dict, Iterable, Literal
import asyncio
import aiohttp
from aiohttp import ContentTypeError
from json import JSONDecodeError


from utils.loggers.logger import logger_manger
from utils.algorithms import binary_search_first_occurrence_async
from utils.configs import CONFIG

COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{}/candles"
COINBASE_RATE_LIMIT = 1/7  # Increase up to 10 at your own risk
MAX_CANDLES = 300 # Max Candles allowed per request 
FUTURE_OFFSET = 10000 # Offset for downloading data continuously 
TIMEOUT = 10 # Request timeout in seconds

class CoinbaseCandleHistory:
     @staticmethod
     async def fetch_timeframe(
          session: aiohttp.ClientSession,
          symbol: str,
          start_time: datetime,
          end_time: datetime | None = None,
          granularity: int = 60) -> Dict[str, str | List[List[float | int]]] | Literal["not_found", "api_failure", "no_data", "timeout_error"]:
          """
          Fetches a specific time range of cryptocurrency candle data from Coinbase API.

          Args:
               session (aiohttp.ClientSession): The aiohttp session.
               symbol (str): The cryptocurrency pair (e.g., "BTC-USDT").
               start_time (datetime): The starting point for fetching data.
               end_time (datetime or None): The ending point for fetching data. Assigned start_time + MAX_CANDLES (minutes) if None provided.
               granularity (int): The candle interval in seconds (defaults to 60s).
 
          Returns:
               dict: {'symbol': symbol, 'data': List[List]} containing fetched OHLCV data.
               str: `"not_found"` if the coin pair wasn't found in database (404 error).
                    `"api_failure"` if the response status was not 200 or returned invalid JSON.
                    `"no_data"` if the response was successful but no candle data present in it.
                    `"timeout_error"` if the request took longer than TIMEOUT seconds.
                    `"rate_limited"` if the request was blocked due to API rate limits (429).
                    `"server_error"` if Coinbase returns a 5xx server error.

          """
          url = COINBASE_CANDLES_URL.format(symbol)
          chunk_size = timedelta(minutes=MAX_CANDLES)

          if end_time is None or end_time <= start_time:
               end_time = start_time + chunk_size
          else:
               end_time = min(start_time + chunk_size, end_time)

          params = {
               "start": start_time.isoformat(),
               "end": end_time.isoformat(),
               "granularity": granularity
          }
          headers = {
               "User-Agent": CONFIG.USER_AGENT,
               "Accept": "application/json",
               "X-Contact-Email": CONFIG.CONTACT_EMAIL,  
               "X-App-Version": CONFIG.VERSION,  
               "X-Repo-Link": CONFIG.REPO_LINK  
          }

          logger = logger_manger.get_logger(symbol)

          try:
               response = await asyncio.wait_for(session.get(url, params=params, headers=headers), timeout=TIMEOUT)
               if response.status == 404:
                    logger.critical(f"❌ {symbol} not found in database")
                    return "not_found"

               if response.status == 429:
                    logger.warning(f"🔄 Rate limit hit for {symbol}. Coinbase suggests retrying later.")
                    return "rate_limited"

               if response.status >= 500:
                    logger.error(f"⚠️ Server error {response.status} for {symbol}.")
                    return "server_error"

               if response.status != 200:
                    logger.error(f"⚠️ fetching {symbol}: ({response.status}) {await response.text()}")
                    return "api_failure" 

               try:
                    data = await response.json()
               except (JSONDecodeError, ContentTypeError):
                    logger.error(f"⚠️ Malformed JSON response for {symbol}: ({response.status})")
                    return "api_failure"

               if not isinstance(data, list):
                    logger.error(f"⚠️ Unexpected response format for {symbol}: {data}")
                    return "api_failure"

               if data:
                    logger.debug(f"📊 Downloaded {len(data)} candles for {symbol}: {start_time} → {end_time}")
                    return {"symbol": symbol, "data": data}

              
               logger.warning(f"⚠️ No data for {symbol}: {start_time} → {end_time}")
               return "no_data" 

          except asyncio.TimeoutError:
               logger.error(f"⏳ Timeout fetching data for {symbol}: {start_time} → {end_time}. Retrying later.")
               return "timeout_error"  # Avoid getting stuck due to connection problems
          
          except aiohttp.ClientError as e:
            logger.error(f"🚨 Network error fetching {symbol}: {e}")
            return "api_failure"
          
     @staticmethod
     async def fetch(
     symbols: Iterable[str],
     start_date: Optional[str] = None,
     end_date: Optional[str] = None,
     granularity: int = 60
     ) -> AsyncGenerator[Dict[str, str | List[List[float | int]]], None]:
          """
          Sequentially fetches historical and live cryptocurrency data for multiple coins.

          Instead of switching between coins in every iteration, it completes fetching **one** 
          coin up until today, then switches to the next.
          """
          async with aiohttp.ClientSession() as session:
               now = datetime.now(timezone.utc)

               if start_date is None:
                    start_date = "2012-01-01"

               start_date: datetime = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)

               if end_date is None or end_date > now:  # If no end date is provided, or it is set too far away set it to today
                    end_date = now  
               else:
                    end_date: datetime = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

               for symbol in symbols:   
                    logger = logger_manger.get_logger(symbol)
                   
                    logger.info(f"🫣 Seeking first occurence of coinbase data for {symbol} from {start_date} to {end_date}")
                    async def condition(timestamp: int) -> bool:
                         datetime_obj = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                         response = await CoinbaseCandleHistory.fetch_timeframe(session, symbol, datetime_obj)
                         if not isinstance(response, dict) or "data" not in response:
                              return False
                         return bool(response["data"])

                    first_available_timestamp = await binary_search_first_occurrence_async(
                         condition, 
                         int(start_date.timestamp()),
                         int(end_date.timestamp()),
                         max_depth=32  # Control recursion depth
                    )
                    logger.info(f"🎉 Found first occurence of coibnase data")
                    logger.info(f"📡 Fetching historical data for {symbol} from {start_date} to {end_date} with {granularity}s granularity.")

                    if first_available_timestamp == -1:
                         logger.warning(f"⚠️ No historical data found for {symbol} within the given range.")
                         continue

                    last_fetched = datetime.fromtimestamp(first_available_timestamp, tz=timezone.utc)
                    finished = False

                    while last_fetched <= end_date and not finished:
                         result = await CoinbaseCandleHistory.fetch_timeframe(
                              session,
                              symbol,
                              last_fetched,
                              end_date,
                              granularity
                         )

                         if not isinstance(result, dict):  
                              logger.error(f"🚨 Unexpected response type for {symbol}: {result}")
                              last_fetched += timedelta(seconds=granularity)
                              continue  

                         if result in ["api_failure", "timeout_error"]:
                              last_fetched += timedelta(seconds=MAX_CANDLES)  
                              logger.warning(f"⚠️ Fetching issue for {symbol}, skipping to {last_fetched}")
                              await asyncio.sleep(COINBASE_RATE_LIMIT)  
                              continue  

                         fetched_timestamps = [candle[0] for candle in result["data"]]
                         if not fetched_timestamps:
                              last_fetched += timedelta(seconds=granularity)
                              logger.warning(f"⚠️ No new data for {symbol}, skipping to next batch.")
                              continue

                         new_last_fetched = datetime.fromtimestamp(max(fetched_timestamps), tz=timezone.utc)

                         if new_last_fetched == last_fetched:  
                              new_last_fetched += timedelta(seconds=granularity)
                              logger.warning(f"⚠️ Stuck on {symbol} at {last_fetched}, forcing move to {new_last_fetched}")

                         last_fetched = new_last_fetched  

                         yield result  

                         now = datetime.now(timezone.utc)
                         if last_fetched.year == now.year and last_fetched.month == now.month:
                              logger.info(f"🔄 Reached current month for {symbol}, switching to next coin.")
                              finished = True
                              break  

                         await asyncio.sleep(COINBASE_RATE_LIMIT)  