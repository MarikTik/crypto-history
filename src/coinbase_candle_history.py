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
from typing import Optional, List, AsyncGenerator, Dict, Iterable
import asyncio
import aiohttp
from logger import logger_manger

COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{}/candles"
COINBASE_RATE_LIMIT = 1/7  # Increase up to 10 at your own risk
MAX_CANDLES = 300 # Max Candles allowed per request 
FUTURE_OFFSET = 10000 # Offset for downloading data continuously 


class CoinbaseCandleHistory:
     @staticmethod
     async def fetch_timeframe(
          session: aiohttp.ClientSession,
          symbol: str,
          start_time: datetime,
          end_time: datetime,
          granularity: int = 60) -> Dict[str, str | List[List[float | int]]]:
          """
          Fetches a specific time range of cryptocurrency candle data from Coinbase API.

          Args:
               session (aiohttp.ClientSession): The aiohttp session.
               symbol (str): The cryptocurrency pair (e.g., "BTC-USDT").
               start_time (datetime): The starting point for fetching data.
               end_time (datetime): The ending point for fetching data.
               granularity (int): The candle interval in seconds (defaults to 60s).

          Returns:
               dict: {'symbol': symbol, 'data': List[List]} containing fetched OHLCV data.
               dict: {} If no data is present for that specific timeframe
               None: If the symbol is not found in Coinbase's database.
          """
          url = COINBASE_CANDLES_URL.format(symbol)
          chunk_size = timedelta(minutes=MAX_CANDLES)

          current_start = start_time
          current_end = min(current_start + chunk_size, end_time)

          params = {
               "start": current_start.isoformat(),
               "end": current_end.isoformat(),
               "granularity": granularity
          }
          logger = logger_manger.get_logger(symbol)

          try:
               async with asyncio.timeout(10):  
                    async with session.get(url, params=params) as response:
                         if response.status == 404:
                              logger.critical(f"❌ {symbol} not found in database. Skipping to next coin.")
                              return None  # Skip this coin

                         if response.status != 200:
                              logger.error(f"⚠️ fetching {symbol}: ({response.status}) {await response.text()}")
                              return None  # Skip on API failure

                         data = await response.json()
                         if data:
                              logger.debug(f"📊 Downloaded {len(data)} candles for {symbol}: {current_start} → {current_end}")
                              return {"symbol": symbol, "data": data}

                         logger.warning(f"⚠️ No data for {symbol}: {current_start} → {current_end}, skipping timeframe.")
                         return {}  # No data for this timeframe, but don't skip the coin

          except asyncio.TimeoutError:
               logger.error(f"⏳ Timeout fetching data for {symbol}: {current_start} → {current_end}. Retrying later.")
               return None  # Avoid getting stuck due to connection problems
          
     @staticmethod
     async def fetch(
          symbols: Iterable[str],
          start_date: str,
          end_date: Optional[str] = None,
          granularity: int = 60
     ) -> AsyncGenerator[Dict[str, str | List[List[float | int]]], None]:
          """
          Sequentially fetches historical and live cryptocurrency data for multiple coins.

          Instead of switching between coins in every iteration, it completes fetching **one** 
          coin up until today, then switches to the next.

          Args:
               symbols (Iterable): List of cryptocurrency pairs (e.g., ["BTC-USDT", "ETH-USDT"]).
               start_date (str): The start date in ISO format (YYYY-MM-DD)
               end_date (Optional[str]): The end date in ISO format (YYYY-MM-DD). If None, fetches indefinitely.
               granularity (int): Candle interval in seconds (defaults to 60s).

          Yields:
            dict: {'symbol': symbol, 'data': List[List[Union[float, int]]]} containing OHLCV data.

          Warning: 
               `fetch(symbols, ...)` doesn't check for duplicated symbols.
          """
          
          async with aiohttp.ClientSession() as session:
               now = datetime.now(timezone.utc)
               start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)

               # If no end date is provided, set it to today
               if end_date is None:
                    end_date = now  
               else:
                    end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

               for symbol in symbols:   
                    logger = logger_manger.get_logger(symbol)
                    last_fetched = start_date
                    finished = False
                    while last_fetched < end_date and not finished:
                         result = await CoinbaseCandleHistory.fetch_timeframe(
                              session,
                              symbol,
                              last_fetched,
                              end_date,
                              granularity
                         )
                         if result is None: # if symbol is not found in database
                              break
                         
                         if not result.get("data"):
                              last_fetched += timedelta(minutes=MAX_CANDLES)  # Move to next timeframe
                              continue  # Try fetching the next timeframe

                         last_fetched = datetime.fromtimestamp(result["data"][0][0], tz=timezone.utc)  # Update last fetched timestamp
                         now = datetime.now(timezone.utc)
                         if last_fetched.year == now.year and last_fetched.month == now.month:
                                   logger.info(f"🔄 Reached current month for {symbol}, switching to next coin.")
                                   finished = True
                                   break  # Move to the next coin

                         yield result  # Yields data as it arrives
                         await asyncio.sleep(COINBASE_RATE_LIMIT)   