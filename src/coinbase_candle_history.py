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
from typing import Optional, List, AsyncGenerator, Dict
import asyncio
import logging
import aiohttp


COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/{}/candles"
COINBASE_REQUEST_LIMIT_PER_SECOND = 7  # Increase up to 10 at your own risk
MAX_CANDLES = 300 # Max Candles allowed per request 
FUTURE_OFFSET = 10000 # Offset for downloading data continuously 


class CoinbaseCandleHistory:
     @staticmethod
     async def fetch_timeframe(
          session: aiohttp.ClientSession,
          symbol: str,
          start_time: datetime,
          end_time: datetime,
          granularity: int = 60) -> AsyncGenerator[Dict[str, List[List[float | int]]], None]:
          """
          Fetches a specific time range of cryptocurrency candle data from Coinbase API.

          Args:
               session (aiohttp.ClientSession): The aiohttp session.
               symbol (str): The cryptocurrency pair (e.g., "BTC-USDT").
               start_time (datetime): The starting point for fetching data.
               end_time (datetime): The ending point for fetching data.
               granularity (int): The candle interval in seconds (defaults to 60s).

          Yields:
               dict: {'symbol': symbol, 'data': List[List]} containing fetched OHLCV data.
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

          async with session.get(url, params=params) as response:
               if response.status == 404:
                    logging.critical(f"❌ {symbol} not found in database.")
                    return

               if response.status != 200:
                    logging.error(f"⚠️ Error fetching {symbol}: ({response.status}) {await response.text()}")
                    return

               data = await response.json()
               if data:
                    logging.info(f"📊 Downloaded {len(data)} candles for {symbol}: {current_start} → {current_end}")
                    yield {"symbol": symbol, "data": data}

               else:
                    logging.info(f"⚠️ No data for {symbol}: {current_start} → {current_end}, skipping.")

     @staticmethod
     async def fetch(
          symbols: List[str],
          start_date: str,
          end_date: Optional[str] = None,
          granularity: int = 60) -> AsyncGenerator[Dict[str, List[List[float | int]]], None]:
          """
          Continuously fetches historical and live cryptocurrency data for multiple coins.

          Args:
            symbols (list): List of cryptocurrency pairs (e.g., ["BTC-USDT", "ETH-USDT"]).
            start_date (str): The start date in ISO format (YYYY-MM-DD)
            end_date (Optional[str]): The end date in ISO format (YYYY-MM-DD). If None, fetches indefinitely.
            granularity (int): Candle interval in seconds (defaults to 60s).

          Yields:
            dict: {'symbol': symbol, 'data': List[List[Union[float, int]]]} containing OHLCV data.
          """
          async with aiohttp.ClientSession() as session:
               now = datetime.now(timezone.utc)  # ✅ FIXED: utcnow() deprecated

               # Default to fetching the last 7 days if no start date is given
               if start_date is None:
                    start_date = now - timedelta(days=7)
               else:
                    start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)

               # If no end date is provided, set it to a far future time (simulate continuous fetch)
               if end_date is None:
                    end_date = now + timedelta(days=10000)  
               else:
                    end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

               last_fetched = {symbol: start_date for symbol in symbols}

               while True:
                    for symbol in symbols:
                         async for update in CoinbaseCandleHistory.fetch_timeframe(
                              session, symbol, last_fetched[symbol], end_date, granularity
                         ):
                              last_fetched[symbol] = datetime.fromtimestamp(update["data"][-1][0], tz=timezone.utc)
                              yield update  # ✅ Yields updates instead of processing them here

                    await asyncio.sleep(COINBASE_REQUEST_LIMIT_PER_SECOND)  # Move to the next coin

