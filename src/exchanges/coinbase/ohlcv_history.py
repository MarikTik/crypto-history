import utils.exchange as exchange
from datetime import datetime, timezone, timedelta
from typing import Optional, List, AsyncGenerator, Dict, Iterable, Literal
import asyncio
import aiohttp
from aiohttp import ContentTypeError
from json import JSONDecodeError
from pathlib import Path

from utils.loggers.logger import logger_manger
from utils.algorithms import binary_search_first_occurrence_async
from utils.configs import CONFIG



class OHLCV_History(exchange.OHLCV_History):

    COINBASE_OHLCV_URI = "https://api.exchange.coinbase.com/products/{}/candles"
    MAX_CANDLES = 300 # Max Candles allowed per request 
    TIMEOUT = 10 # Request timeout in seconds
  
    def __init__(self, product: str, rate_limit: float):
        super.__init__(product)
        self._rate_limit = rate_limit
        self._session: Optional[aiohttp.ClientSession] = None   

    async def __aenter__(self):
        """Async context manager entry: Create session."""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit: Close session properly."""
        if self._session:
            await self._session.close()
            self._session = None
   
    async def fetch_timeframe(
        self,
        start_time: datetime,
        end_time: Optional[datetime] = None,
        granularity: int = 60) -> Dict[str, str | List[List[float | int]]] | Literal["not_found", "api_failure", "no_data", "timeout_error"]:
        """
        Fetches a specific time range of cryptocurrency candle data from Coinbase API.
        Args:
 
            start_time (datetime): The starting point for fetching data.
            end_time (datetime or None): The ending point for fetching data. Assigned start_time + MAX_CANDLES (minutes) if None provided.
            granularity (int): The candle interval in seconds (defaults to 60s).
 
        Returns:
            dict: {'self._product': self._product, 'data': List[List]} containing fetched OHLCV data.
            str: `"not_found"` if the coin pair wasn't found in database (404 error).
                 `"api_failure"` if the response status was not 200 or returned invalid JSON.
                 `"no_data"` if the response was successful but no candle data present in it.
                 `"timeout_error"` if the request took longer than TIMEOUT seconds.
                 `"rate_limited"` if the request was blocked due to API rate limits (429).
                 `"server_error"` if Coinbase returns a 5xx server error.

        """
        url = OHLCV_History.COINBASE_OHLCV_URI.format(self._product)
        chunk_size = timedelta(minutes=OHLCV_History.MAX_CANDLES)

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

        logger = logger_manger.get_logger(Path("logs", "coinbase", "ohlcv", f"{self._product}.log"))

        try:
            async with self._session.get(url, params=params, headers=headers, timeout=self.TIMEOUT) as response:
                if response.status == 404:
                    logger.critical(f"❌ {self._product} not found in database")
                    return "not_found"

                if response.status == 429:
                    logger.warning(f"🔄 Rate limit hit for {self._product}. Coinbase suggests retrying later.")
                    return "rate_limited"

                if response.status >= 500:
                    logger.error(f"⚠️ Server error {response.status} for {self._product}.")
                    return "server_error"

                if response.status != 200:
                    logger.error(f"⚠️ fetching {self._product}: ({response.status}) {await response.text()}")
                    return "api_failure" 

                try:
                    data = await response.json()
                except (JSONDecodeError, ContentTypeError):
                    logger.error(f"⚠️ Malformed JSON response for {self._product}: ({response.status})")
                    return "api_failure"

                if not isinstance(data, list):
                    logger.error(f"⚠️ Unexpected response format for {self._product}: {data}")
                    return "api_failure"

                if data:
                    logger.debug(f"📊 Downloaded {len(data)} candles for {self._product}: {start_time} → {end_time}")
                    return {"self._product": self._product, "data": data}

              
                logger.warning(f"⚠️ No data for {self._product}: {start_time} → {end_time}")
                return "no_data" 

        except asyncio.TimeoutError:
               logger.error(f"⏳ Timeout fetching data for {self._product}: {start_time} → {end_time}. Retrying later.")
               return "timeout_error"  # Avoid getting stuck due to connection problems
          
        except aiohttp.ClientError as e:
            logger.error(f"🚨 Network error fetching {self._product}: {e}")
            return "api_failure"
          