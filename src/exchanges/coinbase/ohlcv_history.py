from datetime import datetime, timezone, timedelta
from typing import Optional, List, AsyncGenerator, Dict, Literal, Union
import asyncio
import aiohttp
from aiohttp import ContentTypeError
from json import JSONDecodeError
from pathlib import Path

from loggers import logger_manager
from ..ohlcv_history import OHLCV_History as OHLCV_HistoryBase
from utils.algorithms import binary_search_first_occurrence_async
from utils.configs import CONFIG



class OHLCV_History(OHLCV_HistoryBase):

    COINBASE_OHLCV_URI = "https://api.exchange.coinbase.com/products/{}/candles"
    MAX_CANDLES = 300 # Max Candles allowed per request 
    TIMEOUT = 10 # Request timeout in seconds
  
    def __init__(self, product: str, granularity: int | None = 60, rate_limit: float = 1/8):
        granularity = granularity if granularity is not None else 60 # This line is required by the base class method `fetch_many`
        super().__init__(product, granularity)
        self._rate_limit = rate_limit
        self._session: Optional[aiohttp.ClientSession] = None   
        self._logger = logger_manager.get_logger(Path("coinbase", "ohlcv", f"{self._product}.log"))

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
        end_time: Optional[datetime] = None) -> List[int | float] | Literal["not_found", "api_failure", "no_data", "timeout_error", "rate_limited", "server_error", "network_error"]:
        """
        Fetches a specific time range of cryptocurrency candle data from Coinbase API.
        Args:
 
            start_time (datetime): The starting point for fetching data.
            end_time (datetime or None): The ending point for fetching data. Assigned start_time + MAX_CANDLES (minutes) if None provided.
 
        Returns:
            List[int | float]: list containing fetched OHLCV data (timestamp: int, open, low, high, close, volume).
            str: `"not_found"` if the coin pair wasn't found in database (404 error).
                 `"api_failure"` if the response status was not 200 or returned invalid JSON.
                 `"no_data"` if the response was successful but no candle data present in it.
                 `"timeout_error"` if the request took longer than TIMEOUT seconds.
                 `"rate_limited"` if the request was blocked due to API rate limits (429).
                 `"server_error"` if Coinbase returns a 5xx server error.
                 `"network_error"` if a netwrok error occurred.

        """
        if not self._session:
            raise RuntimeError("Session not initialized. Use 'async with OHLCV_History(...)'")

        url = OHLCV_History.COINBASE_OHLCV_URI.format(self._product)
        chunk_size = timedelta(minutes=OHLCV_History.MAX_CANDLES)

        if end_time is None or end_time <= start_time:
            end_time = start_time + chunk_size
        else:
            end_time = min(start_time + chunk_size, end_time)

        params = {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "granularity": self._granularity
        }
        headers = {
            "User-Agent": CONFIG.USER_AGENT,
            "Accept": "application/json",
            "X-Contact-Email": CONFIG.CONTACT_EMAIL,  
            "X-App-Version": CONFIG.VERSION,  
            "X-Repo-Link": CONFIG.REPO_LINK  
        }

        logger = self._logger

        try:
            async with self._session.get(url, params=params, headers=headers, timeout=self.TIMEOUT) as response:
                if response.status == 404:
                    return "not_found"

                if response.status == 429:
                    return "rate_limited"

                if response.status >= 500:
                    return "server_error"

                if response.status != 200:
                    return "api_failure" 

                try:
                    data = await response.json()
                except (JSONDecodeError, ContentTypeError):
                    return "api_failure" # f"⚠️ Malformed JSON response for {self._product}: ({response.status})"

                if not isinstance(data, list):
                    return "api_failure"

                if data:
                    logger.debug(f"📊 Downloaded {len(data)} candles for {self._product}: {start_time} → {end_time}")
                    return data

                return "no_data" 

        except asyncio.TimeoutError:
               return "timeout_error"  # Avoid getting stuck due to connection problems
          
        except aiohttp.ClientError as e:
            logger.error(f"🚨 Network error fetching {self._product}: {e}")
            return "network_error"
        

    
    async def fetch(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        default_start_date: str = "2012-01-01"
    ) -> AsyncGenerator[List[int | float], None]:
        """
        Sequentially fetches historical and live cryptocurrency data.
        """        
        now = datetime.now(timezone.utc)
        logger = self._logger

        if start_date is None:
            start_date = default_start_date

        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)

        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

        if end_date is None or end_date > now:
            end_date = now

        elif isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)

        logger.info(f"🫣 Seeking first occurrence of Coinbase data for {self._product} from {start_date} to {end_date}")

        async def condition(timestamp: float) -> bool:
            datetime_obj = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            response = await self.fetch_timeframe(datetime_obj)
            if not isinstance(response, list):
                return False   
            return True
        
        async def search(start_date: datetime, end_date: datetime):
            return await binary_search_first_occurrence_async(
                condition,
                start_date.timestamp(),
                end_date.timestamp(),
                max_depth=32
            )

        first_timestamp = await search(start_date, end_date)

        if first_timestamp == -1:
            logger.error(f"⚠️ No historical data found for {self._product} within the given range.")
            return

        logger.info(f"📡 Fetching historical data for {self._product} from {datetime.fromtimestamp(first_timestamp)} to {end_date} with {self._granularity}s granularity.")

        last_fetched = datetime.fromtimestamp(first_timestamp, tz=timezone.utc)
        finished = False
        
        while last_fetched <= end_date and not finished:
            result = await self.fetch_timeframe(last_fetched, end_date)
            if isinstance(result, str):  
                logger.error(f"🚨 Unexpected response type for {self._product}: {result}")
                last_fetched += timedelta(seconds=self._granularity)

                if result in ["api_failure", "timeout_error"]:
                    last_fetched += timedelta(seconds=OHLCV_History.MAX_CANDLES)  
                    logger.warning(f"⚠️ Fetching issue for {self._product} ({result}). Skipping to {last_fetched}") 
                    continue  
                
                if result == "not_found":
                    logger.error(f"🚫 {self._product} was not found")
                    break

                if result == "no_data":
                    last_fetched += timedelta(seconds=self._granularity)
                    if last_fetched > end_date:
                        logger.info(f"✅ Completed download for {self._product} on {datetime.now(timezone.utc)}")
                        return
                    
                    logger.warning(f"⚠️ No new data for {self._product}, searching next batch from {last_fetched} to {end_date}.")
                    first_timestamp = await search(last_fetched, end_date)
                 
                    if first_timestamp == -1:
                        logger.error(f"❌ End of ohlcv data for {self._product} was reached prematurely.")
                        break

                    last_fetched = datetime.fromtimestamp(first_timestamp, tz=timezone.utc)
                    logger.info(f"Found new block at {last_fetched}")
            else:
                fetched_timestamps = [candle[0] for candle in result] # In OHLCV data the first element is the timestamp
                new_last_fetched = datetime.fromtimestamp(max(fetched_timestamps), tz=timezone.utc)

                if new_last_fetched == last_fetched:  
                    new_last_fetched += timedelta(seconds=self._granularity)
                    logger.warning(f"⚠️ Stuck on {self._product} at {last_fetched}, forcing move to {new_last_fetched}")

                last_fetched = new_last_fetched  

                yield result
           
                if datetime.now(timezone.utc).date() == last_fetched.date():
                    logger.info(f"✅ Completed download for {self._product} on {now}")
                    finished = True
                    break  

                await asyncio.sleep(self._rate_limit)  
