"""
ohlcv_history.py

Implements the `OHLCV_History` abstract base class for the Coinbase exchange.
Provides logic to asynchronously fetch historical and live OHLCV (Open, High, Low, Close, Volume)
data from the Coinbase API.

Classes:
    - OHLCV_History: Coinbase-specific subclass of the base OHLCV_History class.

Functionality:
    - Fetches data in time chunks respecting Coinbase's rate and candle limits
    - Handles network errors, malformed responses, rate limiting, and retries
    - Performs a binary search to locate the first available timestamp in history

Constants:
    - COINBASE_OHLCV_URI: Base URI for Coinbase candle data
    - MAX_CANDLES: Maximum candles per API request
    - TIMEOUT: Request timeout
    - REQUEST_RATE_LIMIT: Request frequency limit (1 per 125ms)
    - NETWORK_COOLDOWN_AFTER_ERROR: Cooldown after certain errors

Example:
    async with OHLCV_History("BTC-USD", granularity=60) as ohlcv:
        async for batch in ohlcv.fetch("2022-01-01", "2022-01-02"):
            process(batch)
"""
from datetime import datetime, timezone, timedelta
from typing import Optional, List, AsyncGenerator, Literal, Union, Dict
import asyncio
import aiohttp
from aiohttp import ContentTypeError
from json import JSONDecodeError
from pathlib import Path

from ..ohlcv_history import OHLCV_History as OHLCV_HistoryBase
from utils.algorithms import binary_search_first_occurrence_async
from utils.configs import CONFIG


class OHLCV_History(OHLCV_HistoryBase):
    """Coinbase-specific implementation of the OHLCV_History abstract base class.

    This class provides asynchronous access to OHLCV candle data from the Coinbase
    exchange using its REST API. It handles batching, rate limiting, error handling,
    and chunked downloads over a specified time range.

    Attributes:
        COINBASE_OHLCV_URI (str): URI pattern for Coinbase candle API.
        MAX_CANDLES (int): Maximum number of candles per request (300).
        TIMEOUT (int): Request timeout duration in seconds.
        REQUEST_RATE_LIMIT (float): Maximum number of requests per second.
        NETWORK_COOLDOWN_AFTER_ERROR (int): Seconds to wait after a network error.
        _session (Optional[aiohttp.ClientSession]): AIOHTTP session used for API calls.

    Methods:
        __aenter__(): Creates the aiohttp session for making API requests.
        __aexit__(): Closes the session cleanly.
        fetch_timeframe(): Downloads a fixed chunk of candle data.
        fetch(): Performs full sequential data fetching across a time range.
        _adjust_end_time(): Ensures end time does not exceed API chunk size.
        _parse_response(): Parses and validates the API JSON response.
        _normalize_date(): Converts ISO strings to timezone-aware datetimes.
        _find_first_valid_timestamp(): Binary search for the first available candle.
        _handle_fetch_error(): Handles retry, skip, or termination logic on error.
    """
    COINBASE_OHLCV_URI = "https://api.exchange.coinbase.com/products/{}/candles"
    MAX_CANDLES = 300  # Max Candles allowed per request
    TIMEOUT = 10  # Request timeout in seconds
    REQUEST_RATE_LIMIT = 1 / 8  # 8 requests per second
    NETWORK_COOLDOWN_AFTER_ERROR = 10  # Seconds

    def __init__(
        self,
        product: str,
        granularity: int | None = 60,
        log_dir=Path("logs", "coinbase", "ohlcv"),
    ):
        super().__init__(
            product, granularity if granularity is not None else 60, log_dir
        )
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
        self, start_time: datetime, end_time: Optional[datetime] = None
    ) -> Union[
        List[List[int | float]],
        Literal[
            "not_found",
            "api_failure",
            "no_data",
            "timeout_error",
            "rate_limited",
            "server_error",
            "network_error",
        ],
    ]:
        """
        Fetches a specific time range of cryptocurrency candle data from Coinbase API.
        Args:

            start_time (datetime): The starting point for fetching data.
            end_time (datetime or None): The ending point for fetching data. Assigned start_time + MAX_CANDLES (minutes) if None provided.

        Returns:
            Union[List[List[int | float]], Literal(str)]: list containing fetched OHLCV data (timestamp: int, open, low, high, close, volume).
            str: `"not_found"` if the coin pair wasn't found in database (404 error).
                 `"api_failure"` if the response status was not 200 or returned invalid JSON.
                 `"no_data"` if the response was successful but no candle data present in it.
                 `"timeout_error"` if the request took longer than TIMEOUT seconds.
                 `"rate_limited"` if the request was blocked due to API rate limits (429).
                 `"server_error"` if Coinbase returns a 5xx server error.
                 `"network_error"` if a netwrok error occurred.
        """
        if not self._session:
            raise RuntimeError(
                "Session not initialized. Use 'async with OHLCV_History(...)'"
            )

        url = self.COINBASE_OHLCV_URI.format(self._product)
        end_time = self._adjust_end_time(start_time, end_time)
        params = self._build_params(start_time, end_time)
        headers = self._build_headers()

        try:
            async with self._session.get(
                url, params=params, headers=headers, timeout=self.TIMEOUT
            ) as response:
                result = await self._parse_response(response)
                return result
        except asyncio.TimeoutError:
            return "timeout_error"
        except aiohttp.ClientError:
            return "network_error"

    async def fetch(
        self,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
        default_start_date: str = "2012-01-01",
    ) -> AsyncGenerator[List[List[int | float]], None]:
        """Sequentially fetches historical and live OHLCV data from Coinbase.

        This method identifies the first valid data timestamp using a binary search,
        and then iteratively downloads batches of OHLCV data until the specified
        end time is reached. It handles errors, retry conditions, and adjusts
        for API limits and known edge cases.

        Args:
            start_date (Union[str, datetime] or None): Start of the range to fetch.
                Can be a string in ISO format or a datetime object. If None, uses `default_start_date`.
            end_date (Union[str, datetime] or None): End of the range to fetch.
                Defaults to current UTC time if not provided.
            default_start_date (str): Fallback ISO string used when `start_date` is None.

        Yields:
            List[List[int | float]]: A list of OHLCV candles, each represented as a list:
            [timestamp, open, low, high, close, volume].

        Logs:
            - Missing data
            - Fetch errors
            - Rate limiting and server issues
            - Completion status

        Stops:
            - When end of data is reached
            - When unrecoverable error like "not_found" or premature data exhaustion occurs
        """
        now = datetime.now(timezone.utc)
        logger = self._logger

        start_dt = self._normalize_date(start_date or default_start_date)
        end_dt = self._normalize_date(end_date or now)
        end_dt = min(end_dt, now)
        logger.info(
            f"🫣 Seeking first occurrence of Coinbase data for {self._product} from {start_dt} to {end_dt}"
        )
        first_timestamp = await self._find_first_valid_timestamp(
            start_dt, end_dt
        )

        if first_timestamp == -1:
            logger.error(
                f"⚠️ No historical data found for {self._product} within the given range."
            )
            return

        logger.info(
            f"📡 Fetching historical data for {self._product} from {datetime.fromtimestamp(first_timestamp)} to {end_dt} with {self._granularity}s granularity."
        )

        last_fetched = datetime.fromtimestamp(first_timestamp, tz=timezone.utc)
        finished = False

        while last_fetched <= end_date and not finished:
            result = await self.fetch_timeframe(last_fetched, end_date)
            if isinstance(result, str):
                should_continue, new_last_fetched = (
                    await self._handle_fetch_error(result, last_fetched, end_dt)
                )
                if not should_continue:
                    break
                last_fetched = new_last_fetched
                continue

            self._logger.debug(
                f"📊 Downloaded {len(result)} candles for {self._product}: {last_fetched} → {self._adjust_end_time(last_fetched, end_date)}"
            )
            fetched_timestamps = [
                candle[0] for candle in result
            ]  # In OHLCV data the first element is the timestamp
            new_last_fetched = datetime.fromtimestamp(
                max(fetched_timestamps), tz=timezone.utc
            )
            if new_last_fetched == last_fetched:
                new_last_fetched += timedelta(seconds=self._granularity)
                logger.warning(
                    f"⚠️ Stuck on {self._product} at {last_fetched}, forcing move to {new_last_fetched}"
                )

            last_fetched = new_last_fetched
            yield result

            if now.date() == last_fetched.date():
                logger.info(
                    f"✅ Completed download for {self._product} on {now}"
                )
                finished = True
                break

            await asyncio.sleep(OHLCV_History.REQUEST_RATE_LIMIT)

    def _build_params(self, start_time: datetime, end_time: datetime) -> Dict:
        return {
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "granularity": self._granularity,
        }

    def _build_headers(self) -> Dict:
        return {
            "User-Agent": CONFIG.USER_AGENT,
            "Accept": "application/json",
            "X-Contact-Email": CONFIG.CONTACT_EMAIL,
            "X-App-Version": CONFIG.VERSION,
            "X-Repo-Link": CONFIG.REPO_LINK,
        }

    def _adjust_end_time(
        self, start_time: datetime, end_time: Optional[datetime]
    ) -> datetime:
        chunk = timedelta(minutes=self.MAX_CANDLES)
        if end_time is None or end_time <= start_time:
            return start_time + chunk
        return min(start_time + chunk, end_time)

    async def _parse_response(
        self, response: aiohttp.ClientResponse
    ) -> Union[List[List[int | float]], str]:
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
            return "api_failure"

        if not isinstance(data, list):
            return "api_failure"
        if not data:
            return "no_data"

        return data

    def _normalize_date(self, date: Union[str, datetime]) -> datetime:
        if isinstance(date, str):
            return datetime.fromisoformat(date).replace(tzinfo=timezone.utc)
        return date.replace(tzinfo=timezone.utc)

    async def _find_first_valid_timestamp(
        self, start: datetime, end: datetime
    ) -> float:
        async def condition(ts: float) -> bool:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            response = await self.fetch_timeframe(dt)
            return isinstance(response, list)

        return await binary_search_first_occurrence_async(
            condition, start.timestamp(), end.timestamp(), max_depth=32
        )

    async def _handle_fetch_error(
        self,
        error_type: str,
        last_fetched: datetime,
        end_time: datetime,
    ) -> tuple[bool, datetime]:
        logger = self._logger
        new_last_fetched = last_fetched + timedelta(seconds=self._granularity)

        match error_type:
            case "api_failure" | "timeout_error":
                skip_to = last_fetched + timedelta(seconds=self.MAX_CANDLES)
                logger.warning(
                    f"⚠️ Fetching issue for {self._product} ({error_type}). Skipping to {skip_to}"
                )
                return True, skip_to

            case "not_found":
                logger.error(f"🚫 {self._product} was not found")
                return False, last_fetched  # Stop loop

            case "network_error":
                logger.error(f"🚨 Network error fetching {self._product}")
                await asyncio.sleep(self.NETWORK_COOLDOWN_AFTER_ERROR)
                return True, new_last_fetched

            case "no_data":
                new_last_fetched = last_fetched + timedelta(
                    seconds=self._granularity
                )
                # Edge case, if the error was caused by crossing the end date boundary and end date is very close to datetime.now()
                if new_last_fetched > end_time:
                    logger.info(
                        f"✅ Completed download for {self._product} on {datetime.now(timezone.utc)}"
                    )
                    return False, last_fetched  # Stop loop

                logger.warning(
                    f"⚠️ No new data for {self._product}, searching next batch from {new_last_fetched} to {end_time}."
                )
                next_ts = await self._find_first_valid_timestamp(
                    new_last_fetched, end_time
                )
                if next_ts == -1:
                    logger.error(
                        f"❌ End of ohlcv data for {self._product} was reached prematurely."
                    )
                    return False, last_fetched  # Stop loop

                new_start = datetime.fromtimestamp(next_ts, tz=timezone.utc)
                logger.info(f"Found new block at {new_start}")
                return True, new_start

            case _:
                logger.error(f"❓ Unknown error type: {error_type}")
                return True, new_last_fetched
