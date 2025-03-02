import pyarrow as pa
import pyarrow.dataset as ds
import duckdb
from deltalake import write_deltalake
from pathlib import Path
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, List
from utils.loggers.logger import logger_manger

class CoinDB:
    def __init__(self, dir: Path):
        """
        Initializes CoinDB for storing cryptocurrency data in Delta Lake format.

        Args:
            dir (Path): The root directory where data will be stored.
        """
        self._dir = Path(dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self.buffers = {}  # Store in-memory batches for each coin & month

    async def store(self, gen: AsyncGenerator[Dict[str, List[List[float | int]]], None]):
        """
        Stores streamed cryptocurrency data from an async generator into Delta Lake partitions.

        Args:
            gen (AsyncGenerator[Dict[str, List[List[float | int]]], None]): 
                An async generator yielding OHLCV data.
        """
        active_month = {}  # Track the current active month for each symbol

        async for update in gen:
            symbol = update["symbol"]
            data = update["data"]

            if not data:
                continue  # Skip if no new data

            # Determine timestamp and partition (year, month)
            last_candle_ts = data[-1][0]  # Last row timestamp in batch
            timestamp = datetime.fromtimestamp(last_candle_ts, tz=timezone.utc)
            year, month = timestamp.strftime("%Y"), timestamp.strftime("%m")

            key = (symbol, year, month)
            if key not in self.buffers:
                self.buffers[key] = []

            # Check if we have switched to a new month
            if symbol in active_month and active_month[symbol] != (year, month):
                prev_year, prev_month = active_month[symbol]
                await self._flush(symbol, prev_year, prev_month)  # 🔹 Flush previous month

            # Update active month for this symbol
            active_month[symbol] = (year, month)

            self.buffers[key].extend(data)  # Append new data **after flushing previous month**

    async def _flush(self, symbol: str, year: str, month: str):
        """
        Flushes accumulated data for a given month to Delta Lake.

        Args:
            symbol (str): The cryptocurrency symbol (e.g., BTC-USD).
            year (str): The year of the data.
            month (str): The month of the data.
        """
        logger = logger_manger.get_logger(Path("logs", "coinbase", symbol))
        key = (symbol, year, month)
        if key not in self.buffers or not self.buffers[key]:
            return  # Nothing to flush
        
        delta_path = str(self._dir / symbol / year / f"{month}.parquet")
        self._dir.joinpath(symbol, year).mkdir(parents=True, exist_ok=True)

        # Convert to Arrow Table
        table = pa.Table.from_pydict(
            {
                "time": [row[0] for row in self.buffers[key]],
                "low": [row[1] for row in self.buffers[key]],
                "high": [row[2] for row in self.buffers[key]],
                "open": [row[3] for row in self.buffers[key]],
                "close": [row[4] for row in self.buffers[key]],
                "volume": [row[5] for row in self.buffers[key]],
                "timestamp": [datetime.fromtimestamp(row[0], tz=timezone.utc) for row in self.buffers[key]]
            }
        )

   
        write_deltalake(delta_path, table, mode="append")
        logger.info(f"✅ Stored {symbol} data for {year}-{month}")

        # Clear the buffer for this month
        self.buffers[key] = []

    def query(self, symbol: str, start_date: str | datetime, end_date: str | datetime):
        """
        Queries historical data efficiently using DuckDB.

        Args:
            symbol (str): Cryptocurrency pair (e.g., "BTC-USD").
            start_date (datetime): Start of the query range.
            end_date (datetime): End of the query range.

        Returns:
            pd.DataFrame: Query results as a Pandas DataFrame.
        """


        if type(start_date) is str:
            start_date = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
        if type(end_date) is str:
            end_date = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) 

        query = f"""
        SELECT * FROM read_parquet('{self._dir}/{symbol}/**/*.parquet')
        WHERE timestamp BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
        ORDER BY timestamp ASC
        """

        return duckdb.query(query).to_df()