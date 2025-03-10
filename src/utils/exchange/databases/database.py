from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime



class Database(ABC):
    def __init__(self, directory: Path):
        directory.mkdir(parents=True, exist_ok=True)
        self._directory = directory

    @abstractmethod
    def insert(self, records: Dict[str, Any]) -> None:
        """Inserts OHLCV or order book data into the database.

        Args:
            records (Dict[str, Any]): Dictionary where keys are symbols (e.g., "BTC-USD")
                                      and values are corresponding OHLCV or order book data.
        """
        pass

    @abstractmethod
    def query(self, symbol: str, from_timestamp: Optional[str | datetime | int], to_timestamp: Optional[str | datetime | int] = None):
        """Queries OHLCV or order book data for a specific cryptocurrency.

        Args:
            symbol (str): The trading pair (e.g., "BTC-USD").
            start_time (str | datetime): The starting datetime for data retrieval.
            end_time (str | datetime, optional): The ending datetime (defaults to now if not provided).
        
        Returns:
            Any: The queried data.
        """
        pass

    def to_unix_timestamp(ts: int | float | str | datetime, to_int: bool = True) -> int | float:
        """Converts a given timestamp into a uniform UNIX timestamp.

        Args:
            ts (int | float | str | datetime): Input timestamp in various formats:
                - `int` → Assumes epoch seconds.
                - `float` → Assumes epoch seconds with milliseconds.
                - `str` → Converts from ISO 8601 format (`YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD`).
                - `datetime` → Converts to UNIX timestamp.
            to_int (bool): If True, rounds to an integer (seconds precision). If False, keeps float (millisecond precision).

        Returns:
            int | float: A standardized UNIX timestamp.
        """
    
        if isinstance(ts, (int, float)):
            return int(ts) if to_int else float(ts)  # Ensure correct type

        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts)
            except ValueError:
                raise ValueError(f"Invalid timestamp format: {ts}. Expected ISO 8601 format.")
            return int(dt.timestamp()) if to_int else dt.timestamp()

        if isinstance(ts, datetime):
            return int(ts.timestamp()) if to_int else ts.timestamp()

        raise TypeError(f"Unsupported timestamp type: {type(ts)}")