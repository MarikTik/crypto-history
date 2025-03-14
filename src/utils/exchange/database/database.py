"""
Abstract Base Class for OHLCV and Order Book Databases.

This module defines `Database`, an abstract base class (ABC) that serves as a foundation 
for implementing structured storage of financial market data, specifically:
- **OHLCV Data**: Open-High-Low-Close-Volume historical data.
- **Order Book Data**: Real-time order book snapshots.

### Key Functionalities:
- `insert()`: Inserts OHLCV or order book data into the database.
- `query()`: Retrieves stored data for a specified timeframe.

### Intended Usage:
This class should be subclassed to implement concrete database solutions (e.g., Delta Lake, 
DuckDB, SQLite). It provides a common interface for inserting and retrieving financial data 
efficiently.

#### Example Subclass Implementation:
```python
class OHLCVDatabase(Database):
    def insert(self, records: Dict[str, Any]) -> None:
        pass  # Custom implementation

    def query(self, symbol: str, from_timestamp: int, to_timestamp: int) -> Any:
        pass  # Custom implementation

"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime



class Database(ABC):
    """
    Abstract Base Class for Financial Data Storage.

    This class provides a structured framework for storing and retrieving OHLCV 
    (Open-High-Low-Close-Volume) or Order Book data. It is designed to be subclassed, 
    allowing for specific database implementations.

    Attributes:
        _directory (Path): The root directory where data is stored.

    Methods:
        insert(records: Dict[str, Any]) -> None:
            Abstract method for inserting data into the database.
        
        query(symbol: str, from_timestamp: Optional[int | float | str | datetime], 
              to_timestamp: Optional[int | float | str | datetime] = None) -> Any:
            Abstract method for querying stored data within a given time range.
    """

    def __init__(self, directory: Path):
        """
        Initializes the database with a specified storage directory.

        Args:
            directory (Path): The root directory for storing OHLCV or order book data.
                              If the directory does not exist, it will be created.
        """
        directory.mkdir(parents=True, exist_ok=True)
        self._directory = directory

    @abstractmethod
    def insert(self, product_records: Dict[str, Any]) -> None:
        """Inserts OHLCV or order book data into the database.

        Args:
            product_records (Dict[str, Any]): Dictionary where keys are symbols (e.g., "BTC-USD")
                                      and values are corresponding OHLCV or order book data.
        """
        pass

    @abstractmethod
    def query(self, product: str, from_timestamp: Optional[str | datetime | int], to_timestamp: Optional[str | datetime | int] = None):
        """Queries OHLCV or order book data for a specific cryptocurrency.

        Args:
            product (str): The trading pair (e.g., "BTC-USD").
            start_time (str | datetime): The starting datetime for data retrieval.
            end_time (str | datetime, optional): The ending datetime (defaults to now if not provided).
        
        Returns:
            Any: The queried data.
        """
        pass