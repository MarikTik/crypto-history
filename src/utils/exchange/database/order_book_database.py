from pathlib import Path
from typing import Dict, Any
from .write_only_database import WriteOnlyDatabase

class OrderBookDatabase(WriteOnlyDatabase):
     
    def __init__(self, directory: Path):
        """
        Initializes the database with a specified storage directory.

        Args:
            directory (Path): The root directory for storing OHLCV or order book data.
                              If the directory does not exist, it will be created.
        """
        directory.mkdir(parents=True, exist_ok=True)
        self._directory = directory

    
    def write(self, product_records: Dict[str, Any]) -> None:
        pass

 
    def __enter__(self):
        return self

   
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass