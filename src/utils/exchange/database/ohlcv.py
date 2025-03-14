from pathlib import Path
from typing import Dict, List
from ..ohlcv_history import OHLCV_History
from .database import Database

class OHLCV_Database(Database):
    def __init__(self, directory: Path):
        super().__init__(directory)
        self._tempdir = directory / "temp"
        self._tempdir.mkdir(parents=True, exist_ok=True)
        self._file_handles = {}

    def insert(self, records: Dict[str, str | List[List[float | int]]]) -> None:
        product: str = records["product"]
        data = records["data"]
        path = self._tempdir / product / ".csv"
