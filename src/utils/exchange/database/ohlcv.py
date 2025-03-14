from pathlib import Path
import csv
import pandas as pd
from typing import Dict, List, Tuple
from io import TextIOWrapper
from datetime import datetime
from dateutil.relativedelta import relativedelta
from ..ohlcv_history import OHLCV_History
from .database import Database

class OHLCV_Database(Database):
    def __init__(self, directory: Path):
        super().__init__(directory)
        self._dir = directory
        self._tempdir = directory / "temp"
        self._tempdir.mkdir(parents=True, exist_ok=True)
        self._file_handles: Dict[Path, Tuple[TextIOWrapper, datetime]] = {}

    def insert(self, product_records: Dict[str, str | List[List[float | int]]]) -> None:
        product: str = product_records["product"]
        ohlcv_dataframes = product_records["data"]

        temp_path = self._tempdir / product / ".csv"
        temp, last_write = self._file_handles.setdefault(temp_path, (temp_path.open("w"), datetime.now()))

        if temp.closed:
            temp = temp_path.open("w")

        writer = csv.writer(temp)
        writer.writerows(ohlcv_dataframes)

        if relativedelta(last_write, datetime.now()).months >= 1:
            self._compress(product)
        
        

        self._close_handlers(except_path=temp) # cleanup

    def _close_handlers(self, except_path: Path):
        for path, (text_io_wrapper, _) in self._file_handles.items():
            if path != except_path:
                text_io_wrapper.close()

    def _compress(self, product: str):
        csv_path = self._tempdir / product / ".csv"
        parquet_path = self._dir / product / ".parquet"
        temp_df = pd.read_csv(str(csv_path))
        temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"]) # Convert timestamp column to a uniform format
        temp_df = temp_df.sort_values(by="timestamp").drop_duplicates(subset=["timestamp"])
        if parquet_path.exists():  # Load previous data
            prev_df = pd.read_parquet(parquet_path)
            prev_df["timestamp"] = pd.to_datetime(prev_df["timestamp"])

            merged_df = pd.concat([prev_df, temp_df])  # No need to sort again
        else:
            merged_df = temp_df  # No previous data, just use the CSV
         
        merged_df.to_parquet(str(parquet_path), index=False)
