"""
File: ohlcv_database.py

This module manages the storage and compression of OHLCV (Open, High, Low, Close, Volume) data 
in a two-phase approach:
1. Write incoming data for each product to a temporary CSV file.
2. Periodically merge that CSV into a global Parquet file when the CSV exceeds a certain size 
   or when switching to a new product.

Assumptions & Restrictions:
- The input data (`product_records["data"]`) must strictly follow the order 
  [timestamp, open, high, low, close, volume] for each row.
- The timestamp must be in a format convertible to a Pandas datetime (no timezones, or if 
  present, must be parseable by `pd.to_datetime`).
- If the CSV does not contain headers, the code will explicitly assign column names 
  ["timestamp", "open", "high", "low", "close", "volume"].
- The process is designed to handle one product at a time in sequence (e.g., finish BTC data, 
  then move on to ETH).
- Sorting is performed by timestamp. If the data is already sorted, the performance cost 
  of sorting in Pandas may still be non-negligible, but is typically reduced.

"""

from pathlib import Path
from typing import Dict, List, Optional
from .write_only_database import WriteOnlyDatabase

import csv
import pandas as pd

class OHLCV_Database(WriteOnlyDatabase):
    """Handles writing OHLCV data to temporary CSV files and merging them into Parquet.

    Attributes:
        TEMPORARY_FILE_COMPRESSION_THRESHOLD_IN_BYTES (int): The maximum size (in bytes) 
            before the CSV is compressed/merged into Parquet. Defaults to 10 MB.
        _dir (Path): The root directory where the final Parquet files will be stored.
        _tempdir (Path): A subdirectory ("temp") used for storing temporary CSVs.
        _last_used_path (Optional[Path]): Keeps track of the last CSV file used 
            (i.e., for the last product).
    """
    TEMPORARY_FILE_COMPRESSION_THRESHOLD_IN_BYTES = 10 * 1024 ** 2 # 10 Mb
    def __init__(self, directory: Path):
        """Initializes the OHLCV_Database.

        Args:
            directory (Path): The directory where Parquet files and temporary CSVs 
                should be stored. A subdirectory called 'temp' will be created if it 
                does not exist.
        """
        super().__init__(directory)
        self._dir = directory
        self._tempdir = directory / "temp"
        self._tempdir.mkdir(parents=True, exist_ok=True)
        self._last_used_path: Optional[Path] = None
        
    def insert(self, product_records: Dict[str, str | List[List[float | int]]]) -> None:
        """Appends new OHLCV data to a temporary CSV, possibly compresses it.

        This method:
         1. Checks if the last product differs from the current product. If so, it merges
            (compresses) the old product's CSV into its Parquet file first.
         2. Appends the new rows to the CSV for the current product.
         3. Checks if the CSV file size exceeds the threshold (`TEMPORARY_FILE_COMPRESSION_THRESHOLD_IN_BYTES`).
            If so, it calls _compress to merge data into the Parquet file.

        Args:
            product_records (Dict[str, str | List[List[float | int]]]): 
                A dictionary with:
                - "product": The product symbol (e.g., "BTC-USD").
                - "data": A list of lists, each list containing 
                          [timestamp, open, high, low, close, volume].
        """
        product: str = product_records["product"]
        ohlcv_dataframes = product_records["data"]

        temp_path = self._tempdir / f"{product}.csv"
        

        if self._last_used_path is not None and self._last_used_path != temp_path:
            old_product = self._last_used_path.stem
            self._compress(old_product)

        with temp_path.open("a", newline="") as temp_file:
            writer = csv.writer(temp_file)
            writer.writerows(ohlcv_dataframes)
            temp_file.flush()

        if temp_path.stat().st_size > OHLCV_Database.TEMPORARY_FILE_COMPRESSION_THRESHOLD_IN_BYTES:
            self._compress(product)
        
        self._last_used_path = temp_path
 
    def _compress(self, product: str):
        """Reads the product's CSV, merges it with an existing Parquet (if any), and clears the CSV.

        Steps:
         1. Reads the CSV into a DataFrame, assigning column names 
            ["timestamp", "open", "high", "low", "close", "volume"].
         2. Sorts the DataFrame by timestamp and drops duplicates.
         3. Merges this DataFrame with the product's existing Parquet data (if it exists),
            again removing any duplicate timestamps.
         4. Writes the merged data back to Parquet in snappy-compressed format.
         5. Empties (truncates) the original CSV file so it can be reused for new data.

        Args:
            product (str): The name of the product, used to find the corresponding CSV 
                (e.g., "BTC") and Parquet files.
        
        Note:
            The excessive amount of sorting in this method is to guard from mixed data frames writing, it is a safety measure.
        """
        csv_path = self._tempdir / f"{product}.csv"
        parquet_path = self._dir / f"{product}.parquet"

        temp_df = pd.read_csv(csv_path, names=["timestamp","open","high","low","close","volume"])
        temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"], utc=True)
        temp_df.sort_values(by="timestamp", inplace=True)
        temp_df.drop_duplicates(subset=["timestamp"], inplace=True)

       
        if parquet_path.exists():  # Merge with existing parquet data
            prev_df = pd.read_parquet(parquet_path)
            prev_df["timestamp"] = pd.to_datetime(prev_df["timestamp"], utc=True)

            merged_df = pd.concat([prev_df, temp_df], ignore_index=True)
            merged_df.drop_duplicates(subset=["timestamp"], inplace=True) # remove duplicates if there's any chance of overlap
            merged_df.sort_values(by="timestamp", inplace=True)
        else:
            merged_df = temp_df

        merged_df.to_parquet(parquet_path, index=False, compression="snappy") # Write back to Parquet

        with csv_path.open("w"): # Clear the CSV
            pass
        
       