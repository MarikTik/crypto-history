"""
File: ohlcv_database.py

This module manages the storage and compression of OHLCV (Open, High, Low, Close, Volume) data
in a two-phase approach:
1. Write incoming data for each product to a temporary SQLite database.
2. Periodically merge that data into a global Parquet file when the database exceeds a certain size
   or when switching to a new product.

Assumptions & Restrictions:
- The input data (`product_records["data"]`) must strictly follow the order
  [timestamp, open, high, low, close, volume] for each row.
- The timestamp must be in a format convertible to a Pandas datetime (no timezones, or if
  present, must be parseable by `pd.to_datetime`).
- The process is designed to handle one product at a time in sequence (e.g., finish BTC data,
  then move on to ETH).

Changed/Additional Notes:
- A `close()` method has been added to handle final cleanup when the `OHLCV_Database`
  instance goes out of scope. This method merges any unmerged SQLite data into Parquet
- Modified to keep the last connection open and close it when a new product arrives.

"""

from pathlib import Path
from typing import Dict, List, Optional
from .write_only_database import WriteOnlyDatabase

import pandas as pd
import sqlite3
import os


class OHLCV_Database(WriteOnlyDatabase):
    """Handles writing OHLCV data to temporary SQLite databases and merging them into Parquet.

    Attributes:
        _dir (Path): The root directory where the final Parquet files will be stored.
        _tempdir (Path): A subdirectory ("temp") used for storing temporary SQLite databases.
        _last_used_path (Optional[Path]): Keeps track of the last database file used
            (i.e., for the last product).
        _conn (Optional[sqlite3.Connection]): Keeps the current database connection.
    """

    def __init__(self, directory: Path):
        """Initializes the OHLCV_Database.

        Args:
            directory (Path): The directory where Parquet files and temporary SQLite databases
                should be stored. A subdirectory called 'temp' will be created if it
                does not exist.
        """
        super().__init__(directory)
        self._dir = directory
        self._tempdir = directory / "temp"
        self._tempdir.mkdir(parents=True, exist_ok=True)
        self._last_used_path: Optional[Path] = None
        self._conn: Optional[sqlite3.Connection] = None

    def write(
        self, product_records: Dict[str, str | List[List[float | int]]]
    ) -> None:
        """Appends new OHLCV data to a temporary SQLite database, possibly compresses it.

        This method:
            1. Checks if the last product differs from the current product. If so, it merges
               (compresses) the old product's SQLite data into its Parquet file first.
            2. Appends the new rows to the SQLite database for the current product.

        Args:
            product_records (Dict[str, str | List[List[float | int]]]):
                A dictionary with:
                - "product": The product symbol (e.g., "BTC-USD").
                - "data": A list of lists, each list containing
                    [timestamp, open, high, low, close, volume].
        """
        product: str = product_records["product"]
        ohlcv_dataframes = product_records["data"]

        temp_path = self._tempdir / f"{product}.db"

        if (
            self._last_used_path is not None
            and self._last_used_path != temp_path
        ):
            old_product = self._last_used_path.stem
            self._compress(old_product)
            if self._conn:
                self._conn.close()
                self._conn = None

        if self._conn is None:
            self._conn = sqlite3.connect(temp_path)

        cursor = self._conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ohlcv (
                timestamp INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
            """
        )
        cursor.executemany(
            "INSERT INTO ohlcv VALUES (?, ?, ?, ?, ?, ?)", ohlcv_dataframes
        )
        self._conn.commit()

        self._last_used_path = temp_path

    def _compress(self, product: str):
        """Reads the product's SQLite data, merges it with an existing Parquet (if any), and clears the SQLite.

        Steps:
            1. Reads the SQLite data into a DataFrame, assigning column names
               ["timestamp", "open", "high", "low", "close", "volume"].
            2. Sorts the DataFrame by timestamp and drops duplicates.
            3. Merges this DataFrame with the product's existing Parquet data (if it exists),
               again removing any duplicate timestamps.
            4. Writes the merged data back to Parquet in snappy-compressed format.
            5. Empties (truncates) the original SQLite database so it can be reused for new data.

        Args:
            product (str): The name of the product, used to find the corresponding SQLite
                (e.g., "BTC") and Parquet files.

        Note:
            The excessive amount of sorting in this method is to guard from mixed data frames writing, it is a safety measure.
        """
        db_path = self._tempdir / f"{product}.db"
        parquet_path = self._dir / f"{product}.parquet"

        conn = sqlite3.connect(db_path)
        temp_df = pd.read_sql_query("SELECT * FROM ohlcv", conn)
        conn.close()

        temp_df["timestamp"] = pd.to_datetime(
            temp_df["timestamp"], unit="s", utc=True
        )
        temp_df["timestamp"] = (
            temp_df["timestamp"].astype("int64") // 1_000_000_000
        ).astype("int32")
        temp_df.sort_values(by="timestamp", inplace=True)
        temp_df.drop_duplicates(subset=["timestamp"], inplace=True)

        if parquet_path.exists():  # Merge with existing parquet data
            prev_df = pd.read_parquet(parquet_path)

            # Ensure it's int32 in case older file still has another format
            if pd.api.types.is_datetime64_any_dtype(prev_df["timestamp"]):
                prev_df["timestamp"] = (
                    prev_df["timestamp"].astype("int64") // 1_000_000_000
                ).astype("int32")
            elif pd.api.types.is_integer_dtype(prev_df["timestamp"]):
                prev_df["timestamp"] = prev_df["timestamp"].astype("int32")
            else:
                raise ValueError(
                    f"❌ Unsupported timestamp format in {product}"
                )

            merged_df = pd.concat([prev_df, temp_df], ignore_index=True)
            merged_df.drop_duplicates(
                subset=["timestamp"], inplace=True
            )  # remove duplicates if there's any chance of overlap
            merged_df.sort_values(by="timestamp", inplace=True)
        else:
            merged_df = temp_df

        merged_df.to_parquet(
            parquet_path, index=False, compression="snappy"
        )  # Write back to Parquet

        os.remove(db_path)

    def __enter__(self) -> "OHLCV_Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """
        Merges any remaining SQLite data into Parquet and deletes the temporary database files.

        This should be called before discarding the OHLCV_Database instance to ensure
        no recent data remains unmerged and to remove the temp folder completely.

        Steps:
            1. If there is a `_last_used_path`, compress (merge) the associated SQLite
                data into its Parquet file.
            2. Remove all SQLite databases from the 'temp' directory.
        """
        if self._last_used_path is not None:
            if os.path.exists(self._last_used_path):
                conn = sqlite3.connect(self._last_used_path)
                cursor = conn.cursor()
                cursor.execute("SELECT count(*) FROM ohlcv")
                row_count = cursor.fetchone()[0]
                conn.close()
                if row_count > 0:
                    last_product = self._last_used_path.stem
                    self._compress(last_product)

        for file_path in self._tempdir.glob("*.db"):
            os.remove(file_path)
