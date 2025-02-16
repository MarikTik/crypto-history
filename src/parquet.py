"""
parquet.py

This module provides a Parquet class to handle conversion of financial data 
from CSV and list formats to Parquet format efficiently.

Features:
- Convert CSV files to Parquet
- Convert lists of historical price data to Parquet
- Ensure sorting of data before saving
- Use Apache Arrow for optimized storage

Usage:
    from parquet import Parquet

    # Example usage
    SCHEMA = pa.schema([
        ("time", pa.int64()),
        ("low", pa.float64()),
        ("high", pa.float64()),
        ("open", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("timestamp", pa.timestamp("s"))
    ])

    nonlocal gen
    Parquet.from_generator(gen=gen, columns=COLUMNS, schema=SCHEMA, sort_by=SORT_KEY).to(Path(dir) / symbol)
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import psutil
from pathlib import Path
from typing import Generator, AsyncGenerator, List

class Parquet:
    def __init__(self, schema=None):
        """Initialize a Parquet object with an optional DataFrame."""
        self.schema = schema

    @staticmethod
    def from_generator(
        gen: Generator[List[List], None, None] | AsyncGenerator[List[List], None],
        columns: List[str],
        schema: pa.Schema,
        sort_by: str = None
        ) -> "Parquet":
        """
        Streams data from a generator (sync or async) and prepares it for writing.

        Args:
            gen (Generator or AsyncGenerator): Data generator yielding lists of rows.
            columns (List[str]): Column names for DataFrame.
            schema (pa.Schema): PyArrow schema for the Parquet file.
            sort_by (str, optional): Column name to sort by before saving. Defaults to None.

        Returns:
            Parquet: An instance of the Parquet class for method chaining.
        """
        instance = Parquet(schema=schema)
        instance.gen = gen
        instance.columns = columns
        instance.sort_by = sort_by
        return instance

    async def to(self, path: Path, name_template: str = "data_", max_ram_usage: float = 0.5) -> None:
        """
        Streams data and stores it efficiently, keeping it in memory until it exceeds the RAM threshold,
        at which point it writes to a file and clears memory.

        Args:
            path (Path): Directory where Parquet files will be stored.
            name_template (str): Prefix for the output files.
            max_ram_usage (float): Fraction of total RAM that can be used before writing to disk.
        """
        path.mkdir(parents=True, exist_ok=True)
        self.buffer = []
        self.file_idx = 0
        self.available_ram = psutil.virtual_memory().total * max_ram_usage  # Allowed RAM usage
        self.total_rows = 0

        if isinstance(self.gen, AsyncGenerator):
            async for chunk in self.gen:
                self._buffer_chunk(chunk, path, name_template)
        elif isinstance(self.gen, Generator):
            for chunk in self.gen:
                self._buffer_chunk(chunk, path, name_template)
        else:
            raise TypeError("Illegal generator type")

        self._flush_buffer(path, name_template)
        print("All data saved successfully.")

    def _buffer_chunk(self, chunk: List[List], path: Path, name_template: str):
        """
        Adds a new chunk to the memory buffer. If the memory threshold is exceeded, it writes to disk.

        Args:
            chunk (List[List]): Data chunk from generator.
            path (Path): Directory for saving files.
            name_template (str): Prefix for file naming.
        """
        self.buffer.extend(chunk)
        self.total_rows += len(chunk)
        estimated_mem_usage = self.total_rows * len(self.columns) * 8  # Approximate memory usage

        if estimated_mem_usage > self.available_ram:
            self._flush_buffer(path, name_template)

    def _flush_buffer(self, path: Path, name_template: str):
        """
        Writes buffered data to a Parquet file and clears the buffer.

        Args:
            path (Path): Directory for saving files.
            name_template (str): Prefix for file naming.
        """
        if not self.buffer:
            return  

        df = pd.DataFrame(self.buffer, columns=self.columns)
        
        if "time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

        if self.sort_by and self.sort_by in df.columns:
            df = df.sort_values(by=self.sort_by, ascending=True)

        table = pa.Table.from_pandas(df, schema=self.schema)
        output_file = path / f"{name_template}{self.file_idx}.parquet"
        pq.write_table(table, output_file, compression="snappy")
        print(f"Saved chunk to {output_file}")

        
        self.buffer.clear()
        self.total_rows = 0
        self.file_idx += 1


    def read(self, path: Path) -> pd.DataFrame:
        """
        Reads Parquet file

        Args: 
            path (Path): File path where the data is stored
        
        Returns:
            pd.DataFrame: DataFrame of the data
 
        """
        return pd.read_parquet(path)
    
    