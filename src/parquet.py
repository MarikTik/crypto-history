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
    parquet = Parquet.from_csv("data.csv", schema, sort_by="time")
    parquet.dump("output.parquet")
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
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

    async def to(self, path: Path, name_template: str = "") -> None:
        """
        Writes streamed data to Parquet chunks in a directory.

        Args:
            path (Path): Directory where Parquet chunks will be stored.
            name_template (str): A template for the output files representing each chunk
        """
        path.mkdir(parents=True, exist_ok=True)
        chunk_idx = 0

        if isinstance(self.gen, AsyncGenerator):
            async for chunk in self.gen:
                self._write_chunk(chunk, path / f"{name_template}{chunk_idx}.parquet")
                chunk_idx += 1
        elif isinstance(self.gen, Generator):
            for chunk in self.gen:
                self._write_chunk(chunk, path / f"{name_template}{chunk_idx}.parquet")
                chunk_idx += 1  
        else:
            raise TypeError("Illegal generator type")

    def list_chunks(self, path: Path):
        """
        Lists all Parquet chunk files in the directory.

        Args:
            path (Path): Directory path where chunks are stored.

        Returns:
            list: List of chunk file paths.
        """
        return sorted(path.glob("*.parquet"))
    
    def read_chunks(self, path: Path):
        """
        Reads Parquet chunk files sequentially (without loading everything into memory).

        Args:
            path (Path): Directory path where chunks are stored.

        Yields:
            pd.DataFrame: DataFrame for each chunk.
        """
        for chunk_path in self.list_chunks(path):
            yield pd.read_parquet(chunk_path)


    def _write_chunk(self, chunk: List[List], path: Path):
        """
        Writes a single chunk of data to a Parquet file.

        Args:
            chunk (List[List]): List of data rows.
            path (Path): File path where the Parquet chunk will be stored.
        """
        df = pd.DataFrame(chunk, columns=self.columns)

        if ("time" in df.columns):
            df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

        if self.sort_by and self.sort_by in df.columns:
            df = df.sort_values(by=self.sort_by, ascending=True)

        table = pa.Table.from_pandas(df, schema=self.schema)
        pq.write_table(table, path, compression="snappy")