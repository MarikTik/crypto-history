from pathlib import Path
from typing import Dict, List, Union, Tuple, Callable, Any
import sqlite3
import numpy as np
import pandas as pd
from deltalake import write_deltalake
import shutil

from .write_only_database import WriteOnlyDatabase


class OrderBookDatabase(WriteOnlyDatabase):

    def _default_compression_condition(storage_dir: Path) -> bool:
        """Returns True if storage_dir's disk usage is above 90%."""
        total, used, _ = shutil.disk_usage(storage_dir.resolve())
        return (used / total) * 100 >= 90  # If used disk space is above 90%

    def __init__(
        self,
        temp_dir: Path,
        storage_dir: Path,
        compression_condition: Callable[
            [Any], bool
        ] = _default_compression_condition,
    ):

        temp_dir.mkdir(parents=True, exist_ok=True)
        storage_dir.mkdir(parents=True, exist_ok=True)
        self._temp_dir = temp_dir
        self._storage_dir = storage_dir
        self._compression_condition = (
            compression_condition or self._default_compression_condition
        )
        self._create_db()
        self._init_db()

    def write(
        self,
        product_snapshots: Dict[
            str, List[Dict[str, Union[int, Tuple[float, float]]]]
        ],
    ) -> None:
        conn = self._conn
        cursor = conn.cursor()

        for product, snapshots in product_snapshots.items():
            product_id = self._get_or_create_product_id(cursor, product)
            for snapshot in snapshots:
                timestamp = snapshot["timestamp"]
                bids_blob = self._pack_side(snapshot["bids"])
                asks_blob = self._pack_side(snapshot["asks"])

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO order_book_snapshots (product_id, timestamp, bids, asks)
                    VALUES (?, ?, ?, ?)
                    """,
                    (product_id, timestamp, bids_blob, asks_blob),
                )
        if self._compression_condition(self._temp_dir):
            self._compress_and_store()

        conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()
        self._temp_dir.rmdir()

    def _create_db(self):
        conn = sqlite3.connect(self._temp_dir / "order_book.db")
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        self._conn = conn

    def _init_db(self):
        conn = self._conn
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """
        )
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS order_book_snapshots (
            product_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            bids BLOB NOT NULL,
            asks BLOB NOT NULL,
            PRIMARY KEY (product_id, timestamp)
        ) WITHOUT ROWID;
        """
        )
        conn.commit()

    def _get_or_create_product_id(self, product_name: str) -> int:
        conn = self._conn
        cursor = conn.cursor()
        """Retrieves or inserts a product into the database."""
        cursor.execute(
            "SELECT id FROM products WHERE name = ?", (product_name,)
        )
        row = cursor.fetchone()
        if row:
            return row[0]
        cursor.execute(
            "INSERT INTO products (name) VALUES (?)", (product_name,)
        )
        conn.commit()
        return cursor.lastrowid

    def _pack_side(self, side: List[Tuple[float, float]]) -> bytes:
        """Encodes order book data as a binary blob."""
        arr = np.array(side, dtype=np.float32)
        return arr.tobytes()

    def _compress_and_store(self):
        """Compresses order book data and writes to Delta Lake."""
        conn = self._conn
        cursor = conn.cursor()

        cursor.execute("SELECT id, name FROM products")
        products = cursor.fetchall()

        for product_id, product_name in products:
            cursor.execute(
                """
                SELECT timestamp, bids, asks FROM order_book_snapshots
                WHERE product_id = ?
                ORDER BY timestamp ASC
                """,
                (product_id,),
            )
            snapshots = cursor.fetchall()

            if not snapshots:
                continue

            # Convert data to a DataFrame
            df = pd.DataFrame(snapshots, columns=["timestamp", "bids", "asks"])
            df["bids"] = df["bids"].apply(
                lambda x: np.frombuffer(x, dtype=np.float32)
                .reshape(-1, 2)
                .tolist()
            )
            df["asks"] = df["asks"].apply(
                lambda x: np.frombuffer(x, dtype=np.float32)
                .reshape(-1, 2)
                .tolist()
            )

            # Save as Delta Lake table
            delta_path = str(self._storage_dir / f"{product_name}_delta")
            write_deltalake(delta_path, df, mode="append")

            # Delete old snapshots
            last_timestamp = df["timestamp"].max()
            cursor.execute(
                """
                DELETE FROM order_book_snapshots
                WHERE product_id = ? AND timestamp <= ?
                """,
                (product_id, last_timestamp),
            )
        conn.commit()
