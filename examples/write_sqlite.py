from pathlib import Path
import sqlite3
import numpy as np

DB_PATH = Path("order_book.db")

# Sample data for demonstration
# In real use, replace this with your actual snapshot data
sample_data = {
    "BTC-USD": {
        "timestamp": 1711171200,  # Unix timestamp (e.g., 2024-03-23T00:00:00Z)
        "bids": [(10000.0, 0.5)] * 25,
        "asks": [(10010.0, 0.6)] * 25
    }
}

def create_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    return conn

def init_db(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_book_snapshots (
        product_id INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        bids BLOB NOT NULL,
        asks BLOB NOT NULL,
        PRIMARY KEY (product_id, timestamp)
    ) WITHOUT ROWID;
    """)
    conn.commit()

def get_or_create_product_id(conn: sqlite3.Connection, product_name: str) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM products WHERE name = ?", (product_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO products (name) VALUES (?)", (product_name,))
    return cursor.lastrowid

def pack_order_book(side: list[tuple[float, float]]) -> bytes:
    arr = np.array(side, dtype=np.float32).flatten()
    return arr.tobytes()

def insert_snapshot(conn: sqlite3.Connection, product_id: int, timestamp: int, bids_blob: bytes, asks_blob: bytes):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR IGNORE INTO order_book_snapshots (product_id, timestamp, bids, asks)
    VALUES (?, ?, ?, ?)
    """, (product_id, timestamp, bids_blob, asks_blob))

def main():
    conn = create_connection(DB_PATH)
    init_db(conn)

    for product, data in sample_data.items():
        product_id = get_or_create_product_id(conn, product)
        bids_blob = pack_order_book(data["bids"])
        asks_blob = pack_order_book(data["asks"])
        insert_snapshot(conn, product_id, data["timestamp"], bids_blob, asks_blob)

    conn.commit()
    conn.close()

main()
