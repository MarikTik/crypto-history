from pathlib import Path
import sqlite3
import numpy as np
import pandas as pd

DB_PATH = Path("order_book.db")

def unpack_blob(blob: bytes, depth: int = 25):
    """Unpacks a blob back into an array of (price, quantity) tuples."""
    floats = np.frombuffer(blob, dtype=np.float32)
    return floats.reshape((depth, 2))

def read_snapshots(product_name: str, limit: int = 10):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get the product_id
    cursor.execute("SELECT id FROM products WHERE name = ?", (product_name,))
    result = cursor.fetchone()
    if not result:
        print(f"❌ Product '{product_name}' not found.")
        return
    product_id = result[0]

    # Fetch snapshots
    cursor.execute("""
        SELECT timestamp, bids, asks FROM order_book_snapshots
        WHERE product_id = ?
        ORDER BY timestamp ASC
        LIMIT ?
    """, (product_id, limit))

    rows = cursor.fetchall()
    snapshots = []
    for timestamp, bids_blob, asks_blob in rows:
        bids = unpack_blob(bids_blob)
        asks = unpack_blob(asks_blob)
        snapshots.append({
            "timestamp": pd.to_datetime(timestamp, unit='s'),
            "bids": bids,
            "asks": asks
        })

    conn.close()
    return snapshots

# Example usage
if __name__ == "__main__":
    product = "BTC-USD"
    snapshots = read_snapshots(product, limit=3)
    if snapshots:
        for snap in snapshots:
            print(f"\n🕒 Timestamp: {snap['timestamp']}")
            print(f"📉 Bids:\n{snap['bids']}")
            print(f"📈 Asks:\n{snap['asks']}")
