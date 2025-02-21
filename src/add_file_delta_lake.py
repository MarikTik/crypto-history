from deltalake import DeltaTable, write_deltalake
import pandas as pd
from pathlib import Path

# Define your Delta table path
delta_path = "delta"

# Create a new row of candle data
new_data = pd.DataFrame([
    {
        "time": 1700000005,  # Unix timestamp
        "low": 36000.5,
        "high": 37000.2,
        "open": 36500.0,
        "close": 36850.1,
        "volume": 125.3,
        "timestamp": pd.to_datetime(1700000001, unit="s", utc=True)
    },
    {
        "time": 1700000006,  # Unix timestamp
        "low": 36000.5,
        "high": 37000.2,
        "open": 36500.0,
        "close": 36850.1,
        "volume": 125.3,
        "timestamp": pd.to_datetime(1700000002, unit="s", utc=True)
    }
])

# Append new data to Delta Table
write_deltalake(delta_path, new_data, mode="append")

print(f"✅ Appended new row to Delta table at {delta_path}")