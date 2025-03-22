import os
import sys
import logging
import pandas as pd
from pathlib import Path

 
log_dir = Path("logs", "ohlcv")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "verification.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(message)s')

def verify_parquet_file(file_path: Path):
    logging.info(f"\nVerifying {file_path.stem}")
    try:
        df = pd.read_parquet(file_path)

        # Check header
        expected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
        if list(df.columns) != expected_columns:
            logging.info(f"❌ Column names incorrect. Found: {list(df.columns)}")
        else:
            logging.info("✅ Column names correct")

        # Check schema: 6 columns, all numeric
        if df.shape[1] != 6:
            logging.info(f"❌ Incorrect number of columns. {df.shape[1]} found instead of 6")
        elif not all(pd.api.types.is_numeric_dtype(dtype) for dtype in df.dtypes[1:]):
            logging.info("❌ Not all columns are numeric")
        else:
            logging.info("✅ Correct schema")

        # Check if timestamps are in order
        timestamps = df.iloc[:, 0]
        if timestamps.is_monotonic_increasing:
            logging.info("✅ Order preserved")
        else:
            logging.info("❌ Timestamps not in order")

        # Check for duplicates in timestamp
        duplicated = timestamps[timestamps.duplicated()]
        if duplicated.empty:
            logging.info("✅ No duplicates")
        else:
            duplicates = duplicated.tolist()
            logging.info(f"❌ Duplicate timestamps found: [{duplicates[0:2]}] ({len(duplicates)} total)")

    except Exception as e:
        logging.info(f"❌ Error reading file {file_path.name}: {e}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python verify_ohlcv.py <parquet_directory>")
        return

    parquet_dir = Path(sys.argv[1])
    if not parquet_dir.is_dir():
        print(f"Directory not found: {parquet_dir}")
        return

    for file in sorted(parquet_dir.glob("*.parquet")):
        verify_parquet_file(file)

main()
