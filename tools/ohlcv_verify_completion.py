import os
import sys
import logging
import pandas as pd
from pathlib import Path


log_dir = Path("logs", "ohlcv")
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "verification.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(message)s")


def verify_header(df: pd.DataFrame):
    expected_columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    if list(df.columns) != expected_columns:
        logging.info(f"❌ Column names incorrect. Found: {list(df.columns)}")
    else:
        logging.info("✅ Column names correct")


def verify_schema(df: pd.DataFrame):
    # Check schema: 6 columns, all numeric
    if df.shape[1] != 6:
        logging.info(
            f"❌ Incorrect number of columns. {df.shape[1]} found instead of 6"
        )
    elif not all(
        pd.api.types.is_numeric_dtype(dtype) for dtype in df.dtypes[1:]
    ):
        logging.info("❌ Not all columns are numeric")
    else:
        logging.info("✅ Correct schema")


def verify_timestamp_consistency(df: pd.DataFrame):
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
        logging.info(
            f"❌ Duplicate timestamps found: [{duplicates[0:2]}] ({len(duplicates)} total)"
        )

    # Check timestamp type is int32
    timestamp_dtype = df.dtypes["timestamp"]
    if timestamp_dtype == "int32":
        logging.info("✅ Timestamp column is int32")
    elif pd.api.types.is_integer_dtype(timestamp_dtype):
        if (
            df["timestamp"].min() >= -(2**31)
            and df["timestamp"].max() <= 2**31 - 1
        ):
            logging.info(
                "⚠️ Timestamp is not stored as int32, but values fit within int32 range"
            )
        else:
            logging.info(
                "❌ Timestamp is not int32 and contains values outside int32 range"
            )
    else:
        logging.info(
            f"❌ Timestamp column is not integer (found: {timestamp_dtype})"
        )


def verify_file_size(file_path: Path):
    if file_path.stat().st_size <= 4096:
        logging.info("⚠️ File size is below 4kb, i.e. empty")
    else:
        logging.info("✅ File size ok")


def verify_parquet_file(file_path: Path):
    logging.info(f"\nVerifying {file_path.stem}")
    verify_file_size(file_path)
    try:
        df = pd.read_parquet(file_path)
        verify_header(df)
        verify_schema(df)
        verify_timestamp_consistency(df)
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
