import pandas as pd
from pathlib import Path
from datetime import datetime


def fix_mixed_timestamp_column(parquet_path: str | Path):
    try:
        df = pd.read_parquet(parquet_path)

        def fix_ts(ts):
            if isinstance(ts, float):
                # Convert float to 10-digit integer UNIX timestamp
                candidate = str(ts).replace(".", "")
                candidate = candidate.ljust(10, "0")  # pad to 10 digits
                fixed = int(candidate[:10])
                return fixed
            elif isinstance(ts, int):
                s = str(ts)
                if len(s) > 10:
                    # Trim right side if too long (e.g., ms precision)
                    return int(s[:10])
                elif len(s) < 10:
                    # Pad if too short (shouldn't happen in real data)
                    return int(s.ljust(10, "0"))
                else:
                    return ts
            elif isinstance(ts, pd.Timestamp):
                return fix_ts(float(ts.timestamp()))
            else:
                raise ValueError(f"Unsupported timestamp format: {ts}")

        df["timestamp"] = df["timestamp"].apply(fix_ts).astype("int32")
        df.sort_values("timestamp", inplace=True)
        df.drop_duplicates(subset="timestamp", inplace=True)

        tmp_path = parquet_path.with_suffix(".tmp.parquet")
        df.to_parquet(tmp_path, index=False, compression="snappy")

        if tmp_path.stat().st_size > 5000:  # Rough sanity check
            tmp_path.replace(parquet_path)
            print(f"✅ Fixed and saved: {parquet_path.name}")
        else:
            print(f"⚠️ Skipped overwrite — output too small: {tmp_path.name}")
            tmp_path.unlink(missing_ok=True)

    except Exception as e:
        print(f"❌ Error processing {parquet_path.name}: {e}")


# Run on all .parquet files
parquet_dir = Path("/home/rusty/storage/crypto_history/data/coinbase/ohlcv")
for path in sorted(parquet_dir.glob("*.parquet")):
    fix_mixed_timestamp_column(path)
