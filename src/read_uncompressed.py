import pandas as pd

# Load the Parquet file
df = pd.read_parquet("data/BTC-USDT.parquet")

# Print only the first 10 rows
print(df.head(100))
