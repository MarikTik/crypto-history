import duckdb

delta_path = "delta"
# Read the Delta table using DuckDB
query = f"SELECT * FROM read_parquet('{delta_path}/*.parquet')"

df = duckdb.query(query).to_df()

print(df)