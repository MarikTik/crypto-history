# Crypto Data Fetcher

## Overview
Crypto Data Fetcher is a Python-based tool for fetching historical cryptocurrency data from Coinbase. The script allows users to specify a coin, date range, and granularity for data retrieval and saves the results in compressed Parquet format for efficient storage and processing.

## Features
- Fetch historical OHLC (Open, High, Low, Close) data from Coinbase.
- Supports multiple cryptocurrencies (e.g., BTC, ETH, SOL, etc.).
- Configurable start and end date for historical data.
- Adjustable granularity (1 minute to 1 day candles).
- Saves data efficiently in Parquet format.

## Installation
### Prerequisites
Ensure you have Python 3.8+ installed along with `pip`.

### Install Dependencies
```bash
pip install -r requirements.txt
```

## Usage
The script is run from the command line with required and optional parameters.

### Command Structure
```bash
python3 src/fetch_coin.py <name> <start_date> [end_date] [granularity] [dir]
```

### Arguments
| Argument     | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `name`      | string | ✅ Yes  | Cryptocurrency symbol pair (e.g., `BTC-USDT`). |
| `start_date`| string | ✅ Yes  | Start date for fetching data (format: `YYYY-MM-DD`). |
| `end_date`  | string | ❌ No  | End date for fetching data (default: current date). |
| `granularity` | int | ❌ No | Time granularity in seconds. Options: `60` (1 min), `300` (5 min), `900` (15 min), `3600` (1 hr), `21600` (6 hr), `86400` (1 day). Default: `60`. |
| `dir`       | string | ❌ No  | Directory where the output file `[name].parquet` is saved (default: `data/`). |

### Example Usage
#### Fetch Bitcoin Data from 2021-01-01 to 2024-01-01
```bash
python3 src/fetch_coin.py BTC-USDT 2021-01-01 2024-01-01 60
```

#### Fetch Ethereum Data with Default End Date
```bash
python3 src/fetch_coin.py ETH-USDT 2022-06-01
```

#### Fetch Solana Data with 1-Hour Granularity
```bash
python3 src/fetch_coin.py SOL-USDT 2023-01-01 2024-01-01 3600
```

## Output Format
The fetched data is stored as a Parquet file with the following structure:
```plaintext
Columns: [timestamp, low, high, open, close, volume]
```
Each row represents an OHLC data point for the specified cryptocurrency and time interval.

## Running as a Background Process
To run the script in the background:
```bash
nohup bash tools/download.sh & disown
```
Monitor logs:
```bash
tail -f logs/*.log
```

## License
MIT License

## Contributing
Contributions are welcome if you think there are good editions.

## Author
Marik T.

