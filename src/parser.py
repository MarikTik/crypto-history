"""
CLI Argument Parser for Fetching Historical Cryptocurrency Data and Order Book Snapshots.

This module provides a flexible command-line argument parser that supports two main functionalities:
1. **OHLCV Data Retrieval** - Fetches Open, High, Low, Close, and Volume (OHLCV) historical data.
2. **Order Book Snapshots** - Captures real-time order book depth at specified intervals.

Users can specify:
- **Exchange** (e.g., Coinbase, Binance, Kraken, Robinhood)
- **Download Type**: `"ohlcv"` or `"order_book"`
- **Input Mode**:
    - `"file"` → Load configurations from a JSON file
    - `"manual"` → Manually specify product and parameters
- **Custom Output Directory**: Defaults to `"data/exchange_name/ohlcv"` or `"data/exchange_name/order_book"`.

---

## **Example Usage** ##
### **📌 Manual OHLCV Mode** ###
Fetch OHLCV data manually for `BTC-USD` from Coinbase, specifying a date range and granularity:
```sh
python src/main.py ohlcv coinbase manual BTC-USD --start_date 2023-01-01 --end_date 2024-01-01 --granularity 60 --dir custom_directory/
```
The --start_date, --end_date, and --granularity options are optional:
```sh
python src/main.py ohlcv coinbase manual BTC-USD
```
Default values will be applied:
- **start_date** : 2012-01-01
- **end_date** : now
- **granularity** : 60 seconds

### **📂 File-Based OHLCV Mode** ###
```sh
python src/main.py ohlcv coinbase file path/to/file.json --dir custom_directory/
```
     📂 File Format (file.json)
     ```json
     {
          "BTC-USD": {
               "start_date": "2023-01-01",
               "end_date": "2024-01-01",
               "granularity": 60
          },
          "ETH-USD": {
               "start_date": "2022-06-15"
          }
     }
```
Any missing parameter will use default values:
- **start_date** : 2012-01-01
- **end_date** : now
- **granularity** : 60 seconds


### **📌 Manual Order Book Mode** ###

Track order book snapshots manually for BTC-USD from Binance:
```sh
python src/main.py order_book binance manual BTC-USD --depth 50 --frequency 5 --end_date 2024-01-01 --dir custom_directory/
```
The --depth, --frequency, and --end_date options are optional:
```sh
python src/main.py order_book binance manual BTC-USD
```
Default values will be applied:
- **depth** : 25 levels
- **frequency** : 5 seconds
- **end_date** : No end date (runs indefinitely)


### **📂File-Based Order Book Mode** ###
Load order book configuration from a JSON file:
```sh
python src/main.py order_book kraken file path/to/order_book_config.json --dir custom_order_book_storage/
```
     📂File Format (order_book_config.json)
     ```json
     {
     "BTC-USD": {
          "depth": 50,
          "frequency": 5,
          "end_date": "2024-01-01"
     },
     "ETH-USD": {
          "depth": 25
     }
     ```
}
Any missing parameter will use default values:
     - **depth** : 25 levels
     - **frequency** : 5 seconds
     - **end_date** : No end date (runs indefinitely)

"""

import argparse
from pathlib import Path
from datetime import timezone, datetime
from typing import Dict, List, Tuple
import json

import exchanges.coinbase as coinbase
import exchanges.binance as binance
import exchanges.kraken as kraken
import exchanges.robinhood as robinhood


##########OHLCV constants##################
DEFAULT_START_DATE = "2012-01-01"
DEFAULT_GRANULARITY = 60
###########################################

########Order book constants###############
DEFAULT_FREQUENCY = 5
DEFAULT_DEPTH = 25
###########################################

class Parser:    
     """
     Parses command-line arguments for fetching historical cryptocurrency data and order book snapshots.

     Supports:
     - **OHLCV Data Retrieval**: Historical Open-High-Low-Close-Volume (OHLCV) data.
     - **Order Book Tracking**: Real-time order book snapshots.

     Functionality:
     - **Two Download Types**: "ohlcv" or "order_book".
     - **Two Input Modes**:
        - "file": Load JSON configurations.
        - "manual": Direct user input.

     Attributes:
          parser (argparse.ArgumentParser): Main argument parser.
          subparsers (argparse._SubParsersAction): Subparser for "ohlcv" and "order_book".

     Methods:
          _add_arguments(): Defines command-line arguments.
          parse(): Parses arguments and returns structured configuration.
          _is_implemented(cls): Checks if an exchange module fully implements a required class.
          _get_supported_exchanges(component): Retrieves supported exchanges for "ohlcv" or "order_book".
     """
     def __init__(self):
          self.parser = argparse.ArgumentParser(
               description="Fetch historical cryptocurrency data or order book snapshots."
          )
          self.subparsers = self.parser.add_subparsers(dest="download_type", required=True)

          self._add_arguments()

     def _add_arguments(self):
          """
          Defines command-line arguments dynamically based on the selected `download_type`.

          This method creates two main parsers:
          1. **ohlcv_parser** → Fetches OHLCV data.
              - `exchange` (str): Target exchange.
              - **Subparsers:**
                  - `file`: JSON configuration file for multiple products.
                  - `manual`: Manually specify product and parameters.

          2. **order_book_parser** → Captures real-time order book data.
              - `exchange` (str): Target exchange.
              - **Subparsers:**
                  - `file`: JSON configuration file.
                  - `manual`: Manually specify product, depth, frequency, and end date.

          Both parsers support an optional `--dir` argument to specify the output storage directory.
          """
     
          # OHLCV Parent Parser
          ohlcv_parser = self.subparsers.add_parser(
               "ohlcv",
               help="Fetch historical OHLCV (Open, High, Low, Close, Volume) data."
          )
          ohlcv_parser.add_argument("exchange", type=str, choices=self._get_supported_exchanges("ohlcv"))
          ohlcv_subparsers = ohlcv_parser.add_subparsers(dest="input_mode", required=True)
 
          ohlcv_file_parser = ohlcv_subparsers.add_parser("file", help="Use a JSON file to specify coins and parameters.")
          ohlcv_file_parser.add_argument("file_path", type=str, help="Path to the JSON file.")
          ohlcv_file_parser.add_argument("--dir", type=str, default=None, help="Database directory. Defaults to data/exchange_name/ohlcv/")

          ohlcv_manual_parser = ohlcv_subparsers.add_parser("manual", help="Manually specify coin and parameters.")
          ohlcv_manual_parser.add_argument("product", type=str, help="Product name (e.g., BTC, ETH, SOL). Naming MUST be appropriate for the chosen exchange")
          ohlcv_manual_parser.add_argument("start_date", type=str, nargs="?", default=DEFAULT_START_DATE, help="Start date (format: YYYY-MM-DD).")
          ohlcv_manual_parser.add_argument("end_date", type=str, nargs="?", default=str(datetime.now(timezone.utc).date()), help="End date.")
          ohlcv_manual_parser.add_argument("granularity", type=int, nargs="?", default=DEFAULT_GRANULARITY, choices=[60, 300, 900, 3600, 21600, 86400], help="Granularity in seconds (default: 60s).")
          ohlcv_manual_parser.add_argument("--dir", type=str, default=None, help="Database directory. Defaults to data/exchange_name/ohlcv/")
        

          # Order Book Parent Parser
          order_book_parser = self.subparsers.add_parser(
               "order_book",
               help="Fetch real-time order book snapshots."
          )
          order_book_parser.add_argument("exchange", type=str, choices=self._get_supported_exchanges("order_book"))
          order_book_subparsers = order_book_parser.add_subparsers(dest="input_mode", required=True)
 
          order_book_file_parser = order_book_subparsers.add_parser("file", help="Use a JSON file to specify coins and parameters.")
          order_book_file_parser.add_argument("file_path", type=str, help="Path to the JSON file.")
          order_book_file_parser.add_argument("--dir", type=str, default=None, help="Database directory. Defaults to data/exchange_name/order_book/")

          order_book_manual_parser = order_book_subparsers.add_parser("manual", help="Manually specify coin and parameters.")
          order_book_manual_parser.add_argument("product", type=str, help="Product name (e.g., BTC, ETH, SOL). Naming MUST be appropriate for the chosen exchange")
          order_book_manual_parser.add_argument("depth", type=int, nargs="?", default=50, help="Number of order book levels (default: 50).")
          order_book_manual_parser.add_argument("frequency", type=int, nargs="?", default=5, help="Snapshot interval in seconds (default: 5s).")
          order_book_manual_parser.add_argument("end_date", type=str, nargs="?", default=None, help="End date (default: never stop).")
          order_book_manual_parser.add_argument("--dir", type=str, default=None, help="Database directory. Defaults to data/exchange_name/order_book/")

     def parse(self) ->  Tuple[str, str, Dict[str, Dict[str, str | int | None]], Path]:
          """
          Parses command-line arguments and structures them into a structured format.

          Returns:
               tuple:
               - `download_type` (str): Either "ohlcv" or "order_book".
               - `exchange` (str): Target exchange (e.g., "coinbase", "binance").
               - `parsed_data` (dict): Contains configuration details for each product.
                    Example for **OHLCV**:
                    ```
                    {
                         "BTC-USD": {
                              "start_date": "2023-01-01",
                              "end_date": "2024-01-01",
                              "granularity": 60
                         }
                    }
                    ```
                    Example for **Order Book**:
                    ```
                    {
                         "BTC-USD": {
                              "depth": 50,
                              "frequency": 5,
                              "end_date": None
                         }
                    }
                    ```
               - `dir` (Path): The database storage directory.
          """

          args = self.parser.parse_args()
          parsed_data = {}
          
          if args.dir is None:    
               args.dir = Path("data", args.exchange, args.download_type) #default directory setting

          if args.input_mode == "file":
               file_path = Path(args.file_path)
               if not file_path.is_file():
                    raise ValueError(f"File not found: {file_path}")

               with open(file_path, "r") as f:
                    config: Dict = json.load(f)

               for product, entry in config.items():
                    if args.download_type == "ohlcv":
                         parsed_data[product] = {
                              "start_date": entry.get("start_date", DEFAULT_START_DATE),
                              "end_date": entry.get("end_date", str(datetime.now(timezone.utc).date())),
                              "granularity": entry.get("granularity", DEFAULT_GRANULARITY),
                         }

                    elif args.download_type == "order_book":
                         parsed_data[product] = {
                              "depth": entry.get("depth", DEFAULT_DEPTH),
                              "frequency": entry.get("frequency", DEFAULT_FREQUENCY),
                              "end_date": entry.get("end_date", None),  # for continuous download
                         }
            

          elif args.input_mode == "manual":  
               if args.download_type == "ohlcv":
                    parsed_data[args.product] = {
                         "start_date": args.start_date,
                         "end_date": args.end_date,
                         "granularity": args.granularity,
                    }

               elif args.download_type == "order_book":
                    parsed_data[args.product] = {
                         "depth": args.depth,
                         "frequency": args.frequency,
                         "end_date": args.end_date,  # Can be None for continuous tracking
                    }
          else:
               raise RuntimeError(f"{args.input_mode} is not an available option for input mode. Use either 'file' or 'manual'.")
          
          return args.download_type, args.exchange, parsed_data, args.dir


     def _is_implemented(self, cls) -> bool:
          """
          Checks whether a given class has fully implemented all required abstract methods.

          Args:
               cls: The class to check.

          Returns:
               bool: True if the class is fully implemented, otherwise False.
          """
          return cls and (not hasattr(cls, "__abstractmethods__") or not bool(cls.__abstractmethods__))
     
     def _get_supported_exchanges(self, component: str) -> List[str]:
          """
          Retrieves a list of exchanges that support a given functionality.

          Args:
               component (str): "ohlcv" or "order_book".

          Returns:
               List[str]: A list of supported exchanges (e.g., ["coinbase", "binance"]).
          """
          exchanges = [coinbase, binance, kraken, robinhood]

          if component == "ohlcv":
               return [ex.__name__.split(".")[-1] for ex in exchanges if self._is_implemented(getattr(ex, "OHLCV_History", None))]
          elif component == "order_book":
               return [ex.__name__.split(".")[-1] for ex in exchanges if self._is_implemented(getattr(ex, "OrderBook", None))]
          return []

