import argparse
from pathlib import Path
from datetime import timezone, datetime
from typing import Dict
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
     def __init__(self):
          self.parser = argparse.ArgumentParser(
               description="Fetch historical cryptocurrency data or order book snapshots."
          )
          self.subparsers = self.parser.add_subparsers(dest="download_type", required=True)

          self._add_arguments()

     def _add_arguments(self):
          """Define command-line arguments dynamically based on `download_type`."""
          
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
          ohlcv_manual_parser.add_argument("--start_date", type=str, default=DEFAULT_START_DATE, help="Start date (format: YYYY-MM-DD).")
          ohlcv_manual_parser.add_argument("--end_date", type=str, default=str(datetime.now(timezone.utc).date()), help="End date.")
          ohlcv_manual_parser.add_argument("--granularity", type=int, default=DEFAULT_GRANULARITY, choices=[60, 300, 900, 3600, 21600, 86400], help="Granularity in seconds (default: 60s).")
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

          order_book_manual_parser.add_argument("--depth", type=int, default=50, help="Number of order book levels (default: 50).")
          order_book_manual_parser.add_argument("--frequency", type=int, default=5, help="Snapshot interval in seconds (default: 5s).")
          order_book_manual_parser.add_argument("--end_date", type=str, default=None, help="End date (default: never stop).")
          order_book_manual_parser.add_argument("--dir", type=str, default=None, help="Database directory. Defaults to data/exchange_name/order_book/")

     def parse(self):
          """Parse command-line arguments and process input."""
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


     def _is_implemented(self, cls):
          """Check if a class fully implements all required abstract methods."""
          return cls and (not hasattr(cls, "__abstractmethods__") or not bool(cls.__abstractmethods__))
     
     def _get_supported_exchanges(self, component: str):
          """
          Get a list of exchanges that have fully implemented the requested component.
        
          Args:
               component (str): "ohlcv" or "order_book"
        
          Returns:
               List[str]: Names of supported exchanges.
          """
          exchanges = [coinbase, binance, kraken, robinhood]

          if component == "ohlcv":
               return [ex.__name__.split(".")[-1] for ex in exchanges if self._is_implemented(getattr(ex, "OHLCV_History", None))]
          elif component == "order_book":
               return [ex.__name__.split(".")[-1] for ex in exchanges if self._is_implemented(getattr(ex, "OrderBook", None))]
          return []

