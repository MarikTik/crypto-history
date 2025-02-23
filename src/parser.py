import argparse
from datetime import datetime, timezone
from pathlib import Path

class Parser:
     def __init__(self):
          self.parser = argparse.ArgumentParser(
               description="Fetch historical cryptocurrency data from Coinbase."
          )
          self._add_arguments()

     def _add_arguments(self):
          """Define command-line arguments."""
          self.parser.add_argument(
               "name_or_file",
               type=str,
               help="The cryptocurrency name (e.g., BTC-USDT) or a file containing a list of symbols (one per line).",
          ) 
          self.parser.add_argument(
               "start_date",
               type=str,
               help="Start date for fetching data (format: YYYY-MM-DD)",
          )
          self.parser.add_argument(
               "end_date",
               type=str,
               nargs="?",
               default=None,  # 
               help="End date for fetching data (default: continuous polling).",
          )
          self.parser.add_argument(
               "granularity",
               type=int,
               nargs="?",
               default=60,
               choices=[60, 300, 900, 3600, 21600, 86400],
               help="Granularity in seconds (default: 60). Options: 60 (1min), 300 (5min), 900 (15min), "
                    "3600 (1hr), 21600 (6hr), 86400 (1day).",
          )
          self.parser.add_argument(
               "dir",
               type=str,
               nargs="?",
               default="data",
               help="The database directory",
          )

     def parse(self):
          """Parse the command-line arguments."""
          args = self.parser.parse_args()
          
          symbols = []
          if Path(args.name_or_file).is_file():
               with open(args.name_or_file, "r") as f:
                    symbols = [line.strip() for line in f.readlines() if line.strip()]
          elif "-USD" in args.name_or_file:
               symbols = [args.name_or_file]  # Treat as a single symbol
          else:
               raise ValueError(f"symbol is neither path nor coin pair - {args.name_or_file}")
          return symbols, args.start_date, args.end_date, args.granularity, args.dir
