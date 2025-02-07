import argparse
from datetime import datetime, timezone

class Parser:
     def __init__(self):
          self.parser = argparse.ArgumentParser(
               description="Fetch historical cryptocurrency data from Coinbase."
          )
          self._add_arguments()

     def _add_arguments(self):
          """Define command-line arguments."""
          self.parser.add_argument(
               "name", type=str, help="The cryptocurrency name (e.g., BTC-USDT)"
          )
          self.parser.add_argument(
               "start_date", type=str, 
               help="Start date for fetching data (format: YYYY-MM-DD)"
          )
          self.parser.add_argument(
               "end_date", type=str, nargs="?", 
               default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
               help="End date for fetching data (default: current date)"
          )
          self.parser.add_argument(
               "granularity", type=int, nargs="?", default=60, choices=[60, 300, 900, 3600, 21600, 86400],
               help="Granularity in seconds (default: 60). Options: 60 (1min), 300 (5min), 900 (15min), "
                  "3600 (1hr), 21600 (6hr), 86400 (1day)."
          )
          self.parser.add_argument(
               "dir", type=str, nargs="?", default="data",
               help="The directory to which the file [name].parquet should be saved" 
          )

     def parse(self):
          """Parse the command-line arguments."""
          return self.parser.parse_args()
