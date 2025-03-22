from pathlib import Path
from datetime import datetime
from rich import print
from sys import argv
 
logs_path = Path("logs", "coinbase", "ohlcv")

def is_finished(last_line: str) -> bool:
    return "Completed download" in last_line

def is_reference_point(line: str) -> bool:
    return "Fetching historical data" in line
def get_color(percent: float) -> str:
    if percent < 33.3:
        return "red"
    if percent < 80:
        return "yellow"
    return "green"

def main():
    for path in logs_path.glob("*.log"):
        with path.open("r") as log:
            lines = log.readlines()
            current_line = lines[-1]
            if not is_finished(current_line):
                print(f"Processing [bold purple]{path.stem}")
                for line in lines:
                    if is_reference_point(line):
                        info_line = line
                        break
                split = info_line.split(" ")
                start_date, end_date = datetime.fromisoformat(split[12]), datetime.fromisoformat(split[15])

                split = current_line.split(" ")
                current_date = datetime.fromisoformat(split[11])


                percent = (current_date - start_date) / (end_date - start_date) * 100 
                fillers = int(percent) * "="

                print(f"Progress: [bold {get_color(percent)}][{fillers: <100}] [bold blue]{percent:.2f}%") 

if __name__== "__main__":
    if len(argv) > 2:
        raise ValueError(f"ohlcv_progress expects 1 or 2 arguments {len(argv)} provded.")
    if len(argv) > 1:
        logs_path = Path(argv[1])
    main()

