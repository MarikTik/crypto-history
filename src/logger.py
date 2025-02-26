import logging
from pathlib import Path

class LoggerManager:
    """Manages loggers for different coins, ensuring logs are stored separately per coin."""

 
    def __init__(self, dir: Path, level):
        dir.mkdir(parents=True, exist_ok=True)
        self._dir = dir
        self._loggers = {}
        self._level = level

    def get_logger(self, symbol: str) -> logging.Logger:
        """
        Returns a logger instance for a given symbol, creating one if it does not exist.

        Args:
            symbol (str): The cryptocurrency pair (e.g., "BTC-USD").

        Returns:
            logging.Logger: A configured logger instance.
        """
        if symbol in self._loggers:
            return self._loggers[symbol]

        else:

            log_file = self._dir / f"{symbol}.log"
            logger = logging.getLogger(symbol)
            logger.setLevel(self._level)

        
            file_handler = logging.FileHandler(log_file, mode="a")
            file_handler.setLevel(self._level)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            self._loggers[symbol] = logger  

        return logger

logger_manger = LoggerManager(Path("logs"), logging.DEBUG)