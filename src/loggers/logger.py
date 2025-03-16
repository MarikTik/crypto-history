import logging
from pathlib import Path
from typing import Dict, Union

class LoggerManager:
    """
    Manages loggers dynamically, ensuring each log file has its own handler 
    while keeping a single logger instance for performance efficiency.
    
    This is a **general-purpose logging manager** for different parts of the application, 
    including OrderBook, OHLCV downloads, and other modules.
    """

    def __init__(self, base_dir: Path, default_level: int):
        """
        Initializes the logger manager.

        Args:
            base_dir (Path): Base directory where log files are stored.
            default_level (int): Default logging level (default: logging.INFO).
        """
        base_dir.mkdir(parents=True, exist_ok=True)
        self._base_dir = base_dir
        self._default_level = default_level
        self._loggers: Dict[str, logging.Logger] = {}  # Store logger instances
        self._handlers: Dict[str, logging.FileHandler] = {}  # Store file handlers

    def get_logger(self, log_name: Union[str, Path], level: int = None) -> logging.Logger:
        """
        Returns a logger that writes to a specific log file.

        Args:
            log_name (Union[str, Path]): The log file name (e.g., "BTC-USD.log").
            level (int, optional): Logging level (default: None, falls back to default level).

        Returns:
            logging.Logger: A logger instance configured for the given file.
        """
        if isinstance(log_name, str):
            log_name = Path(log_name)

        log_file = self._base_dir / log_name
        
        log_file.parent.mkdir(parents=True, exist_ok=True) #  Ensure parent directories exist
        
        if str(log_file) in self._loggers:
            return self._loggers[str(log_file)]  # Reuse existing logger

        # Create a new logger
        logger = logging.getLogger(str(log_file))
        logger.setLevel(level if level else self._default_level)

        # Ensure only one handler is attached to avoid duplicate logs
        if str(log_file) not in self._handlers:
            file_handler = logging.FileHandler(log_file, mode="a")
            file_handler.setLevel(logger.level)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)

            self._handlers[str(log_file)] = file_handler
            logger.addHandler(file_handler)

        self._loggers[str(log_file)] = logger
        return logger
 
logger_manager = LoggerManager(Path("logs"), logging.DEBUG)
