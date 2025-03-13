# src/utils/time_utils.py
from datetime import datetime
from typing import Union

def to_unix_timestamp(ts: Union[int, float, str, datetime], to_int: bool = True) -> Union[int, float]:
    """
    Converts a timestamp into a standardized UNIX timestamp.

    Args:
        ts (int | float | str | datetime): The input timestamp in various formats:
            - `int` → Assumes epoch seconds.
            - `float` → Assumes epoch seconds with milliseconds.
            - `str` → Converts from ISO 8601 format (`YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD`).
            - `datetime` → Converts to UNIX timestamp.
        to_int (bool): If True, rounds to an integer (seconds precision). If False, keeps float (millisecond precision).

    Returns:
        int | float: The UNIX timestamp.
    """
    if isinstance(ts, (int, float)):
        return int(ts) if to_int else float(ts)  

    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {ts}. Expected ISO 8601 format.")
        return int(dt.timestamp()) if to_int else dt.timestamp()

    if isinstance(ts, datetime):
        return int(ts.timestamp()) if to_int else ts.timestamp()

    raise TypeError(f"Unsupported timestamp type: {type(ts)}")