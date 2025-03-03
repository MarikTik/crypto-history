from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union, Optional

class OHLCV_History(ABC):
    def __init__(self, product: str):
        self._product = product
        
    @abstractmethod
    async def fetch_timeframe(
        self,
        start_time: datetime,
        end_time: Optional[datetime],
        granularity: int):
        pass
