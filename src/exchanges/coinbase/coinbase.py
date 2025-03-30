"""
File: coinbase.py

This module provides the Coinbase exchange implementation, extending the base
Exchange class to handle Coinbase-specific API interactions for fetching
trading product information.

It defines the Coinbase class, which is responsible for:

    Fetching lists of traded and non-traded products from the Coinbase API.
    Handling potential API errors and network issues.
    Scheduling updates for product information.

The module also includes example usage for scheduling updates within a specific
UTC timeframe.
"""

import aiohttp
from typing import Dict, List, Tuple
from datetime import time, timedelta, timezone

from ..exchange import Exchange
from .ohlcv_history import OHLCV_History
from .order_book import OrderBook


class Coinbase(Exchange):
    """
    Coinbase Exchange implementation.

    This class provides Coinbase-specific implementations for fetching trading products.
    """

    ohlcv = OHLCV_History
    order_book = OrderBook

    @staticmethod
    async def fetch_products() -> Tuple[set[str], set[str]]:
        """
        Fetches trading and non-trading products from the Coinbase API.

        Returns:
            Tuple[Set[str], Set[str]]:
                - The first set contains currently **traded** products.
                - The second set contains **non-traded** products (including delisted ones).

        Raises:
            RuntimeError: If the API request fails or returns an invalid response.
        """
        PRODUCT_URL = "https://api.exchange.coinbase.com/products"
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(PRODUCT_URL) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"""
                            Invalid response from Coinbase API to update trading products.
                            status = {response.status}
                            """
                        )

                    data: List[Dict] = await response.json()
                    trading_products = {
                        item["id"]
                        for item in data
                        if item.get("status") == "online"
                    }
                    non_trading_products = {
                        item["id"]
                        for item in data
                        if item.get("status") != "online"
                    }
            except aiohttp.ClientError as e:
                raise RuntimeError(f"Network error: {e}")
            except Exception as e:
                raise RuntimeError(f"Unexpected error: {e}")
        return trading_products, non_trading_products


start_utc = time(18, 0, tzinfo=timezone.utc)
end_utc = time(22, 30, tzinfo=timezone.utc)
# scheduling updates with one minute granularity  on the interval 18:00 - 22:30 UTC
Coinbase.schedule_updates([((start_utc, end_utc), timedelta(minutes=1))])
