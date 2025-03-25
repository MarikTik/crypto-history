import aiohttp
from pathlib import Path
from typing import Dict, List

from ..exchange import Exchange
from .ohlcv_history import OHLCV_History
from .order_book import OrderBook


class Coinbase(Exchange):
    ohlcv = OHLCV_History
    order_book = OrderBook

    def __init__(
        self, logs_path: Path = Path("logs", "coinbase", "exchange.log")
    ):
        super().__init__(logs_path)

    async def fetch_products(self):
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
