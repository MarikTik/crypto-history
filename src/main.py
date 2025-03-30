import asyncio
from pathlib import Path
from parser import Parser
from datetime import datetime
from typing import Dict, List
from exchanges import Exchange
from exchanges import *

exchanges: Dict[str, Exchange] = {
    "binance": Binance,
    "coinbase": Coinbase,
    "kraken": Kraken,
    "robinhood": Robinhood,
}


async def download_order_book(
    exchange: Exchange,
    products: List[str],
    depth: int,
    frequency: float | int,
    end_date: datetime,
    directory: Path,
):
    async with exchange.order_book(
        products=products, frequency=frequency, depth=depth
    ) as order_book:
        with exchange.order_book_db(directory) as db:
            async for snapshot in order_book.snapshots(until=end_date):
                db.write(snapshot)


async def download_ohlcv(
    exchange: Exchange, products: Dict[str, Dict], directory: Path
):
    gen = exchange.ohlcv.fetch_many(products=products)
    with exchange.ohlcv_db(directory) as db:
        async for candles in gen:
            db.write(candles)


async def main(*args):
    download_type, exchange_name, products, extras, directory = args
    exchange = exchanges[exchange_name.lower()]
    exchange.start_product_updates()

    if download_type == "ohlcv":
        await download_ohlcv(exchange, products, directory)
    elif download_type == "order_book":
        await download_order_book(
            exchange, list(products.keys()), *extras, directory
        )


if __name__ == "__main__":
    parser = Parser()
    args = parser.parse()
    asyncio.run(main(*args))
