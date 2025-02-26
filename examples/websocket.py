from coinbase.websocket import WSClient, WSUserClient, WebsocketResponse
import os
import time
import json
import psutil


api_key = os.getenv("COINBASE_OBSERVER_API_KEY")
api_secret = os.getenv("COINBASE_OBSERVER_API_KEY_SECRET")

prices = {}


def on_message(msg):
    global prices
    ws_object = WebsocketResponse(json.loads(msg))

    if ws_object.channel == "ticker":
        for event in ws_object.events:
            for ticker in event.tickers:
                prices[ticker.product_id] = float(ticker.price)
    del ws_object
    

# Initialize the WebSocket Client
client = WSClient(api_key=api_key, api_secret=api_secret, on_message=on_message)

# Open connection and subscribe to the ticker channel for a specific coin
client.open()

with open("extras/coin-pairs", "r") as coin_pairs_file:
    coin_pairs = [pair.strip() for pair in  coin_pairs_file.readlines()]
 
client.subscribe(product_ids=coin_pairs, channels=["ticker"])

client.run_forever_with_exception_check()

client.unsubscribe(product_ids=coin_pairs, channels=["ticker"])
client.close()
