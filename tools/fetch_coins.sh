#!/usr/bin/env bash

# to run in the background: nohup tools/fetch_coins.sh & disown

 
START_DATE="2021-05-10"
END_DATE="2025-07-06"
GRANULARITY="60"

# List of 10 cryptocurrencies (popular & volatile mix)
coins=(
    "BTC-USDT"   # Bitcoin
    "ETH-USDT"   # Ethereum
    "SOL-USDT"   # Solana
    "DOGE-USDT"  # Dogecoin
    "SHIB-USDT"  # Shiba Inu
    "AVAX-USDT"  # Avalanche
    "XRP-USDT"   # XRP
    "MATIC-USDT" # Polygon
    "PEPE-USDT"  # Pepe Coin
    "XCN-USDT"   # Chain
    "NEON-USDT"  # Neon EVM
    "ACH-USDT"   # Alchemy Pay
    "ADA-USDT"   # Cardano
    "DOT-USDT"   # Polkadot
    "LTC-USDT"   # Litecoin
    "BNB-USDT"   # Binance Coin
    "ATOM-USDT"  # Cosmos
    "FIL-USDT"   # Filecoin
    "SAND-USDT"  # The Sandbox
)
rm -r data
rm -r logs
mkdir logs
mkdir data

 
for coin in "${coins[@]}"; do
    echo "🚀 Fetching $coin ..."
    python3 src/fetch_coin.py "$coin" "$START_DATE" "$END_DATE" "$GRANULARITY" > "logs/$coin.log" 2>&1
    echo "✅ Completed $coin"
    sleep 5   
done

echo "✅ All tasks started! Use 'tail -f logs/*.log' to monitor progress."
