#!/usr/bin/env bash

# to run in the background: nohup ./fetch_coins.sh & disown

# Define start and end dates
START_DATE="2021-05-10"
END_DATE="2024-01-01"
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
    "LUNA-USDT"  # Terra Luna
    "MATIC-USDT" # Polygon
    "PEPE-USDT"  # Pepe Coin
)

rm -r data
rm -r logs
mkdir logs
mkdir data

# Run each in the background
for coin in "${coins[@]}"; do
     echo "🚀 Fetching $coin ..."
     python3 src/fetch_coin.py "$coin" "$START_DATE" "$END_DATE" "$GRANULARITY" > "logs/$coin.log" 2>&1
     echo "✅ Completed $coin"
     sleep 5  # Small delay before the next request
done

echo "✅ All tasks started! Use 'tail -f logs/*.log' to monitor progress."
