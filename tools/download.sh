#!/usr/bin/env bash

# to run in the background: nohup tools/download.sh & disown

COIN_PAIRS_FILE="extras/coin-pairs"

rm -r logs
mkdir logs

python3 src/main.py "$COIN_PAIRS_FILE"
echo "✅ Completed $coin"

 
