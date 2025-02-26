from src.coin_db import CoinDB
from datetime import datetime, timezone

db = CoinDB("data")

start_date = datetime.fromisoformat("2021-05-29").replace(tzinfo=timezone.utc)
end_date = datetime.fromisoformat("2021-05-30").replace(tzinfo=timezone.utc)
query = db.query("SKL-USD",start_date, end_date)
print(query)