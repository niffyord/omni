import os
import psycopg2
import json
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DB_URL = os.getenv("TIMESCALEDB_URL")  # Ensure your .env is loaded or set this variable
TABLE = "market_indicators"
SYMBOL = os.getenv("SYMBOL", "ETH/USDT:USDT")  # Change as needed
TIMEFRAME = "15m"          # Change as needed
LIMIT = 1                  # Number of rows to fetch

def fetch_history(symbol, tf, limit=5):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT timestamp, indicators
        FROM {TABLE}
        WHERE symbol=%s AND timeframe=%s
        ORDER BY timestamp DESC
        LIMIT %s
        """, (symbol, tf, limit)
    )
    rows = cur.fetchall()
    for ts, indicators_json in rows:
        # If already a dict, use as-is; else, parse string
        if isinstance(indicators_json, dict):
            indicators = indicators_json
        else:
            indicators = json.loads(indicators_json)
        history = indicators.get("history", {})
        print(f"\nTimestamp: {ts}")
        for k, v in history.items():
            print(f"{k}: {v}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_history(SYMBOL, TIMEFRAME, LIMIT)
