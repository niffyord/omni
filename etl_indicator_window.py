"""ETL job to compute a rolling window of indicators for each timeframe.
Results are stored in the market_indicators table for use by tools like get_last_n_indicators."""
import os, json, math, asyncio, logging
from typing import List
from dotenv import load_dotenv
import ccxt

from etl_indicators import compute, ETH_FIELDS, write_rows, ensure_table

load_dotenv()
SYMBOL = os.getenv("SYMBOL", "ETH/USDT:USDT")
TF_LIST = ["1m", "5m", "15m", "1h", "4h", "1d"]
LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", 30))
FETCH_LIMIT = int(os.getenv("LOOKBACK_FETCH_LIMIT", 120))

EX = ccxt.bybit({
    "apiKey": os.getenv("BYBIT_API_KEY") or None,
    "secret": os.getenv("BYBIT_API_SECRET") or None,
    "enableRateLimit": True,
    "options": {"defaultType": "linear"},
    "timeout": 60_000,
})

def fetch_ohlcv(tf: str, limit: int) -> List[List[float]]:
    return EX.fetch_ohlcv(SYMBOL, tf, limit=limit, params={"recvWindow": 60_000})

async def refresh_window() -> None:
    ensure_table()
    rows: list[tuple] = []
    for tf in TF_LIST:
        try:
            ohlcv = fetch_ohlcv(tf, limit=max(FETCH_LIMIT, LOOKBACK_WINDOW + 60))
            if not ohlcv or len(ohlcv) < 20:
                logging.warning(f"Skipping {tf}: insufficient data ({len(ohlcv) if ohlcv else 0} bars)")
                continue
            start = len(ohlcv) - LOOKBACK_WINDOW
            for i in range(max(0, start), len(ohlcv)):
                window = ohlcv[: i + 1]
                ind = compute(window, tf)
                if "error" in ind:
                    continue
                for fld in ETH_FIELDS:
                    ind.setdefault(fld, None)
                for k, v in ind.items():
                    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        ind[k] = None
                ts = int(window[-1][0])
                rows.append((SYMBOL, tf, ts, json.dumps(ind, allow_nan=False)))
        except Exception as e:
            logging.error(f"{tf} refresh failed: {e}")
    if rows:
        write_rows(rows)
        logging.info(f"Wrote {len(rows)} indicator rows")

async def main_loop():
    while True:
        await refresh_window()
        await asyncio.sleep(300)  # 5 min

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Indicator window ETL stopped by user.")
