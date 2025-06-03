import os
import logging
import ccxt
import pandas as pd
from dotenv import load_dotenv
from etl_indicators import compute, write_vol_state_rows, write_bb_width_rows

def get_last_n_indicators(symbol, tf, n, field, ex=None):
    if ex is None:
        ex = ccxt.bybit({
            "apiKey": os.getenv("BYBIT_API_KEY"),
            "secret": os.getenv("BYBIT_API_SECRET"),
            "enableRateLimit": True,
            "options": {"defaultType": "linear"},
            "timeout": 60_000,
        })
    ohlcv = ex.fetch_ohlcv(symbol, tf, limit=n+40)  # get enough bars for rolling calculations
    vals = []
    for i in range(19, len(ohlcv)):
        window = ohlcv[:i+1]
        indicators = compute(window, tf)
        val = indicators.get(field)
        if val is not None:
            vals.append(val)
    return vals[-n:]

def backfill_vol_state_and_bbwidth(symbols=["CORE/USDT:USDT", "ETH/USDT:USDT"], tfs=["1h", "4h", "1d"],
                                   n_max_ratio=30, n_bb_width=20):
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    ex = ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_API_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
        "timeout": 60_000,
    })
    ctx = {"vol_state": {"CORE": {}, "ETH": {}}, "bb_series": {}}
    all_vol_rows = []
    all_bb_rows = []
    for symbol in symbols:
        sym_key = "CORE" if symbol.startswith("CORE") else "ETH"
        for tf in tfs:
            # max_ratio_last30
            max_ratios = get_last_n_indicators(symbol, tf, n_max_ratio, "max_atr_ratio_comp", ex)
            ctx["vol_state"][sym_key][f"max_ratio_last{n_max_ratio}_{tf}"] = max_ratios
            # Prepare DB rows for backfill
            ohlcv = ex.fetch_ohlcv(symbol, tf, limit=n_max_ratio+40)
            for i in range(19, len(ohlcv)):
                window = ohlcv[:i+1]
                indicators = compute(window, tf)
                ts = int(window[-1][0])
                val = indicators.get("max_atr_ratio_comp")
                if val is not None:
                    all_vol_rows.append((symbol, tf, ts, val))
            # bb_width_last20 (only for CORE, but can be extended)
            if sym_key == "CORE":
                bb_widths = get_last_n_indicators(symbol, tf, n_bb_width, "bb_width", ex)
                if tf not in ctx["bb_series"]:
                    ctx["bb_series"][tf] = {}
                ctx["bb_series"][tf][f"bb_width_last{n_bb_width}"] = bb_widths
                # Collect rows for DB
                ohlcv = ex.fetch_ohlcv(symbol, tf, limit=n_bb_width+40)
                for i in range(19, len(ohlcv)):
                    window = ohlcv[:i+1]
                    indicators = compute(window, tf)
                    ts = int(window[-1][0])
                    val = indicators.get("bb_width")
                    if val is not None:
                        all_bb_rows.append((symbol, tf, ts, val))
            logging.info(f"{symbol} {tf}: max_ratio_last{n_max_ratio}={max_ratios[-5:]}, bb_width_last{n_bb_width}={bb_widths[-5:] if sym_key=='CORE' else 'N/A'}")
    # Write all vol_state rows to DB
    write_vol_state_rows(all_vol_rows)
    write_bb_width_rows(all_bb_rows)
    logging.info(f"Backfill complete: wrote {len(all_vol_rows)} vol_state rows and {len(all_bb_rows)} bb_width rows.")
    return ctx

import time

if __name__ == "__main__":
    try:
        while True:
            logging.info("Starting ETL backfill run...")
            ctx = backfill_vol_state_and_bbwidth()
            logging.info("ETL backfill run complete. Sleeping for 1 hour.")
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("ETL stopped by user.")
