import os, json, math, time, asyncio, logging
from datetime import datetime, timezone

from typing import List

import pandas as pd, numpy as np, talib, ccxt, psycopg2
from dotenv import load_dotenv

# --- tf_to_ms utility (module-level, only once) ---
TF_TO_MS = {
    '1m': 60_000, '5m': 300_000, '15m': 900_000,
    '1h': 3_600_000, '4h': 14_400_000, '1d': 86_400_000,
}
def tf_to_ms(tf: str) -> int:
    return TF_TO_MS[tf]


# ─── ENV / CONFIG ────────────────────────────────────────────────────────
load_dotenv()
SYMBOL         = os.getenv("SYMBOL", "CORE/USDT:USDT")
TF_LIST        = ["1m", "5m", "15m", "1h", "4h", "1d"]
LIMIT          = 200
HISTORY_LENGTH = int(os.getenv("HISTORY_LENGTH", 200))  # Increased from 7 to 30 for richer historical context
LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", 30))  # uniform model look-back

DB_URL         = os.getenv("TIMESCALEDB_URL")
API_KEY        = os.getenv("BYBIT_API_KEY")
API_SECRET     = os.getenv("BYBIT_API_SECRET")
TABLE          = "market_indicators"
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 7))
TS_NOW = lambda: int(datetime.now(timezone.utc).timestamp() * 1000)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)

# ─── CCXT EXCHANGE ───────────────────────────────────────────────────────
EX = ccxt.bybit(
    {
        "apiKey": API_KEY or None,
        "secret": API_SECRET or None,
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
        "timeout": 60_000,
    }
)

# ─── TIMESCALE HELPERS ───────────────────────────────────────────────────
def conn():
    return psycopg2.connect(DB_URL)


def ensure_vol_state_metrics_table():
    with conn() as c, c.cursor() as cur:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS vol_state_metrics (
            symbol TEXT,
            timeframe TEXT,
            timestamp BIGINT,
            max_atr_ratio DOUBLE PRECISION,
            PRIMARY KEY(symbol, timeframe, timestamp)
        );"""
        )
        cur.execute(
            "SELECT create_hypertable('vol_state_metrics','timestamp', if_not_exists=>TRUE);"
        )
        c.commit()

def ensure_table():

    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {TABLE}(
          symbol TEXT, timeframe TEXT, timestamp BIGINT, indicators JSONB,
          PRIMARY KEY(symbol,timeframe,timestamp)
        );"""
        )
        try:
            cur.execute(
                f"""
        CREATE OR REPLACE FUNCTION unix_now_ms() RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS $$
            SELECT FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
        $$;
        """
            )
            cur.execute(
                f"SELECT create_hypertable('{TABLE}','timestamp',if_not_exists=>TRUE);"
            )
            # Attach the integer_now_func so retention works on BIGINT timestamps
            try:
                cur.execute(
                    f"SELECT set_integer_now_func('{TABLE}', 'unix_now_ms');"
                )
            except Exception:
                pass  # ignore if already set
        except psycopg2.errors.FeatureNotSupported as e:
            if "is not empty" in str(e):
                c.rollback()
                # Ensure integer_now_func is set on existing hypertable
                try:
                    cur.execute(
                        f"SELECT set_integer_now_func('{TABLE}', 'unix_now_ms');"
                    )
                except Exception:
                    pass
                cur.execute(
                    f"SELECT create_hypertable('{TABLE}','timestamp',if_not_exists=>TRUE, migrate_data=>TRUE);"
                )
            else:
                raise
        # Commit structural changes before attempting retention policy so that they
        # persist even if the retention step fails.
        c.commit()

        # ── Retention policy (runs in a fresh transaction) ──────────────────
        if RETENTION_DAYS:
            try:
                drop_after_ms = RETENTION_DAYS * 24 * 60 * 60 * 1000
                cur.execute(
                    f"SELECT add_retention_policy('{TABLE}', drop_after => {drop_after_ms}, if_not_exists => TRUE);"
                )
                c.commit()
            except Exception as e:
                # Roll back only this retention policy transaction; the table and
                # hypertable are already committed above.
                c.rollback()
                logging.warning(f"Retention policy not applied: {e}")
        c.commit()


def write_rows(rows):
    if not rows:
        return
    with conn() as c, c.cursor() as cur:
        cur.executemany(
            f"""INSERT INTO {TABLE}(symbol,timeframe,timestamp,indicators)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT(symbol,timeframe,timestamp)
                DO UPDATE SET indicators = EXCLUDED.indicators""",
            rows,
        )
        c.commit()

def write_vol_state_rows(rows):
    if not rows:
        return
    with conn() as c, c.cursor() as cur:
        cur.executemany(
            """INSERT INTO vol_state_metrics(symbol, timeframe, timestamp, max_atr_ratio)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT DO NOTHING""",
            rows,
        )
        c.commit()

def write_bb_width_rows(rows):
    """
    Insert (symbol, timeframe, timestamp, bb_width) tuples into bb_width_metrics table.
    Creates the table if it does not exist. Uses ON CONFLICT DO NOTHING.
    """
    if not rows:
        return
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bb_width_metrics (
                symbol TEXT,
                timeframe TEXT,
                timestamp BIGINT,
                bb_width DOUBLE PRECISION,
                PRIMARY KEY(symbol, timeframe, timestamp)
            );
        """)
        cur.executemany(
            """
            INSERT INTO bb_width_metrics (symbol, timeframe, timestamp, bb_width)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
            """, rows)
        c.commit()


# ─── FETCH OHLCV + DAILY/WEEKLY ──────────────────────────────────────────
def fetch_ohlcv(tf: str) -> List[List[float]]:
    return EX.fetch_ohlcv(SYMBOL, tf, limit=LIMIT, params={"recvWindow": 60_000})



# ─── MINIMAL INDICATOR SCHEMA & FEATURE FLAGS ───
CORE_FIELDS = {
    # --- time / price ---
    "timestamp", "last_close", "open", "high", "low", "volume",
    "minute", "hour", "dow", "new_d",
    # --- trend ---
    "ema_fast", "ema_slow", "ema200", "sma21", "ema_cross", "adx",
    # --- momentum ---
    "rsi",
    # --- volatility ---
    "atr14", "atr10", "atr_ratio", "sigma5", "sigma30", "sigma120", "bb_width",
    # --- key levels ---
    "prev_day_high", "prev_day_low",
    "pivot_p", "pivot_r1", "pivot_s1", "vwap",
    # --- risk / stops ---
    "dist_to_swing_high", "dist_to_swing_low", "dist_to_ema200",
    # --- new QA/TA agent fields ---
    "dollar_vol", "sigma30_pctile", "ema_fast_slope", "ema_slow_slope", "return_1h", "return_4h", "atr10d_ratio",
    # --- high-impact TA features ---
    "eth_core_corr_30", "atr10_sigma30_ratio", "close_vs_vwap_pct", "day_range_pct", "vol_percentile_20", "trailing_max_dd_20",
    # --- leverage/seasonality features ---
    "roll_hl_pct", "streak_dir_5", "rvi_14", "obv_slope_20",
    "macd", "macd_signal", "macd_hist", "stoch_k", "stoch_d", "cci20", "mfi14"
}
ENABLE = {k: True for k in CORE_FIELDS}
ENABLE.update({
    "atr10": True, "atr_ratio": True, "bb_width": True, "dollar_vol": True, "sigma30_pctile": True, "ema_fast_slope": True, "ema_slow_slope": True,
    "return_1h": True, "return_4h": True, "atr10d_ratio": True,
    "eth_core_corr_30": True, "atr10_sigma30_ratio": True, "close_vs_vwap_pct": True, "day_range_pct": True, "vol_percentile_20": True, "trailing_max_dd_20": True,
    "roll_hl_pct": True, "streak_dir_5": True, "rvi_14": True, "obv_slope_20": True,
    "macd": True, "macd_signal": True, "macd_hist": True,
    "stoch_k": True, "stoch_d": True, "cci20": True, "mfi14": True
})

def compute(ohlcv: list[list[float]], tf: str, eth_close_series=None) -> dict:
    # Safety check for empty or insufficient data
    if not ohlcv or len(ohlcv) < 20:
        logging.warning(f"Insufficient OHLCV data for {tf}: {len(ohlcv) if ohlcv else 0} bars")
        return {"error": "insufficient_data", "timestamp": TS_NOW()}
    try:
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]).astype(float)
    except Exception as e:
        logging.error(f"Failed to process OHLCV data for {tf}: {e}")
        return {"error": "invalid_data", "timestamp": TS_NOW()}
    df = df.sort_values("ts", ascending=True)
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    last = lambda s: float(s.iat[-1]) if len(s) > 0 and not pd.isna(s.iat[-1]) else 0.0
    out = {}

    # --- raw candle & session flag ---------------------------------
    if ENABLE.get("open"):   out["open"]   = float(df["open"].iat[-1])
    if ENABLE.get("high"):   out["high"]   = float(df["high"].iat[-1])
    if ENABLE.get("low"):    out["low"]    = float(df["low"].iat[-1])
    if ENABLE.get("volume"): out["volume"] = float(df["volume"].iat[-1])

    if ENABLE.get("timestamp"): 
        out["timestamp"] = int(df["ts"].iat[-1])
    if ENABLE.get("last_close"): 
        out["last_close"] = last(close)
    if ENABLE.get("minute") or ENABLE.get("hour") or ENABLE.get("dow"):
        dt = datetime.fromtimestamp(df["ts"].iat[-1] / 1000, tz=timezone.utc)
        if ENABLE.get("minute"): 
            out["minute"] = dt.minute
        if ENABLE.get("hour"): 
            out["hour"] = dt.hour
        if ENABLE.get("dow"): 
            out["dow"] = dt.weekday()

    if ENABLE.get("new_d"):
        # True on the first bar after 00:00 UTC
        prev_ts = df["ts"].iat[-2] if len(df) > 1 else df["ts"].iat[-1]
        prev_day = datetime.fromtimestamp(prev_ts/1000, tz=timezone.utc).day
        curr_day = datetime.fromtimestamp(out["timestamp"]/1000, tz=timezone.utc).day
        out["new_d"] = int(curr_day != prev_day)

    # --- Trend ---
    if ENABLE.get("ema_fast"): 
        out["ema_fast"] = float(talib.EMA(close, 8).iat[-1])
    if ENABLE.get("ema_slow"): 
        out["ema_slow"] = float(talib.EMA(close, 21).iat[-1])
    if ENABLE.get("ema200"):
        out["ema200"] = float(talib.EMA(close, 200).iat[-1])
    if ENABLE.get("sma21"): 
        out["sma21"] = float(talib.SMA(close, 21).iat[-1])
    if ENABLE.get("ema_cross"):
        if out.get("ema_fast") is not None and out.get("ema_slow") is not None:
            if out["ema_fast"] > out["ema_slow"]:
                out["ema_cross"] = "bullish"
            elif out["ema_fast"] < out["ema_slow"]:
                out["ema_cross"] = "bearish"
            else:
                out["ema_cross"] = "none"
    if ENABLE.get("adx"): 
        out["adx"] = float(talib.ADX(high, low, close, 14).iat[-1])

    # --- Momentum ---
    if ENABLE.get("rsi"): 
        out["rsi"] = float(talib.RSI(close, 14).iat[-1])

    # --- Risk helpers (restored)
    if ENABLE.get("dist_to_swing_high"):
        out["dist_to_swing_high"] = last(close) - close.rolling(10).max().iat[-1]
    if ENABLE.get("dist_to_swing_low"):
        out["dist_to_swing_low"] = last(close) - close.rolling(10).min().iat[-1]

    # --- Volatility ---
    if ENABLE.get("atr14"): 
        out["atr14"] = float(talib.ATR(high, low, close, 14).iat[-1])
    if ENABLE.get("atr10"):
        out["atr10"] = float(talib.ATR(high, low, close, 10).iat[-1])
    if ENABLE.get("atr_ratio"):
        lc = out.get("last_close") or 0
        a10 = out.get("atr10") or 0
        out["atr_ratio"] = a10 / lc if lc else None
    # === max-ratio component for this TF =========================
    # NOTE: scaling matches the Stage-2 spec
    scale = {"1h": 24, "4h": 6, "1d": 1}.get(tf, None)
    if scale is not None and out["atr_ratio"] is not None:
        out["max_atr_ratio_comp"] = out["atr_ratio"] * scale
    else:
        out["max_atr_ratio_comp"] = None
    if ENABLE.get("sigma5"): 
        out["sigma5"] = float(close.pct_change().rolling(5).std().iat[-1]) * math.sqrt(5)
    if ENABLE.get("sigma30"): 
        out["sigma30"] = float(close.pct_change().rolling(30).std().iat[-1]) * math.sqrt(30)
    if ENABLE.get("sigma120"):
        out["sigma120"] = (
            float(close.pct_change().rolling(120).std().iat[-1]) * math.sqrt(120)
        )
    if ENABLE.get("bb_width"):
        upper, middle, lower = talib.BBANDS(close, 20)
        out["bb_width"] = float((upper.iat[-1] - lower.iat[-1]) / middle.iat[-1])

    # --- NEW: realised-vol percentile ---
    if len(close) >= 60:
        sigma_series = close.pct_change().rolling(30).std() * math.sqrt(30)
        pctl = (sigma_series.rank(pct=True).iat[-1])  # 0-1
        out["sigma30_pctile"] = float(round(pctl, 4))
    else:
        out["sigma30_pctile"] = None

    # --- NEW: simple return horizons ---
    if len(close) >= 60 and tf in {"1m", "5m", "15m"}:
        out["return_1h"] = float((close.iat[-1] / close.iat[-60] - 1) * 100)
    else:
        out["return_1h"] = None
    if len(close) >= 240 and tf != "1d":
        out["return_4h"] = float((close.iat[-1] / close.iat[-240] - 1) * 100)
    else:
        out["return_4h"] = None

    # --- NEW: EMA slopes (5-bar) ---
    if len(close) >= 13:
        out["ema_fast_slope"] = float(out["ema_fast"] - talib.EMA(close, 8).iat[-6])
        out["ema_slow_slope"] = float(out["ema_slow"] - talib.EMA(close, 21).iat[-6])
    else:
        out["ema_fast_slope"] = None
        out["ema_slow_slope"] = None

    # --- NEW: dollar turnover ---
    out["dollar_vol"] = float((close * volume).iat[-1])

    # --- NEW: ATR-10d ratio (daily only) ---
    if tf == "1d" and len(close) >= 10:
        atr10d = talib.ATR(high, low, close, 10).iat[-1]
        out["atr10d_ratio"] = float(atr10d / close.iat[-1]) if close.iat[-1] else None
    else:
        out["atr10d_ratio"] = None

    # --- Key levels (prev-day and pivot) ---
    try:
        yd = EX.fetch_ohlcv(SYMBOL, "1d", limit=2)[0]  # previous day bar
        yd_high, yd_low, yd_close = yd[2], yd[3], yd[4]
        pivot_p = (yd_high + yd_low + yd_close) / 3
    except Exception:
        current_price = last(close)
        yd_high = yd_low = yd_close = current_price
        pivot_p = current_price
    out.update({
        "prev_day_high": yd_high,
        "prev_day_low": yd_low,
        "pivot_p": pivot_p,
        "pivot_r1": 2 * pivot_p - yd_low,
        "pivot_s1": 2 * pivot_p - yd_high,
        "vwap": float(((high + low + close) / 3 * volume).sum() / (volume.sum() or 1))
    })

    # --- High-impact TA features ---
    # eth_core_corr_30: 30-bar rolling Pearson correlation to ETH/USDT
    if ENABLE.get("eth_core_corr_30") and len(close) >= 30 and eth_close_series is not None and len(eth_close_series) >= 30:
        try:
            minlen = min(len(close), len(eth_close_series))
            core_close = close.iloc[-minlen:]
            eth_close = eth_close_series.iloc[-minlen:]
            out["eth_core_corr_30"] = float(pd.Series(core_close).rolling(30).corr(eth_close).iat[-1])
        except Exception:
            out["eth_core_corr_30"] = None
    else:
        out["eth_core_corr_30"] = None

    # ATR10/sigma30 ratio
    out["atr10_sigma30_ratio"] = ((out["atr10"] / out["last_close"]) / out["sigma30"]) if out.get("sigma30") not in (None, 0) else None

    # --- Leverage/seasonality features ---
    # roll_hl_pct
    out["roll_hl_pct"] = ((high.iat[-1] - low.iat[-1]) / close.iat[-1] * 100) if len(close) > 0 and close.iat[-1] else None

    # streak_dir_5
    if len(close) >= 6:
        out["streak_dir_5"] = int(np.sign(close.diff()).tail(5).sum())
    else:
        out["streak_dir_5"] = None

    # rvi_14
    if len(close) >= 15:
        try:
            out["rvi_14"] = float(talib.RVI(high, low, close, 14).iat[-1])
        except Exception:
            out["rvi_14"] = None
    else:
        out["rvi_14"] = None

    # obv_slope_20
    if len(close) >= 21:
        try:
            obv = talib.OBV(close, volume)
            out["obv_slope_20"] = float(obv.iat[-1] - obv.iat[-21])
        except Exception:
            out["obv_slope_20"] = None
    else:
        out["obv_slope_20"] = None


    # MACD
    try:
        macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        out["macd"] = float(macd.iat[-1])
        out["macd_signal"] = float(macd_signal.iat[-1])
        out["macd_hist"] = float(macd_hist.iat[-1])
    except Exception:
        out["macd"] = out["macd_signal"] = out["macd_hist"] = None

    # Stochastic Oscillator
    try:
        slowk, slowd = talib.STOCH(high, low, close)
        out["stoch_k"] = float(slowk.iat[-1])
        out["stoch_d"] = float(slowd.iat[-1])
    except Exception:
        out["stoch_k"] = out["stoch_d"] = None

    # CCI (20)
    try:
        out["cci20"] = float(talib.CCI(high, low, close, 20).iat[-1])
    except Exception:
        out["cci20"] = None

    # Money Flow Index (14)
    try:
        out["mfi14"] = float(talib.MFI(high, low, close, volume, 14).iat[-1])
    except Exception:
        out["mfi14"] = None

    # Close vs VWAP %
    out["close_vs_vwap_pct"] = ((out["last_close"] / out["vwap"] - 1) * 100) if out.get("vwap") else None

    # Day range position %
    day_range = out["prev_day_high"] - out["prev_day_low"]
    out["day_range_pct"] = ((out["last_close"] - out["prev_day_low"]) / day_range * 100) if day_range else None

    # Volume percentile (20)
    if len(volume) >= 20:
        vol_pct = volume.rank(pct=True).iat[-1]
        out["vol_percentile_20"] = float(round(vol_pct, 4))
    else:
        out["vol_percentile_20"] = None

    # Trailing max drawdown (20)
    if len(close) >= 20:
        peak = close.rolling(20).max()
        dd_series = (close/peak - 1)*100
        out["trailing_max_dd_20"] = float(dd_series.min())
    else:
        out["trailing_max_dd_20"] = None

    if ENABLE.get("dist_to_ema200"):
        out["dist_to_ema200"] = out.get("last_close") - out.get("ema200", 0)

    return out

# ... rest of the code remains the same ...
    ind = compute(ohlcv, tf)
    return SYMBOL, tf, ind["timestamp"], ind

async def cycle():
    """Main processing cycle with error handling"""
    rows = []
    vol_state_rows = []
    eth_cache = {}
    for tf in TF_LIST:
        try:
            ohlcv = fetch_ohlcv(tf)
            if not ohlcv or len(ohlcv) < 20:
                logging.warning(f"Skipping {tf}: insufficient data ({len(ohlcv) if ohlcv else 0} bars)")
                continue
            # ETH close cache for this tf
            if tf not in eth_cache:
                try:
                    eth_ohlcv = EX.fetch_ohlcv("ETH/USDT:USDT", tf, limit=len(ohlcv))
                    eth_cache[tf] = pd.Series([b[4] for b in eth_ohlcv], dtype=float)
                except Exception:
                    eth_cache[tf] = None
            indicators = compute(ohlcv, tf, eth_close_series=eth_cache[tf])
            # Skip if compute returned an error
            if "error" in indicators:
                logging.warning(f"Skipping {tf}: {indicators.get('error')}")
                continue
            # ------- guarantee JSON-safe & complete ----------
            for fld in CORE_FIELDS:            # ensure every key exists
                indicators.setdefault(fld, None)
            for k, v in indicators.items():     # replace NaN / inf
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    indicators[k] = None
            rows.append((SYMBOL, tf, indicators["timestamp"], json.dumps(indicators, allow_nan=False)))
            # --- persist max_atr_ratio_comp ---
            if indicators.get("max_atr_ratio_comp") is not None:
                vol_state_rows.append((SYMBOL, tf, indicators["timestamp"], indicators["max_atr_ratio_comp"]))
            logging.info(f"Processed {tf}: {len(indicators)} indicators")
        except Exception as e:
            logging.error(f"Failed to process timeframe {tf}: {e}")
            continue
    if rows:
        try:
            write_rows(rows)
            write_vol_state_rows(vol_state_rows)
            logging.info(f"Wrote {len(rows)} rows to database")
        except Exception as e:
            logging.error(f"Failed to write to database: {e}")
    else:
        logging.warning("No data to write - all timeframes failed")


async def main_loop():
    """Main loop with proper resource cleanup and Redis history backfill"""
    global EX

    # Initialize database table once at startup
    ensure_table()
    ensure_vol_state_metrics_table()
    logging.info("ETL started.")

    # --- Redis/history backfill removed: indicator_fields, push_history, and related code deleted ---
    try:
        while True:
            start = time.time()
            try:
                await cycle()
            except Exception as e:
                logging.error(f"Cycle error: {e}")
            await asyncio.sleep(max(0, 60 - (time.time() - start)))
    finally:
        # Cleanup exchange connection
        try:
            if hasattr(EX, 'close'):
                await EX.close()
        except Exception as e:
            logging.error(f"Error closing exchange: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("ETL stopped by user.")
