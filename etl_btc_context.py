"""
Light-weight BTC Momentum ETL Pipeline (v0.1)
============================================
Calculates a minimal momentum context for BTC/USDT so that the ETH trader can
avoid trading against broad‑market direction.

• Indicators on BTC: EMA‑8/21, RSI‑14 and MACD histogram.
• Additional cross‑asset metrics:
    – eth_btc_ratio  (ETH price / BTC price)
    – btc_eth_corr   (Pearson correlation of recent returns, N=30)
    – btc_volume_dom (BTC volume dominance vs ETH)
    – atr, bb_width, hv_ann, rsi_vol, macd_norm
• Timeframes processed by default: 1m, 5m, 15m, 1h, 4h, 1d
• Derived flag: btc_momentum = bullish / bearish / neutral
• Stores one JSONB row per timeframe into the existing TimescaleDB table
  `market_indicators` so existing read helpers work unchanged.

Environment variables (via .env):
    TIMESCALEDB_URL        Postgres/TimescaleDB connection string
    BYBIT_API_KEY          Optional (not needed for public OHLCV)
    BYBIT_API_SECRET       Optional
    BTC_SYMBOL             Override default symbol (default: "BTC/USDT:USDT")
    ETH_SYMBOL             (optional) override ETH symbol (default: "ETH/USDT:USDT")
    BTC_TF_LIST            Comma-separated TF list (default: "1m,5m,15m,1h,4h,1d")
    BTC_LIMIT              Bars to fetch (default: 400)
    BTC_LOOP_INTERVAL      Seconds between cycles (default: 60)

Run via:
    python etl_btc_context.py   (uses an async loop)

You can supervise this script with systemd/pm2/etc. alongside the existing ETH
`etl_indicators.py`.
"""

import os, json, math, asyncio, logging, time
from datetime import datetime, timezone
from typing import List

# ─── HISTORY-AWARE CONFIG ─────────────────────────────────────────────
HIST_N_SHORT = int(os.getenv("ETH_HIST_N_SHORT", 3))   # e.g. 3 candles back
HIST_N_LONG  = int(os.getenv("ETH_HIST_N_LONG",  5))   # e.g. 5 candles back

import pandas as pd, numpy as np, talib, ccxt, psycopg2
from dotenv import load_dotenv

# ─── ENV / CONFIG ────────────────────────────────────────────────────────
load_dotenv()
SYMBOL        = os.getenv("BTC_SYMBOL", "BTC/USDT:USDT")
ETH_SYMBOL   = os.getenv("ETH_SYMBOL", "ETH/USDT:USDT")
TF_LIST       = [tf.strip() for tf in os.getenv("BTC_TF_LIST", "1m,5m,15m,1h,4h,1d").split(",") if tf.strip()]
LIMIT         = int(os.getenv("BTC_LIMIT", 400))  # more bars for vol calcs
DB_URL        = os.getenv("TIMESCALEDB_URL")
TABLE         = os.getenv("BTC_TABLE", "market_indicators")  # share table
LOOP_INTERVAL = int(os.getenv("BTC_LOOP_INTERVAL", 60))
# Limit concurrent HTTP requests to avoid exhausting connection pool
MAX_CONCURRENCY = int(os.getenv("BTC_MAX_CONCURRENCY", 8))

TS_NOW = lambda: int(datetime.now(timezone.utc).timestamp() * 1000)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("BTC_ETL")

# Limit asynchronous fetch concurrency
SEM = asyncio.Semaphore(MAX_CONCURRENCY)

# ─── CCXT EXCHANGE ───────────────────────────────────────────────────────
EX = ccxt.bybit(
    {
        "apiKey": os.getenv("BYBIT_API_KEY") or None,
        "secret": os.getenv("BYBIT_API_SECRET") or None,
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
        "timeout": 60_000,
    }
)

# Expand requests connection pool to reduce 'connection pool is full' warnings
try:
    import requests
    adapter = requests.adapters.HTTPAdapter(pool_maxsize=max(10, MAX_CONCURRENCY * 2))
    if hasattr(EX, "session") and isinstance(EX.session, requests.Session):
        EX.session.mount("https://", adapter)
        EX.session.mount("http://", adapter)
except Exception:
    pass

# ─── TIMESCALEDB HELPERS ────────────────────────────────────────────────

def _conn():
    if DB_URL is None:
        raise ValueError("TIMESCALEDB_URL not set in environment.")
    return psycopg2.connect(DB_URL)


def ensure_vol_state_metrics_table():
    with _conn() as c, c.cursor() as cur:
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
    """Ensure the target hypertable exists (reuse schema from main ETL)."""
    with _conn() as c, c.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE}(
                symbol TEXT,
                timeframe TEXT,
                timestamp BIGINT,
                indicators JSONB,
                PRIMARY KEY(symbol,timeframe,timestamp)
            );"""
        )
        # Create helper function & hypertable if not yet present
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
        # Attach integer_now_func so retention works on BIGINT timestamps
        try:
            cur.execute(
                f"SELECT set_integer_now_func('{TABLE}', 'unix_now_ms');"
            )
        except Exception:
            pass
        c.commit()


def write_rows(rows: List[tuple]):
    if not rows:
        return
    with _conn() as c, c.cursor() as cur:
        cur.executemany(
            f"""INSERT INTO {TABLE}(symbol,timeframe,timestamp,indicators)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT(symbol,timeframe,timestamp)
                DO UPDATE SET indicators = EXCLUDED.indicators""",
            rows,
        )
        c.commit()

# ─── UTILITY: HUMAN-FRIENDLY SUMMARY ──────────────────────────────────

def _summarise(metrics: dict) -> str:
    """Return a single-line key=value summary of ALL metrics for easier LLM digestion."""
    kv: list[str] = []

    def add(key: str, label: str | None = None, fmt_str: str = "{:.4f}"):
        val = metrics.get(key)
        if val is None:
            return
        # Convert pandas Series to scalar if needed
        try:
            import pandas as pd
            if isinstance(val, pd.Series):
                val = val.iat[-1]
        except Exception:
            pass
        name = label or key
        if isinstance(val, str):
            kv.append(f"{name}={val}")
        elif isinstance(val, (int, float)):
            if fmt_str == "{}":
                kv.append(f"{name}={val}")
            else:
                try:
                    kv.append(f"{name}=" + fmt_str.format(val))
                except Exception:
                    kv.append(f"{name}={val}")

    # Ordered, comprehensive list of fields
    add("btc_momentum", "momentum", "{}")
    add("btc_rsi", "rsi", "{:.1f}")
    add("rsi_vol", "rsi_vol", "{:.1f}")
    add("btc_macd", "macd", "{:.3f}")
    add("btc_macd_signal", "macd_sig", "{:.3f}")
    add("macd_norm", "macd_norm", "{:.3f}")
    add("btc_ema_fast", "ema_fast", "{:.2f}")
    add("btc_ema_slow", "ema_slow", "{:.2f}")
    add("atr", "atr", "{:.2f}")
    add("hv_ann", "hv", "{:.4f}")
    add("bb_width", "bb_width", "{:.4f}")
    add("eth_btc_ratio", "eth_btc_ratio", "{:.6f}")
    add("btc_eth_corr", "corr", "{:.3f}")
    add("btc_volume_dom", "vol_dom", "{:.4f}")
    add("last_close_eth", "last_eth", "{:.2f}")
    add("last_close_btc", "last_btc", "{:.2f}")

    return "; ".join(kv) if kv else "n/a"

# ─── INDICATOR & CROSS-ASSET COMPUTATION ─────────────────────────────

def tf_to_minutes(tf: str) -> int:
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endswith("h"):
        return int(tf[:-1]) * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 60 * 24
    return 1

N_CORR_DEFAULT = 30

def to_scalar(val):
    import pandas as pd
    if isinstance(val, pd.Series):
        try:
            return val.iat[-1]
        except Exception:
            return None
    return val

def contextualize_metrics(metrics):
    import numpy as np
    def f(x, digits=2):
        """Return formatted number or "N/A". Accepts digits int or str (e.g. '+4')."""
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "N/A"
        # Non-numeric types
        if not isinstance(x, (int, float)):
            try:
                x = float(x)
            except Exception:
                return "N/A"
        try:
            # Handle custom format like '+4'
            if isinstance(digits, str):
                fmt_spec = f"{{:{digits}f}}"
            else:
                fmt_spec = f"{{:.{digits}f}}"
            return fmt_spec.format(x)
        except Exception:
            return str(x)

    # Group indicators for clarity
    out = []
    # --- Momentum ---
    mom = metrics.get('btc_momentum', 'N/A')
    rsi = metrics.get('btc_rsi', 'N/A')
    macd = metrics.get('btc_macd', 'N/A')
    macd_sig = metrics.get('btc_macd_signal', 'N/A')
    ema_fast = metrics.get('btc_ema_fast', 'N/A')
    ema_slow = metrics.get('btc_ema_slow', 'N/A')
    out.append(f"Momentum: {mom} (RSI={f(rsi)} | MACD={f(macd)} vs Signal={f(macd_sig)} | EMA Fast={f(ema_fast)} vs Slow={f(ema_slow)})")
    # --- Volatility ---
    atr = metrics.get('atr', 'N/A')
    atr10d = metrics.get('atr10d', 'N/A')
    hv = metrics.get('hv_ann', 'N/A')
    bb_width = metrics.get('bb_width', 'N/A')
    rsi_vol = metrics.get('rsi_vol', 'N/A')
    out.append(f"Volatility: ATR={f(atr)} (10d={f(atr10d)}), HV={f(hv,4)}, BB Width={f(bb_width,5)}, RSI Vol={f(rsi_vol)}")
    # --- Price ---
    price = metrics.get('last_close_eth', 'N/A')
    price_btc = metrics.get('last_close_btc', 'N/A')
    eth_btc_ratio = metrics.get('eth_btc_ratio', 'N/A')
    out.append(f"Price: ETH={f(price)} | BTC={f(price_btc)} | ETH/BTC Ratio={f(eth_btc_ratio,6)}")
    # --- Correlation ---
    corr = metrics.get('btc_eth_corr', 'N/A')
    out.append(f"Correlation: BTC/ETH Corr={f(corr,3)}")
    # --- Volume ---
    vol_dom = metrics.get('btc_volume_dom', 'N/A')
    out.append(f"Volume: Dominance={f(vol_dom,4)}")
    # --- Risk ---
    atr10d_ratio = metrics.get('atr10d_ratio', 'N/A')
    out.append(f"Risk: ATR10d Ratio={f(atr10d_ratio,6)}")
    # --- Trend/History ---
def compute_metrics(btc_ohlcv: List[List[float]], eth_ohlcv: List[List[float]], tf: str) -> dict:
    """Return momentum + cross-asset & vol metrics for a single timeframe, with short-term history deltas."""
    if (not btc_ohlcv or len(btc_ohlcv) < 30) or (not eth_ohlcv or len(eth_ohlcv) < 30):
        return {"error": "insufficient_data", "timestamp": TS_NOW()}

    close_btc = [x[4] for x in btc_ohlcv]
    close_eth = [x[4] for x in eth_ohlcv]
    volume_btc = [x[5] for x in btc_ohlcv]
    high_btc = [x[2] for x in btc_ohlcv]
    low_btc = [x[3] for x in btc_ohlcv]

    # --- ATR (14) ---
    atr_series = talib.ATR(np.array(high_btc), np.array(low_btc), np.array(close_btc), 14)
    def last_value(arr):
        try:
            if arr is None:
                return None
            if isinstance(arr, (list, np.ndarray)) and len(arr) > 0:
                return float(arr[-1])
            import pandas as pd
            if isinstance(arr, pd.Series) and not arr.empty:
                return float(arr.iat[-1])
        except Exception:
            pass
        return None
    last_atr = last_value(atr_series)
    last_close = float(close_btc[-1])
    atr_ratio = last_atr / last_close if last_close else None
    scale = {"1h": 24, "4h": 6, "1d": 1}.get(tf, None)
    if scale is not None and atr_ratio is not None:
        max_atr_ratio_comp = atr_ratio * scale
    else:
        max_atr_ratio_comp = None

    # Momentum indicators on BTC
    ema_fast = None
    ema_slow = None
    rsi = None
    macd = None
    macd_sig = None
    macd_hist = None
    # Compute indicators only if enough data
    if len(close_btc) > 30 and tf not in {"4h", "1d"}:
        arr = np.array(close_btc, dtype=float)
        try:
            ema_fast = talib.EMA(arr, timeperiod=8)
        except Exception:
            ema_fast = None
        try:
            ema_slow = talib.EMA(arr, timeperiod=21)
        except Exception:
            ema_slow = None
        try:
            rsi = talib.RSI(arr, timeperiod=14)
        except Exception:
            rsi = None
        try:
            macd, macd_sig, macd_hist = talib.MACD(arr, fastperiod=12, slowperiod=26, signalperiod=9)
        except Exception:
            macd = macd_sig = macd_hist = None

    def last_value(arr):
        try:
            if arr is None:
                return None
            if isinstance(arr, (list, np.ndarray)) and len(arr) > 0:
                return float(arr[-1])
            import pandas as pd
            if isinstance(arr, pd.Series) and not arr.empty:
                return float(arr.iat[-1])
        except Exception:
            pass
        return None

    last_ema_fast = last_value(ema_fast)
    last_ema_slow = last_value(ema_slow)
    last_rsi      = last_value(rsi)
    last_macd     = last_value(macd)
    last_macd_sig = last_value(macd_sig)
    last_macd_hist = last_value(macd_hist)

    # Momentum classification
    if (
        last_ema_fast is not None and last_ema_slow is not None and last_rsi is not None and
        last_ema_fast > last_ema_slow and last_rsi > 55
    ):
        momentum = "bullish"
    elif (
        last_ema_fast is not None and last_ema_slow is not None and last_rsi is not None and
        last_ema_fast < last_ema_slow and last_rsi < 45
    ):
        momentum = "bearish"
    else:
        momentum = "neutral"

    # Cross-asset metrics
    eth_btc_ratio = float(close_eth[-1]) / float(close_btc[-1]) if float(close_btc[-1]) != 0 else None

    # Realized vol (annualized)
    minutes = tf_to_minutes(tf)
    if minutes == 0:
        minutes = 1
    ann_factor = (525_600 / minutes) ** 0.5  # sqrt of bars per year
    hv_ann = float(np.std(np.diff(close_btc)) * ann_factor) if len(close_btc) > 5 else None

    # ATR (14) already computed above

    # Volume dominance
    btc_volume_dom = None
    if len(volume_btc) > 0 and len(close_btc) > 0:
        btc_vol_sum = sum(volume_btc)
        eth_vol_sum = sum([x[5] for x in eth_ohlcv])
        if btc_vol_sum + eth_vol_sum > 0:
            btc_volume_dom = float(btc_vol_sum / (btc_vol_sum + eth_vol_sum))

    # --- correlation guard ------------------------------------------
    try:
        base_corr = np.corrcoef(np.diff(close_btc[-N_CORR_DEFAULT:]), np.diff(close_eth[-N_CORR_DEFAULT:]))[0, 1]

    except Exception:
        base_corr = None

    bb_width_val = None
    if len(close_btc) >= 20:
        try:
            upper, middle, lower = talib.BBANDS(np.array(close_btc), 20)
            bb_width_val = float((upper[-1] - lower[-1]) / middle[-1])
        except Exception:
            bb_width_val = None

    rsi_vol_val = None
    if len(close_btc) >= 28:
        try:
            rsi_series = talib.RSI(np.array(close_btc), 14)
            rsi_vol_val = float(np.std(rsi_series[-14:]))
        except Exception:
            rsi_vol_val = None

    metrics = {
        "timestamp": int(btc_ohlcv[-1][0]),
        "btc_momentum": momentum,
        "btc_rsi": last_rsi,
        "btc_macd": last_macd,
        "btc_macd_signal": last_macd_sig,
        "macd_norm": (last_macd_hist / last_close) if (last_macd_hist is not None and last_close) else None,
        "btc_ema_fast": last_ema_fast,
        "btc_ema_slow": last_ema_slow,
        "rsi_vol": rsi_vol_val,
        "eth_btc_ratio": eth_btc_ratio,
        "btc_eth_corr": base_corr,
        "btc_volume_dom": btc_volume_dom,
        "hv_ann": hv_ann,
        "atr": last_atr,
        "bb_width": bb_width_val,
        "last_close_eth": float(close_eth[-1]),
        "last_close_btc": float(close_btc[-1]),
        "atr_ratio": atr_ratio,
        "max_atr_ratio_comp": max_atr_ratio_comp,
    }

    # Add human-readable summary after core fields are set
    metrics["summary"] = contextualize_metrics(metrics)

    # Minimal return expected by caller
    return metrics

# ─── MAIN LOOP ─────────────────────────────────────────────────────────

async def cycle_once():
    now = int(time.time())
    # --- throttle high-TF requests ---
    tf_ready = []
    for tf in TF_LIST:
        if tf in {"4h", "1d"}:
            # Only fetch these once per hour
            if now % 3600 < LOOP_INTERVAL:
                tf_ready.append(tf)
        else:
            tf_ready.append(tf)
    # --- parallel fetch for ETH and BTC OHLCV ---
    async def fetch_ohlcv(symbol, tf):
        async with SEM:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                lambda: EX.fetch_ohlcv(symbol, tf, limit=LIMIT, params={"recvWindow": 60_000}),
            )
    eth_tasks = {tf: asyncio.create_task(fetch_ohlcv(ETH_SYMBOL, tf)) for tf in tf_ready}
    btc_tasks = {tf: asyncio.create_task(fetch_ohlcv(SYMBOL, tf)) for tf in tf_ready}
    rows = []
    vol_state_rows = []
    for tf in tf_ready:
        try:
            eth_ohlcv = await eth_tasks[tf]
            btc_ohlcv = await btc_tasks[tf]
            ind = compute_metrics(btc_ohlcv, eth_ohlcv, tf)
            rows.append((SYMBOL, tf, ind["timestamp"], json.dumps(ind)))
            # --- persist max_atr_ratio_comp ---
            if ind.get("max_atr_ratio_comp") is not None:
                vol_state_rows.append((SYMBOL, tf, ind["timestamp"], ind["max_atr_ratio_comp"]))
        except Exception as e:
            logger.warning(f"[{tf}] fetch/compute failed: {e}")
    try:
        write_rows(rows)
        if vol_state_rows:
            with _conn() as c, c.cursor() as cur:
                cur.executemany(
                    """INSERT INTO vol_state_metrics(symbol, timeframe, timestamp, max_atr_ratio)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING""",
                    vol_state_rows,
                )
                c.commit()
        logger.info(f"Inserted {len(rows)} rows → {SYMBOL} {tf_ready}")
    except Exception as e:
        logger.error(f"DB insert failed: {e}")

async def main_loop():
    ensure_table()
    ensure_vol_state_metrics_table()
    try:
        while True:
            start = time.time()
            await cycle_once()
            elapsed = time.time() - start
            await asyncio.sleep(max(5, LOOP_INTERVAL - elapsed))
    finally:
        try:
            await EX.close()
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main_loop())
