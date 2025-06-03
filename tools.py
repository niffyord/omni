import os
from dotenv import load_dotenv
load_dotenv()

"""
Live pull from Bybit REST
Returns keys used by TechnicalAnalyst-PRO for ETH/USDT:
  • funding_rate
  • open_interest
  • oi_1h_delta_pct (computed via Timescale snapshot)
  • oi_5m_delta_pct / 15m / 4h
  • long_short_ratio (optional)
  • buy_ratio
  • sell_ratio
  • funding_mean/std windows, term-structure & basis metrics
"""

# print("DERIV_METRICS_DB:", os.getenv("DERIV_METRICS_DB", ""))  # Removed for cleaner logs

import ccxt, psycopg2, requests
from datetime import datetime, timezone
import redis
import json
import math

# Global Bybit clients for reuse
_BYBIT_LINEAR = ccxt.bybit({"enableRateLimit":True,"options":{"defaultType":"linear"}})
_BYBIT_SPOT   = ccxt.bybit({"enableRateLimit":True,"options":{"defaultType":"spot"}})

# Remove unused helpers
# (funding_stats_bybit, _okx, etc.)

def _bybit():           # keep for backward compatibility
    return _BYBIT_LINEAR

def fetch_bybit_funding_history(symbol="ETHUSDT", start_time=None, end_time=None, limit=200):
    """
    Fetch historical funding rates from Bybit official REST endpoint.
    Returns a list of dicts with keys: fundingRate, fundingRateTimestamp, etc.
    """
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": limit
    }
    if start_time:
        params["startTime"] = int(start_time)
    if end_time:
        params["endTime"] = int(end_time)
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    result = resp.json().get("result", {})
    return result.get("list", [])

def funding_stats_bybit(symbol="ETHUSDT", window_ms=3600000, now_ms=None):
    """
    Compute mean and stddev of funding rates over a window (ms) using Bybit REST API.
    """
    if now_ms is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = now_ms - window_ms
    rows = fetch_bybit_funding_history(symbol, start_time=start_time, end_time=now_ms)
    rates = []
    for row in rows:
        try:
            rates.append(float(row["fundingRate"]))
        except Exception:
            continue
    if not rates:
        return (None, None)
    mean = sum(rates) / len(rates)
    std = (sum((x - mean) ** 2 for x in rates) / len(rates)) ** 0.5 if len(rates) > 1 else 0.0
    return (mean, std)

def fmt(val, n=8):
    try:
        import math
        if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
            return "N/A"  # Return N/A instead of misleading zeros
        return f"{float(val):.{n}f}"
    except Exception:
        return "N/A"  # Return N/A instead of misleading zeros

# Use DERIV_METRICS_DB for all derivatives metrics DB connections
TS_CONN = os.getenv("DERIV_METRICS_DB", "")


# OKX public connection (no auth needed for market data)
def _okx():
    return ccxt.okx({"enableRateLimit": True})

# ─── helper to fetch 1-h old OI from TSDB ───────────────────────────────―

# Generic delta pct helper for any column & horizon
def _metric_delta_pct(symbol, column: str, horizon_ms: int, now_ms: int):
    allowed = {"open_interest", "long_short_ratio"}
    if column not in allowed:
        print(f"[WARN] _metric_delta_pct: column '{column}' not in lean schema: {allowed}")
        return None
    if not TS_CONN: return None
    try:
        with psycopg2.connect(TS_CONN) as conn, conn.cursor() as cur:
            cur.execute(
                f"""SELECT {column} FROM derivatives_metrics
                    WHERE symbol=%s AND timestamp<=%s
                    ORDER BY timestamp DESC LIMIT 1""",
                (symbol, now_ms - horizon_ms))
            row_old = cur.fetchone()
            if not row_old or row_old[0] is None:
                return None
            old = row_old[0]
            cur.execute(
                f"""SELECT {column} FROM derivatives_metrics
                    WHERE symbol=%s ORDER BY timestamp DESC LIMIT 1""",
                (symbol,))
            row_new = cur.fetchone()
            if not row_new or row_new[0] is None:
                return None
            new = row_new[0]
            return (new - old) / old * 100 if old else None
    except Exception as e:
        print(f"[WARN] _metric_delta_pct({column}) error: {e}")
        return None

# Funding-price divergence flag
def _funding_price_divergence(symbol: str, funding_rate: float) -> bool | None:
    try:
        # Always use spot symbol for spot client
        clean_sym = symbol.split(":")[0] if ":" in symbol else symbol
        ohlcv = _BYBIT_SPOT.fetch_ohlcv(clean_sym, timeframe="1m", limit=6)
        if len(ohlcv) < 6:
            return None
        price_now = ohlcv[-1][4]
        price_5m_ago = ohlcv[0][4]
        price_dir = 1 if price_now > price_5m_ago else -1 if price_now < price_5m_ago else 0
        fund_dir = 1 if funding_rate and funding_rate > 0 else -1 if funding_rate and funding_rate < 0 else 0
        if fund_dir == 0:
            return None
        return price_dir != fund_dir
    except Exception as e:
        print(f"[WARN] funding-price divergence check error: {e}")
        return None

def fetch_long_short_ratio(symbol):
    """
    Fetches the long/short ratio for the given symbol from Bybit API.
    
    Returns:
        tuple: (buy_ratio, sell_ratio) as floats, or (None, None) on failure
    """
    base_url = "https://api.bybit.com/v5/market/account-ratio"
    params = {
        "category": "linear",
        "symbol": symbol.replace("/USDT:USDT", "USDT").replace("/", ""),  # e.g. COREUSDT
        "period": "1h",
        "limit": 1
    }
    try:
        resp = requests.get(base_url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("result") and data["result"].get("list"):
            item = data["result"]["list"][0]
            return float(item.get("buyRatio")), float(item.get("sellRatio"))
    except Exception as e:
        print(f"Failed to fetch L/S ratio: {e}")
    return None, None

def get_orderbook_snapshot(symbol: str = "ETH/USDT:USDT"):
    """
    Fetches the latest orderbook snapshot and returns the raw dictionary (no LLM summary).
    """
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_stream = f"orderbook:{symbol.replace('/','').replace(':','_')}"
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    entries = r.xrevrange(redis_stream, count=1)
    if not entries or not entries[0] or not entries[0][1] or not isinstance(entries[0][1], dict):
        raise RuntimeError(f"No valid orderbook data found in Redis stream {redis_stream}")
    data = entries[0][1].get("data")
    if not data:
        raise RuntimeError("Malformed orderbook entry in Redis")
    snap = json.loads(data)
    return snap

def get_derivatives_metrics(symbol: str):
    """
    Fetches the derivatives metrics for the given symbol and returns ONLY the raw metrics dict (no summary string, no formatting, no extra keys).
    """
    HISTORY_LENGTH = int(os.getenv("DERIV_HISTORY_LENGTH", 90))  # Use 90 for full context
    try:
        with psycopg2.connect(TS_CONN) as conn, conn.cursor() as cur:
            # Fetch latest HISTORY_LENGTH rows, newest first
            cur.execute(
                """SELECT funding_rate, open_interest, oi_1h_delta_pct,
                          long_short_ratio, buy_ratio, sell_ratio,
                          funding_price_div, perp_spot_basis_pct,
                          core_eth_funding_spread, liquidation_8h_long, liquidation_8h_short,
                          timestamp,
                          oi_5m_delta_pct, oi_15m_delta_pct, oi_4h_delta_pct,
                          ls_5m_delta_pct, ls_15m_delta_pct, ls_4h_delta_pct,
                          oi_z_24h, funding_8h_avg, funding_z,
                          perp_quarterly_basis_pct, basis_annualised,
                          liq_imbalance_8h, oi_turnover_ratio
                   FROM derivatives_metrics
                   WHERE symbol=%s
                   ORDER BY timestamp DESC
                   LIMIT %s""",
                (symbol, HISTORY_LENGTH)
            )
            rows = cur.fetchall()
            if not rows:
                return "No derivatives data available."
            # Reverse to get oldest to newest
            rows = rows[::-1]
            # Build history buffer for summarize_derivatives_metrics
            history = []
            colnames = [
                "funding_rate", "open_interest", "oi_1h_delta_pct",
                "long_short_ratio", "buy_ratio", "sell_ratio",
                "funding_price_div", "perp_spot_basis_pct",
                "core_eth_funding_spread", "liquidation_8h_long", "liquidation_8h_short",
                "timestamp",
                "oi_5m_delta_pct", "oi_15m_delta_pct", "oi_4h_delta_pct",
                "ls_5m_delta_pct", "ls_15m_delta_pct", "ls_4h_delta_pct",
                "oi_z_24h", "funding_8h_avg", "funding_z",
                "perp_quarterly_basis_pct", "basis_annualised",
                "liq_imbalance_8h", "oi_turnover_ratio"
            ]
            for row in rows:
                hist_entry = dict(zip(colnames, row))
                history.append(hist_entry)
            # Use the latest row for current metrics
            latest = history[-1]
            metrics = dict(latest)
            # Add all log fields to metrics, using value from latest snapshot or None
            metrics["oi_5m_delta_pct"] = latest.get("oi_5m_delta_pct", None)
            metrics["oi_15m_delta_pct"] = latest.get("oi_15m_delta_pct", None)
            metrics["oi_4h_delta_pct"] = latest.get("oi_4h_delta_pct", None)
            metrics["ls_5m_delta_pct"] = latest.get("ls_5m_delta_pct", None)
            metrics["ls_15m_delta_pct"] = latest.get("ls_15m_delta_pct", None)
            metrics["ls_4h_delta_pct"] = latest.get("ls_4h_delta_pct", None)
            metrics["oi_z_24h"] = latest.get("oi_z_24h", None)
            metrics["funding_8h_avg"] = latest.get("funding_8h_avg", None)
            metrics["funding_z"] = latest.get("funding_z", None)
            metrics["perp_quarterly_basis_pct"] = latest.get("perp_quarterly_basis_pct", None)
            metrics["basis_annualised"] = latest.get("basis_annualised", None)
            metrics["liq_imbalance_8h"] = latest.get("liq_imbalance_8h", None)
            metrics["oi_turnover_ratio"] = latest.get("oi_turnover_ratio", None)
            # Remove any _raw fields from top-level if present
            for k in list(metrics.keys()):
                if k.endswith('_raw'):
                    del metrics[k]
            return metrics
    except Exception as e:
        print(f"[WARN] get_derivatives_metrics error: {e}")
        return f"Error fetching derivatives metrics: {e}"
    # Fallback to live pull if DB fetch fails or no row exists
    now_ms = int(datetime.now(timezone.utc).timestamp()*1000)
    # Funding
    try:
        funding = _BYBIT_LINEAR.fetch_funding_rate(symbol)
        funding_rate = funding.get("fundingRate")
    except Exception: funding_rate = None
    # Open Interest
    try:
        oi  = _BYBIT_LINEAR.fetch_open_interest(symbol)
        open_interest = (
            oi.get("openInterestAmount") or
            float(oi["info"].get("openInterest", 0))
        )
    except Exception: open_interest = None
    # Long/Short Ratios (use HTTP fallback)
    buy_ratio, sell_ratio = fetch_long_short_ratio(symbol)
    long_short_ratio = None
    if buy_ratio is not None and sell_ratio is not None and sell_ratio != 0:
        long_short_ratio = buy_ratio / sell_ratio
    # OI delta horizon pct via generic helper
    oi_1h_delta_pct  = _metric_delta_pct(symbol, "open_interest", 60*60*1000, now_ms)
    funding_price_div = _funding_price_divergence(symbol, funding_rate)
    # --- Perp-spot basis pct ---
    try:
        spot_price = _BYBIT_SPOT.fetch_ticker("CORE/USDT")["last"]
        perp_price = _BYBIT_LINEAR.fetch_ticker("CORE/USDT:USDT")["last"]
        if spot_price and perp_price:
            perp_spot_basis_pct = (perp_price / spot_price - 1) * 100
        else:
            perp_spot_basis_pct = None
    except Exception:
        perp_spot_basis_pct = None
    # --- CORE-ETH funding spread ---
    try:
        core_funding = funding_rate
        eth_funding = None
        eth_funding_resp = _BYBIT_LINEAR.fetch_funding_rate("ETH/USDT:USDT")
        eth_funding = eth_funding_resp.get("fundingRate")
        if core_funding is not None and eth_funding is not None:
            core_eth_funding_spread = core_funding - eth_funding
        else:
            core_eth_funding_spread = None
    except Exception:
        core_eth_funding_spread = None
    # Liquidation stats (not available live here)
    liquidation_8h_long = None
    liquidation_8h_short = None
    metrics = {
        "funding_rate": funding_rate,
        "open_interest": open_interest,
        "oi_1h_delta_pct": oi_1h_delta_pct,
        "long_short_ratio": long_short_ratio,
        "buy_ratio": buy_ratio,
        "sell_ratio": sell_ratio,
        "funding_price_div": funding_price_div,
        "perp_spot_basis_pct": perp_spot_basis_pct,
        "core_eth_funding_spread": core_eth_funding_spread,
        "oi_5m_delta_pct": oi_5m_delta_pct if 'oi_5m_delta_pct' in locals() else None,
        "oi_15m_delta_pct": oi_15m_delta_pct if 'oi_15m_delta_pct' in locals() else None,
        "oi_4h_delta_pct": oi_4h_delta_pct if 'oi_4h_delta_pct' in locals() else None,
        "ls_5m_delta_pct": ls_5m_delta_pct if 'ls_5m_delta_pct' in locals() else None,
        "ls_15m_delta_pct": ls_15m_delta_pct if 'ls_15m_delta_pct' in locals() else None,
        "ls_4h_delta_pct": ls_4h_delta_pct if 'ls_4h_delta_pct' in locals() else None,
        "oi_z_24h": oi_z_24h if 'oi_z_24h' in locals() else None,
        "funding_8h_avg": funding_8h_avg if 'funding_8h_avg' in locals() else None,
        "funding_z": funding_z if 'funding_z' in locals() else None,
        "perp_quarterly_basis_pct": perp_quarterly_basis_pct if 'perp_quarterly_basis_pct' in locals() else None,
        "basis_annualised": basis_annualised if 'basis_annualised' in locals() else None,
        "liq_imbalance_8h": liq_imbalance_8h if 'liq_imbalance_8h' in locals() else None,
        "oi_turnover_ratio": oi_turnover_ratio if 'oi_turnover_ratio' in locals() else None,
        "liquidation_8h_long": liquidation_8h_long,
        "liquidation_8h_short": liquidation_8h_short,
    }
    return metrics

    from derivatives_metrics_scheduler import summarize_derivatives_metrics
    return summarize_derivatives_metrics(metrics)

def get_live_derivatives_metrics(symbol: str):
    """
    Fetches the derivatives metrics for the given symbol directly from Bybit (live),
    without checking or returning any database values.
    Returns:
        dict: metrics with both formatted and raw fields for OI, funding, L/S ratio, and deltas.
    """
    now_ms = int(datetime.now(timezone.utc).timestamp()*1000)
    # Funding
    try:
        funding = _BYBIT_LINEAR.fetch_funding_rate(symbol)
        funding_rate = funding.get("fundingRate")
    except Exception: funding_rate = None
    # Open Interest
    try:
        oi  = _BYBIT_LINEAR.fetch_open_interest(symbol)
        open_interest = (
            oi.get("openInterestAmount") or
            float(oi["info"].get("openInterest", 0))
        )
    except Exception: open_interest = None
    # Long/Short Ratios (use HTTP fallback)
    buy_ratio, sell_ratio = fetch_long_short_ratio(symbol)
    long_short_ratio = None
    if buy_ratio is not None and sell_ratio is not None and sell_ratio != 0:
        long_short_ratio = buy_ratio / sell_ratio
    # OI delta horizon pct via generic helper
    oi_1h_delta_pct  = _metric_delta_pct(symbol, "open_interest", 60*60*1000, now_ms)
    funding_price_div = _funding_price_divergence(symbol, funding_rate)
    # --- Perp-spot basis pct ---
    try:
        spot_price = _BYBIT_SPOT.fetch_ticker("CORE/USDT")["last"]
        perp_price = _BYBIT_LINEAR.fetch_ticker("CORE/USDT:USDT")["last"]
        if spot_price and perp_price:
            perp_spot_basis_pct = (perp_price / spot_price - 1) * 100
        else:
            perp_spot_basis_pct = None
    except Exception:
        perp_spot_basis_pct = None
    # --- CORE-ETH funding spread ---
    try:
        core_funding = funding_rate
        eth_funding = None
        eth_funding_resp = _BYBIT_LINEAR.fetch_funding_rate("ETH/USDT:USDT")
        eth_funding = eth_funding_resp.get("fundingRate")
        if core_funding is not None and eth_funding is not None:
            core_eth_funding_spread = core_funding - eth_funding
        else:
            core_eth_funding_spread = None
    except Exception:
        core_eth_funding_spread = None
    # Liquidation stats (not available live here)
    liquidation_8h_long = None
    liquidation_8h_short = None
    metrics = {
        "funding_rate"     : funding_rate,
        "open_interest"    : open_interest,
        "oi_1h_delta_pct"  : oi_1h_delta_pct,
        "long_short_ratio" : long_short_ratio,
        "buy_ratio"        : buy_ratio,
        "sell_ratio"       : sell_ratio,
        "funding_price_div": funding_price_div,
        "perp_spot_basis_pct": perp_spot_basis_pct,
        "core_eth_funding_spread": core_eth_funding_spread,
        "liquidation_8h_long": liquidation_8h_long,
        "liquidation_8h_short": liquidation_8h_short,
    }
    return metrics
