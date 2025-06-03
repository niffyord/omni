import psycopg2
import json
import os
from dotenv import load_dotenv
from agents import function_tool

load_dotenv()

def get_timescaledb_conn():
    db_url = os.getenv("TIMESCALEDB_URL")
    if db_url is None:
        raise ValueError("TIMESCALEDB_URL not set in environment.")
    return psycopg2.connect(db_url)

@function_tool
def get_latest_indicators(symbol: str, timeframe: str) -> dict:
    """Fetch the latest precomputed indicators for a symbol and timeframe from TimescaleDB."""
    # Normalize timeframe input
    tf_map = {"1 m": "1m", "5 m": "5m", "15 m": "15m", "1m": "1m", "5m": "5m", "15m": "15m"}
    timeframe = tf_map.get(timeframe.strip(), timeframe.replace(" ", ""))
    conn = get_timescaledb_conn()
    cur = conn.cursor()
    cur.execute('''
        SELECT indicators FROM market_indicators
        WHERE symbol = %s AND timeframe = %s
        ORDER BY timestamp DESC LIMIT 1
    ''', (symbol, timeframe))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]  # Already a dict if using psycopg2 with jsonb
    else:
        return {"error": "No data found for this symbol/timeframe."}

def _get_latest_indicators_multi(symbol: str, timeframes: list[str]) -> dict:
    """
    Fetch the latest precomputed indicators for a symbol for multiple timeframes in one query.
    Returns a dict: { timeframe: indicators_dict, ... }
    """
    tf_map = {"1 m": "1m", "5 m": "5m", "15 m": "15m", "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    norm_timeframes = [tf_map.get(tf.strip(), tf.replace(" ", "")) for tf in timeframes]
    conn = get_timescaledb_conn()
    cur = conn.cursor()
    # Use DISTINCT ON to get the latest row per timeframe
    cur.execute('''
        SELECT DISTINCT ON (timeframe) timeframe, indicators
        FROM market_indicators
        WHERE symbol = %s AND timeframe = ANY(%s)
        ORDER BY timeframe, timestamp DESC
    ''', (symbol, norm_timeframes))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    results = {tf: {"error": f"No data found for {symbol}/{tf}"} for tf in norm_timeframes}
    for tf, indicators in rows:
        results[tf] = indicators
    return results

get_latest_indicators_multi = function_tool(_get_latest_indicators_multi)

@function_tool
def get_last_n_vol_state(symbol: str, timeframe: str, n: int = 30) -> list:
    """
    Fetch the last n max_atr_ratio_comp values for a symbol/timeframe from vol_state_metrics table.
    Returns a list of floats (most recent first).
    """
    tf_map = {"1 m": "1m", "5 m": "5m", "15 m": "15m", "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    timeframe = tf_map.get(timeframe.strip(), timeframe.replace(" ", ""))
    conn = get_timescaledb_conn()
    cur = conn.cursor()
    cur.execute('''
        SELECT max_atr_ratio
        FROM vol_state_metrics
        WHERE symbol = %s AND timeframe = %s
        ORDER BY timestamp DESC
        LIMIT %s
    ''', (symbol, timeframe, n))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [float(r[0]) for r in rows if r[0] is not None]

@function_tool
def get_last_n_indicators(symbol: str, timeframe: str, n: int = 30) -> list:
    """Return the last n indicator rows for a symbol/timeframe."""
    tf_map = {"1 m": "1m", "5 m": "5m", "15 m": "15m", "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    timeframe = tf_map.get(timeframe.strip(), timeframe.replace(" ", ""))
    conn = get_timescaledb_conn()
    cur = conn.cursor()
    cur.execute(
        '''SELECT indicators FROM market_indicators
           WHERE symbol = %s AND timeframe = %s
           ORDER BY timestamp DESC LIMIT %s''',
        (symbol, timeframe, n)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # Return oldest first for easier modeling
    return [r[0] for r in rows[::-1]]

def _get_all_history_series(n_max_ratio: int = 30, n_bb_width: int = 20) -> dict:
    """
    Fetch all historical series for CORE and ETH (1h, 4h, 1d):
      - max_atr_ratio_comp (last 30)
      - bb_width (last 20, CORE only)
    Returns a dict for direct use in market context:
      {
        'vol_state': {
           'CORE': {'max_ratio_last30_1h': [...], ...},
           'ETH': {'max_ratio_last30_1h': [...], ...}
        },
        'bb_series': {
           '1h': {'bb_width_last20': [...]}, ...
        }
      }
    """
    symbols = ["CORE/USDT:USDT", "ETH/USDT:USDT"]
    tfs = ["1h", "4h", "1d"]
    ctx = {"vol_state": {"CORE": {}, "ETH": {}}, "bb_series": {}}
    conn = get_timescaledb_conn()
    cur = conn.cursor()
    tf_map = {"1 m": "1m", "5 m": "5m", "15 m": "15m", "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    for symbol in symbols:
        sym_key = "CORE" if symbol.startswith("CORE") else "ETH"
        for tf in tfs:
            # max_atr_ratio_comp
            cur.execute('''
                SELECT max_atr_ratio
                FROM vol_state_metrics
                WHERE symbol = %s AND timeframe = %s
                ORDER BY timestamp DESC LIMIT %s
            ''', (symbol, tf, n_max_ratio))
            rows = cur.fetchall()
            ctx["vol_state"][sym_key][f"max_ratio_last{n_max_ratio}_{tf}"] = [float(r[0]) for r in rows if r[0] is not None]
            # bb_width (CORE only)
            if sym_key == "CORE":
                cur.execute('''
                    SELECT bb_width
                    FROM bb_width_metrics
                    WHERE symbol = %s AND timeframe = %s
                    ORDER BY timestamp DESC LIMIT %s
                ''', (symbol, tf, n_bb_width))
                rows = cur.fetchall()
                bb_vals = [float(r[0]) for r in rows if r[0] is not None]
                if tf not in ctx["bb_series"]:
                    ctx["bb_series"][tf] = {}
                ctx["bb_series"][tf][f"bb_width_last{n_bb_width}"] = bb_vals
    cur.close()
    conn.close()
    return ctx

get_all_history_series = function_tool(_get_all_history_series)
get_all_history_series_py = _get_all_history_series
get_last_n_indicators_py = get_last_n_indicators
