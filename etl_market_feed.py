import os, asyncio, logging, json
from datetime import datetime, timezone
from collections import deque


def short_num(n: float | int | None) -> str:
    """Return a human-friendly representation of large numbers."""
    if n is None:
        return "NA"
    n = float(n)
    abs_n = abs(n)
    if abs_n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if abs_n >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n:.2f}"

import ccxt
import pandas as pd
import psycopg2
import talib
import redis
import websockets
from dotenv import load_dotenv

# --- minimal helpers (standalone) -------------------------------------------
# keep at most ~50k liquidation events (roughly 8 hours at moderate rates)
LIQUIDATION_EVENTS = deque(maxlen=50_000)

async def start_liquidation_listener(symbol: str = "ETHUSDT"):
    url = "wss://stream.bybit.com/v5/public/linear"
    topic = f"liquidation.{symbol}"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if "data" in data:
                events = data["data"]
                if isinstance(events, dict):
                    events = [events]
                for event in events:
                    try:
                        ts = int(event["T"]) // 1000
                        side = event["S"]
                        size = float(event["v"])
                        LIQUIDATION_EVENTS.append((ts, side, size))
                    except (KeyError, ValueError, TypeError):
                        continue
            cutoff = int(datetime.now(timezone.utc).timestamp()) - 8 * 3600
            while LIQUIDATION_EVENTS and LIQUIDATION_EVENTS[0][0] < cutoff:
                LIQUIDATION_EVENTS.popleft()


def get_orderbook_snapshot(symbol: str = "ETH/USDT:USDT") -> dict:
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_stream = f"orderbook:{symbol.replace('/', '').replace(':', '_')}"
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    entries = r.xrevrange(redis_stream, count=1)
    if not entries or not entries[0] or not isinstance(entries[0][1], dict):
        raise RuntimeError(f"No valid orderbook data found in Redis stream {redis_stream}")
    data = entries[0][1].get("data")
    if not data:
        raise RuntimeError("Malformed orderbook entry in Redis")
    snap = json.loads(data)
    for key in ["history", "depth_heat_bid", "depth_heat_ask"]:
        if key in snap:
            if key == "history" and isinstance(snap[key], list):
                snap[key] = snap[key][-10:]
            else:
                snap.pop(key, None)
    return snap


def get_live_derivatives_metrics(symbol: str) -> dict:
    try:
        funding = EX.fetch_funding_rate(symbol)
        funding_rate = funding.get("fundingRate")
    except Exception:
        funding_rate = None
    try:
        oi = EX.fetch_open_interest(symbol)
        open_interest = oi.get("openInterestAmount") or float(oi["info"].get("openInterest", 0))
    except Exception:
        open_interest = None
    return {"funding_rate": funding_rate, "open_interest": open_interest}


load_dotenv()

SYMBOL = os.getenv("SYMBOL", "ETH/USDT:USDT")
DB_URL = os.getenv("TIMESCALEDB_URL")
TABLE = "llm_market_feed"
LOOP_INTERVAL = int(os.getenv("FEED_LOOP_INTERVAL", 60))

EX = ccxt.bybit({
    "enableRateLimit": True,
    "options": {"defaultType": "linear"},
    "timeout": 60_000,
})


def conn():
    return psycopg2.connect(DB_URL)


def ensure_table():
    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            symbol TEXT,
            timestamp BIGINT,
            feed TEXT,
            PRIMARY KEY(symbol, timestamp)
        );"""
        )
        cur.execute(
            f"SELECT create_hypertable('{TABLE}', 'timestamp', if_not_exists => TRUE);"
        )
        c.commit()


def get_liq_totals(minutes: int = 5):
    now = int(datetime.now(timezone.utc).timestamp())
    cutoff = now - minutes * 60
    long_liq = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Sell" and ts >= cutoff)
    short_liq = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Buy" and ts >= cutoff)
    return long_liq, short_liq


def fetch_ohlcv_df(limit: int = 200) -> pd.DataFrame:
    """Fetch recent OHLCV data. Need at least 200 bars for EMA200 warm up."""
    ohlcv = EX.fetch_ohlcv(SYMBOL, "1m", limit=limit)
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
    return df


def format_ohlcv_block(df: pd.DataFrame) -> str:
    lines = ["## OHLCV 1-min (oldest→newest)"]
    for _, row in df.iterrows():
        ts = datetime.fromtimestamp(row["ts"] / 1000, tz=timezone.utc).strftime("%H:%M")
        lines.append(
            f"t={ts} | {short_num(row['open'])} {short_num(row['high'])} {short_num(row['low'])} {short_num(row['close'])} {short_num(row['vol'])}"
        )
    return "\n".join(lines)


def format_orderbook_block(snap: dict) -> str:
    bids = snap.get("bids", [])[:5]
    asks = snap.get("asks", [])[:5]
    bid_str = " ".join(f"{short_num(b[0])}/{short_num(b[1])}" for b in bids)
    ask_str = " ".join(f"{short_num(a[0])}/{short_num(a[1])}" for a in asks)
    spread = snap.get("spread")
    imb = snap.get("bid_ask_imbalance_pct")
    lines = ["## OrderBook (now)"]
    lines.append(f"bid:{bid_str}")
    lines.append(f"ask:{ask_str}")
    lines.append(f"spread:{spread} imbalance:{imb}")
    return "\n".join(lines)


def format_flow_block(snap: dict) -> str:
    buy_qty = snap.get("buy_volume_last_win")
    sell_qty = snap.get("sell_volume_last_win")
    avg_size = snap.get("avg_trade_size")
    net = None
    imb_perc = None
    if buy_qty is not None and sell_qty is not None:
        net = buy_qty - sell_qty
        imb_perc = 100 * net / max(buy_qty + sell_qty, 1)

    lines = ["## Flow 60 s"]
    imb_str = f"{imb_perc:.2f}" if imb_perc is not None else "N/A"
    lines.append(
        f"buys:{buy_qty} sells:{sell_qty} net:{net} imb%:{imb_str} avgSize:{avg_size}"
    )
    return "\n".join(lines)


def compute_indicators(df: pd.DataFrame) -> dict:
    if len(df) < 200:
        return {}

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["vol"].astype(float)

    rsi = talib.RSI(close, 14).iloc[-1]
    macd, macd_sig, macd_hist = talib.MACD(close, 12, 26, 9)
    ema20 = talib.EMA(close, 20).iloc[-1]
    ema50 = talib.EMA(close, 50).iloc[-1]
    ema200 = talib.EMA(close, 200).iloc[-1]
    atr14 = talib.ATR(high, low, close, 14).iloc[-1]
    upper, middle, lower = talib.BBANDS(close, 20)
    bb_width = (upper.iloc[-1] - lower.iloc[-1]) / middle.iloc[-1] * 100
    vwap = ((high + low + close) / 3 * vol).sum() / (vol.sum() or 1)
    obv = talib.OBV(close, vol)
    obv_slope = obv.iloc[-1] - obv.iloc[-6] if len(obv) >= 6 else 0
    last_close = close.iloc[-1]
    return {
        "RSI_14": rsi,
        "MACD_line": macd.iloc[-1],
        "MACD_hist": macd_hist.iloc[-1],
        "EMA_20": (last_close / ema20 - 1) * 100,
        "EMA_50": (last_close / ema50 - 1) * 100,
        "EMA_200": (last_close / ema200 - 1) * 100,
        "ATR_14": atr14,
        "BB_width": bb_width,
        "VWAP_dev_%": (last_close / vwap - 1) * 100,
        "OBV_slope": obv_slope,
    }


def format_indicator_block(ind: dict) -> str:
    if not ind:
        return "## Indicators (warming up)"

    parts = ["## Indicators (latest)"]
    parts.append(
        f"RSI14:{ind['RSI_14']:.2f} MACD:{ind['MACD_line']:.2f}/{ind['MACD_hist']:.2f} "
        f"EMA20Δ:{ind['EMA_20']:.2f}% EMA50Δ:{ind['EMA_50']:.2f}% EMA200Δ:{ind['EMA_200']:.2f}%"
    )
    parts.append(
        f"ATR14:{ind['ATR_14']:.2f} BBwidth:{ind['BB_width']:.2f}% VWAPΔ:{ind['VWAP_dev_%']:.2f}% OBV_slope:{ind['OBV_slope']:.2f}"
    )
    return "\n".join(parts)


def format_context_block() -> str:
    ticker = EX.fetch_ticker(SYMBOL.replace(":USDT", ""))
    prev_close = ticker.get("previousClose")
    high = ticker.get("high")
    low = ticker.get("low")
    # 7d change
    ohlcv = EX.fetch_ohlcv(SYMBOL, "1d", limit=8)
    if len(ohlcv) >= 8:
        change = (ohlcv[-1][4] / ohlcv[-8][4] - 1) * 100
    else:
        change = None
    return (
        "## Context\n"
        f"prevClose:{prev_close}  24hHi/Lo:{high}/{low}  7dChange:{change:.2f}%"
    )


def collect_feed_text() -> str:
    logging.info("Fetching OHLCV data...")
    df = fetch_ohlcv_df()
    logging.info("Fetching orderbook snapshot...")
    ob = get_orderbook_snapshot(SYMBOL)
    logging.info("Computing indicators...")
    ind = compute_indicators(df)
    logging.info("Fetching derivatives metrics...")
    metrics = get_live_derivatives_metrics(SYMBOL)
    logging.info("Calculating liquidation totals...")
    long_liq, short_liq = get_liq_totals()
    ohlcv_block = format_ohlcv_block(df)
    orderbook_block = format_orderbook_block(ob)
    flow_block = format_flow_block(ob)
    deriv_block = (
        "## Derivs 5 min\n" +
        f"OI:{short_num(metrics.get('open_interest'))} funding:{metrics.get('funding_rate')} " +
        f"long_liq:{short_num(long_liq)} short_liq:{short_num(short_liq)}"
    )
    ind_block = format_indicator_block(ind)
    ctx_block = format_context_block()
    return "\n".join([ohlcv_block, orderbook_block, flow_block, deriv_block, ind_block, ctx_block])


def write_row(ts: int, text: str):
    with conn() as c, c.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE}(symbol, timestamp, feed) VALUES (%s,%s,%s) ON CONFLICT(symbol,timestamp) DO UPDATE SET feed=EXCLUDED.feed",
            (SYMBOL, ts, text),
        )
        c.commit()


async def main_loop():
    ensure_table()
    asyncio.create_task(start_liquidation_listener(SYMBOL.replace("/USDT:USDT", "USDT")))
    while True:
        try:
            text = collect_feed_text()
            ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            write_row(ts, text)
            logging.info("Feed row stored")
        except Exception as e:
            import traceback
            logging.error(f"feed error: {e}\n{traceback.format_exc()}")
        await asyncio.sleep(LOOP_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    # Log environment variables for debugging
    logging.info(f"SYMBOL={SYMBOL} DB_URL={'set' if DB_URL else 'NOT SET'} TABLE={TABLE} LOOP_INTERVAL={LOOP_INTERVAL}")
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("Feed ETL stopped")
