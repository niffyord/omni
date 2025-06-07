import sys
import asyncio
import aiohttp
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import os, asyncio, logging, json
from datetime import datetime, timezone, timedelta
from collections import deque
import asyncpg
import ccxt.async_support as ccxt_async

# --- Institutional-grade caches for deltas and state ---
ORDERBOOK_CACHE = deque(maxlen=2)
OI_CACHE = deque(maxlen=2)


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

import pandas as pd
import talib
import redis
import websockets
from dotenv import load_dotenv

# --- minimal helpers (standalone) -------------------------------------------
# keep at most ~50k liquidation events (roughly 8 hours at moderate rates)
LIQUIDATION_EVENTS = deque(maxlen=50_000)
# Store (timestamp, side, size) for rolling trade analysis
TRADE_EVENTS = deque(maxlen=10_000)  # should cover 5+ min at high freq

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

async def start_trade_listener(symbol: str = "ETHUSDT"):
    import logging
    url = "wss://stream.bybit.com/v5/public/linear"
    # Ensure correct topic for Bybit linear contracts
    topic = f"publicTrade.{symbol}"
    logging.info(f"Subscribing to Bybit trade topic: {topic}")
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
        last_ping = datetime.now(timezone.utc)
        while True:
            now = datetime.now(timezone.utc)
            # Ping every 15s for Bybit keepalive
            if (now - last_ping).total_seconds() > 15:
                await ws.send(json.dumps({"op": "ping"}))
                last_ping = now
            msg = await ws.recv()
            data = json.loads(msg)
            if "data" in data:
                trades = data["data"]
                if isinstance(trades, dict):
                    trades = [trades]
                for trade in trades:
                    try:
                        ts = int(trade["T"]) // 1000
                        side = trade["S"]  # 'Buy' or 'Sell'
                        size = float(trade["v"])
                        TRADE_EVENTS.append((ts, side, size))
                        logging.debug(f"Trade: ts={ts}, side={side}, size={size}, TRADE_EVENTS size={len(TRADE_EVENTS)}")
                    except (KeyError, ValueError, TypeError):
                        continue
            # Prune old trades
            cutoff = int(datetime.now(timezone.utc).timestamp()) - 5 * 60
            while TRADE_EVENTS and TRADE_EVENTS[0][0] < cutoff:
                TRADE_EVENTS.popleft()


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


async def get_live_derivatives_metrics(symbol: str) -> dict:
    metrics = {}
    try:
        rest_symbol = symbol.split(":")[0].replace("/", "").upper()
        url = "https://api.bybit.com/v5/market/funding/history"
        params = {
            "category": "linear",
            "symbol": rest_symbol,
            "limit": 1
        }
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as sess:
            async with sess.get(url, params=params) as resp:
                data = await resp.json()
                funding_list = data.get("result", {}).get("list", [])
                if funding_list:
                    funding_rate = float(funding_list[0]["fundingRate"])
                else:
                    import logging
                    logging.warning(f"No funding rate data returned from Bybit for {rest_symbol}")
                    funding_rate = None
    except Exception as e:
        import logging
        logging.warning(f"Failed to fetch funding rate from Bybit REST: {e}")
        funding_rate = None
    metrics["funding_rate"] = funding_rate
    try:
        oi = await EX.fetch_open_interest(symbol)
        open_interest = oi.get("openInterestAmount") or float(oi["info"].get("openInterest", 0))
    except Exception:
        open_interest = None
    return {"funding_rate": funding_rate, "open_interest": open_interest}


load_dotenv()

SYMBOL = os.getenv("SYMBOL", "ETH/USDT:USDT")
DB_URL = os.getenv("TIMESCALEDB_URL")
TABLE = "llm_market_feed"
LOOP_INTERVAL = int(os.getenv("FEED_LOOP_INTERVAL", 60))

EX = ccxt_async.bybit({
    "enableRateLimit": True,
    "options": {"defaultType": "linear"},
    "timeout": 60_000,
})

# Use a global asyncpg pool
DB_POOL = None

async def aconn():
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    return DB_POOL

async def ensure_table():
    pool = await aconn()
    async with pool.acquire() as c:
        await c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                symbol TEXT,
                timestamp BIGINT,
                feed TEXT,
                PRIMARY KEY(symbol, timestamp)
            );""")
        await c.execute(f"SELECT create_hypertable('{TABLE}', 'timestamp', if_not_exists => TRUE);")


def get_liq_totals(minutes: int = 5):
    now = int(datetime.now(timezone.utc).timestamp())
    cutoff = now - minutes * 60
    # Bybit v5: 'S' == 'Buy' means short liquidated (price rising), 'Sell' means long liquidated (price falling)
    long_liq = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Sell" and ts >= cutoff)
    short_liq = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Buy" and ts >= cutoff)
    return long_liq, short_liq


async def fetch_ohlcv_df(limit: int = 200) -> pd.DataFrame:
    """Fetch recent OHLCV data. Need at least 200 bars for EMA200 warm up."""
    ohlcv = await EX.fetch_ohlcv(SYMBOL, "1m", limit=limit)
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
    return df


def format_ohlcv_block(df: pd.DataFrame) -> str:
    # Limit to last 30 bars for token economy
    df = df.tail(30)
    # Compute current price and % changes
    last_close = df['close'].iloc[-1]
    pct_1m = (df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100 if len(df) > 1 else 0
    pct_5m = (df['close'].iloc[-1] / df['close'].iloc[-6] - 1) * 100 if len(df) > 5 else 0
    header = f"## OHLCV 1-min (oldest→newest) curr:{last_close:.2f} Δ1m:{pct_1m:.2f}% Δ5m:{pct_5m:.2f}%"
    lines = [header]
    for _, row in df.iterrows():
        ts = datetime.fromtimestamp(row["ts"] / 1000, tz=timezone.utc).strftime("%H:%M")
        lines.append(
            f"t={ts} | {row['open']:.2f} {row['high']:.2f} {row['low']:.2f} {row['close']:.2f} {row['vol']:.2f}"
        )
    return "\n".join(lines)


import logging

def wall_detect(levels, threshold=20):
    return [(p, s) for p, s in levels if s >= threshold]

def format_orderbook_block(snap: dict) -> str:
    import logging
    bids = snap.get("bids", [])[:5]
    asks = snap.get("asks", [])[:5]
    lines = ["## OrderBook (now)"]
    # Track for delta calculation
    ORDERBOOK_CACHE.append(snap)
    # Debug: log sample bids/asks and cache
    logging.info(f"ORDERBOOK_CACHE len={len(ORDERBOOK_CACHE)}, bids_sample={bids[:3]}, asks_sample={asks[:3]}")
    # Deltas
    delta_imb = delta_spread = None
    if len(ORDERBOOK_CACHE) >= 2:
        prev = ORDERBOOK_CACHE[-2]
        try:
            delta_imb = snap.get("bid_ask_imbalance_pct", 0) - prev.get("bid_ask_imbalance_pct", 0)
            delta_spread = snap.get("spread", 0) - prev.get("spread", 0)
        except Exception:
            delta_imb = delta_spread = None
    # Walls
    wall_bid = wall_detect(bids, threshold=20)
    wall_ask = wall_detect(asks, threshold=20)
    # Debug logging if bids/asks are empty
    if not bids or not asks:
        logging.warning(f"Orderbook snapshot missing bids or asks! bids: {bids}, asks: {asks}")
        lines.append(f"WARNING: Orderbook bids or asks are empty! Raw snapshot: {str(snap)[:300]}")
    bid_str = " ".join(f"{b[0]:.2f}/{b[1]:.5f}" for b in bids) if bids else "EMPTY"
    ask_str = " ".join(f"{a[0]:.2f}/{a[1]:.5f}" for a in asks) if asks else "EMPTY"
    spread = snap.get("spread")
    imb = snap.get("bid_ask_imbalance_pct")
    lines.append(f"bid:{bid_str}")
    lines.append(f"ask:{ask_str}")
    lines.append(f"spread:{spread} imbalance:{imb}")
    if delta_imb is not None and delta_spread is not None:
        lines.append(f"imbΔ:{delta_imb:+.2f}% spreadΔ:{delta_spread:+.5f}")
    if wall_bid:
        lines.append("WallBid:" + ", ".join(f"{s:.0f}@{p:.2f}" for p, s in wall_bid))
    if wall_ask:
        lines.append("WallAsk:" + ", ".join(f"{s:.0f}@{p:.2f}" for p, s in wall_ask))
    return "\n".join(lines)


def compute_trade_stats():
    now = int(datetime.now(timezone.utc).timestamp())
    # 1-min window
    min1 = now - 60
    min5 = now - 5 * 60
    buys_1m = sells_1m = vol_1m = 0
    buys_5m = sells_5m = vol_5m = 0
    trade_sizes_1m = []
    for ts, side, size in TRADE_EVENTS:
        if ts >= min5:
            vol_5m += size
            if side == "Buy":
                buys_5m += size
            else:
                sells_5m += size
        if ts >= min1:
            vol_1m += size
            trade_sizes_1m.append(size)
            if side == "Buy":
                buys_1m += size
            else:
                sells_1m += size
    net_1m = buys_1m - sells_1m
    imb_perc_1m = 100 * net_1m / max(buys_1m + sells_1m, 1) if (buys_1m or sells_1m) else 0
    avg_size_1m = sum(trade_sizes_1m) / len(trade_sizes_1m) if trade_sizes_1m else 0
    # Block-trade: any trade in 1m > mean+3σ
    mean_size = avg_size_1m
    std_size = (sum((x - mean_size) ** 2 for x in trade_sizes_1m) / len(trade_sizes_1m)) ** 0.5 if trade_sizes_1m else 0
    block_flag = "No"
    for s in trade_sizes_1m:
        if s > mean_size + 3 * std_size:
            block_flag = "Yes"
            break
    # Market order pressure tag
    if net_1m > 0 and imb_perc_1m > 10:
        mopress = "Bullish"
    elif net_1m < 0 and imb_perc_1m < -10:
        mopress = "Bearish"
    else:
        mopress = "Neutral"
    return {
        "buys_1m": buys_1m,
        "sells_1m": sells_1m,
        "net_1m": net_1m,
        "imb_perc_1m": imb_perc_1m,
        "avg_size_1m": avg_size_1m,
        "block_flag": block_flag,
        "mopress": mopress,
        "buys_5m": buys_5m,
        "sells_5m": sells_5m,
        "vol_5m": vol_5m,
    }

def format_flow_block(trade_stats: dict) -> str:
    lines = ["## Flow 60 s"]
    imb_str = f"{trade_stats['imb_perc_1m']:.2f}"
    lines.append(
        f"buys:{trade_stats['buys_1m']:.2f} sells:{trade_stats['sells_1m']:.2f} net:{trade_stats['net_1m']:.2f} imb%:{imb_str} avgSize:{trade_stats['avg_size_1m']:.1f} block:{trade_stats['block_flag']} mopress:{trade_stats['mopress']}"
    )
    lines.append(f"## Flow 5 min buys:{trade_stats['buys_5m']:.2f} sells:{trade_stats['sells_5m']:.2f} vol:{trade_stats['vol_5m']:.2f}")
    return "\n".join(lines)



def last(x):
    return x.iloc[-1] if hasattr(x, 'iloc') else x[-1]

def pos(x, n):
    return x.iloc[n] if hasattr(x, 'iloc') else x[n]

def compute_indicators(df: pd.DataFrame) -> dict:
    if len(df) < 200:
        return {}

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    vol = df["vol"].astype(float)

    rsi = last(talib.RSI(close, 14))
    macd, macd_sig, macd_hist = talib.MACD(close, 12, 26, 9)
    macd_line = last(macd)
    macd_hist_v = last(macd_hist)
    ema20 = last(talib.EMA(close, 20))
    ema50 = last(talib.EMA(close, 50))
    ema200 = last(talib.EMA(close, 200))
    atr14 = last(talib.ATR(high, low, close, 14))
    upper, middle, lower = talib.BBANDS(close, 20)
    bb_width = (last(upper) - last(lower)) / last(middle) * 100
    vwap = ((high + low + close) / 3 * vol).sum() / (vol.sum() or 1)
    obv = talib.OBV(close, vol)
    if len(obv) >= 6:
        if hasattr(obv, 'iloc'):
            obv_slope = last(obv) - obv.iloc[len(obv)-6]
        else:
            obv_slope = last(obv) - obv[-6]
    else:
        obv_slope = 0
    last_close = close.iloc[-1]
    return {
        "RSI_14": rsi,
        "MACD_line": macd_line,
        "MACD_hist": macd_hist_v,
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


async def format_context_block() -> str:
    import logging
    ticker = await EX.fetch_ticker(SYMBOL.replace(":USDT", ""))
    prev_close = ticker.get("previousClose") or ticker.get("prevClose")
    high = ticker.get("high")
    low = ticker.get("low")
    ohlcv = await EX.fetch_ohlcv(SYMBOL, "1d", limit=8)
    fallback_used = False
    warn_str = ""
    # Try to extract prevClose from info if missing
    if not prev_close:
        info = ticker.get("info", {})
        prev_close = info.get("prevPrice24h")
        if prev_close:
            warn_str = " [WARNING: prevClose missing, used info.prevPrice24h]"
            logging.warning(f"Used info.prevPrice24h as prevClose: {prev_close}")
        else:
            logging.warning(f"Ticker prevClose missing! Full ticker: {ticker}")
            # Fallback to previous close from OHLCV if available
            if ohlcv and len(ohlcv) >= 2:
                prev_close = ohlcv[-2][4]
                fallback_used = True
                warn_str = " [WARNING: prevClose missing, fallback to OHLCV]"
    if len(ohlcv) >= 8:
        change = (ohlcv[-1][4] / ohlcv[-8][4] - 1) * 100
    else:
        change = None
    return (
        "## Context\n"
        f"prevClose:{prev_close}{warn_str}  24hHi/Lo:{high}/{low}  7dChange:{change:.2f}%"
    )


def format_sentiment_block(trade_stats: dict, ind: dict, df: pd.DataFrame) -> str:
    # Order flow sentiment
    sent_orderflow = trade_stats['mopress']
    # Momentum sentiment
    last_1m = (df['close'].iloc[-1] / df['close'].iloc[-2] - 1) * 100 if len(df) > 1 else 0
    sent_momentum = "Up" if last_1m > 0 and ind.get('MACD_line', 0) > 0 else "Down" if last_1m < 0 else "Sideways"
    return f"## Sentiment\nOF:{sent_orderflow} MOM:{sent_momentum}"

async def collect_feed_text() -> str:
    logging.info("Fetching OHLCV data...")
    df = await fetch_ohlcv_df()
    logging.info("Fetching orderbook snapshot...")
    ob = get_orderbook_snapshot(SYMBOL)
    logging.info("Computing indicators...")
    ind = compute_indicators(df)
    logging.info("Fetching derivatives metrics...")
    metrics = await get_live_derivatives_metrics(SYMBOL)
    logging.info("Calculating liquidation totals...")
    long_liq, short_liq = get_liq_totals()
    # Track OI delta
    oi_now = metrics.get('open_interest') or 0
    OI_CACHE.append(oi_now)
    logging.info(f"OI_CACHE={list(OI_CACHE)}")
    oi_delta = OI_CACHE[-1] - OI_CACHE[-2] if len(OI_CACHE) >= 2 else 0
    # Compute real trade stats
    trade_stats = compute_trade_stats()
    ohlcv_block = format_ohlcv_block(df)
    orderbook_block = format_orderbook_block(ob)
    flow_block = format_flow_block(trade_stats)
    deriv_block = (
        "## Derivs 5 min\n" +
        f"OI:{short_num(metrics.get('open_interest'))} OIΔ:{oi_delta:+.0f} funding:{metrics.get('funding_rate')} " +
        f"long_liq:{short_num(long_liq)} short_liq:{short_num(short_liq)}"
    )
    ind_block = format_indicator_block(ind)
    ctx_block = await format_context_block()
    sentiment_block = format_sentiment_block(trade_stats, ind, df)
    blocks = [ohlcv_block, orderbook_block, flow_block, deriv_block, ind_block, ctx_block, sentiment_block]
    text = "\n".join(blocks)
    # Prompt-length guard: trim OHLCV if too long
    if len(text.split()) > 800:
        logging.warning("Feed too long, trimming OHLCV rows")
        df = df.tail(15)
        blocks[0] = format_ohlcv_block(df)
        text = "\n".join(blocks)
    return text


async def write_row(ts: int, feed: str):
    async with DB_POOL.acquire() as c:
        await c.execute(f"""
            INSERT INTO {TABLE} (symbol, timestamp, feed)
            VALUES ($1, $2, $3)
            ON CONFLICT (symbol, timestamp) DO UPDATE SET feed = $3
        """, SYMBOL, ts, feed)


async def main_loop():
    await ensure_table()
    asyncio.create_task(start_liquidation_listener(SYMBOL.replace("/USDT:USDT", "USDT")))
    asyncio.create_task(start_trade_listener(SYMBOL.replace("/USDT:USDT", "USDT")))
    try:
        while True:
            try:
                text = await collect_feed_text()
                ts = int(datetime.now(timezone.utc).timestamp() * 1000)
                await write_row(ts, text)
                logging.info("Feed row stored")
            except Exception as e:
                import traceback
                logging.error(f"feed error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(LOOP_INTERVAL)
    finally:
        try:
            await EX.close()
        except Exception:
            pass
        global DB_POOL
        if DB_POOL is not None:
            await DB_POOL.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
    # Log environment variables for debugging
    logging.info(f"SYMBOL={SYMBOL} DB_URL={'set' if DB_URL else 'NOT SET'} TABLE={TABLE} LOOP_INTERVAL={LOOP_INTERVAL}")
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("Feed ETL stopped")
