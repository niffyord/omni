"""
Derivatives-Metrics Collector  • ETH/USDT
— stores 1-min snapshots in TimescaleDB

NEW COLUMNS
• funding_rate
• open_interest
• oi_1h_delta_pct    ← % change vs 60 min ago
• long_short_ratio   ← Bybit accounts ratio (optional API)
• buy_ratio
• sell_ratio
• oi_5m_delta_pct    ← % change vs 5 min ago
• oi_15m_delta_pct   ← % change vs 15 min ago
• oi_4h_delta_pct    ← % change vs 4 hours ago
• ls_5m_delta_pct    ← % change vs 5 min ago
• ls_15m_delta_pct   ← % change vs 15 min ago
• ls_4h_delta_pct    ← % change vs 4 hours ago
• funding_price_div  ← funding sign diverges from recent price direction
• liquidation_8h_long  DOUBLE PRECISION,
• liquidation_8h_short DOUBLE PRECISION,
• perp_spot_basis_pct  DOUBLE PRECISION,
• eth_btc_funding_spread DOUBLE PRECISION,
"""

import os
from dotenv import load_dotenv
load_dotenv()

DERIV_METRICS_DB = os.getenv("DERIV_METRICS_DB", "")
print("DERIV_METRICS_DB:", DERIV_METRICS_DB)

import logging
import schedule
import psycopg2
import requests
import time
import json
import ccxt
from datetime import datetime, timezone
from collections import deque
from tools import get_live_derivatives_metrics
import threading
import asyncio
import websockets
import json

# --- Global buffer for liquidation events (timestamp, side, size) ---
LIQUIDATION_EVENTS = deque()

# --- Start Bybit WebSocket listener for liquidations ---
def start_liquidation_listener(symbol="ETHUSDT"): 
    async def listen():
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
                            ts = int(event["T"]) // 1000  # ms to seconds
                            side = event["S"]
                            size = float(event["v"])
                            LIQUIDATION_EVENTS.append((ts, side, size))
                        except (KeyError, ValueError, TypeError):
                            continue
                # Prune events older than 8 hours
                cutoff = int(datetime.now(timezone.utc).timestamp()) - 8*3600
                while LIQUIDATION_EVENTS and LIQUIDATION_EVENTS[0][0] < cutoff:
                    LIQUIDATION_EVENTS.popleft()
    def run():
        asyncio.run(listen())
    threading.Thread(target=run, daemon=True).start()

# --- Aggregation function for 8h liquidation totals ---
def get_8h_liquidation_totals():
    now = int(datetime.now(timezone.utc).timestamp())
    cutoff = now - 8*3600
    long_liq  = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Sell" and ts >= cutoff)
    short_liq = sum(size for ts, side, size in LIQUIDATION_EVENTS if side == "Buy" and ts >= cutoff)
    return {"long_liquidations": long_liq, "short_liquidations": short_liq}

# --- Bybit API setup ---
EX_PRICE = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})

# ─── CONFIG ───────────────────────────────────────────────────────────────
SYMBOL     = "ETH/USDT:USDT"
TABLE_NAME = "derivatives_metrics"
RETENTION_DAYS = int(os.getenv("DERIV_RETENTION_DAYS", 7))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler()]
)

# ─── DB INIT ──────────────────────────────────────────────────────────────
CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    symbol               TEXT    NOT NULL,
    timestamp            BIGINT  NOT NULL,
    funding_rate         DOUBLE PRECISION,
    open_interest        DOUBLE PRECISION,
    oi_1h_delta_pct      DOUBLE PRECISION,
    long_short_ratio     DOUBLE PRECISION,
    buy_ratio            DOUBLE PRECISION,
    sell_ratio           DOUBLE PRECISION,
    funding_price_div    BOOLEAN,
    liquidation_8h_long  DOUBLE PRECISION,
    liquidation_8h_short DOUBLE PRECISION,
    perp_spot_basis_pct  DOUBLE PRECISION,
    eth_btc_funding_spread DOUBLE PRECISION,
    funding_z            DOUBLE PRECISION,
    oi_5m_delta_pct      DOUBLE PRECISION,
    oi_15m_delta_pct     DOUBLE PRECISION,
    oi_4h_delta_pct      DOUBLE PRECISION,
    ls_5m_delta_pct      DOUBLE PRECISION,
    ls_15m_delta_pct     DOUBLE PRECISION,
    ls_4h_delta_pct      DOUBLE PRECISION,
    oi_z_24h            DOUBLE PRECISION,
    funding_8h_avg       DOUBLE PRECISION,
    perp_quarterly_basis_pct DOUBLE PRECISION,
    basis_annualised     DOUBLE PRECISION,
    liq_imbalance_8h     DOUBLE PRECISION,
    oi_turnover_ratio    DOUBLE PRECISION,
    PRIMARY KEY (symbol, timestamp)
);
SELECT create_hypertable('{TABLE_NAME}','timestamp', if_not_exists=>TRUE);
"""


def create_table():
    """Create the derivatives_metrics hypertable and ancillary helpers.

    Each logical DDL block is committed separately so that if one step
    fails (e.g. retention policy needs an extension that isn't present)
    the connection isn't left in an aborted state – avoiding the
    `current transaction is aborted` warnings seen previously.
    """
    with psycopg2.connect(DERIV_METRICS_DB) as conn, conn.cursor() as cur:
        # 1️⃣  helper function for integer_now (milliseconds)
        cur.execute(
            """
        CREATE OR REPLACE FUNCTION unix_now_ms() RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS $$
            SELECT FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
        $$;
        """
        )
        # 2️⃣  create table & hypertable – commit immediately
        cur.execute(CREATE_SQL)
        conn.commit()

        # 3️⃣  attach integer_now func – commit/rollback on error
        try:
            cur.execute(
                f"SELECT set_integer_now_func('{TABLE_NAME}', 'unix_now_ms');"
            )
            conn.commit()
        except Exception as e:
            logging.warning(f"set_integer_now_func not applied: {e}")
            conn.rollback()

        # 4️⃣  retention policy (TimescaleDB)
        if RETENTION_DAYS:
            drop_after_ms = RETENTION_DAYS * 24 * 60 * 60 * 1000
            try:
                cur.execute(
                    f"SELECT add_retention_policy('{TABLE_NAME}', drop_after => {drop_after_ms}, if_not_exists => TRUE);"
                )
                conn.commit()
            except Exception as e:
                logging.warning(f"Retention policy not applied: {e}")
                conn.rollback()

        # 5️⃣  index for fast look-ups
        try:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_derivatives_symbol_timestamp ON derivatives_metrics(symbol, timestamp DESC);"
            )
            conn.commit()
        except Exception as e:
            logging.warning(f"Could not create index: {e}")
            conn.rollback()

# ── helper to pick quarterly contract symbol ────────────────────────────
# ── 3 · quarterly symbol cache ------------------------------------
QUARTERLY_CACHE = {}
def get_quarterly_symbol():
    if 'sym' in QUARTERLY_CACHE and time.time() - QUARTERLY_CACHE['ts'] < 86_400:
        return QUARTERLY_CACHE['sym']
    markets = EX_PRICE.load_markets()
    cands = [m for m in markets.values() if m.get("base")=="ETH" and m.get("linear") and m.get("future")]
    if not cands:
        return None
    cands.sort(key=lambda x: x.get("expiry", float('inf')))
    QUARTERLY_CACHE.update(sym=cands[0]['symbol'], ts=time.time())
    return QUARTERLY_CACHE['sym']

# ─── 1-MIN JOB ────────────────────────────────────────────────────────────
def fetch_oi_1h_ago(cur, now_ms):
    cur.execute(
        f"""SELECT open_interest FROM {TABLE_NAME}
            WHERE symbol=%s AND timestamp<=%s
            ORDER BY timestamp DESC LIMIT 1""",
        (SYMBOL, now_ms - 3_600_000)
    )
    row = cur.fetchone()
    return row[0] if row else None

def fetch_metric_at(cur, col: str, ago_ms: int, now_ms: int):
    cur.execute(
        f"""SELECT {col} FROM {TABLE_NAME}
            WHERE symbol=%s AND timestamp<=%s
            ORDER BY timestamp DESC LIMIT 1""",
        (SYMBOL, now_ms - ago_ms)
    )
    row = cur.fetchone()
    return row[0] if row else None

def funding_price_divergence(symbol: str, funding_rate: float) -> bool | None:
    """Returns True if funding sign diverges from recent price direction (vs 30-min VWAP)."""
    try:
        ohlcv = EX_PRICE.fetch_ohlcv(symbol, timeframe="1m", limit=31)
        if len(ohlcv) < 31:
            return None
        closes = [bar[4] for bar in ohlcv]
        vwap = sum(closes) / len(closes)
        price_now = closes[-1]
        price_dir = 1 if price_now > vwap else -1 if price_now < vwap else 0
        fund_dir  = 1 if funding_rate and funding_rate > 0 else -1 if funding_rate and funding_rate < 0 else 0
        if fund_dir == 0:
            return None
        return price_dir != fund_dir
    except Exception:
        return None

def job():
    import time

    try:
        live = get_live_derivatives_metrics(SYMBOL)
        now_ms = int(datetime.now(timezone.utc).timestamp()*1000)
        now_ms = now_ms // 60_000 * 60_000
    except Exception as e:
        if "429" in str(e) or "too many requests" in str(e).lower():
            print(f"Rate limit hit, backing off for 60 seconds: {e}")
            time.sleep(60)
            return
        else:
            print(f"Error fetching live metrics: {e}")
            return

    # Prune metrics dict to essential fields
    metrics = {
        "funding_rate":   live["funding_rate"],
        "open_interest":  live["open_interest"],
        "long_short_ratio": live.get("long_short_ratio"),
        "buy_ratio":      live.get("buy_ratio"),
        "sell_ratio":     live.get("sell_ratio"),
    }

    DELTAS = {
        "oi_5m_delta_pct":      5    * 60_000,
        "oi_15m_delta_pct":     15   * 60_000,
        "oi_4h_delta_pct":      4    * 3_600_000,
        "ls_5m_delta_pct":      5    * 60_000,
        "ls_15m_delta_pct":     15   * 60_000,
        "ls_4h_delta_pct":      4    * 3_600_000,
    }

    with psycopg2.connect(DERIV_METRICS_DB) as conn, conn.cursor() as cur:
        def pct(old, new, eps=1e-9):
            if new is None or old in (None,):
                return None
            den = old if abs(old) > eps else eps
            return (new - old) / den * 100.0
        # OI/LR deltas
        for col, lag in DELTAS.items():
            base = "open_interest" if col.startswith("oi") else "long_short_ratio"
            old = fetch_metric_at(cur, base, lag, now_ms)
            metrics[col] = pct(old, metrics.get(base))

        # 1h OI delta
        oi_1h_ago = fetch_metric_at(cur, "open_interest", 3_600_000, now_ms)
        metrics["oi_1h_delta_pct"] = pct(oi_1h_ago, metrics["open_interest"])

        # OI z-score 24h
        cur.execute(f"SELECT open_interest FROM {TABLE_NAME} WHERE symbol=%s AND timestamp>=%s", (SYMBOL, now_ms - 24*60*60*1000))
        oi_hist = [r[0] for r in cur.fetchall() if r[0] is not None]
        if len(oi_hist) >= 10:
            import numpy as np
            oi_mu = np.mean(oi_hist)
            oi_sigma = np.std(oi_hist)
            if metrics["open_interest"] is not None and oi_sigma:
                metrics["oi_z_24h"] = (metrics["open_interest"] - oi_mu) / oi_sigma
            else:
                metrics["oi_z_24h"] = None
        else:
            metrics["oi_z_24h"] = None
        # Funding rate z-score 24h (Bybit REST, not DB)
        try:
            from tools import fetch_bybit_funding_history
            from statistics import mean, stdev
            symbol_rest = SYMBOL.replace("/USDT:USDT", "USDT").replace("/", "")
            hist = fetch_bybit_funding_history(symbol_rest, limit=30)
            rates = [float(x["fundingRate"]) for x in hist]
            if len(rates) >= 3:
                latest = rates[-1]
                mu = mean(rates)
                sigma = stdev(rates)
                metrics["funding_z"] = (latest - mu) / sigma if sigma else None
            else:
                metrics["funding_z"] = None
        except Exception:
            metrics["funding_z"] = None

        # Divergence flag
        metrics["funding_price_div"] = funding_price_divergence(SYMBOL, metrics["funding_rate"])

        # Liquidation stats
        liq = get_8h_liquidation_totals()
        metrics["liquidation_8h_long"]  = liq["long_liquidations"]
        metrics["liquidation_8h_short"] = liq["short_liquidations"]
        if (liq["long_liquidations"] + liq["short_liquidations"]) > 0:
            metrics["liq_imbalance_8h"] = (liq["long_liquidations"] - liq["short_liquidations"]) / (liq["long_liquidations"] + liq["short_liquidations"])
        else:
            metrics["liq_imbalance_8h"] = None

        # --- Perp-spot basis pct ---
        try:
            spot_ticker = EX_PRICE.fetch_ticker("ETH/USDT")
            spot_price = spot_ticker.get("last")
            perp_price = EX_PRICE.fetch_ticker("ETH/USDT:USDT").get("last")
            if spot_price and perp_price:
                metrics["perp_spot_basis_pct"] = (perp_price / spot_price - 1) * 100
            else:
                metrics["perp_spot_basis_pct"] = None
        except Exception:
            metrics["perp_spot_basis_pct"] = None

        # --- Perp-Quarterly basis pct ---
        try:
            qsym = get_quarterly_symbol()
            if qsym:
                q_price = EX_PRICE.fetch_ticker(qsym).get("last")
                perp_price = EX_PRICE.fetch_ticker("ETH/USDT:USDT").get("last")
                if q_price and perp_price:
                    metrics["perp_quarterly_basis_pct"] = (perp_price / q_price - 1) * 100
                else:
                    metrics["perp_quarterly_basis_pct"] = None
            else:
                metrics["perp_quarterly_basis_pct"] = None
        except Exception:
            metrics["perp_quarterly_basis_pct"] = None

        # --- Basis annualised ---
        try:
            if metrics.get("perp_quarterly_basis_pct") is not None:
                # Funding interval for perps: 8h, so ~1095 periods/year
                metrics["basis_annualised"] = metrics["perp_quarterly_basis_pct"] * 1095
            else:
                metrics["basis_annualised"] = None
        except Exception:
            metrics["basis_annualised"] = None

        # --- BTC-ETH funding spread ---
        try:
            eth_funding = metrics["funding_rate"]
            btc_funding = None
            # Try get BTC/USDT:USDT funding rate from Bybit, cache for 5 min
            now = time.time()
            if not hasattr(job, "BTC_FUNDING_CACHE"):
                job.BTC_FUNDING_CACHE = {"ts": 0, "val": None}
            if now - job.BTC_FUNDING_CACHE["ts"] > 300:
                btc_funding_resp = EX_PRICE.fetch_funding_rate("BTC/USDT:USDT")
                job.BTC_FUNDING_CACHE = {"ts": now, "val": btc_funding_resp.get("fundingRate")}
            btc_funding = job.BTC_FUNDING_CACHE["val"]
            if eth_funding is not None and btc_funding is not None:
                metrics["eth_btc_funding_spread"] = eth_funding - btc_funding
            else:
                metrics["eth_btc_funding_spread"] = None
        except Exception:
            metrics["eth_btc_funding_spread"] = None

        # --- Funding 8h avg (Bybit REST, not DB) ---
        try:
            from tools import fetch_bybit_funding_history
            symbol_rest = SYMBOL.replace("/USDT:USDT", "USDT").replace("/", "")
            hist = fetch_bybit_funding_history(symbol_rest, limit=8)
            rates = [float(x["fundingRate"]) for x in hist]
            if rates:
                metrics["funding_8h_avg"] = sum(rates) / len(rates)
            else:
                metrics["funding_8h_avg"] = None
        except Exception:
            metrics["funding_8h_avg"] = None

        # --- OI Turnover ratio ---
        try:
            spot_ohlcv = EX_PRICE.fetch_ohlcv("ETH/USDT", timeframe="1d", limit=2)
            if spot_ohlcv and len(spot_ohlcv) >= 2:
                spot_vol = spot_ohlcv[-1][5]
                metrics["oi_turnover_ratio"] = metrics["open_interest"] / spot_vol if spot_vol else None
            else:
                metrics["oi_turnover_ratio"] = None
        except Exception:
            metrics["oi_turnover_ratio"] = None

        # --- Insert with new columns ---
        cur.execute(
            f"""INSERT INTO {TABLE_NAME}
                (symbol, timestamp, funding_rate, open_interest, oi_1h_delta_pct, long_short_ratio, buy_ratio, sell_ratio, funding_price_div, liquidation_8h_long, liquidation_8h_short, perp_spot_basis_pct, eth_btc_funding_spread, funding_z, oi_5m_delta_pct, oi_15m_delta_pct, oi_4h_delta_pct, ls_5m_delta_pct, ls_15m_delta_pct, ls_4h_delta_pct, oi_z_24h, funding_8h_avg, perp_quarterly_basis_pct, basis_annualised, liq_imbalance_8h, oi_turnover_ratio)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(symbol,timestamp) DO UPDATE SET
                    funding_rate=EXCLUDED.funding_rate,
                    open_interest=EXCLUDED.open_interest,
                    oi_1h_delta_pct=EXCLUDED.oi_1h_delta_pct,
                    long_short_ratio=EXCLUDED.long_short_ratio,
                    buy_ratio=EXCLUDED.buy_ratio,
                    sell_ratio=EXCLUDED.sell_ratio,
                    funding_price_div=EXCLUDED.funding_price_div,
                    liquidation_8h_long=EXCLUDED.liquidation_8h_long,
                    liquidation_8h_short=EXCLUDED.liquidation_8h_short,
                    perp_spot_basis_pct=EXCLUDED.perp_spot_basis_pct,
                    eth_btc_funding_spread=EXCLUDED.eth_btc_funding_spread,
                    funding_z=EXCLUDED.funding_z,
                    oi_5m_delta_pct=EXCLUDED.oi_5m_delta_pct,
                    oi_15m_delta_pct=EXCLUDED.oi_15m_delta_pct,
                    oi_4h_delta_pct=EXCLUDED.oi_4h_delta_pct,
                    ls_5m_delta_pct=EXCLUDED.ls_5m_delta_pct,
                    ls_15m_delta_pct=EXCLUDED.ls_15m_delta_pct,
                    ls_4h_delta_pct=EXCLUDED.ls_4h_delta_pct,
                    oi_z_24h=EXCLUDED.oi_z_24h,
                    funding_8h_avg=EXCLUDED.funding_8h_avg,
                    perp_quarterly_basis_pct=EXCLUDED.perp_quarterly_basis_pct,
                    basis_annualised=EXCLUDED.basis_annualised,
                    liq_imbalance_8h=EXCLUDED.liq_imbalance_8h,
                    oi_turnover_ratio=EXCLUDED.oi_turnover_ratio
            """,
            (SYMBOL, now_ms,
             metrics["funding_rate"],
             metrics["open_interest"],
             metrics["oi_1h_delta_pct"],
             metrics["long_short_ratio"],
             metrics["buy_ratio"],
             metrics["sell_ratio"],
             metrics["funding_price_div"],
             metrics["liquidation_8h_long"],
             metrics["liquidation_8h_short"],
             metrics["perp_spot_basis_pct"],
             metrics["eth_btc_funding_spread"],
             metrics["funding_z"],
             metrics["oi_5m_delta_pct"],
             metrics["oi_15m_delta_pct"],
             metrics["oi_4h_delta_pct"],
             metrics["ls_5m_delta_pct"],
             metrics["ls_15m_delta_pct"],
             metrics["ls_4h_delta_pct"],
             metrics["oi_z_24h"],
             metrics["funding_8h_avg"],
             metrics["perp_quarterly_basis_pct"],
             metrics["basis_annualised"],
             metrics["liq_imbalance_8h"],
             metrics["oi_turnover_ratio"])
        )
        conn.commit()
    # Prune liquidation deque every job in case WS dies
    cutoff = int(datetime.now(timezone.utc).timestamp()) - 8*3600
    while LIQUIDATION_EVENTS and LIQUIDATION_EVENTS[0][0] < cutoff:
        LIQUIDATION_EVENTS.popleft()

    def safe_fmt(val, fmtstr, none_val="N/A"):
        try:
            return fmtstr.format(val) if val is not None else none_val
        except Exception:
            return none_val
    logging.info(
        f"stored FR={safe_fmt(metrics['funding_rate'], '{:.8%}')}  "
        f"OI={safe_fmt(metrics['open_interest'], '{:,}')}  "
        f"Δ1h={safe_fmt(metrics['oi_1h_delta_pct'], '{:.2f}%')}  "
        f"L/S={safe_fmt(metrics['long_short_ratio'], '{:.2f}')}  "
        f"Buy={safe_fmt(metrics['buy_ratio'], '{:.2%}')}  Sell={safe_fmt(metrics['sell_ratio'], '{:.2%}')}  "
        f"Div={metrics['funding_price_div']}  "
        f"Perp-Spot={safe_fmt(metrics['perp_spot_basis_pct'], '{:.4f}%')}  "
        f"BTC-ETH funding spread={safe_fmt(metrics['eth_btc_funding_spread'], '{:.8%}')}  "
        f"| OI Δ5m={safe_fmt(metrics['oi_5m_delta_pct'], '{:.2f}%')}  "
        f"OI Δ15m={safe_fmt(metrics['oi_15m_delta_pct'], '{:.2f}%')}  "
        f"OI Δ4h={safe_fmt(metrics['oi_4h_delta_pct'], '{:.2f}%')}  "
        f"L/S Δ5m={safe_fmt(metrics['ls_5m_delta_pct'], '{:.2f}%')}  "
        f"L/S Δ15m={safe_fmt(metrics['ls_15m_delta_pct'], '{:.2f}%')}  "
        f"L/S Δ4h={safe_fmt(metrics['ls_4h_delta_pct'], '{:.2f}%')}  "
        f"OI Z24h={safe_fmt(metrics['oi_z_24h'], '{:.2f}')}  "
        f"Funding8hAvg={safe_fmt(metrics['funding_8h_avg'], '{:.8%}')}  "
        f"Perp-Quarterly={safe_fmt(metrics['perp_quarterly_basis_pct'], '{:.4f}%')}  "
        f"BasisAnn={safe_fmt(metrics['basis_annualised'], '{:.2f}%')}  "
        f"LiqImb8h={safe_fmt(metrics['liq_imbalance_8h'], '{:.2%}')}  "
        f"OI/Vol24h={safe_fmt(metrics['oi_turnover_ratio'], '{:.4f}')}"
    )


def main():
    create_table()
    schedule.every(60).seconds.do(job)
    job()                                      # fire once at start
    logging.info("Scheduler running (60-second cadence)")
    while True:
        schedule.run_pending()
        time.sleep(0.5)

if __name__ == "__main__":
    # Start liquidation listener before main loop
    start_liquidation_listener("ETHUSDT")
    main()


def summarize_derivatives_metrics(metrics: dict) -> str:
    """
    Summarizes the derivatives metrics into a human-readable LLM summary string.
    Handles None values and missing fields gracefully.
    """
    def fmt(val, pct=False, n=4):
        if val is None:
            return "N/A"
        try:
            if pct:
                return f"{float(val)*100:.{n}f}%"
            return f"{float(val):.{n}f}"
        except Exception:
            return str(val)

    fr = metrics.get("funding_rate")
    oi = metrics.get("open_interest")
    oi_1h = metrics.get("oi_1h_delta_pct")
    lsr = metrics.get("long_short_ratio")
    buy = metrics.get("buy_ratio")
    sell = metrics.get("sell_ratio")
    div = metrics.get("funding_price_div")
    basis = metrics.get("perp_spot_basis_pct")
    spread = metrics.get("eth_btc_funding_spread")
    liq_long = metrics.get("liquidation_8h_long")
    liq_short = metrics.get("liquidation_8h_short")
    ts = metrics.get("timestamp", "N/A")

    summary = (
        f"Funding rate: {fmt(fr, pct=True, n=6)} | "
        f"Open interest: {fmt(oi, pct=False, n=0)} | "
        f"OI Δ1h: {fmt(oi_1h, pct=False, n=2)}% | "
        f"L/S ratio: {fmt(lsr, pct=False, n=2)} | "
        f"Buy: {fmt(buy, pct=True, n=2)} | Sell: {fmt(sell, pct=True, n=2)} | "
        f"Divergence: {div} | "
        f"Perp-Spot basis: {fmt(basis, pct=False, n=4)}% | "
        f"BTC-ETH funding spread: {fmt(spread, pct=True, n=6)} | "
        f"8h Liq (Long): {fmt(liq_long, pct=False, n=2)} | 8h Liq (Short): {fmt(liq_short, pct=False, n=2)} | "
        f"Timestamp: {ts}"
    )
    return summary
