import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import os
import ccxt.pro
import redis
import json
from dotenv import load_dotenv

load_dotenv()
SYMBOL = os.getenv("SYMBOL", "ETH/USDT:USDT")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_STREAM = f"orderbook:{SYMBOL.replace('/', '').replace(':', '_')}"

async def collect_and_store_orderbook():
    def make_ex():
        return ccxt.pro.bybit({
            "enableRateLimit": True,
            "options": {"defaultType": "linear"},
            "timeout": 60000,
        })
    ex = make_ex()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    while True:
        try:
            ob = await ex.watch_order_book(SYMBOL.replace(":USDT", ""))
            bids = ob.get("bids", [])[:20]
            asks = ob.get("asks", [])[:20]
            spread = (asks[0][0] - bids[0][0]) if bids and asks else None
            bid_qty = sum(b[1] for b in bids)
            ask_qty = sum(a[1] for a in asks)
            imbalance = 100 * (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1) if (bid_qty or ask_qty) else 0
            snap = {
                "bids": bids,
                "asks": asks,
                "spread": spread,
                "bid_ask_imbalance_pct": imbalance,
            }
            r.xadd(REDIS_STREAM, {"data": json.dumps(snap)}, maxlen=1000)
            print(f"Pushed orderbook snapshot to {REDIS_STREAM}: bids={len(bids)} asks={len(asks)}")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"[collector] Warning: {e}")
            await asyncio.sleep(2)
            try:
                await ex.close()
            except Exception:
                pass
            ex = make_ex()


if __name__ == "__main__":
    asyncio.run(collect_and_store_orderbook())
