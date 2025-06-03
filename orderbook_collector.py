"""
CORE/USDT Order-Book + Trades Collector – v0.8
──────────────────────────────────────────────────────────────────────────────
Δ v0.7 ➜ v0.8
• **Trade-side normalisation**  (“Buy” / “Sell” → “buy” / “sell”)
• Trade window default extended to 15 s for stabler TPS stats
• Minor: defaultType = "future" again (Bybit linear futures)
"""
# pylint: disable=invalid-name, too-many-locals
import sys, os, json, time, asyncio, statistics
import math
from collections import deque
import numpy as np  # For curve_exponent
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import ccxt.pro
import redis

# ──────────────────────────────────────────────
# CONFIG (env-overridable)
# ──────────────────────────────────────────────
SYMBOL           = os.getenv("SYMBOL",          "CORE/USDT:USDT")
BYBIT_MARKET     = SYMBOL
DEPTH          = int(os.getenv("DEPTH", 500))        # Bybit max L2 (supported: 1, 50, 200, 500)
TICK_SIZE      = float(os.getenv("TICK_SIZE", 0.0001))
BPS_LIQUIDITY  = float(os.getenv("BPS_LIQUIDITY", 10))
DEPTH_TIERS    = [1, 3, 5, 10, 25, 50, 75, 100]       # ±1 % depth curve
IMPACT_PCTS    = [0.001, 0.0025, 0.005, 0.01, 0.02]   # up to ±2 %
HEAT_BIN_BPS   = 5                                    # 5 bp resolution
VOL_WINDOW_MS  = int(os.getenv("VOL_WINDOW_MS", 30_000))
TRADE_WIN_MS   = int(os.getenv("TRADE_WIN_MS", 15_000))
VPIN_BUCKET_SIZE = float(os.getenv("VPIN_BUCKET_SIZE", 10.0))
VPIN_NUM_BUCKETS = int(os.getenv("VPIN_NUM_BUCKETS", 20))
SNAP_HISTORY    = int(os.getenv("SNAP_HISTORY", 60))
ROLL_WINS_MS   = [5_000, 15_000, 60_000]
MAX_ROLL_WIN   = max(ROLL_WINS_MS)
REDIS_HOST       = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT       = int(os.getenv("REDIS_PORT", 6379))
REDIS_STREAM     = os.getenv("REDIS_STREAM",
                     f"orderbook:{SYMBOL.replace('/','').replace(':','_')}")
MAXLEN_STREAM    = int(os.getenv("MAXLEN_STREAM", 100_000))
MAX_BACKOFF      = int(os.getenv("MAX_BACKOFF", 60))

redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ──────────────────────────────────────────────
# STATE
# ──────────────────────────────────────────────
prev_book   = None
mid_prices  = deque()                         # (ts, mid)
trade_log   = deque()                         # (ts, side, size)
last_bid_q  = last_ask_q = None
# VPIN
bucket_vol, bucket_abs = 0.0, 0.0
vpin_components = deque(maxlen=VPIN_NUM_BUCKETS)
# queue-drain history for z-score
drain_hist_bid = deque(maxlen=300)   # ~ last few minutes
drain_hist_ask = deque(maxlen=300)
# history buffer for downstream modelling
history_buffer = deque(maxlen=SNAP_HISTORY)  # store key micro-features
# OFI history for rolling aggregates
ofi_hist = deque()
# queue-drain time series for rolling stats
drain_time_hist_bid = deque()
drain_time_hist_ask = deque()

# Institutional-grade microstructure tracking
price_change_hist = deque(maxlen=500)  # (ts, price_change, volume) for Kyle's Lambda
kyle_lambda_hist = deque(maxlen=100)  # Historical Kyle's Lambda values
trade_impact_hist = deque(maxlen=200)  # (ts, price_before, price_after, volume, side)
spread_hist = deque(maxlen=500)  # (ts, spread) for Roll's decomposition

# Liquidity Provider Behavior tracking
quote_refresh_hist = deque(maxlen=500)  # (ts, level, side, old_size, new_size, price)
liquidity_migration_hist = deque(maxlen=300)  # Track cross-level liquidity movements
market_maker_quotes = {}  # Track quote patterns by price level

# Market Maker Inventory estimation
mm_inventory_hist = deque(maxlen=200)  # Estimated market maker inventory changes
inventory_pressure_hist = deque(maxlen=100)  # Inventory-driven price pressure signals
skew_adjustment_hist = deque(maxlen=150)  # Track optimal skew adjustments

# ---------------- helpers ----------------
def depth_profile(side, mid, bin_bp=5, max_bp=100):
    """Return cumulative vol in bp-wide buckets: -100bp…-5bp / 5bp…100bp."""
    bins = {}
    step = bin_bp / 10_000
    for px, sz in side:
        bp = (1 - px/mid) * 10_000 if px < mid else (px/mid - 1) * 10_000
        bp = int(bp // bin_bp * bin_bp)
        if abs(bp) > max_bp:
            break
        bins.setdefault(bp, 0.0)
        bins[bp] += sz
    return [bins.get(b, 0.0) for b in range(bin_bp, max_bp+1, bin_bp)]

def curve_exponent(buckets):
    xs = np.arange(1, len(buckets)+1)
    cs = np.cumsum(buckets)
    if cs[-1] == 0:
        return None
    logx, logc = np.log(xs), np.log(cs + 1e-9)
    slope, _ = np.polyfit(logx, logc, 1)
    return float(slope)

def prune(q, horizon_ms):
    limit = time.time()*1000 - horizon_ms
    while q and q[0][0] < limit:
        q.popleft()

def window_sum(q, horizon_ms):
    lim = time.time()*1000 - horizon_ms
    return sum(v for t, v in q if t >= lim)

def trade_count(q, horizon_ms):
    lim = time.time()*1000 - horizon_ms
    return sum(1 for ts, *_ in q if ts >= lim)

from decimal import Decimal, ROUND_HALF_UP

def round_px(px):
    # Use integer ticks to avoid floating-point bias
    return int(Decimal(str(px)) / Decimal(str(TICK_SIZE)) + Decimal('0.5'))

def calc_ofi(curr, prev):
    DEPTH_OFI = 10  # Only use top 10 levels per side
    if prev is None:
        return 0.0
    bids_now = {round_px(px): sz for px, sz in curr["bids"][:DEPTH_OFI]}
    asks_now = {round_px(px): sz for px, sz in curr["asks"][:DEPTH_OFI]}
    bids_old = {round_px(px): sz for px, sz in prev["bids"][:DEPTH_OFI]}
    asks_old = {round_px(px): sz for px, sz in prev["asks"][:DEPTH_OFI]}
    return sum(s - bids_old.get(p, 0) for p, s in bids_now.items()) - \
           sum(s - asks_old.get(p, 0) for p, s in asks_now.items())

def cum_vol(book_side, side, mid, bp):
    lim = mid*(1 - bp/10_000) if side == "bid" else mid*(1 + bp/10_000)
    vol = 0.0
    for entry in book_side:
        px, sz = entry[0], entry[1]
        if (side == "bid" and px >= lim) or (side == "ask" and px <= lim):
            vol += sz
        else:
            break
    return vol

def liq_cost(book_side, side, mid, pct):
    trg = mid*(1-pct) if side == "bid" else mid*(1+pct)
    cost = 0.0
    for entry in book_side:
        px, sz = entry[0], entry[1]
        if (side == "bid" and px >= trg) or (side == "ask" and px <= trg):
            cost += abs(px-mid)*sz
        else:
            break
    return cost

# ---------------- INSTITUTIONAL MICROSTRUCTURE FUNCTIONS ----------------

def calculate_kyle_lambda(price_changes, volumes, window_size=50):
    """
    Calculate Kyle's Lambda (price impact coefficient).
    λ = E[Δp] / E[√V] - measures permanent price impact per unit volume.
    Critical for institutional execution algorithms.
    """
    try:
        if len(price_changes) < window_size or len(volumes) < window_size:
            return None
        
        recent_changes = list(price_changes)[-window_size:]
        recent_volumes = list(volumes)[-window_size:]
        
        if not recent_changes or not recent_volumes:
            return None
            
        # Calculate expected price change and expected square root of volume
        avg_price_change = np.mean([abs(pc) for pc in recent_changes if pc is not None])
        avg_sqrt_volume = np.mean([np.sqrt(vol) for vol in recent_volumes if vol > 0])
        
        if avg_sqrt_volume == 0:
            return None
            
        kyle_lambda = avg_price_change / avg_sqrt_volume
        return float(kyle_lambda)
        
    except Exception:
        return None

def decompose_roll_spread(spreads, price_changes, trade_directions, window_size=100):
    """
    Roll's Spread Decomposition: separates bid-ask spread into:
    1. Adverse selection component (informed trading cost)
    2. Inventory/order processing component (liquidity provision cost)
    Essential for market maker profitability analysis.
    """
    try:
        if len(spreads) < window_size or len(price_changes) < window_size:
            return None, None, None
            
        recent_spreads = list(spreads)[-window_size:]
        recent_changes = list(price_changes)[-window_size:]
        recent_directions = list(trade_directions)[-window_size:]
        
        # Roll's effective spread decomposition
        # Adverse selection = 2 * Cov(direction_t, midprice_change_t+1)
        # Order processing = Spread - Adverse_selection
        
        direction_values = []
        price_change_values = []
        
        for i in range(len(recent_directions) - 1):
            if recent_directions[i] is not None and recent_changes[i+1] is not None:
                direction_values.append(1 if recent_directions[i] == 'buy' else -1)
                price_change_values.append(recent_changes[i+1])
        
        if len(direction_values) < 20:
            return None, None, None
            
        # Calculate covariance for adverse selection component
        adverse_selection = 2 * np.cov(direction_values, price_change_values)[0, 1]
        avg_spread = np.mean([s for s in recent_spreads if s is not None])
        
        if avg_spread <= 0:
            return None, None, None
            
        order_processing = avg_spread - adverse_selection
        
        # Ensure components are non-negative and sum to spread
        adverse_selection = max(0, min(adverse_selection, avg_spread))
        order_processing = max(0, avg_spread - adverse_selection)
        
        # Calculate percentages
        adverse_pct = (adverse_selection / avg_spread) * 100 if avg_spread > 0 else 0
        
        return float(adverse_selection), float(order_processing), float(adverse_pct)
        
    except Exception:
        return None, None, None

def detect_liquidity_provider_behavior(current_book, previous_book, quote_refresh_hist):
    """
    Detect liquidity provider behavior patterns:
    1. Quote refreshing patterns (market maker activity)
    2. Liquidity migration across price levels
    3. Early warning signals for liquidity withdrawal
    """
    try:
        if previous_book is None:
            return None
            
        ts = time.time() * 1000
        behavior_signals = {
            'quote_refresh_rate': 0,
            'liquidity_migration': 0,
            'withdrawal_risk': 0,
            'market_maker_activity': 0,
            'quote_stuffing_indicator': 0
        }
        
        # Track quote refreshes at each price level
        current_bids = {round_px(p): s for p, s in current_book.get('bids', [])}
        current_asks = {round_px(p): s for p, s in current_book.get('asks', [])}
        previous_bids = {round_px(p): s for p, s in previous_book.get('bids', [])}
        previous_asks = {round_px(p): s for p, s in previous_book.get('asks', [])}
        
        # Count quote refreshes (size changes at same price)
        bid_refreshes = sum(1 for p in current_bids if p in previous_bids and 
                           abs(current_bids[p] - previous_bids[p]) > 0.001)
        ask_refreshes = sum(1 for p in current_asks if p in previous_asks and 
                           abs(current_asks[p] - previous_asks[p]) > 0.001)
        
        behavior_signals['quote_refresh_rate'] = bid_refreshes + ask_refreshes
        
        # Log quote refreshes for pattern analysis
        for price in current_bids:
            if price in previous_bids:
                old_size = previous_bids[price]
                new_size = current_bids[price]
                if abs(new_size - old_size) > 0.001:
                    quote_refresh_hist.append((ts, price, 'bid', old_size, new_size))
        
        for price in current_asks:
            if price in previous_asks:
                old_size = previous_asks[price]
                new_size = current_asks[price]
                if abs(new_size - old_size) > 0.001:
                    quote_refresh_hist.append((ts, price, 'ask', old_size, new_size))
        
        # Analyze recent quote refresh patterns (last 30 seconds)
        recent_refreshes = [r for r in quote_refresh_hist if ts - r[0] <= 30000]
        
        if len(recent_refreshes) > 0:
            # High refresh rate indicates active market making
            refresh_rate = len(recent_refreshes) / 30.0  # per second
            behavior_signals['market_maker_activity'] = min(100, refresh_rate * 10)
            
            # Quote stuffing detection (very high refresh rate with small size changes)
            small_refreshes = [r for r in recent_refreshes if abs(r[4] - r[3]) < 1.0]
            if len(small_refreshes) > 20:  # >20 small refreshes in 30s
                behavior_signals['quote_stuffing_indicator'] = min(100, len(small_refreshes))
        
        # Liquidity migration detection
        # Check if liquidity is moving away from top of book
        if current_book.get('bids') and previous_book.get('bids'):
            curr_top_bid_size = current_book['bids'][0][1] if current_book['bids'] else 0
            prev_top_bid_size = previous_book['bids'][0][1] if previous_book['bids'] else 0
            bid_size_change = curr_top_bid_size - prev_top_bid_size
            
            if bid_size_change < -0.5:  # Significant liquidity reduction
                behavior_signals['withdrawal_risk'] += 30
                
        if current_book.get('asks') and previous_book.get('asks'):
            curr_top_ask_size = current_book['asks'][0][1] if current_book['asks'] else 0
            prev_top_ask_size = previous_book['asks'][0][1] if previous_book['asks'] else 0
            ask_size_change = curr_top_ask_size - prev_top_ask_size
            
            if ask_size_change < -0.5:  # Significant liquidity reduction
                behavior_signals['withdrawal_risk'] += 30
        
        return behavior_signals
        
    except Exception:
        return None

def estimate_market_maker_inventory(trades_hist, ofi_values, spreads, window_minutes=5):
    """
    Estimate market maker inventory position and predict inventory-driven moves.
    Models the "other side" of the trade for institutional execution optimization.
    """
    try:
        current_time = time.time() * 1000
        window_ms = window_minutes * 60 * 1000
        
        inventory_metrics = {
            'estimated_inventory': 0,
            'inventory_pressure': 0,
            'optimal_skew': 0,
            'inventory_risk': 0,
            'reversion_probability': 0
        }
        
        # Filter recent trades
        recent_trades = [t for t in trades_hist if current_time - t[0] <= window_ms]
        
        if len(recent_trades) < 5:
            return inventory_metrics
        
        # Estimate market maker inventory as cumulative imbalance
        # Assumption: MM takes opposite side of order flow imbalance
        cumulative_inventory = 0
        for trade in recent_trades:
            side = trade[1]  # 'buy' or 'sell'
            size = trade[2]
            # Market maker likely sells when buyers come (positive inventory)
            if side == 'buy':
                cumulative_inventory += size  # MM sells (short position)
            else:
                cumulative_inventory -= size  # MM buys (long position)
        
        inventory_metrics['estimated_inventory'] = cumulative_inventory
        
        # Calculate inventory pressure based on position size
        max_reasonable_inventory = np.std([t[2] for t in recent_trades]) * 50
        if max_reasonable_inventory > 0:
            inventory_pressure = abs(cumulative_inventory) / max_reasonable_inventory
            inventory_metrics['inventory_pressure'] = min(100, inventory_pressure * 100)
        
        # Estimate optimal skew adjustment
        # MM should skew quotes away from their inventory position
        if len(recent_trades) >= 10:
            avg_trade_size = np.mean([t[2] for t in recent_trades])
            if avg_trade_size > 0:
                skew_factor = cumulative_inventory / (avg_trade_size * 10)
                inventory_metrics['optimal_skew'] = np.clip(skew_factor * 100, -50, 50)
        
        # Inventory risk assessment
        if abs(cumulative_inventory) > max_reasonable_inventory * 0.7:
            inventory_metrics['inventory_risk'] = 80  # High risk
        elif abs(cumulative_inventory) > max_reasonable_inventory * 0.3:
            inventory_metrics['inventory_risk'] = 40  # Medium risk
        else:
            inventory_metrics['inventory_risk'] = 10   # Low risk
        
        # Mean reversion probability
        # High inventory suggests price will revert to reduce MM risk
        if abs(cumulative_inventory) > max_reasonable_inventory * 0.5:
            inventory_metrics['reversion_probability'] = 70
        elif abs(cumulative_inventory) > max_reasonable_inventory * 0.2:
            inventory_metrics['reversion_probability'] = 40
        else:
            inventory_metrics['reversion_probability'] = 20
        
        return inventory_metrics
        
    except Exception:
        return {
            'estimated_inventory': 0,
            'inventory_pressure': 0,
            'optimal_skew': 0,
            'inventory_risk': 0,
            'reversion_probability': 0
        }

# ---------------- main streams ----------------
async def stream_orderbook(exchange):
    global prev_book, last_bid_q, last_ask_q
    while True:
        ob = await exchange.watch_order_book(BYBIT_MARKET, DEPTH)
        ts = ob["timestamp"] or int(time.time()*1000)
        bids, asks = ob["bids"], ob["asks"]
        if not bids or not asks:
            continue

        best_bid, best_ask = bids[0][0], asks[0][0]
        bid_sz_top, ask_sz_top = bids[0][1], asks[0][1]
        mid  = (best_bid+best_ask)/2
        spread = best_ask-best_bid
        bid_size_sum = sum(entry[1] for entry in bids)
        ask_size_sum = sum(entry[1] for entry in asks)
        imbalance = 100 * (bid_size_sum - ask_size_sum) / (bid_size_sum + ask_size_sum)

        micro_px  = (best_bid*ask_sz_top + best_ask*bid_sz_top)/(bid_sz_top+ask_sz_top)
        # --- Critical fix: snapshot prev_book before update ---
        prev_book_snapshot = {"bids": [tuple(b) for b in bids], "asks": [tuple(a) for a in asks]}
        ofi_delta = calc_ofi(prev_book_snapshot, prev_book)
        ofi_hist.append((ts, ofi_delta)); prune(ofi_hist, MAX_ROLL_WIN)
        # EWMA for ofi_delta (α=0.1)
        ofi_ewma = 0.1*ofi_delta + 0.9*(ofi_hist[-2][1] if len(ofi_hist)>1 else 0)
        # Expose to snapshot for TA agent
        snap = {} if 'snap' not in locals() else snap
        snap["ofi_delta_ewma"] = ofi_ewma  # keep this
        # Only call detect_liquidity_provider_behavior once per loop
        lp_behavior = detect_liquidity_provider_behavior(prev_book_snapshot, prev_book, quote_refresh_hist)
        # Update prev_book at end of loop
        prev_book = prev_book_snapshot

        # Calculate depth tiers and slopes before use
        cum_bid_tiers = [cum_vol(bids, "bid", mid, bp) for bp in DEPTH_TIERS]
        cum_ask_tiers = [cum_vol(asks, "ask", mid, bp) for bp in DEPTH_TIERS]
        slope_in  = (cum_bid_tiers[1] - cum_bid_tiers[0]) / (DEPTH_TIERS[1] - DEPTH_TIERS[0])
        slope_out = (cum_ask_tiers[-1] - cum_ask_tiers[-2]) / (DEPTH_TIERS[-1] - DEPTH_TIERS[-2])
        depth_convexity = slope_out / slope_in if slope_in else None

        # impact ladder (cost and max order size for x bps slippage)
        liq_up = {f"{int(p*10000):04d}": liq_cost(asks,"ask",mid,p) for p in IMPACT_PCTS}
        liq_dn = {f"{int(p*10000):04d}": liq_cost(bids,"bid",mid,p) for p in IMPACT_PCTS}
        # max order size for x bps slippage (for each impact pct)
        def max_order_size_within_bps(book_side, side, mid, pct):
            trg = mid*(1-pct) if side == "bid" else mid*(1+pct)
            acc = 0.0
            for entry in book_side:
                px, sz = entry[0], entry[1]
                if (side == "bid" and px >= trg) or (side == "ask" and px <= trg):
                    acc += sz
                else:
                    break
            return acc
        max_size_up = {f"{int(p*10000):04d}": max_order_size_within_bps(asks,"ask",mid,p) for p in IMPACT_PCTS}
        max_size_dn = {f"{int(p*10000):04d}": max_order_size_within_bps(bids,"bid",mid,p) for p in IMPACT_PCTS}

        # queue drain
        drain_bid = drain_ask = 0.0
        if last_bid_q is not None:
            denom_b = max(last_bid_q, bid_sz_top, 1e-9)
            denom_a = max(last_ask_q, ask_sz_top, 1e-9)
            drain_bid = max(-5, min(5, (last_bid_q - bid_sz_top)/denom_b))
            drain_ask = max(-5, min(5, (last_ask_q - ask_sz_top)/denom_a))
        last_bid_q, last_ask_q = bid_sz_top, ask_sz_top

        # queue-drain z-scores (robust: EWMA and MAD)
        drain_hist_bid.append(drain_bid)
        drain_hist_ask.append(drain_ask)
        drain_z_bid = drain_z_ask = 0.0
        drain_z_bid_ewma = drain_z_ask_ewma = 0.0
        drain_z_bid_mad = drain_z_ask_mad = 0.0
        def ewma(arr, alpha=0.2):
            ewma_val = arr[0] if len(arr) > 0 else 0.0
            for v in arr[1:]:
                ewma_val = alpha * v + (1 - alpha) * ewma_val
            return ewma_val
        def mad(arr):
            med = np.median(arr)
            return np.median(np.abs(arr - med)) if len(arr) > 0 else 1e-9
        arr_bid = np.array(drain_hist_bid)
        arr_ask = np.array(drain_hist_ask)
        if len(arr_bid) > 10:
            mean_ewma = ewma(arr_bid)
            var_ewma = ewma((arr_bid - mean_ewma) ** 2)
            std_ewma = math.sqrt(max(var_ewma, 1e-9))
            drain_z_bid_ewma = (drain_bid - mean_ewma) / std_ewma
            mad_bid = mad(arr_bid) or 1e-9
            drain_z_bid_mad = (drain_bid - np.median(arr_bid)) / mad_bid
            # Use plain z-score only when we have sufficient history (>= 30 samples)
            if len(arr_bid) >= 30:
                drain_z_bid = (drain_bid - np.mean(arr_bid)) / (np.std(arr_bid) or 1e-9)
            else:
                drain_z_bid = drain_z_bid_ewma  # Use EWMA version when sample size is small
        if len(arr_ask) > 10:
            mean_ewma = ewma(arr_ask)
            std_ewma = np.sqrt(ewma((arr_ask - mean_ewma) ** 2)) or 1e-9
            drain_z_ask_ewma = (drain_ask - mean_ewma) / std_ewma
            mad_ask = mad(arr_ask) or 1e-9
            drain_z_ask_mad = (drain_ask - np.median(arr_ask)) / mad_ask
            # Use plain z-score only when we have sufficient history (>= 30 samples)
            if len(arr_ask) >= 30:
                drain_z_ask = (drain_ask - np.mean(arr_ask)) / (np.std(arr_ask) or 1e-9)
            else:
                drain_z_ask = drain_z_ask_ewma  # Use EWMA version when sample size is small

        depth_convexity_sign = None
        if depth_convexity is not None:
            depth_convexity_sign = 1 if depth_convexity > 0 else -1 if depth_convexity < 0 else 0

        # instant impact estimate for first pct bucket (e.g., 0.001 = 10bp)
        impact_key = f"{int(IMPACT_PCTS[0]*10000):04d}"
        instant_impact = None
        if impact_key in liq_up and impact_key in liq_dn and mid:
            instant_impact = (liq_up[impact_key] + liq_dn[impact_key]) / (2*mid)

        # realised σ (pct)
        mid_prices.append((ts, mid)); prune(mid_prices, VOL_WINDOW_MS)
        sigma_pct = None
        if len(mid_prices) > 5:
            sigma_raw = statistics.pstdev(px for _, px in mid_prices)
            sigma_pct = (sigma_raw/mid)*100 if mid else None

        # trade stats window
        prune(trade_log, TRADE_WIN_MS)
        tps    = len(trade_log)/(TRADE_WIN_MS/1000)
        buy_sz = sum(sz for _, sd, sz in trade_log if sd=="buy")
        sell_sz= sum(sz for _, sd, sz in trade_log if sd=="sell")
        avg_sz = statistics.mean([sz for _, _, sz in trade_log]) if trade_log else 0.0
        bs_rat = (buy_sz/sell_sz) if sell_sz else None

        # rolling aggregates (5s / 15s / 60s) for OFI and TPS
        ofi_5s  = window_sum(ofi_hist, 5_000)
        ofi_15s = window_sum(ofi_hist, 15_000)
        ofi_60s = window_sum(ofi_hist, 60_000)

        tps_5s  = trade_count(trade_log, 5_000)/5
        tps_15s = trade_count(trade_log,15_000)/15
        # tps already computed over 60s = tps

        # normalized pressure ratios
        pressure_ratio_1bp = (cum_bid_tiers[0]/cum_ask_tiers[0]) if cum_ask_tiers[0] else None
        slope_ratio_io = (slope_in/slope_out) if slope_out else None

        # append to history buffer
        history_buffer.append({
            "mid": mid,   # restore old key name so summary works
            "imb": imbalance,
            "ofi": ofi_delta,
            "slope_in": slope_in,
            "slope_out": slope_out,
            "depth_convexity": depth_convexity,
        })

        # --- full depth profile -------------------------------------------------
        bid_heat = depth_profile(bids, mid, bin_bp=HEAT_BIN_BPS)
        ask_heat = depth_profile(asks, mid, bin_bp=HEAT_BIN_BPS)
        alpha_bid = curve_exponent(bid_heat) or None
        alpha_ask = curve_exponent(ask_heat) or None
        area_bid = sum(bid_heat)
        area_ask = sum(ask_heat)
        depth_skew_pct = 100 * (area_bid - area_ask) / (area_bid + area_ask + 1e-9)

        # maintain queue-drain time hist for rolling stats
        drain_time_hist_bid.append((ts, drain_bid)); prune(drain_time_hist_bid, 60_000)
        drain_time_hist_ask.append((ts, drain_ask)); prune(drain_time_hist_ask, 60_000)

        drain_mean_60s_bid = statistics.mean(v for _, v in drain_time_hist_bid) if drain_time_hist_bid else None
        drain_mean_60s_ask = statistics.mean(v for _, v in drain_time_hist_ask) if drain_time_hist_ask else None
        drain_std_60s_bid  = statistics.pstdev(v for _, v in drain_time_hist_bid) if len(drain_time_hist_bid) > 1 else None
        drain_std_60s_ask  = statistics.pstdev(v for _, v in drain_time_hist_ask) if len(drain_time_hist_ask) > 1 else None

        # assemble snapshot
        snap = {
            "timestamp": ts,
            "mid_price": mid,
            "best_bid": best_bid, "best_ask": best_ask, "spread": spread,
            "bid_ask_imbalance_pct": imbalance, "ofi_delta": ofi_delta,
            "micro_price": micro_px,
            f"cum_bid_vol_{int(BPS_LIQUIDITY)}bps": cum_bid_tiers[DEPTH_TIERS.index(BPS_LIQUIDITY) if BPS_LIQUIDITY in DEPTH_TIERS else min(range(len(DEPTH_TIERS)), key=lambda i: abs(DEPTH_TIERS[i]-BPS_LIQUIDITY))],
            f"cum_ask_vol_{int(BPS_LIQUIDITY)}bps": cum_ask_tiers[DEPTH_TIERS.index(BPS_LIQUIDITY) if BPS_LIQUIDITY in DEPTH_TIERS else min(range(len(DEPTH_TIERS)), key=lambda i: abs(DEPTH_TIERS[i]-BPS_LIQUIDITY))],
            "depth_slope_inner_3bp": slope_in, "depth_slope_outer_25bp": slope_out,
            "depth_convexity": depth_convexity,
            "depth_convexity_sign": depth_convexity_sign,
            "liquidity_cost_up": liq_up, "liquidity_cost_down": liq_dn,
            "max_order_size_up": max_size_up, "max_order_size_down": max_size_dn,
            "queue_drain_rate_bid": drain_bid, "queue_drain_rate_ask": drain_ask,
            "queue_drain_z_bid": drain_z_bid, "queue_drain_z_ask": drain_z_ask,
            "queue_drain_z_bid_ewma": drain_z_bid_ewma, "queue_drain_z_ask_ewma": drain_z_ask_ewma,
            "queue_drain_z_bid_mad": drain_z_bid_mad, "queue_drain_z_ask_mad": drain_z_ask_mad,
            "realised_sigma_30s_pct": sigma_pct,
            "instant_impact_pct": instant_impact,
            "trades_per_sec": tps, "avg_trade_size": avg_sz, "buy_sell_ratio": bs_rat,
            "tps_5s": tps_5s, "tps_15s": tps_15s,
            "ofi_sum_5s": ofi_5s, "ofi_sum_15s": ofi_15s, "ofi_sum_60s": ofi_60s,
            "pressure_ratio_1bp": pressure_ratio_1bp,
            "slope_ratio_io": slope_ratio_io,
            "drain_mean_60s_bid": drain_mean_60s_bid,
            "drain_mean_60s_ask": drain_mean_60s_ask,
            "drain_std_60s_bid": drain_std_60s_bid,
            "drain_std_60s_ask": drain_std_60s_ask,
            # New fields for LLM summary:
            "buy_volume_last_win": buy_sz,
            "sell_volume_last_win": sell_sz,
            "trade_win_sec": TRADE_WIN_MS // 1000,
        }

        # Track price changes for Kyle's Lambda calculation
        if len(mid_prices) >= 2:
            prev_mid = mid_prices[-2][1] if len(mid_prices) >= 2 else mid
            price_change = mid - prev_mid if prev_mid else 0
            price_change_hist.append((ts, price_change, avg_sz if avg_sz > 0 else 1.0))
            prune(price_change_hist, 300_000)  # 5 minutes of price changes
        
        # 1. Kyle's Lambda (Price Impact Coefficient)
        if len(price_change_hist) >= 20 and len(trade_log) >= 20:
            # Align price changes and trade volumes by synchronous pairs
            paired = list(zip(price_change_hist, trade_log))[-50:]  # last 50 synchronous steps
            price_changes = [abs(pc[1]) for pc, _ in paired]
            trade_volumes = [max(t[2], 1e-9) for _, t in paired]
            if len(price_changes) >= 20 and len(trade_volumes) >= 20:
                kyle_lambda = calculate_kyle_lambda(price_changes, trade_volumes)
                if kyle_lambda is not None:
                    kyle_lambda_hist.append((ts, kyle_lambda))
                    snap["kyle_lambda"] = kyle_lambda
                    # Kyle's Lambda interpretation
                    if kyle_lambda > 0.0001:
                        snap["kyle_lambda_regime"] = "high_impact"
                    elif kyle_lambda > 0.00005:
                        snap["kyle_lambda_regime"] = "medium_impact"
                    else:
                        snap["kyle_lambda_regime"] = "low_impact"
        
        # Always append spread to spread_hist (critical fix)
        spread_hist.append((ts, spread))
        prune(spread_hist, 600_000)  # keep last 10 min
        # 2. Roll's Spread Decomposition
        if len(mid_prices) >= 50 and len(trade_log) >= 50 and len(spread_hist) >= 50:
            spreads = [s[1] for s in list(spread_hist)[-50:] if s and len(s) >= 2]
            price_changes = [pc[1] for pc in list(price_change_hist)[-50:] if pc and len(pc) >= 2]
            trade_directions = [t[1] for t in list(trade_log)[-50:] if t and len(t) >= 2]
            
            # Ensure we have sufficient valid data
            if len(spreads) >= 20 and len(price_changes) >= 20 and len(trade_directions) >= 20:
                adverse_selection, order_processing, adverse_pct = decompose_roll_spread(
                    spreads, price_changes, trade_directions
                )
                
                if adverse_selection is not None:
                    snap["roll_adverse_selection"] = adverse_selection
                    snap["roll_order_processing"] = order_processing
                    snap["roll_adverse_pct"] = adverse_pct
                    
                    # Roll's decomposition interpretation
                    if adverse_pct > 70:
                        snap["roll_regime"] = "high_adverse_selection"
                    elif adverse_pct > 40:
                        snap["roll_regime"] = "balanced_spread"
                    else:
                        snap["roll_regime"] = "inventory_driven"
        
        # 3. Liquidity Provider Behavior Detection
        lp_behavior = detect_liquidity_provider_behavior(
            {"bids": bids, "asks": asks}, 
            prev_book, 
            quote_refresh_hist
        )
        
        if lp_behavior:
            snap["lp_quote_refresh_rate"] = lp_behavior["quote_refresh_rate"]
            snap["lp_market_maker_activity"] = lp_behavior["market_maker_activity"]
            snap["lp_withdrawal_risk"] = lp_behavior["withdrawal_risk"]
            snap["lp_quote_stuffing"] = lp_behavior["quote_stuffing_indicator"]
            
            # Liquidity provider regime detection
            if lp_behavior["withdrawal_risk"] > 50:
                snap["lp_regime"] = "liquidity_withdrawal"
            elif lp_behavior["market_maker_activity"] > 60:
                snap["lp_regime"] = "active_market_making"
            elif lp_behavior["quote_stuffing_indicator"] > 30:
                snap["lp_regime"] = "quote_stuffing_detected"
            else:
                snap["lp_regime"] = "normal_liquidity"
        
        # 4. Market Maker Inventory Estimation
        mm_inventory = estimate_market_maker_inventory(
            list(trade_log), 
            [ofi_5s, ofi_15s, ofi_60s], 
            [spread]
        )
        
        if mm_inventory:
            snap["mm_estimated_inventory"] = mm_inventory["estimated_inventory"]
            snap["mm_inventory_pressure"] = mm_inventory["inventory_pressure"]
            snap["mm_optimal_skew"] = mm_inventory["optimal_skew"]
            snap["mm_inventory_risk"] = mm_inventory["inventory_risk"]
            snap["mm_reversion_probability"] = mm_inventory["reversion_probability"]
            
            # Market maker inventory regime
            if mm_inventory["inventory_risk"] > 60:
                snap["mm_regime"] = "high_inventory_risk"
            elif mm_inventory["reversion_probability"] > 60:
                snap["mm_regime"] = "reversion_likely"
            elif abs(mm_inventory["optimal_skew"]) > 20:
                snap["mm_regime"] = "significant_skew_needed"
            else:
                snap["mm_regime"] = "balanced_inventory"
        
        # Store institutional metrics history for trend analysis
        mm_inventory_hist.append((ts, mm_inventory.get("estimated_inventory", 0)))
        prune(mm_inventory_hist, 600_000)  # 10 minutes of inventory history
        
        # ========== END INSTITUTIONAL ENHANCEMENTS ==========

        if vpin_components:
            snap[f"vpin_{VPIN_NUM_BUCKETS}buckets"] = sum(vpin_components)/len(vpin_components)

        # Add reduced payload - remove large arrays from stream to reduce Redis memory
        # Keep depth_heat_* only in LLM summary, not in the main stream payload
        snap.update({
            "depth_curve_alpha_bid": alpha_bid,
            "depth_curve_alpha_ask": alpha_ask,
            "depth_skew_pct": depth_skew_pct,
            "depth_heat_bid": bid_heat,
            "depth_heat_ask": ask_heat,
        })


        # Backfill guard: drop duplicate timestamps
        global last_ts
        if 'last_ts' not in globals():
            last_ts = None
        if last_ts == ts:
            ts += 1  # guarantee monotonic stream id
        snap["timestamp"] = ts
        last_ts = ts

        # Compressed JSON and direct Redis write (sync)
        payload = json.dumps(snap, separators=(",", ":"))
        redis_cli.xadd(REDIS_STREAM, {"data": payload}, maxlen=MAXLEN_STREAM, approximate=True)
        print(f"[OB] {ts} pushed")

async def stream_trades(exchange):
    global bucket_vol, bucket_abs
    while True:
        trades = await exchange.watch_trades(BYBIT_MARKET)
        for t in trades:
            ts = t.get("timestamp") or int(time.time()*1000)
            side = t["side"].lower()          # ← normalise
            amount = t["amount"]
            trade_log.append((ts, side, amount))

            signed = amount if side=="buy" else -amount
            bucket_vol += signed
            bucket_abs += abs(signed)

            while bucket_abs >= VPIN_BUCKET_SIZE:
                ratio = abs(bucket_vol) / VPIN_BUCKET_SIZE  
                vpin_components.append(ratio)
                bucket_vol = 0.0  
                bucket_abs -= VPIN_BUCKET_SIZE  # carry remainder forward
                # Add z-score calculation and 5-min EWMA, export to snapshot
                vpin_z = None
                vpin_ewma_5m = None
                if len(vpin_components) >= 10:
                    vpin_arr = np.array(vpin_components)
                    vpin_z = (ratio - vpin_arr.mean()) / (vpin_arr.std() + 1e-9)
                    # 5-min EWMA (use α for ~5-min window)
                    alpha = 1 - np.exp(-1/(60/VPIN_BUCKET_SIZE*5))  # rough approx for 5min with bucket size
                    ewma = vpin_arr[0]
                    for v in vpin_arr[1:]:
                        ewma = alpha * v + (1 - alpha) * ewma
                    vpin_ewma_5m = ewma
                # Export vpin_z and vpin_ewma_5m to Redis snapshot if available
                if 'snap' in locals():
                    snap["vpin_z"] = vpin_z
                    snap["vpin_ewma_5m"] = vpin_ewma_5m

# ---------------- resilience wrapper ----------------
async def run_with_backoff(coro, *args):
    backoff = 1
    while True:
        try:
            await coro(*args)
        except Exception as e:
            print(f"[WARN] {coro.__name__} error: {e}. Retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, MAX_BACKOFF)
        else:
            backoff = 1

async def main():
    while True:
        try:
            exchange = ccxt.pro.bybit({
                "enableRateLimit": True,
                "options": {"defaultType": "linear"}    # linear USDT perps
            })
            print(f"[Collector] Connected – running for {SYMBOL}")
            await asyncio.gather(
                run_with_backoff(stream_orderbook, exchange),
                run_with_backoff(stream_trades,   exchange)
            )
        except Exception as e:
            print(f"[CRIT] Loop error: {e}. Restarting in 5 s")
            try: 
                if 'exchange' in locals() and hasattr(exchange, 'close'):  
                    await exchange.close()
            except Exception: 
                pass
            await asyncio.sleep(5)

# --- LLM-friendly Orderbook Summary ---
def fmt(val, n=2):
    try:
        import numpy as np
        if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
            return None  # PATCH: Use None for valid JSON
        return f"{float(val):.{n}f}"
    except Exception:
        return None  # PATCH: Use None for valid JSON

def summarize_orderbook_snapshot(snap, extra=None):
    """
    Enhanced LLM-friendly orderbook summary with contextual, comparative, and regime-aware phrasing.
    Highlights key microstructure signals and flags notable states.
    Now includes:
    - Recent Order Flow: Qualitative summary of buy/sell dominance
    - Microprice Skew: Qualitative description if microprice deviates from mid-price
    - Depth Beyond Top Levels: Qualitative description of liquidity beyond top 5 levels
    """
    import numpy as np
    lines = [f"**Orderbook Microstructure Summary (ts={snap.get('timestamp')})**:"]

    # --- Spread Context ---
    spread = snap.get('spread')
    mid_price = snap.get('mid_price')
    spread_flag = ""
    if spread is not None and mid_price:
        try:
            spread_val = float(spread)
            mid_val = float(mid_price)
            spread_pct = spread_val / mid_val * 100 if mid_val > 0 else 0
            if spread_pct < 0.01:
                spread_flag = "(tight spread; high liquidity)"
            elif spread_pct > 0.05:
                spread_flag = "(wide spread; low liquidity)"
            else:
                spread_flag = "(normal range)"
        except Exception:
            spread_flag = ""
    lines.append(f"- Mid price: {fmt(mid_price)}, Spread: {fmt(spread)} ({fmt(spread_pct, 4)}%) {spread_flag}")  

    # --- Imbalance & OFI Context ---
    imbalance = snap.get('bid_ask_imbalance_pct')
    ofi = snap.get('ofi_delta')
    imbalance_flag = ""
    try:
        imb_val = float(imbalance) if imbalance is not None else 0
        if abs(imb_val) < 1:
            imbalance_flag = "(neutral)"  # PATCH: Label as neutral if <1%
        elif imb_val > 20:
            imbalance_flag = "(aggressive bid-side imbalance; buy pressure)"
        elif imb_val < -20:
            imbalance_flag = "(aggressive ask-side imbalance; sell pressure)"
        elif abs(imb_val) > 10:
            imbalance_flag = "(moderate imbalance)"
        elif imb_val > 0:
            imbalance_flag = "(more bids than asks; net buy bias)"
        elif imb_val < 0:
            imbalance_flag = "(more asks than bids; net sell bias)"
    except Exception:
        pass
    ofi_flag = ""
    try:
        ofi_val = float(ofi) if ofi is not None else 0
        if abs(ofi_val) > 100:
            ofi_flag = "(large OFI shift)"
        elif ofi_val > 0:
            ofi_flag = "(net buying pressure in order flow)"
        elif ofi_val < 0:
            ofi_flag = "(net selling pressure in order flow)"
    except Exception:
        pass
    lines.append(f"- Bid/Ask imbalance: {fmt(imbalance)}% {imbalance_flag} | OFI delta: {fmt(ofi)} {ofi_flag}")

    # --- Recent Order Flow (Buy/Sell Dominance) ---
    buy_sz = snap.get('buy_volume_last_win')
    sell_sz = snap.get('sell_volume_last_win')
    trade_win_sec = snap.get('trade_win_sec', 60)
    if buy_sz is not None and sell_sz is not None:
        try:
            buy_sz = float(buy_sz)
            sell_sz = float(sell_sz)
            if buy_sz > 2 * sell_sz and sell_sz > 0:
                flow_line = f"Aggressive buys dominating recent tape (buy volume {fmt(buy_sz)} is {buy_sz/sell_sz:.1f}x sell volume in last {trade_win_sec}s)"
            elif sell_sz > 2 * buy_sz and buy_sz > 0:
                flow_line = f"Aggressive sells dominating recent tape (sell volume {fmt(sell_sz)} is {sell_sz/buy_sz:.1f}x buy volume in last {trade_win_sec}s)"
            elif buy_sz > sell_sz:
                flow_line = f"Buyers outpacing sellers (buy volume {fmt(buy_sz)} vs sell volume {fmt(sell_sz)} in last {trade_win_sec}s)"
            elif sell_sz > buy_sz:
                flow_line = f"Sellers outpacing buyers (sell volume {fmt(sell_sz)} vs buy volume {fmt(buy_sz)} in last {trade_win_sec}s)"
            else:
                flow_line = f"Buy/sell volume balanced in last {trade_win_sec}s"
            lines.append(f"- Recent Order Flow: {flow_line}")
        except Exception:
            pass

    # --- Microprice Skew ---
    micro_price = snap.get('micro_price')
    mid_price = snap.get('mid_price')
    if micro_price is not None and mid_price is not None:
        try:
            micro_price = float(micro_price)
            mid_price = float(mid_price)
            diff_pct = 100 * (micro_price - mid_price) / mid_price if mid_price else 0
            if abs(diff_pct) > 0.05:
                skew = "bullish skew" if diff_pct > 0 else "bearish skew"
                lines.append(f"- Microprice is {fmt(diff_pct, 3)}% {'above' if diff_pct > 0 else 'below'} mid ({skew})")
        except Exception:
            pass

    # --- Depth/Liquidity Context ---
    cum_bid = snap.get(f'cum_bid_vol_{int(BPS_LIQUIDITY)}bps')
    cum_ask = snap.get(f'cum_ask_vol_{int(BPS_LIQUIDITY)}bps')
    liquidity_flag = ""
    try:
        bid_val = float(cum_bid) if cum_bid is not None else 0
        ask_val = float(cum_ask) if cum_ask is not None else 0
        if bid_val < 100 or ask_val < 100:
            liquidity_flag = "(shallow top-of-book liquidity)"
        elif bid_val > 500 and ask_val > 500:
            liquidity_flag = "(deep liquidity)"
    except Exception:
        pass
    lines.append(f"- Cum. bid/ask vol ({BPS_LIQUIDITY}bps): {fmt(cum_bid)} / {fmt(cum_ask)} {liquidity_flag}")

    # --- Depth Beyond Top Levels ---
    depth_heat_bid = extra.get('depth_heat_bid')
    depth_heat_ask = extra.get('depth_heat_ask')
    if depth_heat_bid and depth_heat_ask and isinstance(depth_heat_bid, list) and isinstance(depth_heat_ask, list):
        try:
            # Top 5 levels vs rest
            sum_top5_bid = sum(depth_heat_bid[:5])
            sum_rest_bid = sum(depth_heat_bid[5:])
            sum_top5_ask = sum(depth_heat_ask[:5])
            sum_rest_ask = sum(depth_heat_ask[5:])
            if sum_rest_bid < 0.5 * sum_top5_bid or sum_rest_ask < 0.5 * sum_top5_ask:
                lines.append("- Depth is thin beyond top 5 levels (liquidity somewhat low deeper in the book)")
            elif sum_rest_bid > sum_top5_bid and sum_rest_ask > sum_top5_ask:
                lines.append("- Depth remains strong beyond top 5 levels (ample liquidity at depth)")
        except Exception:
            pass

    # --- Volatility Context ---
    sigma = snap.get('realised_sigma_30s_pct')
    sigma_flag = ""
    try:
        sigma_val = float(sigma) if sigma is not None else 0
        if sigma_val > 0.2:
            sigma_flag = "(volatility spike)"
        elif sigma_val < 0.05:
            sigma_flag = "(very calm)"
    except Exception:
        pass
    lines.append(f"- Realised sigma 30s: {fmt(sigma)}% {sigma_flag}")

    # --- Queue Drain Context ---
    drain_bid = snap.get('queue_drain_rate_bid')
    drain_ask = snap.get('queue_drain_rate_ask')
    drain_flag = ""
    try:
        db = float(drain_bid) if drain_bid is not None else 0
        da = float(drain_ask) if drain_ask is not None else 0
        if db < -2:
            drain_flag = "(bids draining rapidly)"
        elif da < -2:
            drain_flag = "(asks draining rapidly)"
        elif db > 2:
            drain_flag = "(bids refilling aggressively)"
        elif da > 2:
            drain_flag = "(asks refilling aggressively)"
    except Exception:
        pass
    lines.append(f"- Queue drain rate (bid/ask): {fmt(drain_bid)} / {fmt(drain_ask)} {drain_flag}")

    # --- VPIN Context ---
    vpin_key = f'vpin_{VPIN_NUM_BUCKETS}buckets'
    vpin = snap.get(vpin_key)
    vpin_flag = ""
    vpin_z = None
    try:
        vpin_val = float(vpin) if vpin is not None else 0
        # PATCH: Compute z-score or percentile if possible
        vpin_hist = [float(x) for x in list(vpin_components)] if 'vpin_components' in globals() else []
        if vpin_hist and len(vpin_hist) > 5:
            import numpy as np
            vpin_z = (vpin_val - np.mean(vpin_hist)) / (np.std(vpin_hist) or 1e-9)
            vpin_percentile = int(100 * (np.sum(np.array(vpin_hist) <= vpin_val) / len(vpin_hist)))
            vpin_flag += f"(z={fmt(vpin_z,2)}, pct={vpin_percentile}) "
        if vpin_val > 0.7:
            vpin_flag += "(toxic flow; high order flow toxicity)"
        elif vpin_val < 0.3:
            vpin_flag += "(benign flow)"
    except Exception:
        pass
    lines.append(f"- VPIN: {fmt(vpin)} {vpin_flag}")

    # --- Microstructure Regime Flag ---
    regime = None
    try:
        if spread_flag == "(tight spread; high liquidity)" and liquidity_flag == "(deep liquidity)" and sigma_flag == "(very calm)":
            regime = "**Stable, liquid market** (favorable for large trades)"
        elif spread_flag == "(wide spread; low liquidity)" or liquidity_flag == "(shallow top-of-book liquidity)":
            regime = "**Fragile market** (beware of slippage)"
        elif sigma_flag == "(volatility spike)":
            regime = "**High volatility regime** (price discovery, increased risk)"
        elif vpin_flag == "(toxic flow; high order flow toxicity)":
            regime = "**Toxic order flow** (market makers may widen spreads)"
    except Exception:
        pass
    if regime:
        lines.append(f"- Microstructure Regime: {regime}")

    # --- Institutional Microstructure Analysis ---
    kyle_lambda = snap.get('kyle_lambda')
    if kyle_lambda is not None:
        kyle_regime = snap.get('kyle_lambda_regime', 'unknown')
        kyle_flag = ""
        if kyle_regime == "high_impact":
            kyle_flag = "(high price impact; institutional-size trades move market significantly)"
        elif kyle_regime == "medium_impact":
            kyle_flag = "(moderate price impact; proceed with caution on large orders)"
        else:
            kyle_flag = "(low price impact; market can absorb large orders efficiently)"
        lines.append(f"- Kyle's Lambda: {fmt(kyle_lambda, 6)} {kyle_flag}")
    
    roll_adverse = snap.get('roll_adverse_selection')
    roll_processing = snap.get('roll_order_processing')
    roll_adverse_pct = snap.get('roll_adverse_pct')
    if roll_adverse is not None and roll_processing is not None:
        roll_regime = snap.get('roll_regime', 'unknown')
        roll_flag = ""
        if roll_regime == "high_adverse_selection":
            roll_flag = "(high informed trading; market makers face significant adverse selection)"
        elif roll_regime == "balanced_spread":
            roll_flag = "(balanced spread components; normal market maker operations)"
        else:
            roll_flag = "(inventory-driven spreads; market makers managing position risk)"
        
        lines.append(f"- Roll's Spread: Adverse={fmt(roll_adverse, 4)}, Processing={fmt(roll_processing, 4)} ({fmt(roll_adverse_pct, 1)}% adverse) {roll_flag}")
    
    lp_activity = snap.get('lp_market_maker_activity')
    lp_withdrawal = snap.get('lp_withdrawal_risk')
    lp_stuffing = snap.get('lp_quote_stuffing')
    lp_regime = snap.get('lp_regime')
    
    if lp_activity is not None:
        lp_flag = ""
        if lp_regime == "liquidity_withdrawal":
            lp_flag = "(WARNING: liquidity withdrawal detected; expect wider spreads)"
        elif lp_regime == "active_market_making":
            lp_flag = "(active market making; healthy liquidity provision)"
        elif lp_regime == "quote_stuffing_detected":
            lp_flag = "(WARNING: quote stuffing detected; potential market manipulation)"
        else:
            lp_flag = "(normal liquidity provider behavior)"
        
        lines.append(f"- LP Behavior: Activity={fmt(lp_activity, 0)}, Withdrawal Risk={fmt(lp_withdrawal, 0)}, Quote Stuffing={fmt(lp_stuffing, 0)} {lp_flag}")
    
    mm_inventory = snap.get('mm_estimated_inventory')
    mm_pressure = snap.get('mm_inventory_pressure')
    mm_skew = snap.get('mm_optimal_skew')
    mm_risk = snap.get('mm_inventory_risk')
    mm_reversion = snap.get('mm_reversion_probability')
    mm_regime = snap.get('mm_regime')
    
    if mm_inventory is not None:
        mm_flag = ""
        if mm_regime == "high_inventory_risk":
            mm_flag = "(WARNING: high MM inventory risk; expect price pressure for risk reduction)"
        elif mm_regime == "reversion_likely":
            mm_flag = "(mean reversion likely; MM inventory needs unwinding)"
        elif mm_regime == "significant_skew_needed":
            mm_flag = "(MM should adjust quote skew to manage inventory risk)"
        else:
            mm_flag = "(balanced MM inventory; stable quote provision expected)"
        
        lines.append(f"- MM Inventory: Position={fmt(mm_inventory, 2)}, Pressure={fmt(mm_pressure, 0)}%, Optimal Skew={fmt(mm_skew, 1)}%, Reversion Prob={fmt(mm_reversion, 0)}% {mm_flag}")
    
    execution_guidance = []
    
    # Combine institutional signals for execution advice
    if kyle_lambda and float(kyle_lambda) > 0.0001:
        execution_guidance.append("High price impact detected")
    if lp_regime == "liquidity_withdrawal":
        execution_guidance.append("Liquidity withdrawal risk")
    if mm_regime in ["high_inventory_risk", "reversion_likely"]:
        execution_guidance.append("MM inventory pressure")
    if roll_adverse_pct and float(roll_adverse_pct) > 70:
        execution_guidance.append("High adverse selection")
    
    if execution_guidance:
        guidance_text = " | ".join(execution_guidance)
        lines.append(f"- [INSTITUTIONAL] Execution Alert: {guidance_text} -> Consider breaking large orders, using iceberg strategies, or waiting for better conditions")
    else:
        lines.append(f"- [INSTITUTIONAL] Execution Status: Favorable conditions for large order execution")
    
    # --- Key Reference Metrics (brief, for LLM context) ---
    lines.append(f"- Best bid: {fmt(snap.get('best_bid'))}, Best ask: {fmt(snap.get('best_ask'))}")
    lines.append(f"- Micro price: {fmt(snap.get('micro_price'))}")
    lines.append(f"- Depth slope (inner 3bp/outer 25bp): {fmt(snap.get('depth_slope_inner_3bp'))} / {fmt(snap.get('depth_slope_outer_25bp'))}")
    lines.append(f"- Depth convexity: {fmt(snap.get('depth_convexity'))} (sign: {snap.get('depth_convexity_sign')})")
    lines.append(f"- Liquidity cost up/down (10bp): {fmt(snap.get('liquidity_cost_up',{}).get('0010'))} / {fmt(snap.get('liquidity_cost_down',{}).get('0010'))}")
    lines.append(f"- Max order size up/down (10bp): {fmt(snap.get('max_order_size_up',{}).get('0010'))} / {fmt(snap.get('max_order_size_down',{}).get('0010'))}")
    lines.append(f"- Queue drain z-score (bid/ask): {fmt(snap.get('queue_drain_z_bid'))} / {fmt(snap.get('queue_drain_z_ask'))} | EWMA: {fmt(snap.get('queue_drain_z_bid_ewma'))} / {fmt(snap.get('queue_drain_z_ask_ewma'))} | MAD: {fmt(snap.get('queue_drain_z_bid_mad'))} / {fmt(snap.get('queue_drain_z_ask_mad'))}")
    lines.append(f"- Drain mean 60s (bid/ask): {fmt(snap.get('drain_mean_60s_bid'))} / {fmt(snap.get('drain_mean_60s_ask'))}")
    lines.append(f"- Drain std 60s (bid/ask): {fmt(snap.get('drain_std_60s_bid'))} / {fmt(snap.get('drain_std_60s_ask'))}")
    lines.append(f"- Trades/sec: {fmt(snap.get('trades_per_sec'))}, Avg trade size: {fmt(snap.get('avg_trade_size'))}, Buy/Sell ratio: {fmt(snap.get('buy_sell_ratio'))}")
    lines.append(f"- TPS (5s/15s): {fmt(snap.get('tps_5s'))} / {fmt(snap.get('tps_15s'))}")
    lines.append(f"- OFI sum (5s/15s/60s): {fmt(snap.get('ofi_sum_5s'))} / {fmt(snap.get('ofi_sum_15s'))} / {fmt(snap.get('ofi_sum_60s'))}")
    lines.append(f"- Pressure ratio 1bp: {fmt(snap.get('pressure_ratio_1bp'))}, Slope ratio (in/out): {fmt(snap.get('slope_ratio_io'))}")
    # Depth heat/skew
    lines.append(f"- Depth heat skew: {fmt(snap.get('depth_skew_pct'))}% | Depth alpha (bid/ask): {fmt(snap.get('depth_curve_alpha_bid'))} / {fmt(snap.get('depth_curve_alpha_ask'))}")
    # Profile arrays (summarize)
    if 'depth_heat_bid' in extra and isinstance(extra['depth_heat_bid'], list):
        lines.append(f"- Depth heat bid (max/min/sum): {fmt(np.max(extra['depth_heat_bid']))} / {fmt(np.min(extra['depth_heat_bid']))} / {fmt(np.sum(extra['depth_heat_bid']))}")
    if 'depth_heat_ask' in extra and isinstance(extra['depth_heat_ask'], list):
        lines.append(f"- Depth heat ask (max/min/sum): {fmt(np.max(extra['depth_heat_ask']))} / {fmt(np.min(extra['depth_heat_ask']))} / {fmt(np.sum(extra['depth_heat_ask']))}")
    # Historical context (last 5 snapshots, only most predictive features summarized)
    history = snap.get('history', [])
    predictive_keys = ['imb', 'ofi', 'mid', 'slope_in', 'slope_out', 'depth_convexity', 'avg_trade_size', 'trades_per_sec']
    if history and isinstance(history, list) and len(history) > 2:
        lines.append("\nRecent History (key features):")
        def hist_trend(arr, name):
            arr = [v for v in arr if v is not None]
            if len(arr) < 2:
                return "flat"
            diff = arr[-1] - arr[0]
            if abs(diff) < 1e-8:
                return "flat"
            return "rising" if diff > 0 else "falling"
        last_hist = history[-5:]
        for key in predictive_keys:
            if key in last_hist[0]:
                try:
                    vals = [h.get(key) for h in last_hist]
                    trend = hist_trend(vals, key)
                    lines.append(f"- {key}: {fmt(vals[0])} → {fmt(vals[-1])} ({trend})")
                except Exception:
                    continue
    # All available fields are now contextually summarized above. No raw/extra fields will be output.
    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(main())
