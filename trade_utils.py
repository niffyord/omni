import os
from dotenv import load_dotenv
load_dotenv()
import logging
import ccxt
import pandas as pd
import talib
import numpy as np
from typing import List

def fetch_ohlcv(symbol: str, timeframe: str, limit: int) -> list:
    if limit is None:
        limit = 100
    exchange_config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'linear'},
        'recvWindow': 60000,  # Increased recvWindow to 60 seconds
    }
    # REMOVE PROXY USAGE: Always connect directly for reliability
    # (No USE_PROXY logic)
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    if api_key and api_secret:
        exchange_config['apiKey'] = api_key
        exchange_config['secret'] = api_secret
    exchange = ccxt.bybit(exchange_config)
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit, params={"recvWindow": 60000})
    return ohlcv

def compute_indicators(ohlcv: List[List[float]]) -> dict:
    df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
    close = df['close'].astype(float)
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    volume = df['volume'].astype(float)
    # EMA
    ema_arr = talib.EMA(close, timeperiod=21)
    ema = ema_arr.iat[-1]
    # SMA
    sma_arr = talib.SMA(close, timeperiod=21)
    sma = sma_arr.iat[-1]
    # RSI
    rsi_arr = talib.RSI(close, timeperiod=14)
    rsi = rsi_arr.iat[-1]
    # ATR
    atr_arr = talib.ATR(high, low, close, timeperiod=14)
    atr = atr_arr.iat[-1]
    # MACD
    macd, macdsignal, macdhist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    macd_val = macd.iat[-1]
    macdsignal_val = macdsignal.iat[-1]
    macdhist_val = macdhist.iat[-1]
    # Bollinger Bands
    upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    bb_upper = upper.iat[-1]
    bb_middle = middle.iat[-1]
    bb_lower = lower.iat[-1]
    # Stochastic Oscillator
    slowk, slowd = talib.STOCH(high, low, close)
    stoch_k = slowk.iat[-1]
    stoch_d = slowd.iat[-1]
    # ADX
    adx_arr = talib.ADX(high, low, close, timeperiod=14)
    adx = adx_arr.iat[-1]
    # CCI
    cci_arr = talib.CCI(high, low, close, timeperiod=20)
    cci = cci_arr.iat[-1]
    # OBV
    obv_arr = talib.OBV(close, volume)
    obv = obv_arr.iat[-1]
    # VWAP (session: all bars)
    typical_price = (high + low + close) / 3
    vwap = (typical_price * volume).sum() / volume.sum() if volume.sum() != 0 else np.nan
    # Simple momentum logic: price above EMA and RSI > 55 = bullish, below/RSI < 45 = bearish
    if close.iat[-1] > ema and rsi > 55:
        momentum = 'bullish'
    elif close.iat[-1] < ema and rsi < 45:
        momentum = 'bearish'
    else:
        momentum = 'neutral'

    # Fast/Slow EMA for cross
    ema_fast_arr = talib.EMA(close, timeperiod=8)
    ema_slow_arr = talib.EMA(close, timeperiod=21)
    ema_fast = ema_fast_arr.iat[-1]
    ema_slow = ema_slow_arr.iat[-1]
    if close.size > 1:
        ema_fast_prev = ema_fast_arr.iat[-2]
        ema_slow_prev = ema_slow_arr.iat[-2]
        if ema_fast_prev < ema_slow_prev and ema_fast > ema_slow:
            ema_cross = "bullish"
        elif ema_fast_prev > ema_slow_prev and ema_fast < ema_slow:
            ema_cross = "bearish"
        else:
            ema_cross = "none"
    else:
        ema_cross = "none"

    # Swing high/low (simple: last N bars)
    N = 10
    swing_high = high.rolling(N).max().iat[-1]
    swing_low = low.rolling(N).min().iat[-1]

    # Volume context
    volume_avg_20 = volume.rolling(20).mean().iat[-1]
    volume_spike = float(volume.iat[-1] / volume_avg_20) if volume_avg_20 != 0 else np.nan

    # ATR percent
    atr_pct = float(atr / close.iat[-1] * 100) if close.iat[-1] != 0 else np.nan

    # BB width
    bb_width = float(bb_upper - bb_lower)

    # OBV/ADX slopes
    obv_hist = obv_arr
    obv_slope = float(obv_hist.iat[-1] - obv_hist.iat[-2]) if obv_hist.size > 1 else np.nan
    adx_hist = adx_arr
    adx_slope = float(adx_hist.iat[-1] - adx_hist.iat[-2]) if adx_hist.size > 1 else np.nan

    # Advanced candle pattern detection using TA-Lib
    pattern_funcs = {
        'engulfing': talib.CDLENGULFING,
        'hammer': talib.CDLHAMMER,
        'doji': talib.CDLDOJI,
        'morning_star': talib.CDLMORNINGSTAR,
        'evening_star': talib.CDLEVENINGSTAR,
        'harami': talib.CDLHARAMI,
        'shooting_star': talib.CDLSHOOTINGSTAR,
        'three_white_soldiers': talib.CDL3WHITESOLDIERS,
        'three_black_crows': talib.CDL3BLACKCROWS,
        'three_inside': talib.CDL3INSIDE
    }
    detected_patterns = []
    for name, func in pattern_funcs.items():
        try:
            val = func(df['open'], high, low, close).iat[-1]
            if val > 0:
                detected_patterns.append(f"bullish_{name}")
            elif val < 0:
                detected_patterns.append(f"bearish_{name}")
        except Exception:
            continue
    candle_pattern = detected_patterns if detected_patterns else None

    # Price near BB upper
    near_bb_upper = abs(close.iat[-1] - bb_upper) < 0.1 * atr

    return {
        'ema': float(ema),
        'sma': float(sma),
        'ema_fast': float(ema_fast),
        'ema_slow': float(ema_slow),
        'ema_cross': ema_cross,
        'rsi': float(rsi),
        'atr': float(atr),
        'atr_pct': atr_pct,
        'macd': float(macd_val),
        'macdsignal': float(macdsignal_val),
        'macdhist': float(macdhist_val),
        'bb_upper': float(bb_upper),
        'bb_middle': float(bb_middle),
        'bb_lower': float(bb_lower),
        'bb_width': bb_width,
        'stoch_k': float(stoch_k),
        'stoch_d': float(stoch_d),
        'adx': float(adx),
        'adx_slope': adx_slope,
        'cci': float(cci),
        'obv': float(obv),
        'obv_slope': obv_slope,
        'vwap': float(vwap),
        'momentum': momentum,
        'last_close': float(close.iat[-1]),
        'swing_high': float(swing_high),
        'swing_low': float(swing_low),
        'volume_avg_20': float(volume_avg_20),
        'volume_spike': float(volume_spike),
        'timestamp': int(df['ts'].iat[-1]),
        'candle_pattern': candle_pattern,
        'near_bb_upper': bool(near_bb_upper)
    }
