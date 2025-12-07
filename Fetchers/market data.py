#!/usr/bin/env python3
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
import pandas as pd
import numpy as np
import ta
import MetaTrader5 as mt5

INSTRUMENTS = {
    "XAUUSD": "Gold spot",
    "USA500.IDX": "S&P500 index CFD",
    "VOL.IDX": "Volatility Index (VIX CFD)",
    "DOLLAR.IDX": "Dollar Index (DXY CFD)"
}

def round_to_2_decimals(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float, np.number)):
        return round(float(value), 2)
    return value

def initialize_mt5() -> bool:
    if not mt5.initialize():
        print(f"MT5 initialization failed: {mt5.last_error()}")
        return False
    return True

def fetch_weekly(symbol: str) -> Optional[Dict[str, Any]]:
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_W1, 1, 1)
    if rates is None or len(rates) == 0:
        return None
    candle = rates[0]
    close_time = datetime.fromtimestamp(candle['time'])
    return {
        "high": round_to_2_decimals(candle['high']),
        "low": round_to_2_decimals(candle['low']),
        "time": close_time.isoformat()
    }

def fetch_daily(symbol: str) -> Optional[Dict[str, Any]]:
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_D1, 1, 1)
    if rates is None or len(rates) == 0:
        return None
    candle = rates[0]
    close_time = datetime.fromtimestamp(candle['time'])
    return {
        "high": round_to_2_decimals(candle['high']),
        "low": round_to_2_decimals(candle['low']),
        "time": close_time.isoformat()
    }

def fetch_hourly(symbol: str) -> Optional[Dict[str, Any]]:
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 1, 6)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    start_time = datetime.fromtimestamp(df['time'].iloc[0])
    end_time = datetime.fromtimestamp(df['time'].iloc[-1])
    return {
        "high": round_to_2_decimals(df['high'].max()),
        "low": round_to_2_decimals(df['low'].min()),
        "time": end_time.isoformat()
    }

def fetch_hourly_for_indicators(symbol: str, bars: int = 300) -> Optional[pd.DataFrame]:
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_H1, 0, bars)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df

def get_current_price(symbol: str) -> Optional[float]:
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    return round_to_2_decimals(tick.bid)

def compute_indicators(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 200:
        return None
    
    close = df['close']
    high = df['high']
    low = df['low']
    
    rsi_indicator = ta.momentum.RSIIndicator(close, window=20)
    rsi_value = rsi_indicator.rsi().iloc[-1]
    
    if rsi_value >= 70:
        rsi_status = "Overbought"
    elif rsi_value <= 30:
        rsi_status = "Oversold"
    else:
        rsi_status = "Neutral"
    
    ema50_indicator = ta.trend.EMAIndicator(close, window=50)
    ema200_indicator = ta.trend.EMAIndicator(close, window=200)
    
    ema50_series = ema50_indicator.ema_indicator()
    ema200_series = ema200_indicator.ema_indicator()
    
    ema50_current = ema50_series.iloc[-1]
    ema200_current = ema200_series.iloc[-1]
    ema50_prev = ema50_series.iloc[-2]
    ema200_prev = ema200_series.iloc[-2]
    
    if ema50_current > ema200_current:
        ema_trend = "Bullish"
    else:
        ema_trend = "Bearish"
    
    if ema50_current > ema200_current and ema50_prev <= ema200_prev:
        ema_crossover = "Bullish Crossover"
    elif ema50_current < ema200_current and ema50_prev >= ema200_prev:
        ema_crossover = "Bearish Crossover"
    else:
        ema_crossover = "No Crossover"
    
    macd_indicator = ta.trend.MACD(close, window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_indicator.macd()
    signal_line = macd_indicator.macd_signal()
    histogram = macd_indicator.macd_diff()
    
    macd_current = macd_line.iloc[-1]
    signal_current = signal_line.iloc[-1]
    histogram_current = histogram.iloc[-1]
    histogram_prev = histogram.iloc[-2]
    
    if histogram_current > 0 and histogram_prev <= 0:
        macd_trend_shift = "Bullish Trend Shift"
    elif histogram_current < 0 and histogram_prev >= 0:
        macd_trend_shift = "Bearish Trend Shift"
    else:
        macd_trend_shift = "No Trend Shift"
    
    stoch_indicator = ta.momentum.StochasticOscillator(high, low, close, window=14, smooth_window=3)
    stoch_d_value = stoch_indicator.stoch_signal().iloc[-1]
    
    if stoch_d_value >= 80:
        stoch_status = "Exhaustion Near Highs"
    elif stoch_d_value <= 20:
        stoch_status = "Exhaustion Near Lows"
    else:
        stoch_status = "Neutral"
    
    return {
        "rsi_value": round_to_2_decimals(rsi_value),
        "rsi_status": rsi_status,
        "ema50_value": round_to_2_decimals(ema50_current),
        "ema200_value": round_to_2_decimals(ema200_current),
        "ema_trend": ema_trend,
        "ema_crossover": ema_crossover,
        "macd_value": round_to_2_decimals(macd_current),
        "macd_signal": round_to_2_decimals(signal_current),
        "macd_histogram": round_to_2_decimals(histogram_current),
        "macd_trend_shift": macd_trend_shift,
        "stoch_d_value": round_to_2_decimals(stoch_d_value),
        "stoch_status": stoch_status
    }

def fetch_xauusd_30d() -> Optional[pd.DataFrame]:
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    rates = mt5.copy_rates_range(
        "XAUUSD", 
        mt5.TIMEFRAME_D1, 
        thirty_days_ago,
        now
    )
    
    if rates is None or len(rates) == 0:
        return None
    
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    
    df = df[['time', 'open', 'high', 'low', 'close']]
    
    df = df.tail(30)
    
    return df

def save_xauusd_30d_to_json(df: pd.DataFrame, filename: str = "Fetchers/jsons/xauusd_30d.json"):
    if df is None or df.empty:
        return
    
    records = df.to_dict('records')
    for record in records:
        for key in ['open', 'high', 'low', 'close']:
            record[key] = round_to_2_decimals(record[key])
        record['time'] = str(record['time'])
    
    with open(filename, 'w') as jsonfile:
        json.dump(records, jsonfile, indent=2)

def determine_bias(current_price: float, weekly: Dict, daily: Dict, hourly: Dict, 
                   indicators: Optional[Dict] = None) -> str:
    if not weekly or not daily or not hourly or current_price is None:
        return "NEUTRAL"
    
    if current_price > daily['high']:
        return "BULLISH"
    
    if current_price < daily['low']:
        return "BEARISH"
    
    if current_price > weekly['high']:
        return "BULLISH"
    
    if current_price < weekly['low']:
        return "BEARISH"
    
    if indicators:
        if indicators['ema_trend'] == "Bullish" and indicators['rsi_status'] != "Overbought":
            return "BULLISH"
        if indicators['ema_trend'] == "Bearish" and indicators['rsi_status'] != "Oversold":
            return "BEARISH"
    
    return "NEUTRAL"

def analyze_instrument(symbol: str) -> Optional[Dict[str, Any]]:
    weekly = fetch_weekly(symbol)
    daily = fetch_daily(symbol)
    hourly = fetch_hourly(symbol)
    current_price = get_current_price(symbol)
    
    if not weekly or not daily or not hourly or current_price is None:
        return None
    
    df = fetch_hourly_for_indicators(symbol)
    indicators = compute_indicators(df)
    
    bias = determine_bias(current_price, weekly, daily, hourly, indicators)
    
    result = {
        "instrument": symbol,
        "description": INSTRUMENTS[symbol],
        "timestamp": datetime.now().isoformat(),
        "current_price": current_price,
        "support_resistance": {
            "weekly": weekly,
            "daily": daily,
            "hourly": hourly
        },
        "indicators": indicators if indicators else {},
        "final_bias": bias
    }
    
    return result

def save_to_json(results: List[Dict[str, Any]], filename: str = "Fetchers/jsons/market_analysis.json"):
    if not results:
        return
    with open(filename, 'w') as jsonfile:
        json.dump(results, jsonfile, indent=2, default=str)

def main():
    if not initialize_mt5():
        return
    
    results = []
    
    for symbol in INSTRUMENTS.keys():
        result = analyze_instrument(symbol)
        if result:
            results.append(result)
    
    xauusd_30d = fetch_xauusd_30d()
    if xauusd_30d is not None:
        save_xauusd_30d_to_json(xauusd_30d)
        print(f"XAUUSD 30-day data saved to xauusd_30d.json ({len(xauusd_30d)} days)")
    
    mt5.shutdown()
    
    if results:
        save_to_json(results)
        print(f"Analysis complete. Results saved to market_analysis.json")

if __name__ == "__main__":
    main()