#!/usr/bin/env python3
import json
import requests
import yfinance as yf
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

FRED_API_KEY = "f4e191ba7125013521aa29b4fbe962ee"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred_series_range(series_id: str, start_date: str) -> Optional[List[Dict]]:
    """Fetch FRED series data from a start date to present."""
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start_date
    }
    
    try:
        response = requests.get(FRED_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations", [])
        results = []
        for obs in observations:
            try:
                val = float(obs["value"])
                results.append({"date": obs["date"], "value": round(val, 2)})
            except (ValueError, KeyError):
                continue
        return results if results else None
    except Exception:
        return None

def fetch_monthly_indicator(series_id: str, name: str) -> Dict:
    """Fetch current and previous month for monthly indicators."""
    start_date = (datetime.now() - relativedelta(months=3)).strftime("%Y-%m-%d")
    data = fetch_fred_series_range(series_id, start_date)
    
    result = {}
    
    if data and len(data) >= 1:
        end_date = data[-1]["date"]
        result[name] = [{"date": d["date"], "value": d["value"]} for d in data]
        result[f"{name}_END_DATE"] = end_date
    else:
        result[name] = None
        result[f"{name}_END_DATE"] = None
    
    return result

def fetch_daily_previous_month(series_id: str, name: str) -> Dict:
    """Fetch all values from previous 30 days for daily frequency indicators."""
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    data = fetch_fred_series_range(series_id, start_date)
    
    result = {}
    
    if data and len(data) >= 1:
        end_date = data[-1]["date"]
        result[name] = [{"date": d["date"], "value": d["value"]} for d in data]
        result[f"{name}_END_DATE"] = end_date
    else:
        result[name] = None
        result[f"{name}_END_DATE"] = None
    
    return result

def fetch_weekly_previous_month(series_id: str, name: str) -> Dict:
    """Fetch all values from previous 30 days for weekly frequency indicators."""
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    data = fetch_fred_series_range(series_id, start_date)
    
    result = {}
    
    if data and len(data) >= 1:
        end_date = data[-1]["date"]
        result[name] = [{"date": d["date"], "value": d["value"]} for d in data]
        result[f"{name}_END_DATE"] = end_date
    else:
        result[name] = None
        result[f"{name}_END_DATE"] = None
    
    return result

def fetch_real_interest_rate() -> Dict:
    """Calculate real interest rate (Treasury 10Y - CPI YoY)."""
    start_date = (datetime.now() - relativedelta(months=14)).strftime("%Y-%m-%d")
    treasury_data = fetch_fred_series_range("DGS10", start_date)
    cpi_data = fetch_fred_series_range("CPIAUCSL", start_date)
    
    result = {}
    
    if treasury_data and cpi_data and len(treasury_data) >= 1 and len(cpi_data) >= 13:
        current_10y = treasury_data[-1]["value"]
        
        cpi_values = [d["value"] for d in cpi_data]
        current_cpi = cpi_values[-1]
        prev_year_cpi = cpi_values[-13]
        cpi_yoy_curr = ((current_cpi - prev_year_cpi) / prev_year_cpi) * 100
        
        real_rate_curr = current_10y - cpi_yoy_curr
        result["REAL_RATE"] = round(real_rate_curr, 2)
        result["REAL_RATE_END_DATE"] = treasury_data[-1]["date"]
    else:
        result["REAL_RATE"] = None
        result["REAL_RATE_END_DATE"] = None
    
    return result

def fetch_gold_etf_flows() -> Dict:
    """Fetch GLD and IAU net asset values as proxy for institutional flows."""
    result = {}
    
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        gld = yf.Ticker("GLD")
        gld_hist = gld.history(start=start_date, end=end_date)
        
        if not gld_hist.empty:
            gld_30d = [{"date": idx.strftime("%Y-%m-%d"), "close": round(row["Close"], 2), "volume": int(row["Volume"])} 
                       for idx, row in gld_hist.iterrows()]
            result["GLD"] = gld_30d
            result["GLD_END_DATE"] = gld_hist.index[-1].strftime("%Y-%m-%d")
        else:
            result["GLD"] = None
            result["GLD_END_DATE"] = None
    except Exception:
        result["GLD"] = None
        result["GLD_END_DATE"] = None
    
    try:
        iau = yf.Ticker("IAU")
        iau_hist = iau.history(start=start_date, end=end_date)
        
        if not iau_hist.empty:
            iau_30d = [{"date": idx.strftime("%Y-%m-%d"), "close": round(row["Close"], 2), "volume": int(row["Volume"])} 
                       for idx, row in iau_hist.iterrows()]
            result["IAU"] = iau_30d
            result["IAU_END_DATE"] = iau_hist.index[-1].strftime("%Y-%m-%d")
        else:
            result["IAU"] = None
            result["IAU_END_DATE"] = None
    except Exception:
        result["IAU"] = None
        result["IAU_END_DATE"] = None
    
    return result

def collect_fundamentals() -> Dict:
    """Collect all fundamental economic indicators."""
    print("Collecting fundamental economic data...")
    
    fundamentals = {
        "collection_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_source": "Federal Reserve Economic Data (FRED) + Yahoo Finance"
    }
    
    # ===== DAILY FREQUENCY INDICATORS =====
    print("\n[DAILY FREQUENCY]")
    fundamentals["#DAILY_DATA"] = "Last 30 days, chronological order (oldest to newest)"
    
    fundamentals.update(fetch_daily_previous_month("DGS10", "TREASURY_10Y"))
    fundamentals.update(fetch_daily_previous_month("BAMLH0A0HYM2", "HY_CREDIT_SPREAD"))
    fundamentals.update(fetch_gold_etf_flows())
    
    # ===== WEEKLY FREQUENCY INDICATORS =====
    print("[WEEKLY FREQUENCY]")
    fundamentals["#WEEKLY_DATA"] = "Last 30 days, chronological order (oldest to newest)"
    
    fundamentals.update(fetch_weekly_previous_month("ICSA", "JOBLESS_CLAIMS"))
    
    # ===== MONTHLY FREQUENCY INDICATORS =====
    print("[MONTHLY FREQUENCY]")
    fundamentals["#MONTHLY_DATA"] = "Last 3 months, chronological order (oldest to newest)"
    
    fundamentals.update(fetch_monthly_indicator("CPIAUCSL", "CPI"))
    fundamentals.update(fetch_monthly_indicator("PCEPI", "PCE"))
    fundamentals.update(fetch_monthly_indicator("PPIACO", "PPI"))
    fundamentals.update(fetch_monthly_indicator("UNRATE", "UNEMPLOYMENT"))
    fundamentals.update(fetch_monthly_indicator("PAYEMS", "NFP"))
    fundamentals.update(fetch_monthly_indicator("FEDFUNDS", "FEDFUNDS"))
    fundamentals.update(fetch_monthly_indicator("M2SL", "M2_MONEY_SUPPLY"))
    fundamentals.update(fetch_monthly_indicator("RSAFS", "RETAIL_SALES"))
    fundamentals.update(fetch_monthly_indicator("INDPRO", "INDUSTRIAL_PROD"))
    fundamentals.update(fetch_monthly_indicator("HOUST", "HOUSING_STARTS"))
    
    # ===== CALCULATED INDICATORS =====
    print("[CALCULATED INDICATORS]")
    fundamentals["#CALCULATED_DATA"] = "Derived metrics"
    
    fundamentals.update(fetch_real_interest_rate())
    
    print("\nCollection complete.")
    
    return fundamentals

def main():
    print("=" * 60)
    print("FUNDAMENTALS DATA COLLECTION")
    print("=" * 60)
    
    fundamentals = collect_fundamentals()
    
    output = json.dumps(fundamentals, indent=2)
    
    print("\n" + "=" * 60)
    print("DATA SAVED")
    print("=" * 60)
    
    output_file = "Fetchers/jsons/fundamentals_data.json"
    with open(output_file, "w") as f:
        f.write(output)
    
    print(f"\nFile: {output_file}")

if __name__ == "__main__":
    main()