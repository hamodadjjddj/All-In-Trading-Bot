#!/usr/bin/env python3
import json
import requests
from typing import Dict, List, Optional
from datetime import datetime, timedelta

FRED_API_KEY = "f4e191ba7125013521aa29b4fbe962ee"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred_series(series_id: str, count: int = 2) -> Optional[List[float]]:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": count
    }
    try:
        response = requests.get(FRED_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations", [])
        values = []
        for obs in observations:
            try:
                val = float(obs["value"])
                values.append(val)
            except (ValueError, KeyError):
                continue
        return values[:count] if values else None
    except Exception as e:
        print(f"Error fetching {series_id}: {e}")
        return None

def fetch_cpi_yoy() -> Dict[str, Optional[float]]:
    values = fetch_fred_series("CPIAUCSL", count=13)
    if values and len(values) >= 13:
        current = values[0]
        prev_year = values[12]
        current_yoy = ((current - prev_year) / prev_year) * 100
        
        prev_month = values[1]
        prev_year_for_prev = values[13] if len(values) > 13 else values[12]
        prev_yoy = ((prev_month - prev_year_for_prev) / prev_year_for_prev) * 100
        
        return {
            "CPI_YoY_CURR": round(current_yoy, 2),
            "CPI_YoY_PREV": round(prev_yoy, 2)
        }
    return {"CPI_YoY_CURR": None, "CPI_YoY_PREV": None}

def fetch_pce_yoy() -> Dict[str, Optional[float]]:
    values = fetch_fred_series("PCE", count=13)
    if values and len(values) >= 13:
        current = values[0]
        prev_year = values[12]
        current_yoy = ((current - prev_year) / prev_year) * 100
        
        prev_month = values[1]
        prev_year_for_prev = values[13] if len(values) > 13 else values[12]
        prev_yoy = ((prev_month - prev_year_for_prev) / prev_year_for_prev) * 100
        
        return {
            "PCE_YoY_CURR": round(current_yoy, 2),
            "PCE_YoY_PREV": round(prev_yoy, 2)
        }
    return {"PCE_YoY_CURR": None, "PCE_YoY_PREV": None}

def fetch_unemployment() -> Dict[str, Optional[float]]:
    values = fetch_fred_series("UNRATE", count=2)
    if values and len(values) >= 2:
        return {
            "UNEMPLOYMENT_CURR": round(values[0], 2),
            "UNEMPLOYMENT_PREV": round(values[1], 2)
        }
    return {"UNEMPLOYMENT_CURR": None, "UNEMPLOYMENT_PREV": None}

def fetch_nfp() -> Dict[str, Optional[float]]:
    values = fetch_fred_series("PAYEMS", count=2)
    if values and len(values) >= 2:
        return {
            "NFP_CURR": round(values[0], 2),
            "NFP_PREV": round(values[1], 2)
        }
    return {"NFP_CURR": None, "NFP_PREV": None}

def fetch_real_interest_rate() -> Dict[str, Optional[float]]:
    treasury_10y = fetch_fred_series("DGS10", count=2)
    cpi_values = fetch_fred_series("CPIAUCSL", count=14)
    
    if treasury_10y and cpi_values and len(treasury_10y) >= 2 and len(cpi_values) >= 13:
        current_10y = treasury_10y[0]
        prev_10y = treasury_10y[1]
        
        current_cpi = cpi_values[0]
        prev_year_cpi = cpi_values[12]
        cpi_yoy_curr = ((current_cpi - prev_year_cpi) / prev_year_cpi) * 100
        
        prev_cpi = cpi_values[1]
        prev_year_cpi_for_prev = cpi_values[13]
        cpi_yoy_prev = ((prev_cpi - prev_year_cpi_for_prev) / prev_year_cpi_for_prev) * 100
        
        real_rate_curr = current_10y - cpi_yoy_curr
        real_rate_prev = prev_10y - cpi_yoy_prev
        
        return {
            "REAL_RATE_CURR": round(real_rate_curr, 2),
            "REAL_RATE_PREV": round(real_rate_prev, 2)
        }
    return {"REAL_RATE_CURR": None, "REAL_RATE_PREV": None}

def fetch_gld_flows_7d() -> Dict[str, Optional[List[float]]]:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        url = "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_en.csv"
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        lines = response.text.strip().split('\n')
        
        if len(lines) < 8:
            raise ValueError(f"CSV has only {len(lines)} lines, need at least 8")
        
        flows = []
        for i, line in enumerate(lines[1:8]):
            parts = [p.strip().strip('"') for p in line.split(',')]
            if len(parts) >= 3:
                try:
                    tonnes = float(parts[2])
                    flows.append(round(tonnes, 2))
                except (ValueError, IndexError) as e:
                    print(f"Error parsing line {i}: {parts}")
                    flows.append(0.0)
        
        while len(flows) < 7:
            flows.append(0.0)
        
        print(f"SPDR Success: {flows[:7]}")
        return {"GLD_FLOWS_7D": flows[:7]}
    
    except Exception as e:
        print(f"SPDR failed: {e}")
        
        try:
            import yfinance as yf
            gld = yf.Ticker("GLD")
            hist = gld.history(period="10d")
            
            if not hist.empty and len(hist) >= 7:
                volumes = hist['Volume'].tail(7).tolist()
                flows = [round(v / 1e6, 2) for v in volumes]
                print(f"yfinance Success: {flows}")
                return {"GLD_FLOWS_7D": flows}
        except Exception as e2:
            print(f"yfinance failed: {e2}")
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=14)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            url = f"https://query1.finance.yahoo.com/v7/finance/download/GLD"
            params = {
                "period1": int(start_date.timestamp()),
                "period2": int(end_date.timestamp()),
                "interval": "1d",
                "events": "history"
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            lines = response.text.strip().split('\n')
            flows = []
            
            for line in lines[1:]:
                if 'null' in line.lower():
                    continue
                parts = line.split(',')
                if len(parts) >= 6:
                    try:
                        volume = float(parts[5])
                        flows.append(round(volume / 1e6, 2))
                    except (ValueError, IndexError):
                        continue
            
            flows = flows[-7:] if len(flows) >= 7 else flows
            while len(flows) < 7:
                flows.append(0.0)
            
            print(f"Yahoo CSV Success: {flows}")
            return {"GLD_FLOWS_7D": flows[:7]}
        
        except Exception as e3:
            print(f"Yahoo CSV failed: {e3}")
            return {"GLD_FLOWS_7D": [0.0] * 7}

def fetch_cb_gold_purchases() -> Dict[str, Optional[float]]:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        url = "https://www.gold.org/goldhub/data/monthly-central-bank-statistics"
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        import re
        numbers = re.findall(r'[-+]?\d*\.\d+|\d+', response.text)
        
        if len(numbers) >= 2:
            try:
                curr = float(numbers[0])
                prev = float(numbers[1])
                return {
                    "CB_GOLD_PURCHASES_CURR": round(curr, 2),
                    "CB_GOLD_PURCHASES_PREV": round(prev, 2)
                }
            except:
                pass
    
    except Exception as e:
        print(f"WGC scraping failed: {e}")
    
    try:
        cb_data = fetch_fred_series("CBPUS", count=2)
        if cb_data and len(cb_data) >= 2:
            return {
                "CB_GOLD_PURCHASES_CURR": round(cb_data[0], 2),
                "CB_GOLD_PURCHASES_PREV": round(cb_data[1], 2)
            }
    except Exception as e:
        print(f"FRED CB data failed: {e}")
    
    return {
        "CB_GOLD_PURCHASES_CURR": None,
        "CB_GOLD_PURCHASES_PREV": None
    }

def fetch_fed_funds_rate() -> Dict[str, Optional[float]]:
    values = fetch_fred_series("FEDFUNDS", count=2)
    if values and len(values) >= 2:
        return {
            "FEDFUNDS_CURR": round(values[0], 2),
            "FEDFUNDS_PREV": round(values[1], 2)
        }
    return {"FEDFUNDS_CURR": None, "FEDFUNDS_PREV": None}

def collect_fundamentals() -> Dict:
    fundamentals = {}
    
    fundamentals.update(fetch_cpi_yoy())
    fundamentals.update(fetch_pce_yoy())
    fundamentals.update(fetch_unemployment())
    fundamentals.update(fetch_nfp())
    fundamentals.update(fetch_real_interest_rate())
    fundamentals.update(fetch_gld_flows_7d())
    fundamentals.update(fetch_cb_gold_purchases())
    fundamentals.update(fetch_fed_funds_rate())
    
    return fundamentals

def main():
    print("Collecting fundamentals data...")
    fundamentals = collect_fundamentals()
    
    output = json.dumps(fundamentals, indent=2)
    print(output)
    
    with open("fundamentals_data.json", "w") as f:
        f.write(output)

if __name__ == "__main__":
    main()