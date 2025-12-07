#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

INPUT_FOLDER = "Fetchers/jsons"
OUTPUT_FOLDER = "TEXT/daily_snapshots"

def parse_date(date_str):
    """Parse various date formats"""
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str.split("T")[0] if "T" in date_str else date_str.split()[0] if " " in date_str else date_str, fmt.split("T")[0] if "T" in fmt else fmt.split()[0] if " " in fmt else fmt).date()
        except:
            continue
    return None

def extract_all_dates_and_data(input_path):
    """Extract all dates and their associated data from all files"""
    date_data = defaultdict(lambda: {
        "fundamentals": {},
        "market_analysis": {},
        "xauusd": {},
        "economic_events": [],
        "news": [],
        "reddit": []
    })
    
    # Process fundamentals_data.json
    filepath = input_path / "fundamentals_data.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Daily metrics with history
        daily_keys = [
            "TREASURY_10Y", "HY_CREDIT_SPREAD", "GLD_PRICE", "GLD_VOLUME",
            "IAU_PRICE", "IAU_VOLUME", "JOBLESS_CLAIMS"
        ]
        
        for key in daily_keys:
            if f"{key}_LAST_30_DAYS" in data:
                for entry in data[f"{key}_LAST_30_DAYS"]:
                    date = entry.get("date")
                    if date:
                        date_obj = parse_date(date)
                        if date_obj:
                            if "value" in entry:
                                date_data[date_obj]["fundamentals"][key] = entry["value"]
                            elif "close" in entry:
                                date_data[date_obj]["fundamentals"][f"{key}_CLOSE"] = entry["close"]
                                if "volume" in entry:
                                    date_data[date_obj]["fundamentals"][f"{key}_VOLUME"] = entry["volume"]
        
        # Monthly metrics (add to all dates in that month)
        monthly_keys = [
            "CPI", "PCE", "PPI", "UNEMPLOYMENT", "NFP", 
            "FEDFUNDS", "M2_MONEY_SUPPLY", "RETAIL_SALES",
            "INDUSTRIAL_PROD", "HOUSING_STARTS", "REAL_RATE"
        ]
        
        for key in monthly_keys:
            if f"{key}_CURR" in data and f"{key}_CURR_DATE" in data:
                date = data[f"{key}_CURR_DATE"]
                date_obj = parse_date(date)
                if date_obj:
                    date_data[date_obj]["fundamentals"][key] = data[f"{key}_CURR"]
    
    # Process market_analysis.json
    filepath = input_path / "market_analysis.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for item in data:
                if "timestamp" in item:
                    date_obj = parse_date(item["timestamp"])
                    if date_obj:
                        instrument = item.get("instrument", "UNKNOWN")
                        date_data[date_obj]["market_analysis"][f"{instrument}_PRICE"] = item.get("current_price")
                        date_data[date_obj]["market_analysis"][f"{instrument}_BIAS"] = item.get("final_bias")
                        
                        if "indicators" in item:
                            indicators = item["indicators"]
                            date_data[date_obj]["market_analysis"][f"{instrument}_RSI"] = indicators.get("rsi_value")
                            date_data[date_obj]["market_analysis"][f"{instrument}_MACD"] = indicators.get("macd_value")
    
    # Process xauusd_30d.json
    filepath = input_path / "xauusd_30d.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for entry in data:
                if "time" in entry:
                    date_obj = parse_date(entry["time"])
                    if date_obj:
                        date_data[date_obj]["xauusd"] = {
                            "open": entry.get("open"),
                            "high": entry.get("high"),
                            "low": entry.get("low"),
                            "close": entry.get("close")
                        }
    
    # Process economic_calendar.json
    filepath = input_path / "economic_calendar.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "events" in data:
            for event in data["events"]:
                date = event.get("date")
                date_obj = parse_date(date)
                if date_obj:
                    date_data[date_obj]["economic_events"].append({
                        "time": event.get("time"),
                        "currency": event.get("currency"),
                        "event": event.get("event"),
                        "actual": event.get("actual"),
                        "forecast": event.get("forecast"),
                        "previous": event.get("previous")
                    })
    
    # Process news_30days.json
    filepath = input_path / "news_30days.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "headlines" in data:
            for article in data["headlines"]:
                time = article.get("time")
                if time:
                    date_obj = parse_date(time)
                    if date_obj:
                        date_data[date_obj]["news"].append({
                            "category": article.get("category"),
                            "title": article.get("title"),
                            "ticker": article.get("ticker")
                        })
    
    # Process reddit_news.json
    filepath = input_path / "reddit_news.json"
    if filepath.exists():
        print(f"Scanning {filepath.name}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "posts" in data:
            for post in data["posts"]:
                time = post.get("time")
                if time:
                    date_obj = parse_date(time)
                    if date_obj:
                        date_data[date_obj]["reddit"].append({
                            "title": post.get("title"),
                            "source": post.get("source")
                        })
    
    return date_data

def clean_snapshot_data(data):
    """Remove empty sections from snapshot data"""
    cleaned = {}
    
    for key, value in data.items():
        # Keep dictionaries only if they have content
        if isinstance(value, dict):
            if value:  # Non-empty dict
                cleaned[key] = value
        # Keep lists only if they have content
        elif isinstance(value, list):
            if value:  # Non-empty list
                cleaned[key] = value
        # Keep other values as-is
        else:
            cleaned[key] = value
    
    return cleaned

def main():
    print("\n" + "="*60)
    print("DAILY SNAPSHOT GENERATOR")
    print("="*60 + "\n")
    
    input_path = Path(INPUT_FOLDER)
    if not input_path.exists():
        print(f"ERROR: {INPUT_FOLDER} folder not found")
        return
    
    output_path = Path(OUTPUT_FOLDER)
    output_path.mkdir(exist_ok=True)
    
    # Extract all dates and data
    print("Extracting all dates and data from files...\n")
    date_data = extract_all_dates_and_data(input_path)
    
    if not date_data:
        print("ERROR: No data found in any files")
        return
    
    # Get date range
    all_dates = sorted(date_data.keys())
    oldest_date = all_dates[0]
    newest_date = all_dates[-1]
    
    print(f"\nDate range found:")
    print(f"  Oldest: {oldest_date}")
    print(f"  Newest: {newest_date}")
    print(f"  Total days with data: {len(all_dates)}")
    print("\n" + "="*60)
    
    # Generate snapshots for each date
    print("\nGenerating daily snapshots...\n")
    
    current_date = oldest_date
    snapshot_count = 0
    
    while current_date <= newest_date:
        if current_date in date_data:
            # Clean the data to remove empty sections
            cleaned_data = clean_snapshot_data(date_data[current_date])
            
            # Only create snapshot if there's actual data
            if cleaned_data:
                snapshot = {
                    "date": current_date.isoformat(),
                    "snapshot_generated_at": datetime.now().isoformat(),
                    "data": cleaned_data
                }
                
                # Save snapshot
                filename = f"snapshot_{current_date.isoformat()}.json"
                filepath = output_path / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(snapshot, f, indent=2, ensure_ascii=False)
                
                snapshot_count += 1
                print(f"✓ {filename}")
        
        current_date += timedelta(days=1)
    
    print("\n" + "="*60)
    print(f"Generated {snapshot_count} daily snapshots")
    print(f"Output folder: {OUTPUT_FOLDER}/")
    print("="*60)
    print("FINISHED\n")

if __name__ == "__main__":
    main()