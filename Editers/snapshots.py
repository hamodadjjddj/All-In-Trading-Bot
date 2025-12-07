#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

INPUT_FOLDER = "Fetchers/jsons"
OUTPUT_FOLDER = "TEXT/daily_snapshots"

def parse_date(date_str):
    """Parse various date formats"""
    if not date_str:
        return None
    
    # Clean the date string
    date_str = str(date_str).strip()
    
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y"  # For economic calendar
    ]
    
    for fmt in formats:
        try:
            # Handle datetime strings by taking only the date part
            if "T" in date_str:
                date_str = date_str.split("T")[0]
            elif " " in date_str and ":" in date_str:
                date_str = date_str.split()[0]
            
            return datetime.strptime(date_str, fmt).date()
        except:
            continue
    return None

def extract_monthly_inflation_data(input_path):
    """Extract monthly inflation and economic indicators to separate file"""
    filepath = input_path / "fundamentals_data.json"
    if not filepath.exists():
        return None
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    inflation_data = {
        "generated_at": datetime.now().isoformat(),
        "data_source": data.get("data_source", "Federal Reserve Economic Data (FRED)"),
        "description": "Monthly inflation and economic indicators",
        "indicators": {}
    }
    
    # Monthly indicators to extract
    monthly_keys = [
        "CPI", "PCE", "PPI", "UNEMPLOYMENT", "NFP", 
        "FEDFUNDS", "M2_MONEY_SUPPLY", "RETAIL_SALES",
        "INDUSTRIAL_PROD", "HOUSING_STARTS"
    ]
    
    for key in monthly_keys:
        if key in data and data[key] is not None:
            inflation_data["indicators"][key] = {
                "data": data[key],
                "end_date": data.get(f"{key}_END_DATE")
            }
    
    # Add calculated indicators
    if "REAL_RATE" in data and data["REAL_RATE"] is not None:
        inflation_data["indicators"]["REAL_RATE"] = {
            "value": data["REAL_RATE"],
            "end_date": data.get("REAL_RATE_END_DATE")
        }
    
    return inflation_data

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
        daily_keys = ["TREASURY_10Y", "HY_CREDIT_SPREAD"]
        
        for key in daily_keys:
            if key in data and isinstance(data[key], list):
                for entry in data[key]:
                    date = entry.get("date")
                    if date:
                        date_obj = parse_date(date)
                        if date_obj:
                            date_data[date_obj]["fundamentals"][key] = entry.get("value")
        
        # GLD and IAU (have close and volume)
        for etf in ["GLD", "IAU"]:
            if etf in data and isinstance(data[etf], list):
                for entry in data[etf]:
                    date = entry.get("date")
                    if date:
                        date_obj = parse_date(date)
                        if date_obj:
                            date_data[date_obj]["fundamentals"][f"{etf}_CLOSE"] = entry.get("close")
                            date_data[date_obj]["fundamentals"][f"{etf}_VOLUME"] = entry.get("volume")
        
        # Weekly metrics
        if "JOBLESS_CLAIMS" in data and isinstance(data["JOBLESS_CLAIMS"], list):
            for entry in data["JOBLESS_CLAIMS"]:
                date = entry.get("date")
                if date:
                    date_obj = parse_date(date)
                    if date_obj:
                        date_data[date_obj]["fundamentals"]["JOBLESS_CLAIMS"] = entry.get("value")
        
        # Monthly metrics - show all available data up to each date
        monthly_keys = [
            "CPI", "PCE", "PPI", "UNEMPLOYMENT", "NFP", 
            "FEDFUNDS", "M2_MONEY_SUPPLY", "RETAIL_SALES",
            "INDUSTRIAL_PROD", "HOUSING_STARTS"
        ]
        
        for key in monthly_keys:
            if key in data and isinstance(data[key], list) and data[key]:
                # For each monthly indicator, add complete history up to end_date
                end_date_str = data.get(f"{key}_END_DATE")
                if end_date_str:
                    end_date_obj = parse_date(end_date_str)
                    if end_date_obj:
                        # Add the full monthly array to the end date
                        date_data[end_date_obj]["fundamentals"][key] = data[key]
        
        # Calculated indicators (single values with end dates)
        if "REAL_RATE" in data and data["REAL_RATE"] is not None:
            end_date = data.get("REAL_RATE_END_DATE")
            if end_date:
                date_obj = parse_date(end_date)
                if date_obj:
                    date_data[date_obj]["fundamentals"]["REAL_RATE"] = data["REAL_RATE"]
    
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
                date_str = event.get("date")
                if date_str:
                    date_obj = parse_date(date_str)
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
    
    # Generate inflation data file first
    print("Generating inflation_data.json...\n")
    inflation_data = extract_monthly_inflation_data(input_path)
    if inflation_data and inflation_data["indicators"]:
        inflation_file = output_path / "inflation_data.json"
        with open(inflation_file, 'w', encoding='utf-8') as f:
            json.dump(inflation_data, f, indent=2, ensure_ascii=False)
        print(f"✓ inflation_data.json created\n")
    else:
        print("! No monthly inflation data found\n")
    
    # Extract all dates and data for daily snapshots
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
    
    # Calculate the cutoff date (30 days ago from today)
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=30)
    
    # Delete old snapshot files (older than 30 days)
    print("\nCleaning old snapshots...\n")
    deleted_count = 0
    for snapshot_file in output_path.glob("snapshot_*.json"):
        try:
            # Extract date from filename: snapshot_2025-11-07.json
            date_str = snapshot_file.stem.replace("snapshot_", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            if file_date < cutoff_date:
                snapshot_file.unlink()
                deleted_count += 1
                print(f"✗ Deleted: {snapshot_file.name}")
        except (ValueError, IndexError):
            continue
    
    if deleted_count == 0:
        print("No old snapshots to delete")
    
    # Generate snapshots only for the last 30 days
    print("\nGenerating daily snapshots (last 30 days)...\n")
    
    current_date = max(cutoff_date, oldest_date)  # Start from cutoff or oldest available
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
    print(f"Deleted {deleted_count} old snapshots (older than 30 days)")
    print(f"Generated {snapshot_count} daily snapshots (last 30 days)")
    print(f"Generated 1 inflation data file")
    print(f"Output folder: {OUTPUT_FOLDER}/")
    print("="*60)
    print("FINISHED\n")

if __name__ == "__main__":
    main()