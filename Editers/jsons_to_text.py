#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime

INPUT_FOLDER = "daily_snapshots"
OUTPUT_FOLDER = "daily_summaries"

def format_number(num, prefix="$", decimals=2):
    """Format number with prefix and proper decimals"""
    if num is None:
        return "N/A"
    return f"{prefix}{num:,.{decimals}f}"

def calculate_change(open_price, close_price):
    """Calculate absolute and percentage change"""
    if open_price is None or close_price is None:
        return None, None
    change = close_price - open_price
    pct_change = (change / open_price) * 100
    return change, pct_change

def format_xauusd(data):
    """Convert XAUUSD data to natural language"""
    open_p = data.get("open")
    high = data.get("high")
    low = data.get("low")
    close = data.get("close")
    
    if not all([open_p, high, low, close]):
        return None
    
    change, pct_change = calculate_change(open_p, close)
    range_val = high - low
    
    # Determine direction
    if change > 0:
        direction = "gain"
        change_text = f"+{format_number(change, '', 2)}"
        pct_text = f"+{pct_change:.2f}%"
    elif change < 0:
        direction = "loss"
        change_text = format_number(change, '', 2)
        pct_text = f"{pct_change:.2f}%"
    else:
        direction = "no change"
        change_text = format_number(0, '', 2)
        pct_text = "0.00%"
    
    text = f"Gold (XAU/USD) opened at {format_number(open_p)}, reached a high of {format_number(high)}, "
    text += f"dipped to a low of {format_number(low)}, and closed at {format_number(close)}. "
    text += f"This represents a daily {direction} of {change_text} ({pct_text}) with an intraday range of {format_number(range_val, '', 2)}."
    
    return text

def format_economic_events(events):
    """Convert economic events to natural language"""
    if not events:
        return None
    
    lines = []
    for event in events:
        time = event.get("time", "")
        currency = event.get("currency", "")
        event_name = event.get("event", "")
        actual = event.get("actual", "")
        forecast = event.get("forecast", "")
        previous = event.get("previous", "")
        
        # Build event description
        text = f"At {time}, the {currency} {event_name} was released."
        
        if actual:
            text += f" The actual figure came in at {actual}"
            
            # Compare to forecast if available
            if forecast:
                try:
                    actual_num = float(actual.replace('%', '').replace(',', ''))
                    forecast_num = float(forecast.replace('%', '').replace(',', ''))
                    
                    if actual_num > forecast_num:
                        text += f", beating the forecast of {forecast}"
                    elif actual_num < forecast_num:
                        text += f", missing the forecast of {forecast}"
                    else:
                        text += f", matching the forecast of {forecast}"
                except:
                    text += f" (forecast: {forecast})"
            
            # Compare to previous if available
            if previous:
                try:
                    actual_num = float(actual.replace('%', '').replace(',', ''))
                    previous_num = float(previous.replace('%', '').replace(',', ''))
                    
                    if actual_num > previous_num:
                        text += f" and rising from the previous {previous}."
                    elif actual_num < previous_num:
                        text += f" and falling from the previous {previous}."
                    else:
                        text += f" and unchanged from the previous {previous}."
                except:
                    text += f" (previous: {previous})."
            else:
                text += "."
        
        lines.append(text)
    
    return "\n".join(lines)

def format_fundamentals(data):
    """Convert fundamentals data to natural language"""
    if not data:
        return None
    
    lines = []
    
    # Treasury yields
    if "TREASURY_10Y" in data:
        lines.append(f"The 10-Year Treasury yield stands at {data['TREASURY_10Y']:.2f}%.")
    
    # Credit spreads
    if "HY_CREDIT_SPREAD" in data:
        lines.append(f"High-yield credit spread is at {data['HY_CREDIT_SPREAD']:.2f}%.")
    
    # Inflation metrics
    if "CPI" in data:
        lines.append(f"The Consumer Price Index (CPI) is at {data['CPI']:.2f}%.")
    if "PCE" in data:
        lines.append(f"Personal Consumption Expenditures (PCE) is at {data['PCE']:.2f}%.")
    if "PPI" in data:
        lines.append(f"Producer Price Index (PPI) is at {data['PPI']:.2f}.")
    
    # Employment data
    if "UNEMPLOYMENT" in data:
        lines.append(f"Unemployment rate is {data['UNEMPLOYMENT']:.1f}%.")
    if "NFP" in data:
        lines.append(f"Non-Farm Payrolls (NFP) came in at {data['NFP']:,.0f}K jobs.")
    if "JOBLESS_CLAIMS" in data:
        lines.append(f"Initial jobless claims were {data['JOBLESS_CLAIMS']:,.0f}K.")
    
    # Interest rates
    if "FEDFUNDS" in data:
        lines.append(f"The Federal Funds Rate is currently {data['FEDFUNDS']:.2f}%.")
    if "REAL_RATE" in data:
        lines.append(f"Real interest rate is {data['REAL_RATE']:.2f}%.")
    
    # Money supply & economic activity
    if "M2_MONEY_SUPPLY" in data:
        lines.append(f"M2 money supply stands at ${data['M2_MONEY_SUPPLY']:,.2f}B.")
    if "RETAIL_SALES" in data:
        lines.append(f"Retail sales total ${data['RETAIL_SALES']:,.0f}M.")
    if "INDUSTRIAL_PROD" in data:
        lines.append(f"Industrial production index is at {data['INDUSTRIAL_PROD']:.2f}.")
    if "HOUSING_STARTS" in data:
        lines.append(f"Housing starts are at {data['HOUSING_STARTS']:,.0f}K units.")
    
    # Gold ETFs
    gld_data = []
    if "GLD_PRICE_CLOSE" in data:
        gld_data.append(f"GLD closed at {format_number(data['GLD_PRICE_CLOSE'])}")
    if "GLD_VOLUME" in data:
        gld_data.append(f"volume {data['GLD_VOLUME']:,.0f} shares")
    if gld_data:
        lines.append(f"Gold ETF: {', '.join(gld_data)}.")
    
    iau_data = []
    if "IAU_PRICE_CLOSE" in data:
        iau_data.append(f"IAU closed at {format_number(data['IAU_PRICE_CLOSE'])}")
    if "IAU_VOLUME" in data:
        iau_data.append(f"volume {data['IAU_VOLUME']:,.0f} shares")
    if iau_data:
        lines.append(f"Gold ETF: {', '.join(iau_data)}.")
    
    return " ".join(lines) if lines else None

def interpret_rsi(rsi):
    """Interpret RSI value"""
    if rsi < 30:
        return "oversold conditions, potential buying opportunity"
    elif rsi < 50:
        return "bearish momentum"
    elif rsi < 70:
        return "neutral to bullish momentum"
    else:
        return "overbought conditions, potential reversal risk"

def get_instrument_name(instrument_code):
    """Convert instrument code to readable name"""
    names = {
        "XAUUSD": "Gold (XAU/USD)",
        "USA500.IDX": "S&P 500",
        "VOL.IDX": "VIX (Volatility Index)",
        "DOLLAR.IDX": "US Dollar Index (DXY)"
    }
    return names.get(instrument_code, instrument_code)

def format_market_analysis(data):
    """Convert market analysis to natural language"""
    if not data:
        return None
    
    # Extract unique instruments from the data keys
    instruments = set()
    for key in data.keys():
        if "_PRICE" in key:
            instrument = key.replace("_PRICE", "")
            instruments.add(instrument)
    
    if not instruments:
        return None
    
    lines = []
    
    for instrument in sorted(instruments):
        price = data.get(f"{instrument}_PRICE")
        bias = data.get(f"{instrument}_BIAS", "neutral")
        rsi = data.get(f"{instrument}_RSI")
        macd = data.get(f"{instrument}_MACD")
        
        if price is None:
            continue
        
        # Format price based on instrument
        if instrument == "XAUUSD":
            price_str = format_number(price)
        else:
            price_str = f"{price:,.2f}"
        
        instrument_name = get_instrument_name(instrument)
        text = f"{instrument_name} is trading at {price_str} with a {bias.lower()} bias."
        
        if rsi is not None:
            text += f" RSI: {rsi:.2f} ({interpret_rsi(rsi)})."
        
        if macd is not None:
            momentum = "positive" if macd > 0 else "negative"
            text += f" MACD: {macd:.2f} ({momentum} momentum)."
        
        lines.append(text)
    
    return "\n".join(lines) if lines else None

def format_news(articles):
    """Convert news articles to natural language"""
    if not articles:
        return None
    
    # Group by category
    by_category = {}
    for article in articles:
        category = article.get("category", "general")
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(article)
    
    lines = []
    for category, items in by_category.items():
        for item in items:
            title = item.get("title", "")
            ticker = item.get("ticker", "")
            ticker_text = f" ({ticker})" if ticker else ""
            lines.append(f"[{category.upper()}] {title}{ticker_text}")
    
    summary = "\n".join(lines)
    summary += f"\n[Total: {len(articles)} news items]"
    
    return summary

def format_reddit(posts):
    """Convert reddit posts to natural language"""
    if not posts:
        return None
    
    lines = []
    for post in posts:
        title = post.get("title", "")
        source = post.get("source", "")
        lines.append(f"{source}: \"{title}\"")
    
    summary = "\n".join(lines)
    summary += f"\n[Total: {len(posts)} posts tracked]"
    
    return summary

def convert_snapshot_to_text(snapshot_data):
    """Convert entire snapshot to natural language summary"""
    date = snapshot_data.get("date", "Unknown Date")
    data = snapshot_data.get("data", {})
    
    # Parse date for better formatting
    try:
        date_obj = datetime.fromisoformat(date)
        formatted_date = date_obj.strftime("%B %d, %Y")
    except:
        formatted_date = date
    
    sections = []
    sections.append(f"{'='*70}")
    sections.append(f"MARKET SUMMARY FOR {formatted_date.upper()}")
    sections.append(f"{'='*70}\n")
    
    # Process each category only if it has data
    if "xauusd" in data and data["xauusd"]:
        xauusd_text = format_xauusd(data["xauusd"])
        if xauusd_text:
            sections.append("GOLD PRICE ACTION:")
            sections.append(xauusd_text)
            sections.append("")
    
    if "economic_events" in data and data["economic_events"]:
        events_text = format_economic_events(data["economic_events"])
        if events_text:
            sections.append("ECONOMIC EVENTS:")
            sections.append(events_text)
            sections.append("")
    
    if "fundamentals" in data and data["fundamentals"]:
        fundamentals_text = format_fundamentals(data["fundamentals"])
        if fundamentals_text:
            sections.append("FUNDAMENTALS:")
            sections.append(fundamentals_text)
            sections.append("")
    
    if "market_analysis" in data and data["market_analysis"]:
        analysis_text = format_market_analysis(data["market_analysis"])
        if analysis_text:
            sections.append("TECHNICAL ANALYSIS:")
            sections.append(analysis_text)
            sections.append("")
    
    if "news" in data and data["news"]:
        news_text = format_news(data["news"])
        if news_text:
            sections.append("NEWS HIGHLIGHTS:")
            sections.append(news_text)
            sections.append("")
    
    if "reddit" in data and data["reddit"]:
        reddit_text = format_reddit(data["reddit"])
        if reddit_text:
            sections.append("SOCIAL SENTIMENT:")
            sections.append(reddit_text)
            sections.append("")
    
    return "\n".join(sections)

def main():
    print("\n" + "="*70)
    print("DAILY SNAPSHOT TO NATURAL LANGUAGE CONVERTER")
    print("="*70 + "\n")
    
    input_path = Path(INPUT_FOLDER)
    if not input_path.exists():
        print(f"ERROR: {INPUT_FOLDER} folder not found")
        return
    
    output_path = Path(OUTPUT_FOLDER)
    output_path.mkdir(exist_ok=True)
    
    # Get all snapshot files
    snapshot_files = sorted(input_path.glob("snapshot_*.json"))
    
    if not snapshot_files:
        print(f"ERROR: No snapshot files found in {INPUT_FOLDER}")
        return
    
    print(f"Found {len(snapshot_files)} snapshot files\n")
    print("Converting to natural language...\n")
    
    converted_count = 0
    
    for snapshot_file in snapshot_files:
        try:
            # Read snapshot
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                snapshot_data = json.load(f)
            
            # Convert to text
            text_summary = convert_snapshot_to_text(snapshot_data)
            
            # Save as text file
            date = snapshot_data.get("date", "unknown")
            output_file = output_path / f"summary_{date}.txt"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(text_summary)
            
            converted_count += 1
            print(f"✓ {output_file.name}")
            
        except Exception as e:
            print(f"✗ Error processing {snapshot_file.name}: {e}")
    
    print("\n" + "="*70)
    print(f"Successfully converted {converted_count} snapshots")
    print(f"Output folder: {OUTPUT_FOLDER}/")
    print("="*70)
    print("FINISHED\n")

if __name__ == "__main__":
    main()