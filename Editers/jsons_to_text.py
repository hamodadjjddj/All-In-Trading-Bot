#!/usr/bin/env python3
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

INPUT_FOLDER = "TEXT/daily_snapshots"
OUTPUT_FOLDER = "TEXT/daily_summaries"

class DataFormatter:
    """Handles formatting of various data types"""
    
    @staticmethod
    def format_number(num: Optional[Union[int, float]], prefix: str = "$", decimals: int = 2) -> str:
        """Format number with prefix and proper decimals"""
        if num is None:
            return "N/A"
        try:
            return f"{prefix}{num:,.{decimals}f}"
        except (ValueError, TypeError):
            return str(num)
    
    @staticmethod
    def calculate_change(open_price: Optional[float], close_price: Optional[float]) -> tuple:
        """Calculate absolute and percentage change"""
        if open_price is None or close_price is None or open_price == 0:
            return None, None
        try:
            change = close_price - open_price
            pct_change = (change / open_price) * 100
            return change, pct_change
        except (ValueError, TypeError, ZeroDivisionError):
            return None, None
    
    @staticmethod
    def parse_numeric(value: Any) -> Optional[float]:
        """Safely parse numeric value from various formats"""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Remove common characters and parse
                cleaned = value.replace('%', '').replace(',', '').replace('$', '').replace('B', '').replace('M', '').replace('K', '').strip()
                return float(cleaned)
            except ValueError:
                return None
        return None
    
    @staticmethod
    def interpret_rsi(rsi: Optional[float]) -> str:
        """Interpret RSI value"""
        if rsi is None:
            return "no RSI data"
        if rsi < 30:
            return "oversold conditions, potential buying opportunity"
        elif rsi < 45:
            return "bearish momentum"
        elif rsi < 55:
            return "neutral momentum"
        elif rsi < 70:
            return "bullish momentum"
        else:
            return "overbought conditions, potential reversal risk"
    
    @staticmethod
    def interpret_macd(macd: Optional[float]) -> str:
        """Interpret MACD value"""
        if macd is None:
            return "no MACD data"
        if abs(macd) < 0.5:
            return "neutral momentum"
        elif macd > 0:
            return "positive momentum"
        else:
            return "negative momentum"


class InflationDataFormatter:
    """Formats monthly inflation and economic indicators"""
    
    @staticmethod
    def format(inflation_data: Dict[str, Any]) -> Optional[str]:
        """Convert inflation data to simple, readable format"""
        if not inflation_data:
            return None
        
        indicators = inflation_data.get("indicators", {})
        
        if not indicators:
            return "No inflation data available for this period."
        
        sections = []
        
        # Inflation Indicators
        inflation_items = []
        
        if "CPI" in indicators and indicators["CPI"].get("data"):
            cpi_data = indicators["CPI"]["data"][-1]
            cpi_val = DataFormatter.parse_numeric(cpi_data.get("value"))
            cpi_date = cpi_data.get("date", "")
            if cpi_val is not None:
                try:
                    date_obj = datetime.fromisoformat(cpi_date)
                    month_str = date_obj.strftime("%B %Y")
                    inflation_items.append(f"Consumer Price Index (CPI) stood at {cpi_val:.2f} in {month_str}.")
                except:
                    inflation_items.append(f"Consumer Price Index (CPI) stood at {cpi_val:.2f}.")
        
        if "PCE" in indicators and indicators["PCE"].get("data"):
            pce_data = indicators["PCE"]["data"][-1]
            pce_val = DataFormatter.parse_numeric(pce_data.get("value"))
            pce_date = pce_data.get("date", "")
            if pce_val is not None:
                try:
                    date_obj = datetime.fromisoformat(pce_date)
                    month_str = date_obj.strftime("%B %Y")
                    inflation_items.append(f"Personal Consumption Expenditures (PCE) was {pce_val:.2f} in {month_str}.")
                except:
                    inflation_items.append(f"Personal Consumption Expenditures (PCE) was {pce_val:.2f}.")
        
        if "PPI" in indicators and indicators["PPI"].get("data"):
            ppi_data = indicators["PPI"]["data"][-1]
            ppi_val = DataFormatter.parse_numeric(ppi_data.get("value"))
            ppi_date = ppi_data.get("date", "")
            if ppi_val is not None:
                try:
                    date_obj = datetime.fromisoformat(ppi_date)
                    month_str = date_obj.strftime("%B %Y")
                    inflation_items.append(f"Producer Price Index (PPI) was {ppi_val:.2f} in {month_str}.")
                except:
                    inflation_items.append(f"Producer Price Index (PPI) was {ppi_val:.2f}.")
        
        if inflation_items:
            sections.append("INFLATION INDICATORS:\n" + " ".join(inflation_items))
        
        # Employment
        employment_items = []
        
        if "UNEMPLOYMENT" in indicators and indicators["UNEMPLOYMENT"].get("data"):
            unemp_data = indicators["UNEMPLOYMENT"]["data"][-1]
            unemp_val = DataFormatter.parse_numeric(unemp_data.get("value"))
            unemp_date = unemp_data.get("date", "")
            if unemp_val is not None:
                try:
                    date_obj = datetime.fromisoformat(unemp_date)
                    month_str = date_obj.strftime("%B %Y")
                    employment_items.append(f"Unemployment rate was {unemp_val:.1f}% in {month_str}.")
                except:
                    employment_items.append(f"Unemployment rate was {unemp_val:.1f}%.")
        
        if "NFP" in indicators and indicators["NFP"].get("data"):
            nfp_data = indicators["NFP"]["data"][-1]
            nfp_val = DataFormatter.parse_numeric(nfp_data.get("value"))
            if nfp_val is not None:
                employment_items.append(f"Non-Farm Payrolls totaled {nfp_val:,.0f}K jobs.")
        
        if employment_items:
            sections.append("EMPLOYMENT:\n" + " ".join(employment_items))
        
        # Monetary Policy
        monetary_items = []
        
        if "FEDFUNDS" in indicators and indicators["FEDFUNDS"].get("data"):
            fedfunds_data = indicators["FEDFUNDS"]["data"]
            if fedfunds_data:
                latest = fedfunds_data[-1]
                latest_val = DataFormatter.parse_numeric(latest.get("value"))
                latest_date = latest.get("date", "")
                
                if latest_val is not None:
                    try:
                        date_obj = datetime.fromisoformat(latest_date)
                        month_str = date_obj.strftime("%B %Y")
                        monetary_items.append(f"Federal Funds Rate was {latest_val:.2f}% as of {month_str}.")
                    except:
                        monetary_items.append(f"Federal Funds Rate was {latest_val:.2f}%.")
                
                # Show trend if multiple data points
                if len(fedfunds_data) > 1:
                    first = fedfunds_data[0]
                    first_val = DataFormatter.parse_numeric(first.get("value"))
                    if first_val is not None and latest_val is not None:
                        change = latest_val - first_val
                        if abs(change) > 0.01:
                            trend = "down" if change < 0 else "up"
                            monetary_items.append(f"Rate moved {trend} {abs(change):.2f} percentage points during this period.")
        
        if "M2_MONEY_SUPPLY" in indicators and indicators["M2_MONEY_SUPPLY"].get("data"):
            m2_data = indicators["M2_MONEY_SUPPLY"]["data"][-1]
            m2_val = DataFormatter.parse_numeric(m2_data.get("value"))
            if m2_val is not None:
                monetary_items.append(f"M2 money supply was ${m2_val:,.1f} billion.")
        
        if monetary_items:
            sections.append("MONETARY POLICY:\n" + " ".join(monetary_items))
        
        # Economic Activity
        activity_items = []
        
        if "RETAIL_SALES" in indicators and indicators["RETAIL_SALES"].get("data"):
            retail_data = indicators["RETAIL_SALES"]["data"][-1]
            retail_val = DataFormatter.parse_numeric(retail_data.get("value"))
            if retail_val is not None:
                activity_items.append(f"Retail sales totaled ${retail_val:,.0f} million.")
        
        if "INDUSTRIAL_PROD" in indicators and indicators["INDUSTRIAL_PROD"].get("data"):
            ind_data = indicators["INDUSTRIAL_PROD"]["data"][-1]
            ind_val = DataFormatter.parse_numeric(ind_data.get("value"))
            if ind_val is not None:
                activity_items.append(f"Industrial production index was {ind_val:.2f}.")
        
        if activity_items:
            sections.append("ECONOMIC ACTIVITY:\n" + " ".join(activity_items))
        
        return "\n\n".join(sections) if sections else "No economic data available."


class XAUUSDFormatter:
    """Formats gold price data"""
    
    @staticmethod
    def format(data: Dict[str, Any]) -> Optional[str]:
        """Convert XAUUSD data to natural language"""
        if not data:
            return None
        
        open_p = DataFormatter.parse_numeric(data.get("open"))
        high = DataFormatter.parse_numeric(data.get("high"))
        low = DataFormatter.parse_numeric(data.get("low"))
        close = DataFormatter.parse_numeric(data.get("close"))
        
        if not all([open_p, high, low, close]):
            return None
        
        change, pct_change = DataFormatter.calculate_change(open_p, close)
        if change is None or pct_change is None:
            return None
        
        range_val = high - low
        
        # Determine direction
        if abs(change) < 0.01:
            direction = "no significant change"
            change_text = "flat"
            pct_text = "0.00%"
        elif change > 0:
            direction = "gain"
            change_text = f"+{DataFormatter.format_number(change, '', 2)}"
            pct_text = f"+{pct_change:.2f}%"
        else:
            direction = "loss"
            change_text = DataFormatter.format_number(change, '', 2)
            pct_text = f"{pct_change:.2f}%"
        
        text = f"Gold (XAU/USD) opened at {DataFormatter.format_number(open_p)}, "
        text += f"reached a high of {DataFormatter.format_number(high)}, "
        text += f"dipped to a low of {DataFormatter.format_number(low)}, "
        text += f"and closed at {DataFormatter.format_number(close)}. "
        text += f"This represents a daily {direction} of {change_text} ({pct_text}) "
        text += f"with an intraday range of {DataFormatter.format_number(range_val, '', 2)}."
        
        return text


class EconomicEventsFormatter:
    """Formats economic events data"""
    
    @staticmethod
    def format(events: List[Dict[str, Any]]) -> Optional[str]:
        """Convert economic events to natural language"""
        if not events:
            return None
        
        lines = []
        for event in events:
            try:
                time = event.get("time", "Unknown time")
                currency = event.get("currency", "")
                event_name = event.get("event", "Unknown event")
                actual = event.get("actual", "")
                forecast = event.get("forecast", "")
                previous = event.get("previous", "")
                
                # Build event description
                text = f"At {time}, {currency} {event_name} was released."
                
                if actual:
                    text += f" Actual: {actual}"
                    
                    # Compare to forecast
                    if forecast and actual != forecast:
                        actual_num = DataFormatter.parse_numeric(actual)
                        forecast_num = DataFormatter.parse_numeric(forecast)
                        
                        if actual_num is not None and forecast_num is not None:
                            if actual_num > forecast_num:
                                text += f", beating forecast of {forecast}"
                            elif actual_num < forecast_num:
                                text += f", missing forecast of {forecast}"
                        else:
                            text += f" (forecast: {forecast})"
                    elif forecast:
                        text += f", matching forecast of {forecast}"
                    
                    # Compare to previous
                    if previous and actual != previous:
                        actual_num = DataFormatter.parse_numeric(actual)
                        previous_num = DataFormatter.parse_numeric(previous)
                        
                        if actual_num is not None and previous_num is not None:
                            change = actual_num - previous_num
                            if abs(change) > 0.01:
                                direction = "rising" if change > 0 else "falling"
                                text += f" and {direction} from previous {previous}."
                            else:
                                text += f", unchanged from previous {previous}."
                        else:
                            text += f" (previous: {previous})."
                    elif not forecast:
                        text += "."
                
                lines.append(text)
            except Exception:
                # Skip malformed events but continue processing
                continue
        
        return "\n".join(lines) if lines else None


class FundamentalsFormatter:
    """Formats fundamental data"""
    
    @staticmethod
    def format(data: Dict[str, Any]) -> Optional[str]:
        """Convert fundamentals data to natural language"""
        if not data:
            return None
        
        lines = []
        
        # Treasury yields
        if "TREASURY_10Y" in data:
            val = DataFormatter.parse_numeric(data["TREASURY_10Y"])
            if val is not None:
                lines.append(f"10-Year Treasury yield: {val:.2f}%.")
        
        # Credit spreads
        if "HY_CREDIT_SPREAD" in data:
            val = DataFormatter.parse_numeric(data["HY_CREDIT_SPREAD"])
            if val is not None:
                lines.append(f"High-yield credit spread: {val:.2f}%.")
        
        # Inflation metrics
        if "CPI" in data:
            val = DataFormatter.parse_numeric(data["CPI"])
            if val is not None:
                lines.append(f"Consumer Price Index (CPI): {val:.2f}.")
        
        if "PCE" in data:
            val = DataFormatter.parse_numeric(data["PCE"])
            if val is not None:
                lines.append(f"Personal Consumption Expenditures (PCE): {val:.2f}.")
        
        if "PPI" in data:
            val = DataFormatter.parse_numeric(data["PPI"])
            if val is not None:
                lines.append(f"Producer Price Index (PPI): {val:.2f}.")
        
        # Employment data
        if "UNEMPLOYMENT" in data:
            val = DataFormatter.parse_numeric(data["UNEMPLOYMENT"])
            if val is not None:
                lines.append(f"Unemployment rate: {val:.1f}%.")
        
        if "NFP" in data:
            val = DataFormatter.parse_numeric(data["NFP"])
            if val is not None:
                lines.append(f"Non-Farm Payrolls: {val:,.0f}K jobs.")
        
        if "JOBLESS_CLAIMS" in data:
            val = DataFormatter.parse_numeric(data["JOBLESS_CLAIMS"])
            if val is not None:
                lines.append(f"Initial jobless claims: {val:,.0f}K.")
        
        # Interest rates
        if "FEDFUNDS" in data:
            val = DataFormatter.parse_numeric(data["FEDFUNDS"])
            if val is not None:
                lines.append(f"Federal Funds Rate: {val:.2f}%.")
        
        if "REAL_RATE" in data:
            val = DataFormatter.parse_numeric(data["REAL_RATE"])
            if val is not None:
                lines.append(f"Real interest rate: {val:.2f}%.")
        
        # Money supply & economic activity
        if "M2_MONEY_SUPPLY" in data:
            val = DataFormatter.parse_numeric(data["M2_MONEY_SUPPLY"])
            if val is not None:
                lines.append(f"M2 money supply: ${val:,.2f}B.")
        
        if "RETAIL_SALES" in data:
            val = DataFormatter.parse_numeric(data["RETAIL_SALES"])
            if val is not None:
                lines.append(f"Retail sales: ${val:,.0f}M.")
        
        if "INDUSTRIAL_PROD" in data:
            val = DataFormatter.parse_numeric(data["INDUSTRIAL_PROD"])
            if val is not None:
                lines.append(f"Industrial production index: {val:.2f}.")
        
        if "HOUSING_STARTS" in data:
            val = DataFormatter.parse_numeric(data["HOUSING_STARTS"])
            if val is not None:
                lines.append(f"Housing starts: {val:,.0f}K units.")
        
        # Gold ETFs
        gld_parts = []
        if "GLD_CLOSE" in data:
            val = DataFormatter.parse_numeric(data["GLD_CLOSE"])
            if val is not None:
                gld_parts.append(f"closed at {DataFormatter.format_number(val)}")
        
        if "GLD_VOLUME" in data:
            val = DataFormatter.parse_numeric(data["GLD_VOLUME"])
            if val is not None:
                gld_parts.append(f"volume {val:,.0f}")
        
        if gld_parts:
            lines.append(f"GLD ETF: {', '.join(gld_parts)}.")
        
        iau_parts = []
        if "IAU_CLOSE" in data:
            val = DataFormatter.parse_numeric(data["IAU_CLOSE"])
            if val is not None:
                iau_parts.append(f"closed at {DataFormatter.format_number(val)}")
        
        if "IAU_VOLUME" in data:
            val = DataFormatter.parse_numeric(data["IAU_VOLUME"])
            if val is not None:
                iau_parts.append(f"volume {val:,.0f}")
        
        if iau_parts:
            lines.append(f"IAU ETF: {', '.join(iau_parts)}.")
        
        return " ".join(lines) if lines else None


class MarketAnalysisFormatter:
    """Formats market analysis data"""
    
    INSTRUMENT_NAMES = {
        "XAUUSD": "Gold (XAU/USD)",
        "USA500.IDX": "S&P 500",
        "USA100.IDX": "Nasdaq 100",
        "USA30.IDX": "Dow Jones",
        "VOL.IDX": "VIX (Volatility Index)",
        "DOLLAR.IDX": "US Dollar Index (DXY)",
        "BTC": "Bitcoin",
        "ETH": "Ethereum"
    }
    
    @staticmethod
    def format(data: Dict[str, Any]) -> Optional[str]:
        """Convert market analysis to natural language"""
        if not data:
            return None
        
        # Extract unique instruments
        instruments = set()
        for key in data.keys():
            if "_PRICE" in key:
                instrument = key.replace("_PRICE", "")
                instruments.add(instrument)
        
        if not instruments:
            return None
        
        lines = []
        
        for instrument in sorted(instruments):
            try:
                price = DataFormatter.parse_numeric(data.get(f"{instrument}_PRICE"))
                bias = data.get(f"{instrument}_BIAS", "neutral")
                rsi = DataFormatter.parse_numeric(data.get(f"{instrument}_RSI"))
                macd = DataFormatter.parse_numeric(data.get(f"{instrument}_MACD"))
                
                if price is None:
                    continue
                
                # Format price
                if instrument == "XAUUSD":
                    price_str = DataFormatter.format_number(price)
                else:
                    price_str = f"{price:,.2f}"
                
                instrument_name = MarketAnalysisFormatter.INSTRUMENT_NAMES.get(instrument, instrument)
                text = f"{instrument_name}: trading at {price_str} with {bias.lower()} bias."
                
                if rsi is not None:
                    text += f" RSI: {rsi:.2f} ({DataFormatter.interpret_rsi(rsi)})."
                
                if macd is not None:
                    text += f" MACD: {macd:.2f} ({DataFormatter.interpret_macd(macd)})."
                
                lines.append(text)
            except Exception:
                continue
        
        return "\n".join(lines) if lines else None


class NewsFormatter:
    """Formats news articles"""
    
    @staticmethod
    def format(articles: List[Dict[str, Any]]) -> Optional[str]:
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
        total_count = 0
        
        for category in sorted(by_category.keys()):
            items = by_category[category]
            lines.append(f"\n{category.upper()} ({len(items)} items):")
            
            for item in items:
                title = item.get("title", "").strip()
                ticker = item.get("ticker", "").strip()
                
                if title:
                    ticker_text = f" [{ticker}]" if ticker else ""
                    lines.append(f"  • {title}{ticker_text}")
                    total_count += 1
        
        if lines:
            lines.append(f"\n[Total: {total_count} news items]")
            return "\n".join(lines)
        
        return None


class RedditFormatter:
    """Formats Reddit posts"""
    
    @staticmethod
    def format(posts: List[Dict[str, Any]]) -> Optional[str]:
        """Convert reddit posts to natural language"""
        if not posts:
            return None
        
        # Group by subreddit
        by_subreddit = {}
        for post in posts:
            source = post.get("source", "unknown")
            if source not in by_subreddit:
                by_subreddit[source] = []
            by_subreddit[source].append(post)
        
        lines = []
        total_count = 0
        
        for subreddit in sorted(by_subreddit.keys()):
            items = by_subreddit[subreddit]
            lines.append(f"\n{subreddit} ({len(items)} posts):")
            
            for post in items:
                title = post.get("title", "").strip()
                if title:
                    lines.append(f"  • {title}")
                    total_count += 1
        
        if lines:
            lines.append(f"\n[Total: {total_count} posts tracked]")
            return "\n".join(lines)
        
        return None


class SnapshotConverter:
    """Main converter class"""
    
    @staticmethod
    def convert_to_text(snapshot_data: Dict[str, Any], is_inflation_file: bool = False) -> str:
        """Convert entire snapshot to natural language summary"""
        
        if is_inflation_file:
            # Simple handling for inflation_data.json
            generated_at = snapshot_data.get("generated_at", "")
            
            try:
                gen_date = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                date_header = gen_date.strftime("%B %d, %Y")
            except:
                date_header = "Monthly Indicators"
            
            sections = []
            sections.append(date_header)
            sections.append("")
            
            formatted = InflationDataFormatter.format(snapshot_data)
            if formatted:
                sections.append(formatted)
            else:
                sections.append("No economic data available.")
            
            sections.append("")
            sections.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
            return "\n".join(sections)
        
        # Standard daily snapshot handling
        date = snapshot_data.get("date", "Unknown Date")
        data = snapshot_data.get("data", {})
        
        # Parse date for header
        try:
            date_obj = datetime.fromisoformat(date)
            date_header = date_obj.strftime("%B %d, %Y")
        except:
            date_header = date
        
        sections = []
        sections.append(date_header)
        sections.append("")
        
        # Process each category
        formatters = [
            ("xauusd", "GOLD PRICE ACTION", XAUUSDFormatter.format),
            ("economic_events", "ECONOMIC EVENTS", EconomicEventsFormatter.format),
            ("fundamentals", "FUNDAMENTALS", FundamentalsFormatter.format),
            ("market_analysis", "TECHNICAL ANALYSIS", MarketAnalysisFormatter.format),
            ("news", "NEWS HIGHLIGHTS", NewsFormatter.format),
            ("reddit", "SOCIAL SENTIMENT", RedditFormatter.format),
        ]
        
        for key, title, formatter in formatters:
            if key in data and data[key]:
                try:
                    formatted = formatter(data[key])
                    if formatted:
                        sections.append(f"{title}:")
                        sections.append(formatted)
                        sections.append("")
                except Exception as e:
                    sections.append(f"{title}: [Error processing data: {str(e)}]")
                    sections.append("")
        
        # Footer with timestamp only
        sections.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        return "\n".join(sections)


def main():
    print("\n" + "="*70)
    print("ENHANCED DAILY SNAPSHOT TO NATURAL LANGUAGE CONVERTER")
    print("="*70 + "\n")
    
    input_path = Path(INPUT_FOLDER)
    if not input_path.exists():
        print(f"ERROR: {INPUT_FOLDER} folder not found")
        return
    
    output_path = Path(OUTPUT_FOLDER)
    output_path.mkdir(exist_ok=True)
    
    # Check for inflation data first
    inflation_file = input_path / "inflation_data.json"
    snapshot_files = []
    
    if inflation_file.exists():
        snapshot_files.append(inflation_file)
        print("Found inflation_data.json - processing as monthly overview\n")
    
    # Get all daily snapshot files
    daily_files = sorted(input_path.glob("snapshot_*.json"))
    snapshot_files.extend(daily_files)
    
    if not snapshot_files:
        print(f"ERROR: No snapshot files found in {INPUT_FOLDER}")
        return
    
    print(f"Found {len(snapshot_files)} files to process\n")
    
    converted_count = 0
    error_count = 0
    
    for snapshot_file in snapshot_files:
        try:
            # Read snapshot
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                snapshot_data = json.load(f)
            
            # Check if this is the inflation data file
            is_inflation = snapshot_file.name == "inflation_data.json"
            
            # Convert to text
            text_summary = SnapshotConverter.convert_to_text(snapshot_data, is_inflation_file=is_inflation)
            
            # Determine output filename
            if is_inflation:
                output_file = output_path / "summary_monthly_indicators.txt"
            else:
                date = snapshot_data.get("date", "unknown")
                output_file = output_path / f"summary_{date}.txt"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(text_summary)
            
            converted_count += 1
            print(f"✓ {output_file.name}")
            
        except Exception as e:
            error_count += 1
            print(f"✗ Error processing {snapshot_file.name}: {str(e)}")
    
    print("\n" + "="*70)
    print(f"Successfully converted: {converted_count}")
    print(f"Errors encountered: {error_count}")
    print(f"Output folder: {OUTPUT_FOLDER}/")
    print("="*70)
    print("FINISHED\n")


if __name__ == "__main__":
    main()