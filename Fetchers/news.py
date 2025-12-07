import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import time
import random

TICKERS = {
    "gold": ["GLD", "IAU", "GLDM", "GDX", "GDXJ", "NUGT", "RING", "SGOL", "AAAU"],
    "market": ["SPY", "QQQ", "DIA", "IWM"],
    "volatility": ["VXX", "VIXY", "UVXY", "VIXM", "SVXY", "SVIX", "UVIX"],
    "dollar": ["UUP", "USDU"]
}

class NewsFetcher:
    def __init__(self):
        self.logs = []
    
    def _log(self, msg):
        entry = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
        self.logs.append(entry)
        print(entry)
    
    def _clean(self, text):
        if not text:
            return ""
        try:
            text = re.sub(r'http[s]?://\S+', '', text)
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'#\w+', '', text)
            return ' '.join(text.split()).strip()
        except:
            return ""
    
    def _parse_date(self, date_str, time_str, last_date):
        try:
            if date_str:
                if '-' in date_str:
                    dt = datetime.strptime(f"{date_str} {time_str}", "%b-%d-%y %I:%M%p")
                else:
                    dt = datetime.strptime(f"{date_str} {time_str}", "%b-%d %I:%M%p")
                    dt = dt.replace(year=datetime.now().year)
                return dt, dt
            else:
                if last_date:
                    dt = datetime.strptime(f"{last_date.strftime('%b-%d-%y')} {time_str}", "%b-%d-%y %I:%M%p")
                    return dt, last_date
                else:
                    dt = datetime.strptime(f"{datetime.now().strftime('%b-%d-%y')} {time_str}", "%b-%d-%y %I:%M%p")
                    return dt, dt
        except:
            return None, last_date
    
    def fetch_finviz(self, ticker, category, retries=3):
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(1.5, 3.0))
                
                req = Request(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                html = urlopen(req, timeout=30).read()
                soup = BeautifulSoup(html, 'html.parser')
                news_table = soup.find(id='news-table')
                
                if not news_table:
                    self._log(f"  {ticker}: No news table found")
                    return []
                
                news_data = []
                last_date = None
                
                for row in news_table.find_all('tr'):
                    try:
                        title_elem = row.find('a')
                        date_elem = row.find('td')
                        
                        if not title_elem or not date_elem:
                            continue
                        
                        title = title_elem.get_text().strip()
                        
                        date_data = date_elem.text.strip().split()
                        
                        if len(date_data) >= 2:
                            date_str = date_data[0]
                            time_str = date_data[1]
                        else:
                            date_str = None
                            time_str = date_data[0] if date_data else "12:00AM"
                        
                        dt, last_date = self._parse_date(date_str, time_str, last_date)
                        
                        if dt:
                            news_data.append({
                                'category': category,
                                'ticker': ticker,
                                'title': self._clean(title),
                                'time': dt.isoformat(),
                                'timestamp': dt
                            })
                    except:
                        continue
                
                self._log(f"  {ticker}: Fetched {len(news_data)} articles")
                return news_data
                
            except Exception as e:
                if "429" in str(e) and attempt < retries - 1:
                    wait_time = (attempt + 1) * 5
                    self._log(f"  {ticker}: Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    self._log(f"  {ticker}: Error - {str(e)}")
                    return []
        
        return []
    
    def fetch_all(self):
        all_news = []
        
        for category, tickers in TICKERS.items():
            self._log(f"Fetching {category.upper()} news ({len(tickers)} tickers)...")
            for ticker in tickers:
                news = self.fetch_finviz(ticker, category)
                all_news.extend(news)
        
        self._log(f"Total raw articles: {len(all_news)}")
        return all_news
    
    def filter_last_30_days(self, news):
        now = datetime.now()
        cutoff = now - timedelta(days=30)
        filtered = [n for n in news if n['timestamp'] >= cutoff]
        self._log(f"Filtered to last 30 days: {len(filtered)} articles")
        return filtered
    
    def deduplicate(self, news):
        by_category = {}
        for item in news:
            cat = item['category']
            if cat not in by_category:
                by_category[cat] = []
            by_category[cat].append(item)
        
        deduplicated = []
        total_removed = 0
        
        for category, items in by_category.items():
            title_map = {}
            for item in items:
                title = item['title']
                if title not in title_map:
                    title_map[title] = []
                title_map[title].append(item)
            
            for title, duplicates in title_map.items():
                duplicates.sort(key=lambda x: x['timestamp'])
                deduplicated.append(duplicates[0])
                total_removed += len(duplicates) - 1
        
        self._log(f"Removed {total_removed} duplicates")
        return deduplicated
    
    def run(self):
        self._log("="*60)
        self._log("News Fetcher - Last 30 Days")
        self._log("="*60)
        
        all_news = self.fetch_all()
        
        if not all_news:
            self._log("No news collected")
            return {}
        
        filtered_news = self.filter_last_30_days(all_news)
        deduped_news = self.deduplicate(filtered_news)
        deduped_news.sort(key=lambda x: x['timestamp'])
        
        by_category = {}
        for h in deduped_news:
            cat = h['category']
            if cat not in by_category:
                by_category[cat] = 0
            by_category[cat] += 1
        
        headlines = []
        for item in deduped_news:
            headlines.append({
                'category': item['category'],
                'ticker': item['ticker'],
                'title': item['title'],
                'time': item['time']
            })
        
        output = {
            'fetch_time': datetime.now().isoformat(),
            'source': 'FinViz',
            'period': 'Last 30 days',
            'total_articles': len(headlines),
            'by_category': by_category,
            'headlines': headlines
        }
        
        self._log(f"\nTotal articles: {len(headlines)}")
        self._log(f"By category: {by_category}")
        self._log("="*60)
        
        out_path = "Fetchers/jsons/news_30days.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        self._log(f"Saved to {out_path}")
        
        return output

if __name__ == "__main__":
    fetcher = NewsFetcher()
    result = fetcher.run()
    
    print("\n" + "="*60)
    if result:
        print(f"SUCCESS: {result['total_articles']} articles fetched")
    else:
        print("WARNING: No articles fetched")
    print("="*60)