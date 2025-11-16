import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import spacy
import time
import random

try:
    nlp = spacy.load("en_core_web_sm")
except:
    print("Downloading spaCy model...")
    import os
    os.system("python -m spacy download en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

TICKERS = {
    "gold": ["GLD", "IAU", "GLDM", "GDX", "GDXJ", "NUGT", "RING", "SGOL", "AAAU"],
    "market": ["SPY", "QQQ", "DIA", "IWM"],
    "volatility": ["VXX", "VIXY", "UVXY", "VIXM", "SVXY", "SVIX", "UVIX"],
    "dollar": ["UUP", "USDU"]
}

EVENT_KEYWORDS = {
    "meeting": ["FOMC", "Fed meeting", "Fed decision", "central bank meeting", "Powell speech", "ECB meeting"],
    "policy": ["rate cut", "rate hike", "interest rate", "QE", "QT", "quantitative", "dovish", "hawkish"],
    "geopolitical": ["tariff", "trade war", "sanctions", "conflict", "war", "election", "crisis"],
    "market_event": ["crash", "rally", "correction", "ATH", "all-time high", "selloff", "breakout", "earnings"]
}

class NewsSentimentLayer:
    def __init__(self):
        self.nlp = nlp
        self.output_dir = Path("data")
        self.output_dir.mkdir(exist_ok=True)
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
    
    def extract_entities(self, text):
        doc = self.nlp(text)
        entities = {}
        
        for ent in doc.ents:
            if ent.label_ in ['PERSON', 'ORG', 'GPE', 'DATE', 'MONEY', 'CARDINAL']:
                if ent.label_ not in entities:
                    entities[ent.label_] = []
                entities[ent.label_].append(ent.text)
        
        return entities
    
    def extract_events(self, text):
        text_lower = text.lower()
        events = []
        
        for event_type, keywords in EVENT_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    events.append(keyword)
        
        return list(set(events))
    
    def is_question(self, text):
        return '?' in text
    
    def fetch_all(self):
        all_news = []
        
        for category, tickers in TICKERS.items():
            self._log(f"Fetching {category.upper()} news ({len(tickers)} tickers)...")
            for ticker in tickers:
                news = self.fetch_finviz(ticker, category)
                all_news.extend(news)
        
        self._log(f"Total raw articles: {len(all_news)}")
        return all_news
    
    def dedup_by_category(self, news, timeframe_name):
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
        
        self._log(f"{timeframe_name}: Removed {total_removed} duplicates within categories")
        return deduplicated
    
    def dedup_across_timeframes(self, timeframes):
        all_titles = {}
        
        for tf_name in ['30D', '7D', '24H']:
            if tf_name not in timeframes:
                continue
            for item in timeframes[tf_name]:
                title = item['title']
                if title not in all_titles:
                    all_titles[title] = []
                all_titles[title].append({'tf': tf_name, 'item': item})
        
        removed_count = {}
        for tf in ['30D', '7D', '24H']:
            removed_count[tf] = 0
        
        for title, occurrences in all_titles.items():
            if len(occurrences) <= 1:
                continue
            
            tfs_present = [occ['tf'] for occ in occurrences]
            
            if '24H' in tfs_present and '7D' in tfs_present:
                for occ in occurrences:
                    if occ['tf'] == '7D':
                        if occ['item'] in timeframes['7D']:
                            timeframes['7D'].remove(occ['item'])
                            removed_count['7D'] += 1
            
            if '24H' in tfs_present and '30D' in tfs_present:
                for occ in occurrences:
                    if occ['tf'] == '30D':
                        if occ['item'] in timeframes['30D']:
                            timeframes['30D'].remove(occ['item'])
                            removed_count['30D'] += 1
            
            if '7D' in tfs_present and '30D' in tfs_present and '24H' not in tfs_present:
                for occ in occurrences:
                    if occ['tf'] == '30D':
                        if occ['item'] in timeframes['30D']:
                            timeframes['30D'].remove(occ['item'])
                            removed_count['30D'] += 1
        
        self._log(f"Cross-timeframe dedup: 30D removed {removed_count['30D']}, 7D removed {removed_count['7D']}, 24H removed {removed_count['24H']}")
        return timeframes
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
        
        self._log(f"{timeframe_name}: Removed {total_removed} duplicates within categories")
        return deduplicated
    
    def filter_timeframe(self, news, hours=None, days=None):
        now = datetime.now()
        
        if hours:
            cutoff = now - timedelta(hours=hours)
            filtered = [n for n in news if n['timestamp'] >= cutoff]
        elif days:
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff = today_start - timedelta(days=days)
            filtered = [n for n in news if cutoff <= n['timestamp'] < today_start]
        else:
            return news
        
        filtered.sort(key=lambda x: x['timestamp'])
        return filtered
    
    def process_headlines(self, news):
        processed = []
        
        for item in news:
            processed.append({
                'category': item['category'],
                'ticker': item['ticker'],
                'title': item['title'],
                'time': item['time'],
                'is_question': self.is_question(item['title'])
            })
        
        return processed
    
    def process_for_aggregation(self, news):
        for item in news:
            item['entities'] = self.extract_entities(item['title'])
            item['events'] = self.extract_events(item['title'])
        return news
    
    def detect_contradictions(self, headlines):
        contradictions = []
        
        risk_on = []
        risk_off = []
        fed_dovish = []
        fed_hawkish = []
        
        for h in headlines:
            title_lower = h['title'].lower()
            
            if any(w in title_lower for w in ['risk-on', 'risk on', 'rally', 'bullish sentiment']):
                risk_on.append(h)
            if any(w in title_lower for w in ['risk-off', 'risk off', 'safe haven', 'flight to safety']):
                risk_off.append(h)
            
            if any(w in title_lower for w in ['dovish', 'rate cut', 'easing', 'stimulus']):
                fed_dovish.append(h)
            if any(w in title_lower for w in ['hawkish', 'rate hike', 'tightening', 'restrictive']):
                fed_hawkish.append(h)
        
        if risk_on and risk_off:
            contradictions.append({
                'type': 'risk_sentiment',
                'risk_on_count': len(risk_on),
                'risk_off_count': len(risk_off),
                'risk_on_samples': [h['title'] for h in risk_on[:2]],
                'risk_off_samples': [h['title'] for h in risk_off[:2]]
            })
        
        if fed_dovish and fed_hawkish:
            contradictions.append({
                'type': 'fed_stance',
                'dovish_count': len(fed_dovish),
                'hawkish_count': len(fed_hawkish),
                'dovish_samples': [h['title'] for h in fed_dovish[:2]],
                'hawkish_samples': [h['title'] for h in fed_hawkish[:2]]
            })
        
        return contradictions
    
    def aggregate_entities(self, headlines):
        entity_freq = {}
        
        for h in headlines:
            for ent_type, ent_list in h['entities'].items():
                if ent_type not in entity_freq:
                    entity_freq[ent_type] = {}
                for ent in ent_list:
                    if ent not in entity_freq[ent_type]:
                        entity_freq[ent_type][ent] = 0
                    entity_freq[ent_type][ent] += 1
        
        for ent_type in entity_freq:
            entity_freq[ent_type] = dict(sorted(entity_freq[ent_type].items(), key=lambda x: x[1], reverse=True))
        
        return entity_freq
    
    def aggregate_events(self, headlines):
        event_freq = {}
        
        for h in headlines:
            for event in h['events']:
                if event not in event_freq:
                    event_freq[event] = 0
                event_freq[event] += 1
        
        return dict(sorted(event_freq.items(), key=lambda x: x[1], reverse=True))
    
    def run(self):
        self._log("="*60)
        self._log("High-Quality 24H News Intelligence Layer")
        self._log("="*60)
        
        all_news = self.fetch_all()
        
        if not all_news:
            self._log("No news collected")
            return {}
        
        timeframes = {
            "30D": self.filter_timeframe(all_news, days=30),
            "7D": self.filter_timeframe(all_news, days=7),
            "24H": self.filter_timeframe(all_news, hours=24)
        }
        
        self._log(f"\nBefore cross-timeframe dedup: 30D={len(timeframes['30D'])}, 7D={len(timeframes['7D'])}, 24H={len(timeframes['24H'])}")
        timeframes = self.dedup_across_timeframes(timeframes)
        self._log(f"After cross-timeframe dedup: 30D={len(timeframes['30D'])}, 7D={len(timeframes['7D'])}, 24H={len(timeframes['24H'])}")
        
        output = {}
        
        for tf_name, tf_news in timeframes.items():
            self._log(f"\nProcessing {tf_name}...")
            
            deduped = self.dedup_by_category(tf_news, tf_name)
            
            deduped.sort(key=lambda x: x['timestamp'], reverse=False)
            
            for_aggregation = self.process_for_aggregation(list(deduped))
            contradictions = self.detect_contradictions(for_aggregation)
            entity_freq = self.aggregate_entities(for_aggregation)
            event_freq = self.aggregate_events(for_aggregation)
            
            processed = self.process_headlines(deduped)
            
            by_category = {}
            for h in processed:
                cat = h['category']
                if cat not in by_category:
                    by_category[cat] = 0
                by_category[cat] += 1
            
            output[tf_name] = {
                'timeframe': tf_name,
                'total_articles': len(processed),
                'by_category': by_category,
                'raw_headlines': processed,
                'contradictions': contradictions,
                'entity_frequency': entity_freq,
                'event_frequency': event_freq
            }
            
            self._log(f"{tf_name}: {len(processed)} articles")
            self._log(f"  Categories: {by_category}")
            self._log(f"  Contradictions: {len(contradictions)}")
            self._log(f"  Top events: {list(event_freq.keys())[:5]}")
        
        self._log("="*60)
        
        final_output = {
            'fetch_time': datetime.now().isoformat(),
            'source': 'FinViz',
            'categories': TICKERS,
            **output
        }
        
        out_path = self.output_dir / "news_sentiment_layer.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=2, ensure_ascii=False)
        self._log(f"Saved to {out_path}")
        
        log_path = self.output_dir / "fetch_logs.txt"
        with open(log_path, 'w') as f:
            f.write('\n'.join(self.logs))
        
        return final_output

if __name__ == "__main__":
    layer = NewsSentimentLayer()
    result = layer.run()
    
    print("\n" + "="*60)
    if result:
        total = sum(result[tf]['total_articles'] for tf in ['30D', '7D', '24H'])
        print(f"SUCCESS: {total} articles across all timeframes")
        print(f"24H focus: {result['24H']['total_articles']} articles")
    else:
        print("WARNING: No articles fetched")
    print("="*60)