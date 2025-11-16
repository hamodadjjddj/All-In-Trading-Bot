import json
import re
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import sqlite3
import hashlib
from urllib.request import urlopen, Request
import feedparser

import spacy
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from textblob import TextBlob

try:
    nltk.data.find('vader_lexicon')
except:
    nltk.download('vader_lexicon', quiet=True)

try:
    nlp = spacy.load("en_core_web_sm")
except:
    import os
    os.system("python -m spacy download en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

TICKERS = {
    "gold": ["GLD", "GOLD", "XAUUSD", "GC_F"],
    "market": ["SPY", "QQQ", "DIA", "IWM"],
    "volatility": ["VIX", "VXX", "VIXY", "UVXY"],
    "dollar": ["DXY", "USDX", "UUP"]
}

SUBREDDITS = ["wallstreetbets", "stocks", "investing", "Gold", "economy", "StockMarket"]

SOURCE_WEIGHTS = {
    "reddit": 0.4,
    "stocktwits": 0.4,
    "twitter": 0.2
}

QUALITY_THRESHOLDS = {
    "reddit_score": 5,
    "min_post_length": 15,
    "max_post_length": 2000,
    "stocktwits_likes": 1
}

NITTER_INSTANCES = [
    "nitter.net",
    "nitter.poast.org",
    "nitter.privacydev.net"
]

class DatabaseManager:
    def __init__(self, db_path="data/social_cache.db"):
        Path("data").mkdir(exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.setup_tables()
    
    def setup_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id TEXT PRIMARY KEY,
                source TEXT,
                ticker TEXT,
                title TEXT,
                content TEXT,
                author TEXT,
                timestamp TEXT,
                score INTEGER,
                url TEXT,
                fetch_time TEXT
            )
        """)
        self.conn.commit()
    
    def add_post(self, post_data):
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO posts 
                (id, source, ticker, title, content, author, timestamp, score, url, fetch_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_data['id'],
                post_data['source'],
                post_data.get('ticker'),
                post_data['title'],
                post_data.get('content', ''),
                post_data['author'],
                post_data['timestamp'],
                post_data.get('score', 0),
                post_data.get('url'),
                datetime.now().isoformat()
            ))
            self.conn.commit()
            return True
        except:
            return False
    
    def close(self):
        self.conn.close()

class RedditFetcher:
    def __init__(self, db_manager):
        self.db = db_manager
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def fetch_posts(self, subreddits, tickers, limit=50):
        posts = []
        
        for subreddit in subreddits:
            try:
                url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
                
                req = Request(url, headers=self.headers)
                response = urlopen(req, timeout=15)
                data = json.loads(response.read().decode())
                
                for child in data['data']['children']:
                    post = child['data']
                    
                    post_id = f"reddit_{post['id']}"
                    title = post.get('title', '')
                    selftext = post.get('selftext', '')
                    
                    matched_ticker = self._match_ticker(title + " " + selftext, tickers)
                    
                    post_data = {
                        'id': post_id,
                        'source': 'reddit',
                        'ticker': matched_ticker,
                        'title': title,
                        'content': selftext[:500],
                        'author': post.get('author', 'unknown'),
                        'timestamp': datetime.fromtimestamp(post['created_utc']).isoformat(),
                        'score': post.get('score', 0),
                        'url': f"https://reddit.com{post['permalink']}",
                        'num_comments': post.get('num_comments', 0)
                    }
                    
                    self.db.add_post(post_data)
                    posts.append(post_data)
                
                print(f"  r/{subreddit}: {len(data['data']['children'])} posts")
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"  r/{subreddit}: Error - {str(e)}")
        
        return posts
    
    def _match_ticker(self, text, tickers):
        text_upper = text.upper()
        for category, ticker_list in tickers.items():
            for ticker in ticker_list:
                patterns = [f"${ticker}", f" {ticker} ", f"${ticker.lower()}", f" {ticker.lower()} "]
                if any(pattern in text_upper or pattern.lower() in text.lower() for pattern in patterns):
                    return ticker
        return None

class StockTwitsFetcher:
    def __init__(self, db_manager):
        self.db = db_manager
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def fetch_posts(self, tickers, limit=30):
        posts = []
        
        all_tickers = []
        for ticker_list in tickers.values():
            all_tickers.extend(ticker_list)
        
        for ticker in all_tickers:
            try:
                url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
                
                req = Request(url, headers=self.headers)
                response = urlopen(req, timeout=15)
                data = json.loads(response.read().decode())
                
                for message in data.get('messages', [])[:limit]:
                    post_id = f"stocktwits_{message['id']}"
                    
                    user = message['user']
                    sentiment = message.get('entities', {}).get('sentiment', {}).get('basic')
                    
                    post_data = {
                        'id': post_id,
                        'source': 'stocktwits',
                        'ticker': ticker,
                        'title': message['body'],
                        'content': message['body'],
                        'author': user['username'],
                        'timestamp': message['created_at'],
                        'score': message.get('likes', {}).get('total', 0),
                        'url': f"https://stocktwits.com/{user['username']}/message/{message['id']}",
                        'stocktwits_sentiment': sentiment
                    }
                    
                    self.db.add_post(post_data)
                    posts.append(post_data)
                
                print(f"  {ticker}: {len(data.get('messages', [])[:limit])} messages")
                time.sleep(random.uniform(2, 3))
                
            except Exception as e:
                print(f"  {ticker}: Error - {str(e)}")
        
        return posts

class NitterFetcher:
    def __init__(self, db_manager):
        self.db = db_manager
        self.current_instance = 0
    
    def fetch_posts(self, tickers, limit=20):
        posts = []
        
        all_tickers = []
        for ticker_list in tickers.values():
            all_tickers.extend(ticker_list[:3])
        
        for ticker in all_tickers:
            try:
                instance = NITTER_INSTANCES[self.current_instance % len(NITTER_INSTANCES)]
                url = f"https://{instance}/search/rss?f=tweets&q=%24{ticker.replace('_F', '').replace('USDX', 'DXY')}"
                
                feed = feedparser.parse(url)
                
                for entry in feed.entries[:limit]:
                    post_id = f"twitter_{hashlib.md5(entry.link.encode()).hexdigest()}"
                    
                    post_data = {
                        'id': post_id,
                        'source': 'twitter',
                        'ticker': ticker,
                        'title': entry.get('title', ''),
                        'content': entry.get('summary', ''),
                        'author': entry.get('author', 'unknown'),
                        'timestamp': datetime(*entry.published_parsed[:6]).isoformat() if hasattr(entry, 'published_parsed') else datetime.now().isoformat(),
                        'score': 0,
                        'url': entry.get('link', '')
                    }
                    
                    self.db.add_post(post_data)
                    posts.append(post_data)
                
                print(f"  ${ticker}: {len(feed.entries[:limit])} tweets")
                self.current_instance += 1
                time.sleep(random.uniform(3, 5))
                
            except Exception as e:
                print(f"  ${ticker}: Error - {str(e)}")
        
        return posts

class NoiseFilter:
    def __init__(self, thresholds):
        self.thresholds = thresholds
        self.seen_hashes = {}
    
    def filter_posts(self, posts):
        filtered = []
        
        for post in posts:
            if not self._passes_quality_check(post):
                continue
            
            if self._is_spam_or_bot(post):
                continue
            
            if self._is_duplicate(post):
                continue
            
            post['cleaned_title'] = self._clean_text(post['title'])
            
            if len(post['cleaned_title']) < self.thresholds['min_post_length']:
                continue
            
            filtered.append(post)
        
        return filtered
    
    def _passes_quality_check(self, post):
        if post['source'] == 'reddit':
            if post.get('score', 0) < self.thresholds['reddit_score']:
                return False
        
        if post['source'] == 'stocktwits':
            if post.get('score', 0) < self.thresholds['stocktwits_likes']:
                return False
        
        text_len = len(post['title'])
        if text_len > self.thresholds['max_post_length']:
            return False
        
        return True
    
    def _is_spam_or_bot(self, post):
        text = post['title'].lower()
        
        spam_keywords = ['click here', 'buy now', 'limited offer', 'dm me', 'check bio']
        if any(keyword in text for keyword in spam_keywords):
            return True
        
        url_count = len(re.findall(r'http[s]?://', text))
        if url_count > 2:
            return True
        
        if len(text.split()) < 3:
            return True
        
        return False
    
    def _is_duplicate(self, post):
        text_hash = hashlib.md5(post['title'].encode()).hexdigest()
        
        if text_hash in self.seen_hashes:
            time_diff = (datetime.fromisoformat(post['timestamp']) - 
                        datetime.fromisoformat(self.seen_hashes[text_hash])).days
            if time_diff < 3:
                return True
        
        self.seen_hashes[text_hash] = post['timestamp']
        return False
    
    def _clean_text(self, text):
        text = re.sub(r'http[s]?://\S+', '', text)
        text = re.sub(r'#\w+', '', text)
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]+', '', text)
        return ' '.join(text.split()).strip()

class SentimentProcessor:
    def __init__(self):
        self.sia = SentimentIntensityAnalyzer()
        self.nlp = nlp
    
    def analyze(self, posts):
        for post in posts:
            text = post['cleaned_title']
            
            vader_score = self.sia.polarity_scores(text)['compound']
            
            blob = TextBlob(text)
            textblob_score = blob.sentiment.polarity
            
            combined_score = (vader_score + textblob_score) / 2
            
            if post['source'] == 'stocktwits' and post.get('stocktwits_sentiment'):
                if post['stocktwits_sentiment'] == 'Bullish':
                    combined_score = (combined_score + 0.5) / 2
                elif post['stocktwits_sentiment'] == 'Bearish':
                    combined_score = (combined_score - 0.5) / 2
            
            post['sentiment_score'] = round(combined_score, 4)
            
            post['entities'] = self._extract_entities(text)
            post['events'] = self._extract_events(text)
        
        return posts
    
    def _extract_entities(self, text):
        doc = self.nlp(text)
        entities = {}
        for ent in doc.ents:
            if ent.label_ in ['PERSON', 'ORG', 'GPE', 'DATE', 'MONEY']:
                if ent.label_ not in entities:
                    entities[ent.label_] = []
                entities[ent.label_].append(ent.text)
        return entities
    
    def _extract_events(self, text):
        text_lower = text.lower()
        events = []
        
        event_keywords = {
            'fed_meeting': ['fomc', 'fed meeting', 'powell'],
            'rate_decision': ['rate cut', 'rate hike', 'interest rate'],
            'earnings': ['earnings', 'eps', 'quarterly'],
            'geopolitical': ['war', 'tariff', 'sanctions', 'election'],
            'market_crash': ['crash', 'selloff', 'panic', 'collapse'],
            'rally': ['rally', 'moon', 'breakout', 'ath']
        }
        
        for event_type, keywords in event_keywords.items():
            if any(kw in text_lower for kw in keywords):
                events.append(event_type)
        
        return list(set(events))

class MomentumTracker:
    def calculate_momentum(self, posts, timeframe_hours=24):
        now = datetime.now()
        cutoff = now - timedelta(hours=timeframe_hours)
        
        ticker_mentions = defaultdict(int)
        hourly_mentions = defaultdict(lambda: defaultdict(int))
        
        for post in posts:
            try:
                post_time = datetime.fromisoformat(post['timestamp'].replace('Z', '+00:00'))
            except:
                continue
            
            if post_time < cutoff:
                continue
            
            ticker = post.get('ticker')
            if ticker:
                ticker_mentions[ticker] += 1
                hour_bucket = post_time.replace(minute=0, second=0, microsecond=0)
                hourly_mentions[ticker][hour_bucket] += 1
        
        momentum_scores = {}
        for ticker, mentions in ticker_mentions.items():
            hourly_data = list(hourly_mentions[ticker].values())
            if len(hourly_data) > 1:
                avg_mentions = sum(hourly_data) / len(hourly_data)
                recent_mentions = hourly_data[-1] if hourly_data else 0
                momentum_scores[ticker] = round(recent_mentions / (avg_mentions + 1), 2)
            else:
                momentum_scores[ticker] = 1.0
        
        return {
            'ticker_mentions': dict(ticker_mentions),
            'momentum_scores': momentum_scores
        }

class InfluenceScorer:
    def score_posts(self, posts):
        for post in posts:
            base_score = 0.5
            
            if post['source'] == 'reddit':
                score_factor = min(post.get('score', 0) / 100, 1.0) * 0.3
                comment_factor = min(post.get('num_comments', 0) / 50, 1.0) * 0.2
                base_score += score_factor + comment_factor
            
            elif post['source'] == 'stocktwits':
                like_factor = min(post.get('score', 0) / 20, 1.0) * 0.4
                base_score += like_factor
            
            elif post['source'] == 'twitter':
                base_score += 0.2
            
            post['influence_score'] = round(min(base_score, 1.0), 2)
        
        return posts

class RegimeClassifier:
    def calculate_fear_greed(self, sentiments):
        if not sentiments:
            return {"fear": 50, "greed": 50}
        
        avg = sum(sentiments) / len(sentiments)
        
        greed_pct = int(((avg + 1) / 2) * 100)
        fear_pct = 100 - greed_pct
        
        return {"fear": fear_pct, "greed": greed_pct}
        if not posts:
            return "Normal"
        
        sentiments = [p['sentiment_score'] for p in posts if 'sentiment_score' in p]
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        
        momentum_values = list(momentum_data['momentum_scores'].values())
        avg_momentum = sum(momentum_values) / len(momentum_values) if momentum_values else 1.0
        
        if avg_sentiment < -0.3 and avg_momentum > 2.0:
            return "Elevated Fear"
        elif avg_sentiment > 0.4 and avg_momentum > 3.0:
            return "Euphoria"
        elif abs(avg_sentiment) < 0.1:
            return "Disagreement"
        elif avg_momentum > 5.0:
            return "Volatility Warning"
        elif avg_sentiment > 0.5:
            return "Hype Cycle"
        else:
            return "Normal"

class SocialSentimentLayer:
    def __init__(self):
        self.db = DatabaseManager()
        self.reddit_fetcher = RedditFetcher(self.db)
        self.stocktwits_fetcher = StockTwitsFetcher(self.db)
        self.nitter_fetcher = NitterFetcher(self.db)
        self.noise_filter = NoiseFilter(QUALITY_THRESHOLDS)
        self.sentiment_processor = SentimentProcessor()
        self.momentum_tracker = MomentumTracker()
        self.influence_scorer = InfluenceScorer()
        self.regime_classifier = RegimeClassifier()
        self.output_dir = Path("data")
        self.output_dir.mkdir(exist_ok=True)
    
    def run(self):
        print("="*60)
        print("Social Media Sentiment Layer - No API Keys")
        print("="*60)
        
        print("\nFetching Reddit posts...")
        reddit_posts = self.reddit_fetcher.fetch_posts(SUBREDDITS, TICKERS, limit=50)
        print(f"Fetched {len(reddit_posts)} Reddit posts")
        
        print("\nFetching StockTwits posts...")
        stocktwits_posts = self.stocktwits_fetcher.fetch_posts(TICKERS, limit=30)
        print(f"Fetched {len(stocktwits_posts)} StockTwits posts")
        
        print("\nFetching Twitter posts via Nitter...")
        twitter_posts = self.nitter_fetcher.fetch_posts(TICKERS, limit=20)
        print(f"Fetched {len(twitter_posts)} Twitter posts")
        
        all_posts = reddit_posts + stocktwits_posts + twitter_posts
        print(f"\nTotal raw posts: {len(all_posts)}")
        
        print("\nFiltering noise...")
        filtered_posts = self.noise_filter.filter_posts(all_posts)
        print(f"After filtering: {len(filtered_posts)} posts")
        
        print("\nAnalyzing sentiment...")
        analyzed_posts = self.sentiment_processor.analyze(filtered_posts)
        
        print("\nScoring influence...")
        scored_posts = self.influence_scorer.score_posts(analyzed_posts)
        
        timeframes = {
            "30D": self._filter_timeframe(scored_posts, days=30),
            "7D": self._filter_timeframe(scored_posts, days=7),
            "24H": self._filter_timeframe(scored_posts, hours=24)
        }
        
        output = {}
        
        for tf_name, tf_posts in timeframes.items():
            tf_posts.sort(key=lambda x: x['timestamp'])
            
            momentum_data = self.momentum_tracker.calculate_momentum(tf_posts, 
                timeframe_hours=24 if tf_name == "24H" else 168 if tf_name == "7D" else 720)
            
            regime = self.regime_classifier.classify(tf_posts, momentum_data)
            
            sentiments = [p['sentiment_score'] for p in tf_posts]
            fear_greed = self.regime_classifier.calculate_fear_greed(sentiments)
            
            clean_posts = []
            for p in tf_posts:
                clean_posts.append({
                    'source': p['source'],
                    'title': p['cleaned_title'],
                    'time': p['timestamp']
                })
            
            avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
            
            by_source = defaultdict(int)
            for p in tf_posts:
                by_source[p['source']] += 1
            
            if tf_name == "24H":
                high_influence = [p for p in tf_posts if p.get('influence_score', 0) > 0.7]
                event_posts = [p for p in tf_posts if p.get('events')]
                
                output[tf_name] = {
                    'timeframe': tf_name,
                    'total_posts': len(tf_posts),
                    'by_source': dict(by_source),
                    'fear_greed_index': fear_greed,
                    'sentiment_regime': regime,
                    'momentum': momentum_data,
                    'posts': clean_posts,
                    'high_influence_posts': [
                        {
                            'source': p['source'],
                            'title': p['cleaned_title'],
                            'time': p['timestamp'],
                            'influence_score': p['influence_score']
                        } for p in high_influence[:10]
                    ],
                    'event_driven_posts': [
                        {
                            'source': p['source'],
                            'title': p['cleaned_title'],
                            'time': p['timestamp'],
                            'events': p['events']
                        } for p in event_posts[:10]
                    ]
                }
            else:
                output[tf_name] = {
                    'timeframe': tf_name,
                    'total_posts': len(tf_posts),
                    'by_source': dict(by_source),
                    'fear_greed_index': fear_greed,
                    'posts': clean_posts
                }
            
            print(f"\n{tf_name}: {len(tf_posts)} posts, Fear: {fear_greed['fear']}% / Greed: {fear_greed['greed']}%, regime: {regime}")
        
        total_posts = sum(result[tf]['total_posts'] for tf in ['30D', '7D', '24H'])
        
        benchmarks = {
            "data_sources": len(set([p['source'] for tf in ['30D', '7D', '24H'] for p in result[tf]['posts']])),
            "total_posts_collected": total_posts,
            "reddit_posts": sum(result[tf]['by_source'].get('reddit', 0) for tf in ['30D', '7D', '24H']),
            "stocktwits_posts": sum(result[tf]['by_source'].get('stocktwits', 0) for tf in ['30D', '7D', '24H']),
            "twitter_posts": sum(result[tf]['by_source'].get('twitter', 0) for tf in ['30D', '7D', '24H']),
            "noise_filtered": "yes",
            "deduplication": "3-day window",
            "sentiment_engine": "VADER + TextBlob",
            "24h_special_analysis": {
                "high_influence_tracked": len(result['24H'].get('high_influence_posts', [])),
                "event_driven_tracked": len(result['24H'].get('event_driven_posts', []))
            }
        }
        
        final_output = {
            'benchmarks': benchmarks,
            'fetch_time': datetime.now().isoformat(),
            'sources': ['reddit', 'stocktwits', 'twitter'],
            'source_weights': SOURCE_WEIGHTS,
            **output
        }
        
        out_path = self.output_dir / "social_sentiment_layer.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(final_output, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"Saved to {out_path}")
        print("="*60)
        
        self.db.close()
        return final_output
    
    def _filter_timeframe(self, posts, hours=None, days=None):
        now = datetime.now()
        
        if hours:
            cutoff = now - timedelta(hours=hours)
            return [p for p in posts if self._parse_time(p['timestamp']) >= cutoff]
        elif days:
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff = today_start - timedelta(days=days)
            return [p for p in posts if cutoff <= self._parse_time(p['timestamp']) < today_start]
        
        return posts
    
    def _parse_time(self, timestamp):
        try:
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            return datetime.now()

if __name__ == "__main__":
    layer = SocialSentimentLayer()
    result = layer.run()
    
    total = sum(result[tf]['total_posts'] for tf in ['30D', '7D', '24H'])
    print(f"\n✓ Total posts across all timeframes: {total}")