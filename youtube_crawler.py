import os
from pytube import YouTube
import json
import sqlite3
from datetime import datetime
import time
from pathlib import Path
import concurrent.futures
from langdetect import detect
import argparse
import random
import logging
from urllib.parse import urlparse, parse_qs, quote
import requests
from bs4 import BeautifulSoup
import backoff

class RateLimiter:
    def __init__(self, max_per_minute=30):
        self.max_per_minute = max_per_minute
        self.requests = []
        
    def wait(self):
        now = time.time()
        minute_ago = now - 60
        
        # Remove requests older than 1 minute
        self.requests = [req_time for req_time in self.requests if req_time > minute_ago]
        
        if len(self.requests) >= self.max_per_minute:
            sleep_time = self.requests[0] - minute_ago
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.requests = self.requests[1:]
            
        self.requests.append(now)

class YouTubeAudiobookCrawler:
    def __init__(self, min_duration_minutes=60, db_path='data/crawler_data.db',
                 max_workers=2, user_agent=None):
        self.min_duration = min_duration_minutes * 60
        self.db_path = db_path
        self.max_workers = max_workers
        self.rate_limiter = RateLimiter(max_per_minute=30)
        self.setup_logging()
        self.initialize_database()
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        if user_agent:
            self.user_agents.append(user_agent)

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('crawler.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def initialize_database(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.executescript('''
            CREATE TABLE IF NOT EXISTS crawled_videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                channel_id TEXT,
                channel_title TEXT,
                duration_seconds INTEGER,
                view_count INTEGER,
                language TEXT,
                quality TEXT,
                filesize_bytes INTEGER,
                first_crawled_at TEXT,
                last_updated_at TEXT,
                error_count INTEGER DEFAULT 0,
                last_error TEXT,
                is_blocked BOOLEAN DEFAULT 0,
                audio_streams TEXT,
                subtitles_available BOOLEAN,
                age_restricted BOOLEAN,
                rating REAL,
                description TEXT,
                keywords TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_duration ON crawled_videos(duration_seconds);
            CREATE INDEX IF NOT EXISTS idx_language ON crawled_videos(language);
            CREATE INDEX IF NOT EXISTS idx_channel ON crawled_videos(channel_id);
            CREATE INDEX IF NOT EXISTS idx_blocked ON crawled_videos(is_blocked);
        ''')
        
        conn.commit()
        conn.close()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, Exception),
        max_tries=5,
        max_time=300
    )
    def make_request(self, url, headers=None):
        self.rate_limiter.wait()
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response

    def get_random_user_agent(self):
        return random.choice(self.user_agents)

    def search_videos(self, query, max_results=50):
        """Search for videos with improved rate limiting and error handling"""
        videos = []
        try:
            search_url = (
                f"https://www.youtube.com/results?search_query={quote(query)}+audiobook"
                "&sp=EgIQAQ%253D%253D"  # Filter for videos
            )
            
            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = self.make_request(search_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for script in soup.find_all('script'):
                if 'ytInitialData' in str(script):
                    data = str(script).split('var ytInitialData = ')[1].split(';</script>')[0]
                    try:
                        json_data = json.loads(data)
                        contents = (json_data.get('contents', {})
                                  .get('twoColumnSearchResultsRenderer', {})
                                  .get('primaryContents', {})
                                  .get('sectionListRenderer', {})
                                  .get('contents', [{}])[0]
                                  .get('itemSectionRenderer', {})
                                  .get('contents', []))
                        
                        for content in contents:
                            if 'videoRenderer' in content:
                                video_id = content['videoRenderer'].get('videoId')
                                if video_id and not self.is_video_crawled(video_id):
                                    videos.append(video_id)
                                    if len(videos) >= max_results:
                                        break
                    except json.JSONDecodeError:
                        continue
            
        except Exception as e:
            self.logger.error(f"Search error: {str(e)}")
        
        # Randomize order to avoid sequential processing
        random.shuffle(videos)
        return videos[:max_results]

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=3,
        max_time=120
    )
    def get_video_metadata(self, video_id):
        if not video_id or self.is_video_blocked(video_id):
            return None

        # Respect rate limits
        self.rate_limiter.wait()
        time.sleep(random.uniform(1, 2))  # Additional random delay

        # Set default headers globally
        import pytube.__main__
        pytube.__main__.request.default_headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }

        try:
            yt = YouTube(f'https://youtube.com/watch?v={video_id}')
            
            # Force video info fetch with retry
            retries = 3
            while retries > 0:
                try:
                    title = yt.title
                    break
                except:
                    retries -= 1
                    time.sleep(2)
            else:
                return None

            audio_streams = [{
                'itag': stream.itag,
                'mime_type': stream.mime_type,
                'abr': stream.abr,
                'filesize': stream.filesize
            } for stream in yt.streams.filter(only_audio=True)]

            if not audio_streams:
                return None

            metadata = {
                'video_id': video_id,
                'title': yt.title,
                'channel_id': yt.channel_id,
                'channel_title': yt.author,
                'duration_seconds': yt.length,
                'view_count': yt.views,
                'description': yt.description,
                'keywords': yt.keywords or [],
                'rating': yt.rating,
                'age_restricted': yt.age_restricted,
                'audio_streams': json.dumps(audio_streams),
                'subtitles_available': bool(yt.captions),
                'language': self.detect_language(f"{yt.title} {yt.description}"),
                'quality': max((stream.abr for stream in yt.streams.filter(only_audio=True)), default=0),
                'filesize_bytes': max((stream.filesize for stream in yt.streams.filter(only_audio=True)), default=0)
            }

            # Skip if duration is too short
            if metadata['duration_seconds'] < self.min_duration:
                return None

            return metadata

        except Exception as e:
            self.logger.error(f"Error processing video {video_id}: {str(e)}")
            if "403" in str(e):
                time.sleep(random.uniform(30, 60))  # Longer delay on rate limit
            return None

    def continuous_search(self, queries, batch_size=25, save_interval_minutes=5, time_limit_minutes=None):
        self.logger.info("Starting continuous search...")
        start_time = time.time()
        
        def check_time_limit():
            if time_limit_minutes:
                elapsed_minutes = (time.time() - start_time) / 60
                return elapsed_minutes >= time_limit_minutes
            return False

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                while not check_time_limit():
                    for query in queries:
                        self.logger.info(f"Searching for: {query}")
                        videos = self.search_videos(query, batch_size)
                        
                        if not videos:
                            continue
                        
                        # Process videos with rate limiting
                        futures = []
                        results = []
                        
                        for video_id in videos:
                            time.sleep(random.uniform(1, 2))  # Rate limit between submissions
                            futures.append(executor.submit(self.process_video, video_id))
                        
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    results.append(result)
                            except Exception as e:
                                self.logger.error(f"Error processing video: {str(e)}")
                        
                        if results:
                            self.save_results_to_json(results)
                        
                        # Check time limit after each query
                        if check_time_limit():
                            break
                        
                        # Sleep between queries
                        time.sleep(random.uniform(5, 10))
                    
                    if not check_time_limit():
                        self.logger.info("Completed search cycle, sleeping...")
                        time.sleep(random.uniform(30, 60))
                        
        except KeyboardInterrupt:
            self.logger.info("Gracefully shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            time.sleep(60)

    def process_video(self, video_id):
        try:
            if self.is_video_crawled(video_id):
                return None

            metadata = self.get_video_metadata(video_id)
            if metadata:
                self.save_video_metadata(metadata)
                self.logger.info(f"Successfully processed: {metadata['title']}")
                return metadata

        except Exception as e:
            self.logger.error(f"Error processing video {video_id}: {str(e)}")
            self.update_video_error(video_id, str(e))
        return None

    def save_results_to_json(self, results):
        if not results:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'data/audiobooks_{timestamp}.json'
        os.makedirs('data', exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'min_duration_minutes': self.min_duration // 60,
                    'total_results': len(results),
                    'crawled_at': datetime.now().isoformat()
                },
                'audiobooks': results
            }, f, ensure_ascii=False, indent=2)
            
        self.logger.info(f"Saved {len(results)} results to {output_file}")

    def is_video_crawled(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT 1 FROM crawled_videos WHERE video_id = ?', (video_id,))
        result = c.fetchone() is not None
        conn.close()
        return result

    def is_video_blocked(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT is_blocked FROM crawled_videos WHERE video_id = ?', (video_id,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else False

    def save_video_metadata(self, metadata):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        now = datetime.now().isoformat()
        
        c.execute('''
            INSERT OR REPLACE INTO crawled_videos 
            (video_id, title, channel_id, channel_title, duration_seconds,
             view_count, language, quality, filesize_bytes, audio_streams,
             subtitles_available, age_restricted, rating, description,
             keywords, first_crawled_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   COALESCE((SELECT first_crawled_at FROM crawled_videos WHERE video_id = ?), ?),
                   ?)
        ''', (
            metadata['video_id'], metadata['title'], metadata['channel_id'],
            metadata['channel_title'], metadata['duration_seconds'],
            metadata['view_count'], metadata['language'], metadata['quality'],
            metadata['filesize_bytes'], metadata['audio_streams'],
            metadata['subtitles_available'], metadata['age_restricted'],
            metadata['rating'], metadata['description'],
            json.dumps(metadata['keywords']),
            metadata['video_id'], now, now
        ))
        
        conn.commit()
        conn.close()

    def update_video_error(self, video_id, error):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            UPDATE crawled_videos 
            SET error_count = error_count + 1,
                last_error = ?,
                last_updated_at = ?,
                is_blocked = CASE 
                    WHEN error_count >= 3 THEN 1 
                    ELSE is_blocked 
                END
            WHERE video_id = ?
        ''', (error, datetime.now().isoformat(), video_id))
        
        conn.commit()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='YouTube Audiobook Crawler')
    parser.add_argument('--min-duration', type=int, default=60,
                      help='Minimum duration in minutes (default: 60)')
    parser.add_argument('--queries', nargs='+', 
                      default=['audiobook', 'full audiobook', 'complete audiobook'],
                      help='Search queries (default: audiobook-related terms)')
    parser.add_argument('--save-interval', type=int, default=5,
                      help='How often to save results (in minutes, default: 5)')
    parser.add_argument('--batch-size', type=int, default=25,
                      help='Number of results to process in each batch (default: 25)')
    parser.add_argument('--max-workers', type=int, default=2,
                      help='Maximum number of concurrent workers (default: 2)')
    parser.add_argument('--user-agent', type=str,
                      help='Custom user agent string')
    
    args = parser.parse_args()
    
    crawler = YouTubeAudiobookCrawler(
        min_duration_minutes=args.min_duration,
        max_workers=args.max_workers,
        user_agent=args.user_agent
    )
    
    crawler.continuous_search(
        queries=args.queries,
        batch_size=args.batch_size,
        save_interval_minutes=args.save_interval,
        time_limit_minutes=8
    )

if __name__ == '__main__':
    main()
