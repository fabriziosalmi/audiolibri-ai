import os
from pytube import YouTube, Search
from pytube.exceptions import PytubeError
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
from urllib.parse import urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
import re

class YouTubeAudiobookCrawler:
    def __init__(self, min_duration_minutes=60, db_path='crawler_data.db',
                 max_workers=4, user_agent=None):
        """
        Initialize the crawler
        
        Args:
            min_duration_minutes (int): Minimum duration in minutes to consider
            db_path (str): Path to SQLite database
            max_workers (int): Maximum number of concurrent workers
            user_agent (str): Custom user agent string
        """
        self.min_duration = min_duration_minutes * 60
        self.db_path = db_path
        self.max_workers = max_workers
        self.initialize_database()
        self.setup_logging()
        
        # Rotate between different user agents to avoid blocks
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        if user_agent:
            self.user_agents.append(user_agent)
            
        # Initialize proxy list (you can add more)
        self.proxies = []
        
    def setup_logging(self):
        """Configure logging"""
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
        """Initialize SQLite database with improved schema"""
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
            
            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT PRIMARY KEY,
                title TEXT,
                subscriber_count INTEGER,
                video_count INTEGER,
                last_checked TEXT,
                is_audiobook_channel BOOLEAN,
                language TEXT,
                country TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_duration ON crawled_videos(duration_seconds);
            CREATE INDEX IF NOT EXISTS idx_language ON crawled_videos(language);
            CREATE INDEX IF NOT EXISTS idx_channel ON crawled_videos(channel_id);
        ''')
        
        conn.commit()
        conn.close()

    def get_random_user_agent(self):
        """Get a random user agent from the pool"""
        return random.choice(self.user_agents)
        
    def get_random_proxy(self):
        """Get a random proxy from the pool"""
        return random.choice(self.proxies) if self.proxies else None

    def detect_language(self, text):
        """Detect language with error handling"""
        try:
            return detect(text)
        except:
            return 'unknown'

    def extract_video_id(self, url):
        """Extract video ID from various YouTube URL formats"""
        if not url:
            return None
            
        parsed = urlparse(url)
        if parsed.hostname == 'youtu.be':
            return parsed.path[1:]
        if parsed.hostname in ('www.youtube.com', 'youtube.com'):
            if parsed.path == '/watch':
                return parse_qs(parsed.query)['v'][0]
            if parsed.path[:7] == '/embed/':
                return parsed.path.split('/')[2]
            if parsed.path[:3] == '/v/':
                return parsed.path.split('/')[2]
        return None

    def get_video_metadata(self, url_or_id):
        """Get comprehensive video metadata using pytube"""
        try:
            video_id = self.extract_video_id(url_or_id) if '/' in url_or_id else url_or_id
            if not video_id:
                return None

            if self.is_video_blocked(video_id):
                return None

            # Add random delay between requests
            time.sleep(random.uniform(1, 3))

            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }

            yt = YouTube(
                f'https://youtube.com/watch?v={video_id}',
                use_oauth=False,
                allow_oauth_cache=True,
                headers=headers
            )

            # Get all available audio stream information
            audio_streams = [{
                'itag': stream.itag,
                'mime_type': stream.mime_type,
                'abr': stream.abr,
                'filesize': stream.filesize
            } for stream in yt.streams.filter(only_audio=True)]

            metadata = {
                'video_id': video_id,
                'title': yt.title,
                'channel_id': yt.channel_id,
                'channel_title': yt.author,
                'duration_seconds': yt.length,
                'view_count': yt.views,
                'description': yt.description,
                'keywords': yt.keywords,
                'rating': yt.rating,
                'age_restricted': yt.age_restricted,
                'audio_streams': json.dumps(audio_streams),
                'subtitles_available': bool(yt.captions),
                'language': self.detect_language(f"{yt.title} {yt.description}"),
                'quality': max((stream.abr for stream in yt.streams.filter(only_audio=True)), default=0),
                'filesize_bytes': max((stream.filesize for stream in yt.streams.filter(only_audio=True)), default=0)
            }

            return metadata

        except Exception as e:
            self.logger.error(f"Error processing video {url_or_id}: {str(e)}")
            if "403" in str(e):
                time.sleep(random.uniform(5, 10))  # Longer delay on 403
            self.update_video_error(url_or_id, str(e))
            return None

    def search_videos(self, query, max_results=100):
        """Search for videos with improved error handling"""
        videos = []
        try:
            s = Search(query)
            while len(videos) < max_results:
                try:
                    # Get next batch of results
                    results = s.results
                    if not results:
                        break
                        
                    videos.extend(results)
                    
                    # Try to get more results
                    s.get_next_results()
                    
                except Exception as e:
                    self.logger.warning(f"Error getting next batch: {str(e)}")
                    break
                    
        except Exception as e:
            self.logger.error(f"Search error: {str(e)}")
            
        return videos[:max_results]

    def process_video(self, video_id):
        """Process a single video with comprehensive error handling"""
        try:
            if self.is_video_crawled(video_id):
                return None

            metadata = self.get_video_metadata(video_id)
            if not metadata:
                return None

            if metadata['duration_seconds'] < self.min_duration:
                return None

            self.save_video_metadata(metadata)
            self.update_channel_info(metadata['channel_id'], metadata['channel_title'])
            
            return metadata

        except Exception as e:
            self.logger.error(f"Error processing video {video_id}: {str(e)}")
            self.update_video_error(video_id, str(e))
            return None

    def continuous_search(self, queries, batch_size=50, save_interval_minutes=60, time_limit_minutes=None):
        """Continuously search for videos with multiple queries"""
        self.logger.info("Starting continuous search...")
        
        while True:
            try:
                for query in queries:
                    self.logger.info(f"Searching for: {query}")
                    videos = self.search_videos(query, batch_size)
                    
                    # Process videos concurrently
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [executor.submit(self.process_video, video.video_id) 
                                 for video in videos]
                        
                        # Collect results
                        results = []
                        for future in concurrent.futures.as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    results.append(result)
                            except Exception as e:
                                self.logger.error(f"Error processing video: {str(e)}")
                    
                    if results:
                        self.save_results_to_json(results)
                        
                    # Sleep between queries to avoid rate limiting
                    time.sleep(random.uniform(10, 30))
                    
                # Sleep between search cycles
                self.logger.info("Completed search cycle, sleeping...")
                if check_time_limit():
                    self.logger.info("Time limit reached, shutting down...")
                    break
                    
                if not time_limit_minutes:
                    time.sleep(save_interval_minutes * 60)
                
            except KeyboardInterrupt:
                self.logger.info("Gracefully shutting down...")
                break
                
            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                time.sleep(300)

    def save_results_to_json(self, results):
        """Save results to JSON with improved formatting"""
        if not results:
            return
            
        output_file = f'audiobooks_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'min_duration_minutes': self.min_duration // 60,
                    'total_results': len(results),
                    'crawled_at': datetime.now().isoformat(),
                    'query': results[0].get('query', 'unknown')
                },
                'audiobooks': results
            }, f, ensure_ascii=False, indent=2)
            
        self.logger.info(f"Saved {len(results)} results to {output_file}")

    def is_video_crawled(self, video_id):
        """Check if video has been crawled"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT video_id FROM crawled_videos WHERE video_id = ?', (video_id,))
        result = c.fetchone() is not None
        conn.close()
        return result

    def is_video_blocked(self, video_id):
        """Check if video is blocked"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT is_blocked FROM crawled_videos WHERE video_id = ?', (video_id,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else False

    def save_video_metadata(self, metadata):
        """Save video metadata to database"""
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
        """Update video error information"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            UPDATE crawled_videos 
            SET error_count = error_count + 1,
                last_error = ?,
                last_updated_at = ?
            WHERE video_id = ?
        ''', (error, datetime.now().isoformat(), video_id))
        
        conn.commit()
        conn.close()

    def update_channel_info(self, channel_id, channel_title):
        """Update channel information"""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        c.execute('''
            INSERT OR REPLACE INTO channels 
            (channel_id, title, last_checked)
            VALUES (?, ?, ?)
        ''', (channel_id, channel_title, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='YouTube Audiobook Crawler (Pytube Version)')
    parser.add_argument('--min-duration', type=int, default=60,
                      help='Minimum duration in minutes (default: 60)')
    parser.add_argument('--queries', nargs='+', 
                      default=['audiobook', 'full audiobook', 'complete audiobook'],
                      help='Search queries (default: audiobook-related terms)')
    parser.add_argument('--save-interval', type=int, default=60,
                      help='How often to save results (in minutes, default: 60)')
    parser.add_argument('--batch-size', type=int, default=50,
                      help='Number of results to process in each batch (default: 50)')
    parser.add_argument('--max-workers', type=int, default=4,
                      help='Maximum number of concurrent workers (default: 4)')
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
