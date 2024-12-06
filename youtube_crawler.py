from pytube import Search, YouTube
import json
import time
import random
import sqlite3
from datetime import datetime
import os
from langdetect import detect

class AudiobookCrawler:
    def __init__(self, min_duration=60, db_path='data/crawler.db'):
        self.min_duration = min_duration * 60
        self.db_path = db_path
        os.makedirs('data', exist_ok=True)
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                channel TEXT,
                duration INTEGER,
                language TEXT,
                quality TEXT,
                crawled_at TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def is_crawled(self, video_id):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT 1 FROM videos WHERE video_id = ?', (video_id,))
        exists = c.fetchone() is not None
        conn.close()
        return exists

    def save_video(self, data):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''
            INSERT INTO videos VALUES (?,?,?,?,?,?,?)
        ''', (
            data['video_id'],
            data['title'],
            data['channel'],
            data['duration'],
            data['language'],
            data['quality'],
            datetime.now().isoformat()
        ))
        conn.commit()
        conn.close()

    def process_video(self, video_id):
        if self.is_crawled(video_id):
            return None
            
        try:
            time.sleep(random.uniform(2, 4))  # Rate limiting
            yt = YouTube(f'https://youtube.com/watch?v={video_id}')
            
            if yt.length < self.min_duration:
                return None
                
            streams = yt.streams.filter(only_audio=True)
            if not streams:
                return None
                
            data = {
                'video_id': video_id,
                'title': yt.title,
                'channel': yt.author,
                'duration': yt.length,
                'language': detect(yt.title),
                'quality': max(s.abr for s in streams),
            }
            
            self.save_video(data)
            print(f"Saved: {data['title']}")
            return data
            
        except Exception as e:
            print(f"Error processing {video_id}: {str(e)}")
            time.sleep(10)  # Longer delay on error
            return None

    def crawl(self, query='audiobook', max_results=50, time_limit_minutes=8):
        start_time = time.time()
        results = []
        
        while True:
            if (time.time() - start_time) / 60 >= time_limit_minutes:
                break
                
            try:
                s = Search(query)
                videos = [result.video_id for result in s.results]
                
                for video_id in videos[:max_results]:
                    if (time.time() - start_time) / 60 >= time_limit_minutes:
                        break
                    result = self.process_video(video_id)
                    if result:
                        results.append(result)
                
                time.sleep(random.uniform(5, 10))
                
            except Exception as e:
                print(f"Search error: {str(e)}")
                time.sleep(30)
                
            self.save_results(results)
            results = []

    def save_results(self, results):
        if not results:
            return
            
        filename = f'data/audiobooks_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Saved {len(results)} results to {filename}")

if __name__ == '__main__':
    crawler = AudiobookCrawler(min_duration=60)
    crawler.crawl(query='audiobook', max_results=50)
