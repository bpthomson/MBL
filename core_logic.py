import requests
import time
import unicodedata
import csv
import os
import shutil
import re
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from zipfile import ZipFile, ZIP_DEFLATED
import cloudscraper
import xml.etree.ElementTree as ET
from xml.dom import minidom
from config import Config

class BahamutCrawler:
    def __init__(self, user_id):
        self.user_id = user_id
        self.rq = cloudscraper.create_scraper()
        self.rq.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.collection_api = "https://wall.gamer.com.tw/api/user_join_fanpage.php?"
        self.detail_api = "https://api.gamer.com.tw/acg/v1/acg_list.php"

    def get_collections(self):
        acg_list = []
        for star in range(1, 6):
            params = {'userid': self.user_id, 'kind': f'S{star}', 'page': 1, 'category': 4}
            try:
                response = self.rq.get(self.collection_api, params=params).json()
                if 'data' not in response: continue
                data = response['data']
                if 'list' in data:
                    for e in data['list']: acg_list.append({'ch_name': e['name'], 'id': e['id']})
                for page in range(2, data.get('tpage', 1) + 1):
                    params['page'] = page
                    f = self.rq.get(self.collection_api, params=params).json()['data']
                    for e in f['list']: acg_list.append({'ch_name': e['name'], 'id': e['id']})
            except: pass
        return list({v['id']: v for v in acg_list}.values())

    def get_detail(self, sn_id):
        try:
            data = self.rq.get(self.detail_api, params={'sn': sn_id}).json()['data']['acg']['all'].items()
            for sn, info in data:
                det = info.get('detailed', {})
                if det.get('platform', {}).get('value') == '動畫':
                    time_val = det.get('localDebut', {}).get('value')
                    return {
                        'ch_name': info.get('title'),
                        'eng_name': info.get('title_en'),
                        'jp_name': info.get('title_jp'),
                        'year': int(time_val[:4]) if time_val and time_val[:4].isdigit() else None,
                        'month': int(time_val[5:7]) if time_val and len(time_val) >= 7 and time_val[5:7].isdigit() else None,
                        'day': int(time_val[8:]) if time_val and len(time_val) >= 10 and time_val[8:].isdigit() else None
                    }
        except: pass
        return None

    def fetch_all_details(self, simple_list):
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(self.get_detail, [e['id'] for e in simple_list]))
        return [r for r in results if r]


class MalMatcher:
    def __init__(self, cache_file=Config.CACHE_CSV_FILE):
        self.rq = cloudscraper.create_scraper()
        self.search_api = "https://api.jikan.moe/v4/anime"
        self.allowed_types = ['TV', 'MOVIE', 'OVA', 'TV SPECIAL', 'ONA', 'SPECIAL']
        self.cache = {}
        self.cache_file = cache_file
        self.load_cache()

    def load_cache(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, mode='r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('ch_name') and row.get('mal_id'):
                            mal_title = row.get('mal_title')
                            if not mal_title: mal_title = row['ch_name']
                            self.cache[row['ch_name'].strip()] = {
                                'mal_id': int(row['mal_id']),
                                'img_url': row.get('img_url', ''),
                                'mal_title': mal_title
                            }
            except: pass

    def clean_text(self, text):
        if not text: return None
        return unicodedata.normalize("NFKC", str(text)).replace('劇場版', '').strip()

    def search_jikan(self, query):
        if not query: return []
        try:
            time.sleep(0.1) 
            resp = self.rq.get(self.search_api, params={'q': query, 'limit': 5}, timeout=10)
            if resp.status_code == 429:
                time.sleep(0.5)
                return self.search_jikan(query)
            if resp.status_code == 200:
                return resp.json().get('data', [])
        except: pass
        return []

    def get_days_diff(self, api_date_str, target_date):
        if not target_date or not api_date_str: return 99999
        try:
            api_date = datetime.strptime(api_date_str.split('T')[0], "%Y-%m-%d")
            return abs((api_date - target_date).days)
        except: return 99999

    def check_animethemes(self, mal_id, retry=0):
        url = "https://api.animethemes.moe/anime"
        params = {
            "filter[has]": "resources",
            "filter[site]": "MyAnimeList",
            "filter[external_id]": mal_id,
            "include": "animethemes"
        }
        try:
            resp = self.rq.get(url, params=params, timeout=5)
            if resp.status_code == 429:
                if retry > 2: return False
                time.sleep(1)
                return self.check_animethemes(mal_id, retry + 1)
            
            if resp.status_code == 200:
                data = resp.json()
                if data.get('anime', []):
                    return True
        except: pass
        return False

    def resolve_mal_id(self, row):
        ch_name = row.get('ch_name', '').strip()

        if ch_name in self.cache:
            c = self.cache[ch_name]
            img = c.get('img_url') or 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
            return {
                'mal_id': c['mal_id'],
                'title': c['mal_title'],
                'url': f"https://myanimelist.net/anime/{c['mal_id']}",
                'img_url': img
            }, "Cache Hit"

        target_date = None
        if row.get('year'):
            try: target_date = datetime(row['year'], row.get('month', 1), row.get('day', 1))
            except: pass

        queries = []
        if row.get('jp_name'): queries.append((1, 'JP', self.clean_text(row['jp_name'])))
        if row.get('eng_name'): queries.append((2, 'ENG', self.clean_text(row['eng_name'])))
        
        candidates = []
        seen_ids = set()

        for priority, src, q in queries:
            results = self.search_jikan(q)
            for idx, res in enumerate(results):
                if str(res.get('type')).upper() not in self.allowed_types: continue
                if res['mal_id'] in seen_ids: continue
                seen_ids.add(res['mal_id'])

                diff = self.get_days_diff(res.get('aired', {}).get('from'), target_date)
                is_group_1 = (diff <= 30)

                candidates.append({
                    'mal_id': res['mal_id'],
                    'title': res['title'],
                    'img_url': res.get('images', {}).get('jpg', {}).get('image_url'),
                    'url': res['url'],
                    'diff': diff,
                    'is_group_1': is_group_1,
                    'priority': priority, 
                    'idx': idx            
                })

        if not candidates:
            return None, "Not Found"

        candidates.sort(key=lambda x: (
            0 if x['is_group_1'] else 1, 
            x['idx'], 
            x['priority']
        ))
        
        winner = candidates[0]
        has_themes = self.check_animethemes(winner['mal_id'])
        status = ""

        if winner['is_group_1'] and has_themes:
            status = "High Confidence"
        else:
            if not winner['is_group_1']:
                status = f"Low Confidence (Diff {winner['diff']} days)"
            else:
                status = "Low Confidence (No Audio)"

        return winner, status


class MalXmlGenerator:
    def generate_xml(self, anime_data_list, user_id):
        root = ET.Element("myanimelist")
        info = ET.SubElement(root, "myinfo")
        ET.SubElement(info, "user_id").text = str(user_id)
        ET.SubElement(info, "user_name").text = str(user_id)
        ET.SubElement(info, "user_export_type").text = "1"

        for data in anime_data_list:
            anime = ET.SubElement(root, "anime")
            ET.SubElement(anime, "series_animedb_id").text = str(data['mal_id'])
            ET.SubElement(anime, "series_title").text = str(data.get('title', 'Unknown'))
            ET.SubElement(anime, "my_status").text = "Completed"
            ET.SubElement(anime, "my_watched_episodes").text = "0"
            ET.SubElement(anime, "my_start_date").text = "0000-00-00"
            ET.SubElement(anime, "my_finish_date").text = "0000-00-00"
            ET.SubElement(anime, "my_score").text = "0"
            ET.SubElement(anime, "update_on_import").text = "1"

        return minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")


class ThemeDownloader:
    def __init__(self, max_workers=3):
        self.rq = cloudscraper.create_scraper()
        self.rq.headers.update({
            'User-Agent': 'Mozilla/5.0'
        })
        self.search_url = "https://api.animethemes.moe/anime"
        self.max_workers = max_workers

    def sanitize_filename(self, name):
        return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()

    def get_theme_links(self, mal_id, retry=0):
        themes = []
        try:
            params = {
                "filter[has]": "resources",
                "filter[site]": "MyAnimeList",
                "filter[external_id]": mal_id,
                "include": "animethemes,animethemes.song,animethemes.animethemeentries.videos.audio"
            }
            resp = self.rq.get(self.search_url, params=params, timeout=10)
            
            if resp.status_code == 429:
                if retry >= 3: return []
                time.sleep(2 * (retry + 1))
                return self.get_theme_links(mal_id, retry + 1)
                
            if resp.status_code != 200: return []
            data = resp.json().get('anime', [])
            if not data: return []
            
            for t in data[0].get('animethemes', []):
                try:
                    slug = t.get('slug')
                    title = t.get('song', {}).get('title')
                    entry = t.get('animethemeentries', [])[0]
                    video = entry.get('videos', [])[0]
                    audio = video.get('audio', {})
                    link = audio.get('link')
                    mime = audio.get('mimetype', '')
                    ext = 'mp3' if 'mp3' in mime else 'webm' if 'webm' in mime else 'ogg'
                    if link:
                        themes.append({'type': slug, 'title': title, 'link': link, 'ext': ext})
                except: continue
            return themes
        except: return []

    def _download_file(self, url, path):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with self.rq.get(url, stream=True, timeout=20) as r:
                if r.status_code == 200:
                    with open(path, 'wb') as f: shutil.copyfileobj(r.raw, f, length=1024*1024)
                    return True
        except: pass
        return False

    def process_anime_task(self, item, temp_dir):
        songs = self.get_theme_links(item['mal_id'])
        if not songs: return None
        
        s_title = self.sanitize_filename(item['title'])
        count = 0
        for s in songs:
            fname = f"{s['type']} - {self.sanitize_filename(s['title'])}.{s['ext']}"
            full_path = os.path.join(temp_dir, s_title, fname)
            if self._download_file(s['link'], full_path):
                count += 1
        
        return {'title': s_title, 'count': count}

    def download_and_zip_generator(self, data_list, output_path):
        yield {'msg': f"準備處理 {len(data_list)} 部動畫...", 'progress': '0%'}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = {ex.submit(self.process_anime_task, item, temp_dir): item for item in data_list}
                done_count = 0
                total = len(data_list)
                
                for f in as_completed(futures):
                    done_count += 1
                    item_info = futures[f]
                    try:
                        result = f.result()
                        progress_val = int((done_count / total) * 90)
                        
                        if result and result['count'] > 0:
                            yield {
                                'msg': f"[{done_count}/{total}] 下載: {result['title']} ({result['count']}首)", 
                                'progress': f"{progress_val}%"
                            }
                        else:
                            yield {
                                'msg': f"[{done_count}/{total}] 跳過: {item_info['title']} (無音源)", 
                                'progress': f"{progress_val}%"
                            }
                    except Exception as e:
                        yield {'msg': f"錯誤: {str(e)}", 'progress': f"{int((done_count/total)*90)}%"}

            yield {'msg': "打包壓縮中...", 'progress': '95%'}
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with ZipFile(output_path, 'w', compression=ZIP_DEFLATED) as z:
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        z.write(file_path, os.path.relpath(file_path, temp_dir))
            
        yield {'msg': "完成", 'progress': '100%', 'done': True, 'filename': os.path.basename(output_path)}