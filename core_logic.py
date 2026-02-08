import requests
import time
import unicodedata
import csv
import os
import shutil
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from zipfile import ZipFile
from tqdm import tqdm
import cloudscraper
import xml.etree.ElementTree as ET
from xml.dom import minidom

# ==========================================
# 1. 巴哈姆特爬蟲
# ==========================================
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
                pages = data.get('tpage', 1)
                if 'list' in data:
                    for e in data['list']: acg_list.append({'ch_name': e['name'], 'id': e['id']})
                for page in range(2, pages + 1):
                    params['page'] = page
                    f = self.rq.get(self.collection_api, params=params).json()['data']
                    for e in f['list']: acg_list.append({'ch_name': e['name'], 'id': e['id']})
            except: pass
        unique_list = {v['id']: v for v in acg_list}.values()
        return list(unique_list)

    def get_detail(self, sn_id):
        params = {'sn': sn_id}
        try:
            data = self.rq.get(self.detail_api, params=params).json()['data']['acg']['all'].items()
            for sn, info in data:
                title = info.get('title')
                det = info.get('detailed', {})
                platform_type = det.get('platform', {}).get('value')
                time_val = det.get('localDebut', {}).get('value')
                if platform_type == '動畫':
                    return {
                        'ch_name': title,
                        'eng_name': info.get('title_en'),
                        'jp_name': info.get('title_jp'),
                        'year': int(time_val[:4]) if time_val and time_val[:4].isdigit() else None,
                        'month': int(time_val[5:7]) if time_val and len(time_val) >= 7 and time_val[5:7].isdigit() else None,
                        'day': int(time_val[8:]) if time_val and len(time_val) >= 10 and time_val[8:].isdigit() else None
                    }
        except: pass
        return None

    def fetch_all_details(self, simple_list):
        id_list = [e['id'] for e in simple_list]
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(self.get_detail, id_list))
        return [r for r in results if r]

# ==========================================
# 2. MAL 配對器 (含圖片快取)
# ==========================================
class MalMatcher:
    def __init__(self, cache_file='mal_id.csv'):
        self.rq = cloudscraper.create_scraper()
        self.search_api = "https://api.jikan.moe/v4/anime"
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
                            self.cache[row['ch_name'].strip()] = {
                                'mal_id': int(row['mal_id']),
                                'img_url': row.get('img_url', '')
                            }
            except: pass

    def clean_text(self, text):
        if not text: return None
        return unicodedata.normalize("NFKC", str(text)).replace('劇場版', '').strip()

    def search_jikan(self, query):
        if not query: return []
        params = {'q': query, 'limit': 5}
        try:
            time.sleep(0.7) 
            resp = self.rq.get(self.search_api, params=params, timeout=10)
            if resp.status_code == 429:
                time.sleep(2)
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

    def resolve_mal_id(self, row):
        ch_name = row.get('ch_name', '').strip()
        # 1. 檢查快取
        if ch_name in self.cache:
            cached = self.cache[ch_name]
            img = cached.get('img_url')
            if not img: img = 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
            return {
                'mal_id': cached['mal_id'],
                'title': ch_name,
                'url': f"https://myanimelist.net/anime/{cached['mal_id']}",
                'img_url': img
            }, "Cache Hit"

        # 2. API 搜尋
        target_date = None
        if row.get('year') and row.get('month') and row.get('day'):
            try: target_date = datetime(row['year'], row['month'], row['day'])
            except: pass

        queries = []
        if row.get('jp_name'): queries.append((1, 'JP', self.clean_text(row['jp_name'])))
        if row.get('eng_name'): queries.append((2, 'ENG', self.clean_text(row['eng_name'])))
        
        candidates = []
        for priority, src, q in queries:
            results = self.search_jikan(q)
            for res in results:
                if str(res.get('type')).upper() not in ['TV', 'MOVIE', 'OVA', 'TV SPECIAL', 'ONA', 'SPECIAL']: continue
                aired_from = res.get('aired', {}).get('from')
                diff = self.get_days_diff(aired_from, target_date)
                candidates.append({
                    'priority': priority, 'diff': diff, 'mal_id': res['mal_id'],
                    'title': res['title'], 'url': res['url'], 'img_url': res['images']['jpg']['image_url']
                })
            if candidates and any(c['diff'] <= 30 for c in candidates): break
        
        if not candidates: return None, "Not Found"
        
        candidates.sort(key=lambda x: x['diff'])
        result = candidates[0]
        status = "High Confidence" if result['diff'] <= 30 else f"Low Confidence ({result['diff']} days)"
        return result, status

# ==========================================
# 3. XML 生成器
# ==========================================
class MalXmlGenerator:
    def generate_xml(self, anime_data_list, user_id="bahamut_user"):
        root = ET.Element("myanimelist")
        info = ET.SubElement(root, "myinfo")
        ET.SubElement(info, "user_id").text = str(user_id)
        ET.SubElement(info, "user_name").text = str(user_id)
        ET.SubElement(info, "user_export_type").text = "1"
        
        for data in anime_data_list:
            if not data or 'mal_id' not in data: continue
            anime = ET.SubElement(root, "anime")
            ET.SubElement(anime, "series_animedb_id").text = str(data['mal_id'])
            ET.SubElement(anime, "series_title").text = str(data.get('title', 'Unknown'))
        return minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")

    def save_xml(self, xml_content, filename="mal_import.xml"):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(xml_content)

# ==========================================
# 4. 主題曲下載器 (支援串流)
# ==========================================
class ThemeDownloader:
    def __init__(self, download_dir='temp_music', max_workers=4):
        self.rq = cloudscraper.create_scraper()
        # [Debug] 設定 User-Agent 避免被擋
        self.rq.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.search_url = "https://api.animethemes.moe/anime"
        self.download_dir = download_dir
        self.max_workers = max_workers
        if not os.path.exists(self.download_dir): os.makedirs(self.download_dir)

    def sanitize_filename(self, name):
        return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()

    def get_theme_links(self, mal_id):
        print(f"[Debug] 正在搜尋 ID: {mal_id} ...") # [Debug]
        params = {
            "filter[has]": "resources", "filter[site]": "MyAnimeList", "filter[external_id]": mal_id,
            "include": "animethemes,images,animethemes.song,animethemes.animethemeentries.videos.audio"
        }
        themes_list = []
        try:
            resp = self.rq.get(self.search_url, params=params, timeout=15)
            
            if resp.status_code != 200: 
                print(f"[Debug] ID {mal_id} API 回傳錯誤: {resp.status_code}") # [Debug]
                return []
                
            data_json = resp.json()
            anime_list = data_json.get('anime', [])
            if not anime_list: 
                print(f"[Debug] ID {mal_id} 在 AnimeThemes 找不到資料") # [Debug]
                return []
                
            anime = anime_list[0]
            for theme in anime.get('animethemes', []):
                try:
                    theme_type = theme.get('slug', 'Unknown')
                    song_title = theme.get('song', {}).get('title', 'Unknown')
                    entries = theme.get('animethemeentries', [])
                    if not entries: continue
                    videos = entries[0].get('videos', [])
                    if not videos: continue
                    audio = videos[0].get('audio', {})
                    link = audio.get('link')
                    mimetype = audio.get('mimetype', 'audio/ogg')
                    
                    # 簡單判斷副檔名
                    ext = 'ogg'
                    if 'mp3' in mimetype: ext = 'mp3'
                    elif 'webm' in mimetype: ext = 'webm'
                    
                    if link: 
                        themes_list.append({'type': theme_type, 'title': song_title, 'link': link, 'ext': ext})
                except Exception as e: 
                    print(f"[Debug] 解析歌曲錯誤: {e}")
                    continue
            
            print(f"[Debug] ID {mal_id} 找到 {len(themes_list)} 首歌") # [Debug]
            return themes_list
        except Exception as e: 
            print(f"[Debug] ID {mal_id} 連線失敗: {e}") # [Debug]
            return []

    def _download_single_file(self, task):
        url, save_path = task
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with self.rq.get(url, stream=True, timeout=30) as r:
                if r.status_code == 200:
                    with open(save_path, 'wb') as f: shutil.copyfileobj(r.raw, f)
                    return True
                else:
                    print(f"[Debug] 下載失敗 {r.status_code}: {url}")
        except Exception as e:
            print(f"[Debug] 下載異常 {url}: {e}")
        return False

    def download_and_zip_generator(self, anime_data_list, output_zip_name="anime_songs.zip"):
        print(f"[Debug] 啟動下載流程，共 {len(anime_data_list)} 部動畫") # [Debug]
        yield {'msg': f"正在搜尋 {len(anime_data_list)} 部動畫的音樂連結...", 'progress': '0%'}
        
        download_tasks = []
        if os.path.exists(self.download_dir): shutil.rmtree(self.download_dir)
        os.makedirs(self.download_dir)

        # 1. 搜尋連結
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_anime = {executor.submit(self.get_theme_links, item['mal_id']): item for item in anime_data_list}
            completed = 0
            for future in as_completed(future_to_anime):
                item = future_to_anime[future]
                completed += 1
                try:
                    songs = future.result()
                    if songs:
                        safe_title = self.sanitize_filename(item['title'])
                        for song in songs:
                            safe_song = self.sanitize_filename(song['title'])
                            fname = f"{song['type']} - {safe_song}.{song['ext']}"
                            path = os.path.join(self.download_dir, safe_title, fname)
                            display = f"{safe_title[:10]}.. | {song['type']}"
                            download_tasks.append((song['link'], path, display))
                except Exception as e: 
                    print(f"[Debug] 搜尋執行緒錯誤: {e}")
                
                pct = int((completed / len(anime_data_list)) * 20)
                yield {'msg': f"搜尋中: {item['title']}", 'progress': f"{pct}%"}

        if not download_tasks:
            print("[Debug] 錯誤：沒有建立任何下載任務") # [Debug]
            yield {'msg': "找不到任何可下載的音樂 (可能是 API 沒回應)", 'progress': '100%', 'error': 'API No Result'}
            return

        yield {'msg': f"找到 {len(download_tasks)} 首歌，開始下載...", 'progress': '20%'}
        
        # 2. 下載
        finished = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._download_single_file, (url, path)) for url, path, name in download_tasks]
            for f in as_completed(futures):
                f.result()
                finished += 1
                pct = 20 + int((finished / len(download_tasks)) * 70)
                # 這裡不回傳 msg 避免刷太快，只更新進度條
                if finished % 2 == 0: # 每兩首更新一次
                    yield {'progress': f"{pct}%"}

        yield {'msg': "正在壓縮打包...", 'progress': '95%'}
        
        try:
            with ZipFile(output_zip_name, 'w') as zipf:
                for root, dirs, files in os.walk(self.download_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, self.download_dir)
                        zipf.write(file_path, arcname)
            
            print(f"[Debug] 打包完成: {output_zip_name}")
            yield {'msg': "完成！", 'progress': '100%', 'done': True, 'filename': os.path.basename(output_zip_name)}
        except Exception as e:
            print(f"[Debug] 打包失敗: {e}")
            yield {'error': f"打包失敗: {str(e)}"}