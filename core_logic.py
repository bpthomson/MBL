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
import cloudscraper # 用於繞過 Cloudflare
import xml.etree.ElementTree as ET
from xml.dom import minidom

# ==========================================
# 1. 巴哈姆特爬蟲 (BahamutCrawler)
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
        """取得用戶的所有動畫收藏 ID"""
        print(f"正在擷取用戶 {self.user_id} 的收藏清單...")
        acg_list = []
        for star in range(0, 6):
            params = {
                'userid': self.user_id,
                'kind': f'S{star}',
                'page': 1,
                'category': 4 # 4 代表動畫
            }
            try:
                response = self.rq.get(self.collection_api, params=params).json()
                if 'data' not in response:
                    continue
                data = response['data']
                pages = data.get('tpage', 1)
                
                if 'list' in data:
                    for e in data['list']:
                        acg_list.append({'ch_name': e['name'], 'id': e['id']})

                for page in range(2, pages + 1):
                    params['page'] = page
                    f = self.rq.get(self.collection_api, params=params).json()['data']
                    for e in f['list']:
                        acg_list.append({'ch_name': e['name'], 'id': e['id']})
            except Exception as e:
                print(f"Error fetching collections page: {e}")
        
        unique_list = {v['id']: v for v in acg_list}.values()
        print(f"共找到 {len(unique_list)} 筆收藏。")
        return list(unique_list)

    def get_detail(self, sn_id):
        """取得單一作品的詳細資訊 (標題、年份)"""
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
        except Exception:
            pass
        return None

    def fetch_all_details(self, simple_list):
        """多執行緒取得所有詳細資料"""
        print("正在擷取詳細資料 (年份、日文標題)...")
        id_list = [e['id'] for e in simple_list]
        MAX_WORKERS = 8
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(tqdm(executor.map(self.get_detail, id_list), total=len(id_list)))
        return [r for r in results if r]


# ==========================================
# 2. MAL 配對器 (MalMatcher) - 支援 CSV 圖片
# ==========================================
class MalMatcher:
    def __init__(self, cache_file='mal_id.csv'):
        self.rq = cloudscraper.create_scraper()
        self.search_api = "https://api.jikan.moe/v4/anime"
        self.cache = {}
        self.cache_file = cache_file
        self.load_cache()

    def load_cache(self):
        """讀取 CSV 快取檔案 (含 img_url)"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, mode='r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('ch_name') and row.get('mal_id'):
                            # 將資料存成字典，包含 img_url
                            self.cache[row['ch_name'].strip()] = {
                                'mal_id': int(row['mal_id']),
                                'img_url': row.get('img_url', '') # 如果 CSV 該行沒圖片，存為空字串
                            }
                print(f"已載入快取，共 {len(self.cache)} 筆資料。")
            except Exception as e:
                print(f"讀取快取失敗: {e}")
        else:
            print("未發現快取檔案 (mal_id.csv)，將使用全 API 模式。")

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
        except Exception as e:
            print(f"Jikan API Error: {e}")
        return []

    def get_days_diff(self, api_date_str, target_date):
        if not target_date or not api_date_str: return 99999
        try:
            api_date = datetime.strptime(api_date_str.split('T')[0], "%Y-%m-%d")
            return abs((api_date - target_date).days)
        except:
            return 99999

    def resolve_mal_id(self, row):
        ch_name = row.get('ch_name', '').strip()
        
        # --- 1. 檢查快取 (直接使用 CSV 的圖片) ---
        if ch_name in self.cache:
            cached_item = self.cache[ch_name]
            mal_id = cached_item['mal_id']
            img_url = cached_item.get('img_url')

            # 如果 CSV 裡沒有圖片網址，給一個預設圖
            if not img_url:
                img_url = 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'

            return {
                'mal_id': mal_id,
                'title': ch_name, # 快取命中時，直接用中文標題當作顯示標題 (因為沒查 API)
                'url': f"https://myanimelist.net/anime/{mal_id}",
                'img_url': img_url
            }, "Cache Hit"

        # --- 2. API 搜尋 (原本的邏輯) ---
        target_date = None
        if row.get('year') and row.get('month') and row.get('day'):
            try:
                target_date = datetime(row['year'], row['month'], row['day'])
            except:
                pass

        queries = []
        if row.get('jp_name'):
            queries.append((1, 'JP', self.clean_text(row['jp_name'])))
        if row.get('eng_name'):
            queries.append((2, 'ENG', self.clean_text(row['eng_name'])))
        
        candidates = []
        for priority, src, q in queries:
            results = self.search_jikan(q)
            for res in results:
                if str(res.get('type')).upper() not in ['TV', 'MOVIE', 'OVA', 'TV SPECIAL', 'ONA', 'SPECIAL']: 
                    continue
                
                aired_from = res.get('aired', {}).get('from')
                diff = self.get_days_diff(aired_from, target_date)
                
                candidates.append({
                    'priority': priority,
                    'diff': diff,
                    'mal_id': res['mal_id'],
                    'title': res['title'],
                    'url': res['url'],
                    'img_url': res['images']['jpg']['image_url']
                })
            
            if candidates and any(c['diff'] <= 30 for c in candidates):
                break
        
        if not candidates:
            return None, "Not Found"

        tier1 = [c for c in candidates if c['diff'] <= 30]
        tier2 = [c for c in candidates if c['diff'] > 30]

        result = None
        status = "Unknown"
        if tier1:
            tier1.sort(key=lambda x: x['diff'])
            result = tier1[0]
            status = "High Confidence"
        elif tier2:
            tier2.sort(key=lambda x: x['diff'])
            result = tier2[0]
            status = f"Low Confidence (Diff {result['diff']} days)"
            
        if result:
            return result, status
        return None, "No Match"
    def get_anime_by_id(self, mal_id):
        """[新增] 根據 ID 查詢動畫詳情 (用於手動修正)"""
        url = f"https://api.jikan.moe/v4/anime/{mal_id}"
        try:
            # 避免頻繁請求
            time.sleep(0.5)
            resp = self.rq.get(url, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json().get('data')
                return {
                    'mal_id': data['mal_id'],
                    'title': data['title'],
                    'url': data['url'],
                    'img_url': data['images']['jpg']['image_url']
                }
        except Exception as e:
            print(f"Fetch ID {mal_id} error: {e}")
        return None


# ==========================================
# 3. XML 生成器 (MalXmlGenerator)
# ==========================================
class MalXmlGenerator:
    def generate_xml(self, anime_data_list, user_id="bahamut_user"):
        root = ET.Element("myanimelist")
        info = ET.SubElement(root, "myinfo")
        ET.SubElement(info, "user_id").text = str(user_id)
        ET.SubElement(info, "user_name").text = str(user_id)
        ET.SubElement(info, "user_export_type").text = "1"
        
        for data in anime_data_list:
            if not data or 'mal_id' not in data:
                continue
            anime = ET.SubElement(root, "anime")
            ET.SubElement(anime, "series_animedb_id").text = str(data['mal_id'])
            ET.SubElement(anime, "series_title").text = str(data.get('title', 'Unknown'))
            ET.SubElement(anime, "my_status").text = "6" 
            ET.SubElement(anime, "my_watched_episodes").text = "0"
            ET.SubElement(anime, "my_start_date").text = "0000-00-00"
            ET.SubElement(anime, "my_finish_date").text = "0000-00-00"
            ET.SubElement(anime, "my_score").text = "0"
            ET.SubElement(anime, "update_on_import").text = "0"

        return minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")

    def save_xml(self, xml_content, filename="mal_import.xml"):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(xml_content)
        print(f"檔案已儲存為: {filename}")


# ==========================================
# 4. 主題曲下載器 (ThemeDownloader) - 優化版
# ==========================================
class ThemeDownloader:
    def __init__(self, download_dir='temp_music', max_workers=5):
        self.rq = cloudscraper.create_scraper()
        self.search_url = "https://api.animethemes.moe/anime"
        self.download_dir = download_dir
        self.max_workers = max_workers
        
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def sanitize_filename(self, name):
        return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip()

    def get_theme_links(self, mal_id):
        params = {
            "filter[has]": "resources",
            "filter[site]": "MyAnimeList",
            "filter[external_id]": mal_id,
            "include": "animethemes,images,animethemes.song,animethemes.animethemeentries.videos.audio"
        }
        
        themes_list = []
        try:
            resp = self.rq.get(self.search_url, params=params, timeout=10)
            if resp.status_code != 200:
                return []
                
            data_json = resp.json()
            anime_list = data_json.get('anime', [])
            
            if not anime_list:
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
                    
                    ext = 'ogg'
                    if 'mp3' in mimetype: ext = 'mp3'
                    elif 'webm' in mimetype: ext = 'webm'
                    
                    if link:
                        themes_list.append({
                            'type': theme_type,
                            'title': song_title,
                            'link': link,
                            'ext': ext
                        })
                except:
                    continue
            return themes_list

        except Exception as e:
            return []

    def _download_single_file(self, task):
        url, save_path = task
        try:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with self.rq.get(url, stream=True, timeout=30) as r:
                if r.status_code == 200:
                    with open(save_path, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)
                    return True
        except Exception as e:
            print(f"Download failed {url}: {e}")
        return False

    def download_and_zip(self, anime_data_list, output_zip_name="anime_songs.zip"):
        print(f"準備搜尋 {len(anime_data_list)} 部動畫的音樂連結...")
        
        download_tasks = []
        
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
        os.makedirs(self.download_dir)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_anime = {executor.submit(self.get_theme_links, item['mal_id']): item for item in anime_data_list}
            
            for future in tqdm(as_completed(future_to_anime), total=len(anime_data_list), desc="搜尋連結"):
                anime_item = future_to_anime[future]
                try:
                    songs = future.result()
                    if songs:
                        safe_anime_title = self.sanitize_filename(anime_item['title'])
                        for song in songs:
                            safe_song_title = self.sanitize_filename(song['title'])
                            filename = f"{song['type']} - {safe_song_title}.{song['ext']}"
                            save_path = os.path.join(self.download_dir, safe_anime_title, filename)
                            download_tasks.append((song['link'], save_path))
                except Exception as e:
                    print(f"Fetch metadata error: {e}")

        if not download_tasks:
            print("找不到任何可下載的音樂。")
            return None

        print(f"共找到 {len(download_tasks)} 首歌曲，開始並行下載...")

        success_count = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(tqdm(executor.map(self._download_single_file, download_tasks), total=len(download_tasks), desc="下載進度"))
            success_count = sum(results)

        print("正在打包壓縮檔...")
        with ZipFile(output_zip_name, 'w') as zipf:
            for root, dirs, files in os.walk(self.download_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, self.download_dir)
                    zipf.write(file_path, arcname)
        
        print(f"完成，檔案: {output_zip_name}")
        return output_zip_name