import csv
import time
import os
from config import Config
from core_logic import ThemeCacheManager

def sync_themes():
    if not os.path.exists(Config.CACHE_CSV_FILE):
        print(f"錯誤：找不到快取檔案 {Config.CACHE_CSV_FILE}")
        return

    # 初始化共用快取管理器
    theme_mgr = ThemeCacheManager()

    print(f"正在讀取 {Config.CACHE_CSV_FILE}...")
    
    # 讀取 CSV
    with open(Config.CACHE_CSV_FILE, mode='r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    print(f"共找到 {total} 筆動畫資料，開始同步音源快取 (theme_cache.json)...\n")
    
    new_count = 0
    skip_count = 0
    error_count = 0

    for i, row in enumerate(rows, 1):
        mal_id = row.get('mal_id')
        ch_name = row.get('ch_name', 'Unknown')
        
        if not mal_id or not str(mal_id).strip():
            continue
            
        mal_id_str = str(mal_id).strip()
        
        # 檢查是否已存在於快取字典中
        if mal_id_str in theme_mgr.cache:
            skip_count += 1
            print(f"[{i}/{total}] 略過 (已快取): {ch_name}")
            continue
            
        print(f"[{i}/{total}] 抓取中: {ch_name} (MAL ID: {mal_id_str})... ", end="", flush=True)
        
        try:
            # 呼叫管理器抓取並寫入
            themes = theme_mgr.get_themes(mal_id_str)
            print(f"成功 (找到 {len(themes)} 首歌曲)")
            new_count += 1
            
            # 友善延遲，避免觸發 429 速率限制
            time.sleep(0.3)
            
        except Exception as e:
            error_count += 1
            print(f"失敗 ({str(e)})")

    print("\n--- 同步作業完成 ---")
    print(f"新增寫入: {new_count} 筆")
    print(f"略過快取: {skip_count} 筆")
    print(f"發生錯誤: {error_count} 筆")

if __name__ == "__main__":
    sync_themes()