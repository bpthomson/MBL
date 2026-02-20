import gspread
import csv
import os
from oauth2client.service_account import ServiceAccountCredentials
from config import Config

def update_local_cache(target_tab_name):
    print(f"正在連線 Google Sheets... 目標分頁: {target_tab_name}")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    if not os.path.exists(Config.CREDENTIALS_FILE):
        print(f"錯誤：找不到 {Config.CREDENTIALS_FILE}")
        return

    creds = ServiceAccountCredentials.from_json_keyfile_name(Config.CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    
    try:
        sheet = client.open(Config.SPREADSHEET_NAME).worksheet(target_tab_name)
    except Exception as e:
        print(f"無法開啟工作表 '{target_tab_name}': {e}")
        return

    print("正在讀取候選名單...")
    rows = sheet.get_all_values()
    
    if len(rows) < 2:
        print(f"分頁 {target_tab_name} 是空的，沒有資料需要更新。\n")
        return

    existing_keys = set()
    if os.path.exists(Config.CACHE_CSV_FILE):
        with open(Config.CACHE_CSV_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get('ch_name'):
                    existing_keys.add(r['ch_name'].strip())
    
    print(f"目前本地快取已有 {len(existing_keys)} 筆資料。")

    new_entries = []
    skipped_count = 0
    duplicate_count = 0
    
    for row in rows[1:]:
        if len(row) < 5: continue
        
        ch_name = row[1].strip()
        mal_id = row[2].strip()
        mal_title = row[3].strip() 
        img_url = row[4].strip()
        check_status = row[7].strip().upper() if len(row) > 7 else ""
        
        if check_status == 'X':
            skipped_count += 1
            print(f"[排除] {ch_name}")
            continue
            
        if ch_name in existing_keys:
            duplicate_count += 1
            continue
        
        new_entries.append([ch_name, mal_id, mal_title, img_url])
        existing_keys.add(ch_name)

    if new_entries:
        print(f"正在寫入 {len(new_entries)} 筆新資料到 {Config.CACHE_CSV_FILE}...")
        
        with open(Config.CACHE_CSV_FILE, mode='a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_entries)
            
        print("更新完成！")
        print(f" - 新增: {len(new_entries)} 筆")
        print(f" - 重複: {duplicate_count} 筆")
        print(f" - 排除: {skipped_count} 筆")
        
        ans = input(f"\n是否要清空 Google Sheet 分頁 '{target_tab_name}' 上的資料? (y/n): ")
        if ans.lower() == 'y':
            sheet.resize(rows=1)
            sheet.resize(rows=1000)
            headers = ['Time', 'CH Title', 'MAL ID', 'MAL Title', 'Img URL', 'Preview', 'Status', 'Check(X)']
            sheet.update(range_name='A1:H1', values=[headers])
            print(f"分頁 {target_tab_name} 已清空。\n")
            
    else:
        print("沒有需要新增的資料。\n")

if __name__ == "__main__":
    update_local_cache(Config.CANDIDATE_SHEET_NAME)
    update_local_cache(Config.DEBUG_SHEET_NAME)