# debug_tool.py
# 用來測試 core_logic.py 是否正常運作，不透過網頁
import os
import sys

print("=== MyBahaList 自我檢測工具 ===")

# 1. 檢查檔案結構
required_files = ['app.py', 'core_logic.py', 'templates/index.html', 'templates/processing.html']
missing_files = [f for f in required_files if not os.path.exists(f)]

if missing_files:
    print(f"❌ 嚴重錯誤：缺少以下檔案: {missing_files}")
    print("請確保檔案都在正確的資料夾中 (HTML 檔要在 templates 資料夾內)")
    sys.exit()
else:
    print("✅ 檔案結構檢查通過")

# 2. 檢查套件引用
try:
    print("正在測試載入 core_logic...")
    from core_logic import BahamutCrawler, MalMatcher, ThemeDownloader
    print("✅ 核心模組載入成功")
except ImportError as e:
    print(f"❌ 載入失敗，請檢查是否安裝了所有套件: {e}")
    print("建議執行: pip install requests cloudscraper tqdm flask")
    sys.exit()
except Exception as e:
    print(f"❌ core_logic.py 程式碼有錯誤: {e}")
    sys.exit()

# 3. 實際連線測試
user_id = input("\n請輸入您的巴哈姆特 ID 進行測試: ").strip()
if not user_id:
    print("未輸入 ID，跳過測試")
    sys.exit()

try:
    print(f"\n[1/3] 測試爬取巴哈姆特 (ID: {user_id})...")
    crawler = BahamutCrawler(user_id)
    collections = crawler.get_collections()
    
    if not collections:
        print("⚠️ 警告：抓不到收藏清單。可能是 ID 錯誤、收藏清單未公開，或 API 被阻擋。")
    else:
        print(f"✅ 成功抓到 {len(collections)} 筆收藏。")
        
        # 測試抓取第一筆詳細資料
        first_item = collections[0]
        print(f"      正在抓取詳細資料: {first_item['ch_name']}...")
        details = crawler.fetch_all_details([first_item])
        
        if details:
            print(f"✅ 詳細資料抓取成功: {details[0]['ch_name']} ({details[0]['year']})")
            
            print("\n[2/3] 測試 MAL 配對...")
            matcher = MalMatcher()
            mal_data, status = matcher.resolve_mal_id(details[0])
            print(f"✅ 配對結果: {status}")
            if mal_data:
                print(f"      MAL ID: {mal_data['mal_id']} | Title: {mal_data['title']}")
                print(f"      Image: {mal_data.get('img_url')}")
                
                print("\n[3/3] 測試音樂連結搜尋...")
                downloader = ThemeDownloader()
                links = downloader.get_theme_links(mal_data['mal_id'])
                print(f"✅ 音樂搜尋結果: 找到 {len(links)} 首歌")
            else:
                print("⚠️ 未配對到 MAL 資料，跳過音樂測試")
        
    print("\n=== 測試結束 ===")
    print("如果以上都顯示 ✅，代表核心邏輯沒問題，問題出在 app.py 或 HTML。")
    print("如果出現 ❌，請告訴我錯誤訊息。")

except Exception as e:
    print(f"\n❌ 執行過程中發生錯誤: {e}")
    import traceback
    traceback.print_exc()