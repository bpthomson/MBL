import os
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import Config

def get_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if not os.path.exists(Config.CREDENTIALS_FILE): 
        return None
    creds = ServiceAccountCredentials.from_json_keyfile_name(Config.CREDENTIALS_FILE, scope)
    return gspread.authorize(creds)

def append_to_sheet(data_row):
    try:
        client = get_client()
        if not client: return False
        sheet = client.open(Config.SPREADSHEET_NAME).sheet1
        sheet.append_row(data_row)
        return True
    except Exception as e:
        print(f"[Sheet Error] {e}")
        return False

def log_candidates_to_sheet(candidates):
    if not candidates: return
    try:
        client = get_client()
        if not client: return
        sheet = client.open(Config.SPREADSHEET_NAME)
        
        high_conf_rows = []
        low_conf_rows = []
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        
        for item in candidates:
            img_url = item["img_url"]
            img_formula = f'=HYPERLINK("{img_url}", IMAGE("{img_url}"))'
            
            row_data = [now, item['baha_title'], str(item['mal_id']), item['mal_title'], img_url, img_formula, item['status']]
            
            if item['status'] == "High Confidence":
                high_conf_rows.append(row_data)
            else:
                low_conf_rows.append(row_data)

        def write_rows(sheet_name, data):
            if not data: return
            try:
                ws = sheet.worksheet(sheet_name)
            except:
                ws = sheet.add_worksheet(title=sheet_name, rows="1000", cols="10")
                ws.append_row(['Time', 'CH Title', 'MAL ID', 'MAL Title', 'Img URL', 'Preview', 'Status'])
            ws.append_rows(data, value_input_option='USER_ENTERED')
            
        if high_conf_rows:
            write_rows(Config.CANDIDATE_SHEET_NAME, high_conf_rows)
            print(f"[Log] 上傳 {len(high_conf_rows)} 筆 High Confidence 資料")
            
        if low_conf_rows:
            write_rows(Config.DEBUG_SHEET_NAME, low_conf_rows)
            print(f"[Log] 上傳 {len(low_conf_rows)} 筆 Low Confidence 資料")
            
    except Exception as e:
        print(f"[Log Error] 上傳失敗: {e}")