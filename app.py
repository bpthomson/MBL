import csv
import datetime # [FIX] 改用標準 import
from flask import Flask, render_template, request, send_file, Response, redirect, url_for, jsonify
from core_logic import BahamutCrawler, MalMatcher, MalXmlGenerator, ThemeDownloader
import os
import json
import time

app = Flask(__name__)
app.secret_key = 'some_secret_key'

OUTPUT_FOLDER = 'outputs'
MUSIC_FOLDER = 'temp_music'
TEMP_RESULTS = {}
FINAL_RESULTS = {}

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# ... (index, stream_progress, select_results, confirm_selection, show_result, download_xml, prepare_music, download_music_exec路由 保持不變)
# 為了節省篇幅，請保留您原本的中間路由，只替換下面的 report_match 與 imports

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        limit = request.form.get('limit')
        if not user_id:
            return render_template('index.html', error="請輸入 User ID")
        return render_template('processing.html', user_id=user_id, limit=limit)
    return render_template('index.html')

@app.route('/stream_progress')
def stream_progress():
    user_id = request.args.get('user_id')
    limit = request.args.get('limit')
    final_redirect_url = url_for('select_results', user_id=user_id)

    def generate():
        yield f"data: {json.dumps({'msg': '正在連接巴哈姆特 API...'})}\n\n"
        crawler = BahamutCrawler(user_id)
        try:
            simple_collections = crawler.get_collections()
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return
        if not simple_collections:
            yield f"data: {json.dumps({'error': '找不到任何收藏'})}\n\n"
            return
        yield f"data: {json.dumps({'msg': f'共找到 {len(simple_collections)} 筆收藏，開始取得詳細資料...'})}\n\n"
        if limit and limit.isdigit():
            target_list = simple_collections[:int(limit)]
        else:
            target_list = simple_collections
        details = crawler.fetch_all_details(target_list)
        yield f"data: {json.dumps({'msg': '開始進行 MAL 配對...'})}\n\n"
        matcher = MalMatcher()
        results = []
        for i, item in enumerate(details):
            mal_data, status = matcher.resolve_mal_id(item)
            is_low_confidence = 'Low' in status or 'Not Found' in status
            row = {
                'id': i,
                'baha_title': item['ch_name'],
                'mal_title': mal_data['title'] if mal_data else '-',
                'mal_id': mal_data['mal_id'] if mal_data else None,
                'status': status,
                'img_url': mal_data.get('img_url', 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png') if mal_data else 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png',
                'mal_url': mal_data['url'] if mal_data else '#',
                'is_low': is_low_confidence
            }
            results.append(row)
            event_data = {
                'type': 'image',
                'img_url': row['img_url'],
                'title': row['baha_title'],
                'is_low': is_low_confidence,
                'progress': f"{i+1}/{len(details)}"
            }
            yield f"data: {json.dumps(event_data)}\n\n"
        TEMP_RESULTS[user_id] = results
        yield f"data: {json.dumps({'done': True, 'redirect_url': final_redirect_url})}\n\n"
        yield ": keep-alive\n\n" 
    return Response(generate(), mimetype='text/event-stream')

@app.route('/select/<user_id>')
def select_results(user_id):
    results = TEMP_RESULTS.get(user_id)
    if not results: return redirect(url_for('index'))
    return render_template('select.html', results=results, user_id=user_id)

@app.route('/confirm_selection', methods=['POST'])
def confirm_selection():
    user_id = request.form.get('user_id')
    selected_indices = request.form.getlist('selected_items') 
    all_results = TEMP_RESULTS.get(user_id)
    if not all_results: return redirect(url_for('index'))
    final_list = []
    for idx in selected_indices:
        try:
            item = all_results[int(idx)]
            if item['mal_id']: 
                final_list.append(item)
        except: continue
    FINAL_RESULTS[user_id] = final_list
    generator = MalXmlGenerator()
    xml_data = [{'mal_id': item['mal_id'], 'title': item['mal_title']} for item in final_list]
    xml_content = generator.generate_xml(xml_data, user_id)
    filename = f"{user_id}_mal_import.xml"
    filepath = os.path.join(OUTPUT_FOLDER, filename)
    generator.save_xml(xml_content, filepath)
    return redirect(url_for('show_result', user_id=user_id))

@app.route('/result/<user_id>')
def show_result(user_id):
    results = FINAL_RESULTS.get(user_id)
    if not results: return redirect(url_for('index'))
    filename = f"{user_id}_mal_import.xml"
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, filename)): filename = None
    return render_template('result.html', results=results, filename=filename, user_id=user_id)

@app.route('/download_xml/<filename>')
def download_xml(filename):
    filepath = os.path.join(OUTPUT_FOLDER, filename)
    if os.path.exists(filepath): return send_file(filepath, as_attachment=True)
    return "File not found."

@app.route('/prepare_music', methods=['POST'])
def prepare_music():
    user_id = request.form.get('user_id')
    selected_mal_ids = request.form.getlist('selected_music')
    if not selected_mal_ids: return "未選擇任何動畫"
    user_results = FINAL_RESULTS.get(user_id, [])
    selected_items_display = []
    lookup = {str(item['mal_id']): item['mal_title'] for item in user_results}
    for mid in selected_mal_ids:
        title = lookup.get(mid, f"ID: {mid}")
        selected_items_display.append(f"{mid}|{title}")
    return render_template('downloading.html', selected_items=selected_items_display)

@app.route('/download_music_exec', methods=['POST'])
def download_music_exec():
    selected_items = request.form.getlist('selected_anime')
    anime_list = []
    for item in selected_items:
        try:
            mid, title = item.split('|', 1)
            anime_list.append({'mal_id': int(mid), 'title': title})
        except: continue
    if not anime_list: return "資料解析錯誤"
    downloader = ThemeDownloader(download_dir=MUSIC_FOLDER, max_workers=8)
    try:
        zip_name = "my_anime_songs.zip"
        zip_path = os.path.join(OUTPUT_FOLDER, zip_name)
        final_zip = downloader.download_and_zip(anime_list, output_zip_name=zip_path)
        if final_zip and os.path.exists(final_zip): return send_file(final_zip, as_attachment=True)
        else: return "下載失敗"
    except Exception as e: return f"發生錯誤: {e}"

# --- 修正後的 Report Route ---

@app.route('/report_match', methods=['POST'])
def report_match():
    data = request.json
    user_id = data.get('user_id')
    item_id = int(data.get('item_id'))
    user_msg = data.get('message')

    results = TEMP_RESULTS.get(user_id)
    if not results or item_id >= len(results):
        return jsonify({'success': False, 'msg': '資料過期'})

    item = results[item_id]
    baha_title = item['baha_title']
    current_mal_id = item['mal_id']
    
    csv_file = 'reports.csv'
    file_exists = os.path.isfile(csv_file)
    
    try:
        with open(csv_file, mode='a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['Time', 'Baha_Title', 'Current_MAL_ID', 'User_Message'])
            
            # [FIX] 使用 datetime.datetime.now()
            writer.writerow([
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                baha_title,
                current_mal_id,
                user_msg
            ])
            
        return jsonify({'success': True})
    except Exception as e:
        print(f"回報寫入失敗: {e}")
        return jsonify({'success': False, 'msg': '伺服器寫入失敗'})

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)