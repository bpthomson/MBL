import csv
import datetime
import os
import json
import traceback
from flask import Flask, render_template, request, send_file, Response, redirect, url_for, jsonify
from core_logic import BahamutCrawler, MalMatcher, MalXmlGenerator, ThemeDownloader

app = Flask(__name__)
app.secret_key = 'secret'

OUTPUT_FOLDER = 'outputs'
MUSIC_FOLDER = 'temp_music'
TEMP_RESULTS = {}    # 爬蟲暫存
FINAL_RESULTS = {}   # XML暫存
MUSIC_QUEUE = {}     # 音樂下載暫存

if not os.path.exists(OUTPUT_FOLDER): os.makedirs(OUTPUT_FOLDER)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # [FIX] 加入 strip() 去除前後空白
        user_id = request.form.get('user_id', '').strip()
        limit = request.form.get('limit')
        
        if not user_id:
            return render_template('index.html', error="請輸入 User ID")
            
        print(f"[App] 接收到 ID: '{user_id}', Limit: '{limit}'") # [Debug]
        return render_template('processing.html', user_id=user_id, limit=limit)
    return render_template('index.html')

@app.route('/stream_progress')
def stream_progress():
    user_id = request.args.get('user_id', '').strip()
    limit = request.args.get('limit')
    
    try:
        final_url = url_for('select_results', user_id=user_id)
    except Exception as e:
        print(f"[Error] url_for 失敗: {e}")
        return Response(f"data: {json.dumps({'error': '系統錯誤: url_for'})}\n\n", mimetype='text/event-stream')

    def generate():
        print(f"[Stream] 開始處理用戶: {user_id}")
        
        yield f"data: {json.dumps({'msg': '正在連接巴哈姆特 API...'})}\n\n"
        crawler = BahamutCrawler(user_id)
        
        try:
            collections = crawler.get_collections()
            print(f"[Stream] 抓到收藏數: {len(collections)}")
        except Exception as e:
            err_msg = f"爬取失敗: {str(e)}"
            print(f"[Error] {err_msg}")
            traceback.print_exc()
            yield f"data: {json.dumps({'error': err_msg})}\n\n"
            return
        
        if not collections:
            msg = "找不到任何收藏 (API 回傳空值，請確認 ID 是否正確或公開)"
            print(f"[Stream] {msg}")
            yield f"data: {json.dumps({'error': msg})}\n\n"
            return
            
        if limit and limit.isdigit():
            target_list = collections[:int(limit)]
        else:
            target_list = collections
        
        yield f"data: {json.dumps({'msg': f'發現 {len(collections)} 筆，準備處理前 {len(target_list)} 筆...'})}\n\n"
        
        try:
            details = crawler.fetch_all_details(target_list)
        except Exception as e:
            print(f"[Error] 詳細資料抓取失敗: {e}")
            yield f"data: {json.dumps({'error': '詳細資料抓取失敗'})}\n\n"
            return

        yield f"data: {json.dumps({'msg': '開始配對...'})}\n\n"
        
        matcher = MalMatcher()
        results = []
        for i, item in enumerate(details):
            try:
                mal_data, status = matcher.resolve_mal_id(item)
                is_low = 'Low' in status or 'Not Found' in status
                img = mal_data.get('img_url', '') if mal_data else ''
                if not img: img = 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
                
                row = {
                    'id': i, 'baha_title': item['ch_name'],
                    'mal_title': mal_data['title'] if mal_data else '-',
                    'mal_id': mal_data['mal_id'] if mal_data else None,
                    'status': status, 'img_url': img, 'is_low': is_low
                }
                results.append(row)
                
                # [FIX] 修正這裡的語法錯誤：先定義字典，再轉 JSON，最後 yield
                event_data = {
                    'type': 'image',
                    'img_url': img,
                    'title': item['ch_name'],
                    'is_low': is_low,
                    'progress': f"{i+1}/{len(details)}"
                }
                yield f"data: {json.dumps(event_data)}\n\n"
                
            except Exception as e:
                print(f"[Error] 處理單筆資料錯誤: {item['ch_name']} - {e}")
                continue
        
        TEMP_RESULTS[user_id] = results
        print(f"[Stream] 完成，存入暫存 (Size: {len(results)})")
        
        yield f"data: {json.dumps({'done': True, 'redirect_url': final_url})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/select/<user_id>')
def select_results(user_id):
    results = TEMP_RESULTS.get(user_id)
    if not results:
        print(f"[App] Select 頁面找不到 user_id: {user_id} 的暫存資料")
        return redirect(url_for('index'))
    return render_template('select.html', results=results, user_id=user_id)

@app.route('/dispatch_action', methods=['POST'])
def dispatch_action():
    user_id = request.form.get('user_id')
    selected = request.form.getlist('selected_items')
    action = request.form.get('action')
    
    raw = TEMP_RESULTS.get(user_id)
    if not raw: return redirect(url_for('index'))
    
    final = []
    for i in selected:
        try:
            item = raw[int(i)]
            if item['mal_id']: final.append(item)
        except: continue
    
    if action == 'xml':
        FINAL_RESULTS[user_id] = final
        gen = MalXmlGenerator()
        xml_data = [{'mal_id': i['mal_id'], 'title': i['mal_title']} for i in final]
        path = os.path.join(OUTPUT_FOLDER, f"{user_id}_mal_import.xml")
        gen.save_xml(gen.generate_xml(xml_data, user_id), path)
        return redirect(url_for('show_xml_result', user_id=user_id))
        
    elif action == 'music':
        # [FIX] 這裡確保有複製一份資料到 MUSIC_QUEUE
        MUSIC_QUEUE[user_id] = [{'mal_id': i['mal_id'], 'title': i['mal_title']} for i in final]
        print(f"[App] 加入音樂下載佇列: {len(MUSIC_QUEUE[user_id])} 首")
        return render_template('music_processing.html', user_id=user_id)
        
    return redirect(url_for('index'))

@app.route('/result_xml/<user_id>')
def show_xml_result(user_id):
    fname = f"{user_id}_mal_import.xml"
    if not os.path.exists(os.path.join(OUTPUT_FOLDER, fname)): fname = None
    return render_template('result.html', filename=fname, user_id=user_id)

@app.route('/stream_music_download')
def stream_music_download():
    user_id = request.args.get('user_id')
    q = MUSIC_QUEUE.get(user_id)
    
    if not q: 
        print(f"[Music] 找不到 Queue (User: {user_id})")
        return Response("data: "+json.dumps({'error':'下載排程過期，請重新操作'})+"\n\n", mimetype='text/event-stream')
    
    def generate():
        dl = ThemeDownloader(MUSIC_FOLDER, 6)
        path = os.path.join(OUTPUT_FOLDER, f"{user_id}_anime_songs.zip")
        try:
            for st in dl.download_and_zip_generator(q, path):
                yield f"data: {json.dumps(st)}\n\n"
        except Exception as e:
            print(f"[Music Error] {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield ": keep-alive\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/report_match', methods=['POST'])
def report_match():
    data = request.json
    uid, idx, msg = data.get('user_id'), int(data.get('item_id')), data.get('message')
    res = TEMP_RESULTS.get(uid)
    if not res or idx >= len(res): return jsonify({'success':False})
    
    try:
        exists = os.path.isfile('reports.csv')
        with open('reports.csv', 'a', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            if not exists: w.writerow(['Time','BahaTitle','MalID','Msg'])
            w.writerow([datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), res[idx]['baha_title'], res[idx]['mal_id'], msg])
        return jsonify({'success':True})
    except Exception as e: 
        print(f"[Error] 回報失敗: {e}")
        return jsonify({'success':False})

@app.route('/download/<path:filename>')
def download_file(filename):
    return send_file(os.path.join(OUTPUT_FOLDER, filename), as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)