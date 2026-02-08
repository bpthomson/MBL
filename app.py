import csv
import datetime
import os
import json
import traceback
import io
from flask import Flask, render_template, request, send_file, Response, redirect, url_for, jsonify, after_this_request
from core_logic import BahamutCrawler, MalMatcher, MalXmlGenerator, ThemeDownloader

app = Flask(__name__)
app.secret_key = 'secret'

OUTPUT_FOLDER = 'outputs'
TEMP_RESULTS = {}
FINAL_RESULTS = {}
MUSIC_QUEUE = {}

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user_id = request.form.get('user_id', '').strip()
        limit = request.form.get('limit')
        if not user_id:
            return render_template('index.html', error="請輸入 ID")
        return render_template('processing.html', user_id=user_id, limit=limit)
    return render_template('index.html')

@app.route('/stream_progress')
def stream_progress():
    user_id = request.args.get('user_id', '').strip()
    limit = request.args.get('limit')
    
    try:
        final_url = url_for('select_results', user_id=user_id)
    except:
        return Response("data: error\n\n", mimetype='text/event-stream')

    def generate():
        crawler = BahamutCrawler(user_id)
        
        # 1. 爬取列表
        try:
            collections = crawler.get_collections()
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return
        
        if not collections:
            yield f"data: {json.dumps({'error': '找不到任何收藏'})}\n\n"
            return
            
        target_list = collections[:int(limit)] if limit and limit.isdigit() else collections
        yield f"data: {json.dumps({'msg': f'發現 {len(collections)} 筆，讀取 {len(target_list)} 筆...'})}\n\n"
        
        # 2. 讀取詳細資料
        try:
            details = crawler.fetch_all_details(target_list)
        except:
            yield f"data: {json.dumps({'error': '資料讀取失敗'})}\n\n"
            return

        yield f"data: {json.dumps({'msg': '開始配對...'})}\n\n"
        
        # 3. 配對
        matcher = MalMatcher()
        results = []
        total = len(details)
        
        for i, item in enumerate(details):
            try:
                mal_data, status = matcher.resolve_mal_id(item)
                is_low = 'Low' in status or 'Not Found' in status
                img = mal_data.get('img_url', '') if mal_data else 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
                
                row = {
                    'id': i,
                    'baha_title': item['ch_name'],
                    'mal_title': mal_data['title'] if mal_data else '-',
                    'mal_id': mal_data['mal_id'] if mal_data else None,
                    'status': status,
                    'img_url': img,
                    'is_low': is_low
                }
                results.append(row)
                
                # 回傳進度
                yield f"""data: {json.dumps({
                    'type': 'image',
                    'img_url': img,
                    'title': item['ch_name'],
                    'is_low': is_low,
                    'current': i+1,
                    'total': total
                })}\n\n"""
            except:
                continue
        
        TEMP_RESULTS[user_id] = results
        yield f"data: {json.dumps({'done': True, 'redirect_url': final_url})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/select/<user_id>')
def select_results(user_id):
    results = TEMP_RESULTS.get(user_id)
    if not results:
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
        gen = MalXmlGenerator()
        xml_data = [{'mal_id': i['mal_id'], 'title': i['mal_title']} for i in final]
        FINAL_RESULTS[user_id] = gen.generate_xml(xml_data, user_id)
        return redirect(url_for('show_xml_result', user_id=user_id))
        
    elif action == 'music':
        MUSIC_QUEUE[user_id] = [{'mal_id': i['mal_id'], 'title': i['mal_title']} for i in final]
        return render_template('music_processing.html', user_id=user_id)
        
    return redirect(url_for('index'))

@app.route('/result_xml/<user_id>')
def show_xml_result(user_id):
    if user_id not in FINAL_RESULTS:
        return redirect(url_for('index'))
    return render_template('result.html', user_id=user_id)

@app.route('/download_xml_mem/<user_id>')
def download_xml_mem(user_id):
    content = FINAL_RESULTS.get(user_id)
    if not content: return "無效請求", 404
    
    mem = io.BytesIO()
    mem.write(content.encode('utf-8'))
    mem.seek(0)
    
    return send_file(
        mem,
        as_attachment=True,
        download_name=f"{user_id}_mal_import.xml",
        mimetype='application/xml'
    )

@app.route('/stream_music_download')
def stream_music_download():
    user_id = request.args.get('user_id')
    q = MUSIC_QUEUE.get(user_id)
    
    if not q:
        return Response("data: "+json.dumps({'error':'排程已過期'})+"\n\n", mimetype='text/event-stream')
    
    def generate():
        dl = ThemeDownloader(max_workers=16)
        path = os.path.join(OUTPUT_FOLDER, f"{user_id}_anime_songs.zip")
        try:
            for st in dl.download_and_zip_generator(q, path):
                yield f"data: {json.dumps(st)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield ": keep-alive\n\n"
        
    return Response(generate(), mimetype='text/event-stream')

@app.route('/download/<path:filename>')
def download_file(filename):
    path = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(path): return "檔案不存在", 404

    @after_this_request
    def remove_file(response):
        try: os.remove(path)
        except: pass
        return response

    return send_file(path, as_attachment=True)

@app.route('/report_match', methods=['POST'])
def report_match():
    data = request.json
    uid, idx, msg = data.get('user_id'), int(data.get('item_id')), data.get('message')
    res = TEMP_RESULTS.get(uid)
    if not res: return jsonify({'success': False})
    
    try:
        exists = os.path.isfile('reports.csv')
        with open('reports.csv', 'a', encoding='utf-8-sig', newline='') as f:
            w = csv.writer(f)
            if not exists: w.writerow(['Time', 'BahaTitle', 'MalID', 'Msg'])
            w.writerow([
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                res[idx]['baha_title'],
                res[idx]['mal_id'],
                msg
            ])
        return jsonify({'success': True})
    except:
        return jsonify({'success': False})

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)