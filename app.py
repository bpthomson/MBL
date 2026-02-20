import datetime
import os
import json
import uuid
import threading
import io
from flask import Flask, render_template, request, send_file, Response, redirect, url_for, jsonify, after_this_request, session

from config import Config
from core_logic import BahamutCrawler, MalMatcher, MalXmlGenerator, ThemeDownloader
from services.sheets_service import append_to_sheet, log_candidates_to_sheet

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']

if not os.path.exists(app.config['OUTPUT_FOLDER']): 
    os.makedirs(app.config['OUTPUT_FOLDER'])

TEMP_RESULTS = {}
FINAL_RESULTS = {}
MUSIC_QUEUE = {}
USER_SELECTIONS = {} 
GAME_QUEUE = {}
READY_PLAYLISTS = {}

@app.before_request
def ensure_session_id():
    session.permanent = True
    if 'uid' not in session:
        session['uid'] = str(uuid.uuid4())

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user_id = request.form.get('user_id', '').strip()
        limit = request.form.get('limit')
        if not user_id: return render_template('index.html', error="Target ID is required.")
        return render_template('processing.html', user_id=user_id, limit=limit)
    return render_template('index.html')

@app.route('/stream_progress')
def stream_progress():
    user_id = request.args.get('user_id', '').strip()
    limit = request.args.get('limit')
    sid = session['uid']

    try: final_url = url_for('select_results', user_id=user_id)
    except: return Response("data: error\n\n", mimetype='text/event-stream')

    def generate():
        if sid in USER_SELECTIONS: del USER_SELECTIONS[sid]
        
        crawler = BahamutCrawler(user_id)
        try: collections = crawler.get_collections()
        except Exception as e: yield f"data: {json.dumps({'error': str(e)})}\n\n"; return
        if not collections: yield f"data: {json.dumps({'error': 'No valid collection records detected.'})}\n\n"; return
            
        target_list = collections[:int(limit)] if limit and limit.isdigit() else collections
        yield f"data: {json.dumps({'msg': f'Detected {len(collections)} records. Initializing stream for {len(target_list)} items...'})}\n\n"
        
        try: details = crawler.fetch_all_details(target_list)
        except: yield f"data: {json.dumps({'error': 'Data stream extraction failed.'})}\n\n"; return

        yield f"data: {json.dumps({'msg': 'Initiating feature matching protocol...'})}\n\n"
        matcher = MalMatcher()
        results = []
        new_candidates = []
        total = len(details)
        
        for i, item in enumerate(details):
            try:
                mal_data, status = matcher.resolve_mal_id(item)
                is_low = True
                if status and (status == "Cache Hit" or status == "High Confidence"):
                    is_low = False
                
                img = mal_data.get('img_url', '') if mal_data else 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
                
                # 確保年份被記錄
                row = {
                    'id': i,
                    'baha_title': item['ch_name'],
                    'mal_title': mal_data['title'] if mal_data else '-',
                    'mal_id': mal_data['mal_id'] if mal_data else None,
                    'status': status,
                    'img_url': img,
                    'is_low': is_low,
                    'year': item.get('year')
                }
                results.append(row)
                
                if status != "Cache Hit" and mal_data:
                    new_candidates.append(row)

                yield f"""data: {json.dumps({
                    'type': 'image',
                    'img_url': img,
                    'title': item['ch_name'],
                    'status': status,
                    'is_low': is_low,
                    'current': i+1,
                    'total': total
                })}\n\n"""
            except: continue
        
        TEMP_RESULTS[sid] = results
        
        if new_candidates:
            threading.Thread(target=log_candidates_to_sheet, args=(new_candidates,)).start()

        yield f"data: {json.dumps({'done': True, 'redirect_url': final_url})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')

@app.route('/select/<user_id>')
def select_results(user_id):
    sid = session['uid']
    results = TEMP_RESULTS.get(sid)
    if not results: return redirect(url_for('index'))
    saved_sel = USER_SELECTIONS.get(sid)
    return render_template('select.html', results=results, user_id=user_id, saved_sel=saved_sel)

@app.route('/dispatch_action', methods=['POST'])
def dispatch_action():
    user_id = request.form.get('user_id')
    selected = request.form.getlist('selected_items')
    action = request.form.get('action')
    sid = session['uid']
    USER_SELECTIONS[sid] = selected
    
    raw = TEMP_RESULTS.get(sid)
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
        FINAL_RESULTS[sid] = gen.generate_xml(xml_data, user_id)
        return redirect(url_for('show_xml_result', user_id=user_id))
    elif action == 'music':
        MUSIC_QUEUE[sid] = [{'mal_id': i['mal_id'], 'title': i['baha_title']} for i in final]
        return render_template('music_processing.html', user_id=user_id)
    elif action == 'guess':
        # 確保年份從 TEMP_RESULTS 傳遞到遊戲佇列
        GAME_QUEUE[sid] = [{'mal_id': i['mal_id'], 'title': i['baha_title'], 'img_url': i['img_url'], 'year': i.get('year')} for i in final]
        return render_template('guess_processing.html', user_id=user_id)
        
    return redirect(url_for('index'))

@app.route('/stream_guess_playlist')
def stream_guess_playlist():
    sid = session['uid']
    q = GAME_QUEUE.get(sid)
    if not q: return Response("data: "+json.dumps({'error': 'Session expired or invalid queue.'})+"\n\n", mimetype='text/event-stream')
    
    def generate():
        dl = ThemeDownloader(max_workers=3) 
        try:
            for st in dl.build_playlist_generator(q):
                if st.get('done'):
                    READY_PLAYLISTS[sid] = st.get('playlist', [])
                yield f"data: {json.dumps(st)}\n\n"
        except Exception as e: 
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield ": keep-alive\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/play_game')
def play_game():
    sid = session['uid']
    user_id = request.args.get('user_id', '') # 接收前端跳轉時夾帶的 user_id
    playlist = READY_PLAYLISTS.get(sid, [])
    if not playlist: return redirect(url_for('index'))
    return render_template('guess_game.html', playlist=json.dumps(playlist), user_id=user_id)

@app.route('/result_xml/<user_id>')
def show_xml_result(user_id):
    sid = session['uid']
    if sid not in FINAL_RESULTS: return redirect(url_for('index'))
    return render_template('result.html', user_id=user_id)

@app.route('/download_xml_mem/<user_id>')
def download_xml_mem(user_id):
    sid = session['uid']
    content = FINAL_RESULTS.get(sid)
    if not content: return "Invalid Request", 404
    mem = io.BytesIO(); mem.write(content.encode('utf-8')); mem.seek(0)
    return send_file(mem, as_attachment=True, download_name=f"{user_id}_mal_import.xml", mimetype='application/xml')

@app.route('/stream_music_download')
def stream_music_download():
    user_id = request.args.get('user_id')
    sid = session['uid']
    q = MUSIC_QUEUE.get(sid)
    if not q: return Response("data: "+json.dumps({'error': 'Session expired or invalid queue.'})+"\n\n", mimetype='text/event-stream')
    
    def generate():
        dl = ThemeDownloader(max_workers=3) 
        try:
            for st in dl.download_and_zip_generator(q, os.path.join(app.config['OUTPUT_FOLDER'], f"{user_id}_anime_songs.zip")):
                yield f"data: {json.dumps(st)}\n\n"
        except Exception as e: yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield ": keep-alive\n\n"
    return Response(generate(), mimetype='text/event-stream')

@app.route('/download/<path:filename>')
def download_file(filename):
    path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(path): return "File not found.", 404
    @after_this_request
    def remove_file(response):
        try: os.remove(path)
        except: pass
        return response
    return send_file(path, as_attachment=True)

@app.route('/report_match', methods=['POST'])
def report_match():
    data = request.json
    uid = data.get('user_id')
    sid = session['uid']
    idx, msg = int(data.get('item_id')), data.get('message')
    res = TEMP_RESULTS.get(sid)
    
    if not res: return jsonify({'success': False})
    row = [datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), res[idx]['baha_title'], str(res[idx]['mal_id']), msg]
    success = append_to_sheet(row)
    return jsonify({'success': success})

@app.route('/api/audio-proxy')
def audio_proxy():
    import requests
    url = request.args.get('url')
    if not url: return "URL parameter is missing", 400
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, stream=True, timeout=15, headers=headers)
        res.raise_for_status()
        return Response(res.iter_content(chunk_size=8192), content_type=res.headers.get('Content-Type'))
    except requests.exceptions.RequestException as e:
        return str(e), 502
    
if __name__ == '__main__': 
    app.run(debug=True, port=5001, threaded=True)