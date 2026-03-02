import datetime
import os
import json
import uuid
import threading
import io
import time
import requests
import xml.etree.ElementTree as ET
from flask import Flask, render_template, request, send_file, Response, redirect, url_for, jsonify, after_this_request, session

from config import Config
from core_logic import BahamutCrawler, MalMatcher, MalXmlGenerator, ThemeDownloader, MalAnalyticsFetcher
from services.sheets_service import append_to_sheet, log_candidates_to_sheet
from collections import Counter

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
ANALYTICS_QUEUE = {}
ANALYTICS_RESULTS = {}
MAL_IMPORT_QUEUE = {}

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

@app.route('/import_mal_xml', methods=['POST'])
def import_mal_xml():
    if 'mal_file' not in request.files:
        return render_template('index.html', error="未提供檔案。")
        
    file = request.files['mal_file']
    if file.filename == '':
        return render_template('index.html', error="未選擇檔案。")
        
    sid = session['uid']
    user_id = request.form.get('user_id', '').strip() or 'MAL_User'
    
    try:
        tree = ET.parse(file)
        root = tree.getroot()
        parsed_data = []
        
        for i, anime in enumerate(root.findall('anime')):
            mal_id_node = anime.find('series_animedb_id')
            title_node = anime.find('series_title')
            
            if mal_id_node is None or title_node is None:
                continue
                
            mal_id = mal_id_node.text
            title = title_node.text
            
            parsed_data.append({
                'id': i,
                'baha_title': title, 
                'mal_title': title,
                'mal_id': int(mal_id) if mal_id.isdigit() else None
            })
            
        if not parsed_data:
            return render_template('index.html', error="XML 檔案中未找到有效的動畫資料。")
            
        MAL_IMPORT_QUEUE[sid] = parsed_data
        
        return render_template('mal_processing.html', user_id=user_id)
    except ET.ParseError:
        return render_template('index.html', error="無效的 XML 格式。")
    except Exception as e:
        return render_template('index.html', error=f"解析發生錯誤: {str(e)}")

@app.route('/stream_mal_import')
def stream_mal_import():
    user_id = request.args.get('user_id', 'MAL_User').strip()
    sid = session['uid']
    q = MAL_IMPORT_QUEUE.get(sid)
    
    try: 
        final_url = url_for('select_results', user_id=user_id)
    except Exception: 
        return Response("data: error\n\n", mimetype='text/event-stream')

    if not q:
        return Response("data: " + json.dumps({'error': 'Queue invalid or expired.'}) + "\n\n", mimetype='text/event-stream')

    def generate():
        yield f"data: {json.dumps({'msg': f'Detected {len(q)} records. Fetching metadata...'})}\n\n"
        
        matcher = MalMatcher()
        id_cache = {v['mal_id']: v for v in matcher.cache.values() if v.get('mal_id')}
        
        results = []
        total = len(q)
        
        with requests.Session() as req_session:
            for i, item in enumerate(q):
                mal_id = item['mal_id']
                title = item['mal_title']
                
                img = 'https://cdn.myanimelist.net/img/sp/icon/apple-touch-icon-256.png'
                year = None
                status = "MAL Import"
                is_low = False 
                
                if mal_id in id_cache:
                    cached = id_cache[mal_id]
                    img = cached.get('img_url') or img
                    year = cached.get('mal_year')
                    status = "Cache Hit"
                else:
                    time.sleep(0.4) 
                    try:
                        resp = req_session.get(f"https://api.jikan.moe/v4/anime/{mal_id}", timeout=10)
                        if resp.status_code == 429:
                            time.sleep(1.5)
                            resp = req_session.get(f"https://api.jikan.moe/v4/anime/{mal_id}", timeout=10)
                            
                        if resp.status_code == 200:
                            data = resp.json().get('data', {})
                            img = data.get('images', {}).get('jpg', {}).get('image_url', img)
                            aired_from = data.get('aired', {}).get('from')
                            if aired_from and len(aired_from) >= 4 and aired_from[:4].isdigit():
                                year = int(aired_from[:4])
                            status = "API Fetched"
                    except Exception:
                        status = "API Failed"
                
                row = {
                    'id': i,
                    'baha_title': title,
                    'mal_title': title,
                    'mal_id': mal_id,
                    'status': status,
                    'img_url': img,
                    'is_low': is_low,
                    'year': year
                }
                results.append(row)
                
                yield f"data: {json.dumps({'type': 'image', 'img_url': img, 'title': title, 'status': status, 'is_low': is_low, 'current': i+1, 'total': total})}\n\n"
            
        TEMP_RESULTS[sid] = results
        yield f"data: {json.dumps({'done': True, 'redirect_url': final_url})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')

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
                
                mal_year = mal_data.get('mal_year') if mal_data else None
                final_year = mal_year if mal_year else item.get('year')

                row = {
                    'id': i,
                    'baha_title': item['ch_name'],
                    'mal_title': mal_data['title'] if mal_data else '-',
                    'mal_id': mal_data['mal_id'] if mal_data else None,
                    'status': status,
                    'img_url': img,
                    'is_low': is_low,
                    'year': final_year
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
        q = [{'mal_id': i['mal_id'], 'title': i['baha_title'], 'img_url': i['img_url'], 'year': i.get('year')} for i in final]
        GAME_QUEUE[sid] = q
        valid_years = [int(i['year']) for i in q if i.get('year')]
        def_min = min(valid_years) if valid_years else 2000
        def_max = max(valid_years) if valid_years else datetime.datetime.now().year
        return render_template('guess_setup.html', user_id=user_id, def_min=def_min, def_max=def_max, total=len(q))
    elif action == 'analytics':
        ANALYTICS_QUEUE[sid] = [
            {'mal_id': i['mal_id'], 'year': i.get('year'), 'baha_title': i['baha_title'], 'img_url': i['img_url']} 
            for i in final
        ]
        return render_template('analytics_processing.html', user_id=user_id)
        
    return redirect(url_for('index'))

@app.route('/start_guess_game', methods=['POST'])
def start_guess_game():
    sid = session['uid']
    if sid not in GAME_QUEUE:
        return redirect(url_for('index'))
    
    user_id = request.form.get('user_id')
    min_year = request.form.get('min_year')
    max_year = request.form.get('max_year')
    include_na = request.form.get('include_na') == 'on'
    
    min_year = int(min_year) if min_year and min_year.isdigit() else 0
    max_year = int(max_year) if max_year and max_year.isdigit() else 9999
    
    filtered_queue = []
    for item in GAME_QUEUE[sid]:
        y = item.get('year')
        if not y:
            if include_na: 
                filtered_queue.append(item)
        elif min_year <= int(y) <= max_year:
            filtered_queue.append(item)
            
    GAME_QUEUE[sid] = filtered_queue
    
    return render_template('guess_processing.html', user_id=user_id)

@app.route('/stream_guess_playlist')
def stream_guess_playlist():
    sid = session['uid']
    q = GAME_QUEUE.get(sid)
    if not q: return Response("data: "+json.dumps({'error': 'Session expired or invalid queue.'})+"\n\n", mimetype='text/event-stream')
    
    def generate():
        yield f"data: {json.dumps({'msg': 'Fetching reviews for sses3205...', 'progress': '0%'})}\n\n"
        crawler = BahamutCrawler("sses3205")
        reviews_dict = crawler.get_reviews("sses3205")
        
        dl = ThemeDownloader(max_workers=3) 
        try:
            for st in dl.build_playlist_generator(q, reviews_dict):
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
    user_id = request.args.get('user_id', '')
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
    url = request.args.get('url')
    if not url: return "URL parameter is missing", 400
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        with requests.Session() as req_session:
            res = req_session.get(url, stream=True, timeout=15, headers=headers)
            res.raise_for_status()
            return Response(res.iter_content(chunk_size=8192), content_type=res.headers.get('Content-Type'))
    except requests.exceptions.RequestException as e:
        return str(e), 502

@app.route('/stream_analytics')
def stream_analytics():
    sid = session['uid']
    q = ANALYTICS_QUEUE.get(sid)
    
    user_id = request.args.get('user_id')
    final_redirect_url = url_for('show_analytics', user_id=user_id)
    
    if not q: return Response("data: {\"error\": \"Invalid queue.\"}\n\n", mimetype='text/event-stream')
    
    def generate():
        fetcher = MalAnalyticsFetcher()
        total = len(q)
        results = []
        
        for idx, item in enumerate(q):
            title = item.get('baha_title', 'Unknown')
            yield f"data: {json.dumps({'msg': f'Extracting: {title} [{idx+1}/{total}]', 'progress': f'{int(((idx+1)/total)*100)}%'})}\n\n"
            
            details = fetcher.fetch_details(item['mal_id'])
            if details:
                details['year'] = item.get('year')
                details['baha_title'] = item.get('baha_title')
                details['img_url'] = item.get('img_url')
                results.append(details)
        ANALYTICS_RESULTS[sid] = results
        
        yield f"data: {json.dumps({'done': True, 'redirect_url': final_redirect_url})}\n\n"
        yield ": keep-alive\n\n"
        
    return Response(generate(), mimetype='text/event-stream')

@app.route('/analytics/<user_id>')
def show_analytics(user_id):
    sid = session['uid']
    data = ANALYTICS_RESULTS.get(sid)
    if not data: return redirect(url_for('index'))
    
    years = [str(i['year']) for i in data if i.get('year')]
    genres = [g for i in data for g in i.get('genres', [])]
    studios = [s for i in data for s in i.get('studios', [])]
    sources = [i.get('source') for i in data if i.get('source')]
    scores = [i.get('score') for i in data if i.get('score') > 0]
    
    demographics = [d for i in data for d in i.get('demographics', [])]
    total_eps = 0
    total_mins = 0
    ep_prefs = {"Movie/OVA (1)": 0, "Short (2-13)": 0, "Medium (14-26)": 0, "Long (27+)": 0}

    for i in data:
        eps = i.get('episodes', 0)
        mins = i.get('duration_mins', 0)
        
        total_eps += eps
        total_mins += (eps * mins)
        
        if eps == 1:
            ep_prefs["Movie/OVA (1)"] += 1
        elif 1 < eps <= 13:
            ep_prefs["Short (2-13)"] += 1
        elif 13 < eps <= 26:
            ep_prefs["Medium (14-26)"] += 1
        elif eps > 26:
            ep_prefs["Long (27+)"] += 1

    total_hours = round(total_mins / 60)

    ranked_data = [i for i in data if i.get('rank') and i.get('rank') < 99999]
    ranked_data.sort(key=lambda x: x['rank'])
    all_ranked = [{'title': i.get('baha_title') or i['title'], 'val': i['rank'], 'img': i.get('img_url')} for i in ranked_data]

    pop_data = [i for i in data if i.get('popularity') and i.get('popularity') < 99999]
    pop_data.sort(key=lambda x: x['popularity'])
    all_pop = [{'title': i.get('baha_title') or i['title'], 'val': i['popularity'], 'img': i.get('img_url')} for i in pop_data]
    
    stats = {
        'years': dict(Counter(years).most_common()),
        'genres': dict(Counter(genres).most_common(10)),
        'studios': dict(Counter(studios).most_common(8)),
        'sources': dict(Counter(sources).most_common()),
        'demographics': dict(Counter(demographics).most_common()),
        'ep_prefs': ep_prefs,
        'total_eps': total_eps,
        'total_hours': total_hours,
        'avg_score': round(sum(scores)/len(scores), 2) if scores else 0,
        'total_watched': len(data),
        'all_ranked': all_ranked,
        'all_popular': all_pop,
        'raw_data': data
    }
    
    return render_template('analytics.html', user_id=user_id, stats=stats)
    

@app.route('/ping')
def ping():
    return "OK", 200

if __name__ == '__main__': 
    app.run(debug=True, port=5001, threaded=True)