import json
from flask import Blueprint, request, session, jsonify, Response

from core_logic import BahamutCrawler, ThemeDownloader
from state import GAME_QUEUE, READY_PLAYLISTS

guess_bp = Blueprint('guess', __name__)

@guess_bp.route('/guess/preview')
def guess_preview():
    sid = session.get('uid')
    if not sid or sid not in GAME_QUEUE:
        return jsonify({'ok': False, 'years': [], 'na_count': 0})
    q = GAME_QUEUE[sid]
    years = [int(i['year']) for i in q if i.get('year') and str(i['year']).isdigit()]
    na_count = len([1 for i in q if not i.get('year') or not str(i['year']).isdigit()])
    return jsonify({'ok': True, 'years': years, 'na_count': na_count})


@guess_bp.route('/guess/start', methods=['POST'])
def start_guess_game():
    sid = session['uid']
    if sid not in GAME_QUEUE:
        return jsonify({'ok': False, 'error': 'Session expired'})
    
    data = request.json
    user_id = data.get('user_id')
    min_year = data.get('min_year')
    max_year = data.get('max_year')
    include_na = data.get('include_na')
    
    min_year = int(min_year) if min_year is not None and str(min_year).isdigit() else 0
    max_year = int(max_year) if max_year is not None and str(max_year).isdigit() else 9999
    
    filtered_queue = []
    for item in GAME_QUEUE[sid]:
        y = item.get('year')
        if not y:
            if include_na: 
                filtered_queue.append(item)
        elif min_year <= int(y) <= max_year:
            filtered_queue.append(item)
            
    GAME_QUEUE[sid] = filtered_queue
    return jsonify({'ok': True})

@guess_bp.route('/stream/guess-playlist')
def stream_guess_playlist():
    sid = session['uid']
    q = GAME_QUEUE.get(sid)
    if not q:
        return Response("data: " + json.dumps({'error': 'Session expired or invalid queue.'}) + "\n\n", mimetype='text/event-stream')
    
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

@guess_bp.route('/guess/playlist')
def get_guess_playlist():
    sid = session['uid']
    playlist = READY_PLAYLISTS.get(sid, [])
    if not playlist:
        return jsonify({'ok': False, 'error': 'No playlist ready'})
    return jsonify({'ok': True, 'playlist': playlist})
