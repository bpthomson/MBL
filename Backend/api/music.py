import os
import json
from flask import Blueprint, request, session, Response, send_file, current_app, after_this_request

from core_logic import ThemeDownloader
from state import MUSIC_QUEUE

music_bp = Blueprint('music', __name__)

@music_bp.route('/stream/music')
def stream_music_download():
    user_id = request.args.get('user_id')
    sid = session['uid']
    q = MUSIC_QUEUE.get(sid)
    
    if not q:
        return Response("data: " + json.dumps({'error': 'Session expired or invalid queue.'}) + "\n\n", mimetype='text/event-stream')
    
    def generate():
        dl = ThemeDownloader(max_workers=3) 
        try:
            output_path = os.path.join(current_app.config['OUTPUT_FOLDER'], f"{user_id}_anime_songs.zip")
            for st in dl.download_and_zip_generator(q, output_path):
                yield f"data: {json.dumps(st)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        yield ": keep-alive\n\n"
        
    return Response(generate(), mimetype='text/event-stream')

@music_bp.route('/download/<path:filename>')
def download_file(filename):
    path = os.path.join(current_app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(path):
        return "File not found.", 404
        
    @after_this_request
    def remove_file(response):
        try: os.remove(path)
        except: pass
        return response
        
    return send_file(path, as_attachment=True)
