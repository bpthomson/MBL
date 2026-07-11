import json
import time
import requests
import xml.etree.ElementTree as ET
from flask import Blueprint, request, session, jsonify, Response

from core_logic import MalMatcher
from state import MAL_IMPORT_QUEUE, TEMP_RESULTS

mal_bp = Blueprint('mal', __name__)

@mal_bp.route('/import-mal-xml', methods=['POST'])
def import_mal_xml():
    if 'mal_file' not in request.files:
        return jsonify({'ok': False, 'error': "未提供檔案。"})
        
    file = request.files['mal_file']
    if file.filename == '':
        return jsonify({'ok': False, 'error': "未選擇檔案。"})
        
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
            return jsonify({'ok': False, 'error': "XML 檔案中未找到有效的動畫資料。"})
            
        MAL_IMPORT_QUEUE[sid] = parsed_data
        return jsonify({'ok': True, 'user_id': user_id})
        
    except ET.ParseError:
        return jsonify({'ok': False, 'error': "無效的 XML 格式。"})
    except Exception as e:
        return jsonify({'ok': False, 'error': f"解析發生錯誤: {str(e)}"})

@mal_bp.route('/stream/mal-import')
def stream_mal_import():
    sid = session['uid']
    q = MAL_IMPORT_QUEUE.get(sid)

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
        yield f"data: {json.dumps({'done': True})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')
