import json
import threading
from flask import Blueprint, request, session, Response

from core_logic import BahamutCrawler, MalMatcher
from services.sheets_service import log_candidates_to_sheet
from state import TEMP_RESULTS, USER_SELECTIONS

crawl_bp = Blueprint('crawl', __name__)

@crawl_bp.route('/stream/progress')
def stream_progress():
    user_id = request.args.get('user_id', '').strip()
    limit = request.args.get('limit')
    sid = session['uid']

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

        yield f"data: {json.dumps({'done': True})}\n\n"
        yield ": keep-alive\n\n"

    return Response(generate(), mimetype='text/event-stream')
