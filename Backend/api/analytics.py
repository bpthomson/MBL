import json
from collections import Counter
from flask import Blueprint, request, session, jsonify, Response

from core_logic import MalAnalyticsFetcher
from state import ANALYTICS_QUEUE, ANALYTICS_RESULTS

analytics_bp = Blueprint('analytics', __name__)

@analytics_bp.route('/stream/analytics')
def stream_analytics():
    sid = session['uid']
    q = ANALYTICS_QUEUE.get(sid)
    user_id = request.args.get('user_id')
    
    if not q:
        return Response("data: {\"error\": \"Invalid queue.\"}\n\n", mimetype='text/event-stream')
    
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
        yield f"data: {json.dumps({'done': True})}\n\n"
        yield ": keep-alive\n\n"
        
    return Response(generate(), mimetype='text/event-stream')

@analytics_bp.route('/analytics')
def get_analytics():
    sid = session['uid']
    data = ANALYTICS_RESULTS.get(sid)
    
    if not data:
        return jsonify({'ok': False, 'error': 'No analytics data available'})
    
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
    
    return jsonify({
        'ok': True,
        'stats': stats
    })
