import datetime
from flask import Blueprint, request, session, jsonify

from core_logic import MalXmlGenerator
from services.sheets_service import append_to_sheet
from state import TEMP_RESULTS, USER_SELECTIONS, FINAL_RESULTS, MUSIC_QUEUE, GAME_QUEUE, ANALYTICS_QUEUE

select_bp = Blueprint('select', __name__)

@select_bp.route('/results')
def get_results():
    sid = session['uid']
    user_id = request.args.get('user_id', '')
    results = TEMP_RESULTS.get(sid)
    if not results:
        return jsonify({'ok': False, 'error': 'No results found'})
    saved_sel = USER_SELECTIONS.get(sid)
    return jsonify({
        'ok': True,
        'results': results,
        'user_id': user_id,
        'saved_sel': saved_sel
    })

@select_bp.route('/dispatch', methods=['POST'])
def dispatch_action():
    data = request.json
    user_id = data.get('user_id')
    selected = data.get('selected_items', [])
    action = data.get('action')
    sid = session['uid']
    USER_SELECTIONS[sid] = selected
    
    raw = TEMP_RESULTS.get(sid)
    if not raw:
        return jsonify({'ok': False, 'error': 'Session expired'})
        
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
        return jsonify({'ok': True, 'action': 'xml', 'user_id': user_id})
        
    elif action == 'music':
        MUSIC_QUEUE[sid] = [{'mal_id': i['mal_id'], 'title': i['baha_title']} for i in final]
        return jsonify({'ok': True, 'action': 'music', 'user_id': user_id})
        
    elif action == 'guess':
        q = [{'mal_id': i['mal_id'], 'title': i['baha_title'], 'img_url': i['img_url'], 'year': i.get('year')} for i in final]
        GAME_QUEUE[sid] = q
        valid_years = [int(i['year']) for i in q if i.get('year')]
        def_min = min(valid_years) if valid_years else 2000
        def_max = max(valid_years) if valid_years else datetime.datetime.now().year
        return jsonify({
            'ok': True, 
            'action': 'guess', 
            'user_id': user_id,
            'def_min': def_min,
            'def_max': def_max,
            'total': len(q)
        })
        
    elif action == 'analytics':
        ANALYTICS_QUEUE[sid] = [
            {'mal_id': i['mal_id'], 'year': i.get('year'), 'baha_title': i['baha_title'], 'img_url': i['img_url']} 
            for i in final
        ]
        return jsonify({'ok': True, 'action': 'analytics', 'user_id': user_id})
        
    return jsonify({'ok': False, 'error': 'Unknown action'})

@select_bp.route('/report', methods=['POST'])
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
