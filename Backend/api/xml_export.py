import io
from flask import Blueprint, session, jsonify, send_file

from state import FINAL_RESULTS

xml_export_bp = Blueprint('xml_export', __name__)

@xml_export_bp.route('/xml-status/<user_id>')
def xml_status(user_id):
    sid = session['uid']
    ready = sid in FINAL_RESULTS
    return jsonify({'ready': ready})

@xml_export_bp.route('/download/xml/<user_id>')
def download_xml(user_id):
    sid = session['uid']
    content = FINAL_RESULTS.get(sid)
    if not content: return "Invalid Request", 404
    mem = io.BytesIO()
    mem.write(content.encode('utf-8'))
    mem.seek(0)
    return send_file(mem, as_attachment=True, download_name=f"{user_id}_mal_import.xml", mimetype='application/xml')
