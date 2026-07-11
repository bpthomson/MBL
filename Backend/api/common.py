import requests
from flask import Blueprint, request, Response

common_bp = Blueprint('common', __name__)

@common_bp.route('/audio-proxy')
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

@common_bp.route('/ping')
def ping():
    return "OK", 200
