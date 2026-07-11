import os
import uuid
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from config import Config

# Import blueprints
from api.crawl import crawl_bp
from api.mal import mal_bp
from api.select import select_bp
from api.xml_export import xml_export_bp
from api.music import music_bp
from api.guess import guess_bp
from api.analytics import analytics_bp
from api.common import common_bp

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']

# Enable CORS for Vue frontend
CORS(app, supports_credentials=True)

if not os.path.exists(app.config['OUTPUT_FOLDER']): 
    os.makedirs(app.config['OUTPUT_FOLDER'])

@app.before_request
def ensure_session_id():
    session.permanent = True
    if 'uid' not in session:
        session['uid'] = str(uuid.uuid4())

# Register Blueprints
app.register_blueprint(crawl_bp, url_prefix='/api')
app.register_blueprint(mal_bp, url_prefix='/api')
app.register_blueprint(select_bp, url_prefix='/api')
app.register_blueprint(xml_export_bp, url_prefix='/api')
app.register_blueprint(music_bp, url_prefix='/api')
app.register_blueprint(guess_bp, url_prefix='/api')
app.register_blueprint(analytics_bp, url_prefix='/api')
app.register_blueprint(common_bp, url_prefix='/api')

if __name__ == '__main__': 
    app.run(debug=True, port=5001, threaded=True)
