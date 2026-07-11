import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'secret_key_for_session_management')
    OUTPUT_FOLDER = 'outputs'
    SPREADSHEET_NAME = 'MyBahaList_Reports'
    CANDIDATE_SHEET_NAME = 'Cache_Candidates'
    DEBUG_SHEET_NAME = 'Low_Confidence_Debug'
    CREDENTIALS_FILE = 'credentials.json'
    CACHE_CSV_FILE = 'mal_id.csv'