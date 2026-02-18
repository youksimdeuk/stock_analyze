"""
기업분석 자동화 설정 파일 (환경변수 기반)

필수 환경변수:
- DART_API_KEY
- OPENAI_API_KEY
- NAVER_CLIENT_ID
- NAVER_CLIENT_SECRET

Google 인증:
- GOOGLE_CREDENTIALS_PATH / GOOGLE_TOKEN_PATH (파일 경로 사용)
또는
- GOOGLE_CREDENTIALS_JSON / GOOGLE_TOKEN_JSON (Secrets 문자열 사용)
"""

import os


def _getenv(name, default=""):
    return (os.getenv(name, default) or "").strip()


# API 키
DART_API_KEY = _getenv("DART_API_KEY")
OPENAI_API_KEY = _getenv("OPENAI_API_KEY")
NAVER_CLIENT_ID = _getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = _getenv("NAVER_CLIENT_SECRET")

# Google Sheets OAuth (파일 경로)
GOOGLE_CREDENTIALS_PATH = _getenv("GOOGLE_CREDENTIALS_PATH", "/home/youksimdeuk/credentials.json")
GOOGLE_TOKEN_PATH = _getenv("GOOGLE_TOKEN_PATH", "/home/youksimdeuk/token.json")

# Google Sheets OAuth (Secrets 문자열)
GOOGLE_CREDENTIALS_JSON = _getenv("GOOGLE_CREDENTIALS_JSON")
GOOGLE_TOKEN_JSON = _getenv("GOOGLE_TOKEN_JSON")
