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

WordPress 발행 (선택):
- WP_URL          예: https://myblog.com
- WP_USERNAME
- WP_APP_PASSWORD  Application Password
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


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

# WordPress REST API (선택 — 값 없으면 발행 스킵)
WP_URL          = _getenv("WP_URL")
WP_USERNAME     = _getenv("WP_USERNAME")
WP_APP_PASSWORD = _getenv("WP_APP_PASSWORD")

# 발행 완료 알림 Webhook (선택 — Slack 또는 Discord, 값 없으면 스킵)
# Slack:   https://hooks.slack.com/services/...
# Discord: https://discord.com/api/webhooks/...
PUBLISH_WEBHOOK_URL = _getenv("PUBLISH_WEBHOOK_URL")
