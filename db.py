"""
Supabase DB 연동 모듈

테이블:
- stock_posts   : 기업분석 콘텐츠 원본
- publish_runs  : 채널별 발행 이력 (wp_ko / wp_en / substack_ko / ...)

환경변수 SUPABASE_URL, SUPABASE_KEY 없으면 모든 함수는 None 반환 후 조용히 skip.
"""

from config import SUPABASE_URL, SUPABASE_KEY

_client = None


def get_db():
    """Supabase 클라이언트 반환. 환경변수 없으면 None."""
    global _client
    if _client is not None:
        return _client
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        return _client
    except Exception as e:
        print(f"  [DB] Supabase 연결 실패 (DB 저장 스킵): {e}")
        return None


# ──────────────────────────────────────────────────────────
# stock_posts
# ──────────────────────────────────────────────────────────

def upsert_post(stock_code: str, stock_name: str, period_key: str, **kwargs):
    """
    stock_posts에 upsert.
    반환: post id (str) or None (DB 없거나 실패)

    kwargs 허용 키: content_ko, content_en, content_naver, sheet_done,
                   wp_url, wp_en_url, sector, key_metrics,
                   summary_en, investment_rating
    """
    db = get_db()
    if db is None:
        return None
    _allowed = {
        "content_ko", "content_en", "content_naver", "sheet_done",
        "wp_url", "wp_en_url", "sector", "key_metrics",
        "summary_en", "investment_rating",
    }
    try:
        data = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "period_key": period_key,
            **{k: v for k, v in kwargs.items() if k in _allowed},
        }
        res = (
            db.table("stock_posts")
            .upsert(data, on_conflict="stock_code,period_key")
            .execute()
        )
        return res.data[0]["id"] if res.data else None
    except Exception as e:
        print(f"  [DB] stock_posts upsert 실패 (스킵): {e}")
        return None


def update_post(post_id: str, **kwargs):
    """post_id 기준으로 stock_posts 컬럼 업데이트."""
    db = get_db()
    if db is None or not post_id:
        return
    try:
        db.table("stock_posts").update(kwargs).eq("id", post_id).execute()
    except Exception as e:
        print(f"  [DB] stock_posts update 실패 (스킵): {e}")


def get_post(stock_code: str, period_key: str):
    """stock_code + period_key 로 post 조회. 없으면 None."""
    db = get_db()
    if db is None:
        return None
    try:
        res = (
            db.table("stock_posts")
            .select("*")
            .eq("stock_code", stock_code)
            .eq("period_key", period_key)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"  [DB] stock_posts 조회 실패 (스킵): {e}")
        return None


def get_latest_post_by_stock(stock_code: str):
    """stock_code 기준 가장 최근 post 반환. 없으면 None."""
    db = get_db()
    if db is None:
        return None
    try:
        res = (
            db.table("stock_posts")
            .select("*")
            .eq("stock_code", stock_code)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"  [DB] stock_posts 조회 실패 (스킵): {e}")
        return None


# ──────────────────────────────────────────────────────────
# publish_runs
# ──────────────────────────────────────────────────────────

def log_publish(post_id: str, channel: str, status: str,
                platform_id=None, url=None, error=None):
    """
    publish_runs에 발행 이력 기록.
    channel 예시: wp_ko / wp_en / substack_ko / substack_en
    status: success / failed / pending
    """
    db = get_db()
    if db is None or not post_id:
        return
    try:
        db.table("publish_runs").insert({
            "post_id":     post_id,
            "channel":     channel,
            "status":      status,
            "platform_id": platform_id,
            "url":         url,
            "error":       error,
        }).execute()
    except Exception as e:
        print(f"  [DB] publish_runs 기록 실패 (스킵): {e}")


def get_last_publish(post_id: str, channel: str):
    """post_id + channel 의 가장 최근 발행 이력 반환."""
    db = get_db()
    if db is None or not post_id:
        return None
    try:
        res = (
            db.table("publish_runs")
            .select("*")
            .eq("post_id", post_id)
            .eq("channel", channel)
            .order("published_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        print(f"  [DB] publish_runs 조회 실패 (스킵): {e}")
        return None


def is_channel_published(post_id: str, channel: str) -> bool:
    """해당 post_id + channel 의 최근 상태가 success인지 확인."""
    run = get_last_publish(post_id, channel)
    return bool(run and run.get("status") == "success")
