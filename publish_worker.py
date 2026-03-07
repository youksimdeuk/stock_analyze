"""
publish_worker.py — 채널별 재발행 워커

역할:
- main.py의 run_analysis가 신규 분석 + 최초 발행 담당
- publish_worker는 DB에서 미발행 건을 찾아 재발행하는 독립 실행 스크립트

사용법:
    python publish_worker.py                      # wp_ko + wp_en 미발행 전체 처리
    python publish_worker.py --channel wp_ko      # 특정 채널만

향후 채널 확장:
    substack_ko, substack_en 등 채널별 publisher 함수를 추가하면 됨
"""

import argparse

from db import get_db, log_publish, update_post, is_channel_published


def _get_pending_posts(channel: str):
    """publish_runs에 success 없는 stock_posts 목록 반환"""
    db = get_db()
    if db is None:
        print("[publish_worker] Supabase 연결 없음 — 스킵")
        return []
    try:
        res = db.table("stock_posts").select("*").eq("sheet_done", True).execute()
        posts = res.data or []
        return [p for p in posts if not is_channel_published(p['id'], channel)]
    except Exception as e:
        print(f"[publish_worker] 미발행 조회 실패: {e}")
        return []


def _publish_wp_ko(post: dict):
    """wp_ko 채널 재발행"""
    from wp_publisher import publish_post, get_related_posts, CATEGORY_NAME
    from config import WP_URL, WP_USERNAME, WP_APP_PASSWORD

    if not (WP_URL and WP_USERNAME and WP_APP_PASSWORD):
        print("  [wp_ko] WP 설정 없음 — 스킵")
        return

    content_ko = post.get('content_ko')
    if not content_ko:
        print(f"  [wp_ko] {post['stock_name']}: content_ko 없음 — 스킵 (main.py 재실행 필요)")
        return

    try:
        related_posts = get_related_posts(CATEGORY_NAME, exclude_title=post['stock_name'])
        post_url = publish_post(
            title        = f"{post['stock_name']} 기업분석 {post['period_key']}",
            content      = content_ko,
            company_data = {
                'company_name':         post['stock_name'],
                'stock_code':           post['stock_code'],
                'annual_financials':    {},
                'quarterly_financials': [],
            },
        )
        log_publish(post['id'], 'wp_ko', 'success', url=post_url)
        print(f"  ✅ [wp_ko] {post['stock_name']} 발행 완료: {post_url}")
    except Exception as e:
        log_publish(post['id'], 'wp_ko', 'failed', error=str(e))
        print(f"  ❌ [wp_ko] {post['stock_name']} 발행 실패: {e}")


def _publish_wp_en(post: dict):
    """wp_en 채널 재발행"""
    from wp_publisher import publish_post_en
    from config import WP_URL, WP_USERNAME, WP_APP_PASSWORD

    if not (WP_URL and WP_USERNAME and WP_APP_PASSWORD):
        print("  [wp_en] WP 설정 없음 — 스킵")
        return

    content_en = post.get('content_en')
    if not content_en:
        print(f"  [wp_en] {post['stock_name']}: content_en 없음 — 스킵 (main.py 재실행 필요)")
        return

    try:
        en_url = publish_post_en(
            article      = {
                'content': content_en,
                'title':   f"{post['stock_name']} Analysis {post['period_key']}",
            },
            company_data = {
                'company_name':         post['stock_name'],
                'stock_code':           post['stock_code'],
                'annual_financials':    {},
                'quarterly_financials': [],
            },
        )
        log_publish(post['id'], 'wp_en', 'success', url=en_url)
        print(f"  ✅ [wp_en] {post['stock_name']} 발행 완료: {en_url}")
    except Exception as e:
        log_publish(post['id'], 'wp_en', 'failed', error=str(e))
        print(f"  ❌ [wp_en] {post['stock_name']} 발행 실패: {e}")


# 채널 → 핸들러 매핑 (향후 substack_ko 등 추가)
CHANNEL_HANDLERS = {
    'wp_ko': _publish_wp_ko,
    'wp_en': _publish_wp_en,
}


def run(channels: list):
    print(f"\n[publish_worker] 시작 — 채널: {', '.join(channels)}")

    for channel in channels:
        handler = CHANNEL_HANDLERS.get(channel)
        if not handler:
            print(f"  [주의] '{channel}' 채널 핸들러 없음 — 스킵")
            continue

        pending = _get_pending_posts(channel)
        print(f"\n  [{channel}] 미발행 {len(pending)}건")
        for post in pending:
            handler(post)

    print(f"\n[publish_worker] 완료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="채널별 미발행 재처리 워커")
    parser.add_argument('--channel', nargs='+', default=['wp_ko', 'wp_en'],
                        help="처리할 채널 (예: wp_ko wp_en)")
    args = parser.parse_args()
    run(args.channel)
