"""
ko_restore.py — 구글 시트 기저장 데이터로 KO 아티클만 재발행 (1회성 복원용)
사용법: python ko_restore.py <SPREADSHEET_ID>
"""

import sys
from datetime import datetime

from main import (
    get_google_client, find_worksheet,
    ANNUAL_DATA_ROWS, ANNUAL_YEAR_START, QUARTERLY_SECTIONS,
)
from wp_content_generator import generate_wp_article
from wp_publisher import publish_post, upsert_post, get_or_create_category, get_or_create_tags, CATEGORY_NAME
from wp_publisher import _inject_visuals_html, _inject_anchors, _enhance_readability, _build_faq_schema_ld, _check_seo_quality
import re


COMP_COLS = [
    '기업명', '국가', '최근3년매출액', '최근3년영업이익', '시장점유율(%)',
    '순위(국내/글로벌)', '주요 제품(매출액/비중)', '강점', '약점/리스크',
    'CAPEX/증설', '최근3년 기업활동 뉴스', '뉴스 원본 링크', '투자 고민 포인트', '비고',
]


def _to_float(v):
    try:
        return float(str(v).replace(',', '')) if str(v).strip() not in ('', '-') else None
    except (ValueError, TypeError):
        return None


def read_annual(all_vals):
    result = []
    for year in range(ANNUAL_YEAR_START, datetime.now().year + 1):
        col_idx = year - ANNUAL_YEAR_START
        metrics = {}
        for metric, row in ANNUAL_DATA_ROWS.items():
            try:
                val = _to_float(all_vals[row - 1][col_idx])
                if val is not None:
                    metrics[metric] = val
            except IndexError:
                pass
        if metrics.get('매출액') is not None:
            result.append((year, metrics))
    return result


def read_quarterly(all_vals):
    result = {}
    for section in QUARTERLY_SECTIONS:
        for year_idx, year in enumerate(section['years']):
            base_col = year_idx * 4
            qdata = {}
            for q in range(1, 5):
                col_idx = base_col + q - 1
                metrics = {}
                for metric, row in section['data_rows'].items():
                    try:
                        val = _to_float(all_vals[row - 1][col_idx])
                        if val is not None:
                            metrics[metric] = val
                    except IndexError:
                        pass
                if metrics.get('매출액') is not None:
                    qdata[q] = metrics
            if qdata:
                result[year] = qdata
    return result


def read_analysis(all_vals):
    analysis = {}
    for row_idx in range(2, min(14, len(all_vals))):
        row = all_vals[row_idx]
        key = str(row[0]).strip() if len(row) > 0 else ''
        val = str(row[1]).strip() if len(row) > 1 else ''
        if key and val:
            analysis[key] = val
    return analysis


def read_competition(all_vals):
    competitors = []
    for row in all_vals[1:]:
        if not row or not str(row[0]).strip():
            continue
        c = {col: (str(row[i]).strip() if i < len(row) else '') for i, col in enumerate(COMP_COLS)}
        if c.get('기업명'):
            competitors.append(c)
    return {'경쟁사목록': competitors}


def read_news(all_vals):
    news_items, investment_points = [], []
    for row in all_vals[1:]:
        title_desc = str(row[1]).strip() if len(row) > 1 else ''
        if not title_desc:
            break
        lines = title_desc.split('\n', 1)
        news_items.append({
            'title':       lines[0].strip(),
            'description': lines[1].strip() if len(lines) > 1 else '',
            'pubDate':     str(row[0]).strip() if len(row) > 0 else '',
            'link':        '',
        })
        point = str(row[3]).strip() if len(row) > 3 else ''
        if point:
            investment_points.append({'번호': len(investment_points) + 1, '투자포인트': point})
    return news_items, investment_points


def main():
    # 사용법: python ko_restore.py <SPREADSHEET_ID> [POST_ID]
    # POST_ID 지정 시 find_existing_post 없이 해당 포스트 직접 업데이트
    sheet_id = sys.argv[1] if len(sys.argv) > 1 else ''
    force_post_id = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if not sheet_id:
        print("사용법: python ko_restore.py <SPREADSHEET_ID> [POST_ID]")
        sys.exit(1)

    print(f"구글 시트 연결 중... ({sheet_id})")
    gc = get_google_client()
    spreadsheet = gc.open_by_key(sheet_id)
    print(f"시트명: {spreadsheet.title}")

    ws_corp   = find_worksheet(spreadsheet, 'corp_map')
    corp_data = ws_corp.get_all_values()
    row2 = (corp_data[1] + ['', '', ''])[:3] if len(corp_data) > 1 else ['', '', '']
    stock_code   = str(row2[0]).strip().zfill(6)
    company_name = str(row2[2]).strip() or spreadsheet.title.replace('-기업분석', '').strip()
    print(f"기업: {company_name} ({stock_code})")

    ws_stock_vals    = find_worksheet(spreadsheet, '주식분석 값 입력').get_all_values()
    ws_industry_vals = find_worksheet(spreadsheet, '산업 이해 및 기업 상황').get_all_values()
    ws_comp_vals     = find_worksheet(spreadsheet, '경쟁현황').get_all_values()
    ws_news_vals     = find_worksheet(spreadsheet, '뉴스수집').get_all_values()

    annual_metrics_by_year = read_annual(ws_stock_vals)
    quarterly_by_year      = read_quarterly(ws_stock_vals)
    analysis               = read_analysis(ws_industry_vals)
    competition            = read_competition(ws_comp_vals)
    news_items, investment_points = read_news(ws_news_vals)

    print(f"  연간 {len(annual_metrics_by_year)}년 | 뉴스 {len(news_items)}개")

    print("\nKO 아티클 생성 중 (GPT 호출)...")
    article = generate_wp_article(
        company_name           = company_name,
        stock_code             = stock_code,
        annual_metrics_by_year = annual_metrics_by_year,
        analysis               = analysis,
        competition            = competition,
        news_items             = news_items,
        investment_points      = investment_points,
        quarterly_by_year      = quarterly_by_year,
        related_posts          = [],
    )

    print("\nWordPress KO 재발행 중...")
    if force_post_id:
        # ── 직접 지정 POST ID로 업데이트 (find_existing_post 우회) ──────────
        print(f"  [직접업데이트] 포스트 ID={force_post_id} 명시")
        annual_fin   = article.get('annual_financials', {})
        quarterly_fin = article.get('quarterly_financials', [])
        content_raw  = article.get('content', '')
        focus_kw     = article.get('focus_keyword', '')

        wp_content = _inject_visuals_html(content_raw, annual_fin, company_name, quarterly_fin)
        wp_content = _inject_anchors(wp_content)
        wp_content = _enhance_readability(wp_content)
        if focus_kw:
            wp_content, _ = _check_seo_quality(wp_content, focus_kw)
        wp_content = re.sub(r'<script[^>]*>.*?</script>', '', wp_content, flags=re.IGNORECASE | re.DOTALL)

        category_id = get_or_create_category(CATEGORY_NAME)
        tag_names   = article.get('tags', []) or [company_name, stock_code, '주식분석']
        tag_ids     = get_or_create_tags(tag_names)
        faq_schema  = _build_faq_schema_ld(article.get('faq_json', ''))

        post_payload = {
            'post_id':          force_post_id,
            'title':            article.get('title', ''),
            'content':          wp_content,
            'slug':             article.get('slug', ''),
            'status':           'draft',
            'categories':       [category_id],
            'tags':             tag_ids,
            'seo_title':        article.get('seo_title', ''),
            'meta_description': article.get('meta_description', ''),
            'focus_keyword':    focus_kw,
            'post_meta_extra':  {'_faq_schema_json': faq_schema} if faq_schema else {},
        }
        _, ko_url = upsert_post(post_payload)
    else:
        ko_url = publish_post(
            title        = article.get('title', ''),
            content      = article.get('content', ''),
            company_data = {
                'company_name':         company_name,
                'stock_code':           stock_code,
                'annual_financials':    article.get('annual_financials', {}),
                'quarterly_financials': article.get('quarterly_financials', []),
            },
            seo_data = {
                'seo_title':        article.get('seo_title', ''),
                'meta_description': article.get('meta_description', ''),
                'focus_keyword':    article.get('focus_keyword', ''),
                'slug':             article.get('slug', ''),
                'tags':             article.get('tags', []),
                'faq_json':         article.get('faq_json', ''),
            },
        )
    print(f"\n✅ KO 재발행 완료: {ko_url}")


if __name__ == '__main__':
    main()
