"""
en_batch_refresh.py — 특정 종목코드의 EN 아티클을 Drive에서 자동 찾아 갱신

사용법:
    python en_batch_refresh.py                        # 로그의 6개 기업 전체
    python en_batch_refresh.py 226340 069330          # 특정 코드만
"""

import sys
import time

from main import get_google_client, find_analysis_spreadsheets, find_worksheet
from en_only import (
    read_annual, read_quarterly, read_analysis,
    read_competition, read_news,
)
from wp_en_content_generator import generate_en_article, load_peer_mapping
from wp_publisher import publish_post_en

# EN 글이 발행된 종목코드 목록 (wp_publish_log.jsonl 기준)
DEFAULT_CODES = {'226340', '069330', '464280', '131760', '331520', '009270'}


def get_stock_code(spreadsheet) -> str:
    """corp_map 시트에서 종목코드 읽기"""
    try:
        ws = find_worksheet(spreadsheet, 'corp_map')
        vals = ws.get_all_values()
        if len(vals) > 1:
            return str(vals[1][0]).strip().zfill(6)
    except Exception:
        pass
    return ''


def refresh_en(spreadsheet, company_name: str, stock_code: str):
    print(f"\n{'='*50}")
    print(f"[{company_name} / {stock_code}] EN 갱신 시작")

    ws_stock_vals    = find_worksheet(spreadsheet, '주식분석 값 입력').get_all_values()
    annual_metrics   = read_annual(ws_stock_vals)
    quarterly_by_year = read_quarterly(ws_stock_vals)

    ws_industry_vals = find_worksheet(spreadsheet, '산업 이해 및 기업 상황').get_all_values()
    analysis         = read_analysis(ws_industry_vals)

    ws_comp_vals     = find_worksheet(spreadsheet, '경쟁현황').get_all_values()
    competition      = read_competition(ws_comp_vals)

    ws_news_vals     = find_worksheet(spreadsheet, '뉴스수집').get_all_values()
    news_items, investment_points = read_news(ws_news_vals)

    # 밸류에이션
    valuation_data = {}
    try:
        ws_calc = find_worksheet(spreadsheet, '주식분석 산출값')
        def _cv(addr):
            try:
                v = ws_calc.acell(addr).value
                return str(v).strip() if v and str(v).strip() else None
            except Exception:
                return None
        mc = _cv('J24'); per = _cv('J27'); pbr = _cv('N32'); idea = _cv('S31')
        if mc:   valuation_data['market_cap'] = mc
        if per:  valuation_data['per']        = per
        if pbr:  valuation_data['pbr']        = pbr
        if idea: valuation_data['user_idea']  = idea
    except Exception:
        pass

    peer_map = load_peer_mapping()
    peers    = peer_map.get(stock_code, {})

    print("  GPT EN 아티클 생성 중...")
    en_article = generate_en_article(
        company_name           = company_name,
        stock_code             = stock_code,
        annual_metrics_by_year = annual_metrics,
        analysis               = analysis,
        competition            = competition,
        news_items             = news_items,
        investment_points      = investment_points,
        quarterly_by_year      = quarterly_by_year,
        related_posts          = [],
        peers                  = peers,
        valuation_data         = valuation_data,
    )

    print("  WordPress EN 업데이트 중...")
    en_url = publish_post_en(
        article      = en_article,
        company_data = {
            'company_name':         company_name,
            'stock_code':           stock_code,
            'annual_financials':    en_article['annual_financials'],
            'quarterly_financials': en_article['quarterly_financials'],
        },
    )
    print(f"  ✅ 완료: {en_url}")
    return en_url


def main():
    target_codes = set(sys.argv[1:]) if len(sys.argv) > 1 else DEFAULT_CODES
    target_codes = {c.zfill(6) for c in target_codes}

    print(f"대상 종목코드: {sorted(target_codes)}")
    print("Google Drive 스프레드시트 목록 조회 중...")

    gc    = get_google_client()
    files = find_analysis_spreadsheets(gc)
    print(f"  총 {len(files)}개 시트 발견")

    matched = []
    for f in files:
        try:
            sp   = gc.open_by_key(f['id'])
            code = get_stock_code(sp)
            if code in target_codes:
                matched.append((sp, f['name'], code))
                print(f"  → 매칭: {f['name']} ({code})")
        except Exception as e:
            print(f"  ⚠️ {f['name']} 열기 실패 (스킵): {e}")

    if not matched:
        print("매칭된 시트 없음. 종료.")
        return

    results = []
    for sp, name, code in matched:
        try:
            company_name = name.replace('-기업분析', '').strip()
            url = refresh_en(sp, company_name, code)
            results.append((code, company_name, '✅', url))
        except Exception as e:
            print(f"  ❌ {name} 갱신 실패: {e}")
            results.append((code, name, '❌', str(e)))
        time.sleep(2)

    print(f"\n{'='*50}")
    print("갱신 결과 요약:")
    for code, name, status, info in results:
        print(f"  {status} {name} ({code}): {info}")


if __name__ == '__main__':
    main()
