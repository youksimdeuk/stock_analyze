"""
en_only.py — 구글 시트의 기저장 데이터로 EN 아티클만 생성·발행

사용법:
    python en_only.py <SPREADSHEET_ID>
"""

import sys
from datetime import datetime

from main import get_google_client, find_worksheet, ANNUAL_DATA_ROWS, ANNUAL_YEAR_START, QUARTERLY_SECTIONS
from wp_en_content_generator import generate_en_article, load_peer_mapping
from wp_publisher import publish_post_en


# ── 경쟁현황 시트 컬럼 순서 (write_competition_data 기준) ──────────────────
COMP_COLS = [
    '기업명', '국가', '최근3년매출액', '최근3년영업이익', '시장점유율(%)',
    '순위(국내/글로벌)', '주요 제품(매출액/비중)', '강점', '약점/리스크',
    'CAPEX/증설', '최근3년 기업활동 뉴스', '뉴스 원본 링크', '투자 고민 포인트', '비고',
]

# 산업 이해 및 기업 상황 섹션 (write_industry_analysis 기준)
INDUSTRY_SECTIONS = [
    '산업 개요', '산업 구조 및 특징', '산업 현재 업황', '기업의 해자(경쟁우위)',
    '주요 제품', '주요 제품 설명', '주요 원재료 및 원가 구조',
    '주요 고객 구조', '기업 상황 (재무 중심)', '매출 구조 및 이익 변동 요인',
    '최신 기술 트렌드', '투자 관점 핵심 리스크',
]


def _to_float(v):
    try:
        return float(str(v).replace(',', '')) if str(v).strip() not in ('', '-') else None
    except (ValueError, TypeError):
        return None


def read_annual(all_vals):
    """주식분석 값 입력 시트 2D 배열 → annual_metrics_by_year"""
    result = []
    current_year = datetime.now().year
    for year in range(ANNUAL_YEAR_START, current_year + 1):
        col_idx = year - ANNUAL_YEAR_START  # 0-based
        metrics = {}
        for metric, row in ANNUAL_DATA_ROWS.items():
            row_idx = row - 1  # 0-based
            try:
                val = _to_float(all_vals[row_idx][col_idx])
                if val is not None:
                    metrics[metric] = val
            except IndexError:
                pass
        if metrics.get('매출액') is not None:
            result.append((year, metrics))
    return result


def read_quarterly(all_vals):
    """주식분석 값 입력 시트 2D 배열 → quarterly_by_year"""
    result = {}
    for section in QUARTERLY_SECTIONS:
        for year_idx, year in enumerate(section['years']):
            base_col = year_idx * 4
            qdata = {}
            for q in range(1, 5):
                col_idx = base_col + q - 1
                metrics = {}
                for metric, row in section['data_rows'].items():
                    row_idx = row - 1
                    try:
                        val = _to_float(all_vals[row_idx][col_idx])
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
    """산업 이해 및 기업 상황 시트 → analysis dict"""
    analysis = {}
    # A3:B14 → index 2~13
    for row_idx in range(2, min(14, len(all_vals))):
        row = all_vals[row_idx]
        key = str(row[0]).strip() if len(row) > 0 else ''
        val = str(row[1]).strip() if len(row) > 1 else ''
        if key and val:
            analysis[key] = val
    return analysis


def read_competition(all_vals):
    """경쟁현황 시트 → competition dict"""
    competitors = []
    for row in all_vals[1:]:  # A2부터 (헤더 제외)
        if not row or not str(row[0]).strip():
            continue
        c = {}
        for i, col_name in enumerate(COMP_COLS):
            c[col_name] = str(row[i]).strip() if i < len(row) else ''
        if c.get('기업명'):
            competitors.append(c)
    return {'경쟁사목록': competitors}


def read_news(all_vals):
    """뉴스수집 시트 → news_items, investment_points"""
    news_items, investment_points = [], []
    for row in all_vals[1:]:  # A2부터
        title_desc = str(row[1]).strip() if len(row) > 1 else ''
        if not title_desc:
            break
        lines = title_desc.split('\n', 1)
        title = lines[0].strip()
        desc  = lines[1].strip() if len(lines) > 1 else ''
        pub_date = str(row[0]).strip() if len(row) > 0 else ''
        point    = str(row[3]).strip() if len(row) > 3 else ''

        news_items.append({
            'title':       title,
            'description': desc,
            'pubDate':     pub_date,
            'link':        '',
        })
        if point:
            investment_points.append(point)
    return news_items, investment_points


def main():
    sheet_id = sys.argv[1] if len(sys.argv) > 1 else ''
    if not sheet_id:
        print("사용법: python en_only.py <SPREADSHEET_ID>")
        sys.exit(1)

    print(f"구글 시트 연결 중... ({sheet_id})")
    gc = get_google_client()
    spreadsheet = gc.open_by_key(sheet_id)
    print(f"시트명: {spreadsheet.title}")

    # ── 1. 기업 정보 ─────────────────────────────────
    ws_corp = find_worksheet(spreadsheet, 'corp_map')
    corp_data = ws_corp.get_all_values()
    row2 = (corp_data[1] + ['', '', ''])[:3] if len(corp_data) > 1 else ['', '', '']
    stock_code   = str(row2[0]).strip().zfill(6)
    company_name = str(row2[2]).strip() or spreadsheet.title.replace('-기업분석', '').strip()
    print(f"기업: {company_name} ({stock_code})")

    # ── 2. 재무 데이터 (1회 일괄 호출) ──────────────
    print("재무 데이터 읽는 중...")
    ws_stock_vals = find_worksheet(spreadsheet, '주식분석 값 입력').get_all_values()
    annual_metrics_by_year = read_annual(ws_stock_vals)
    quarterly_by_year      = read_quarterly(ws_stock_vals)
    print(f"  연간: {[y for y, _ in annual_metrics_by_year]} | 분기: {sorted(quarterly_by_year.keys())}")

    # ── 3. 산업분석 (1회 일괄 호출) ─────────────────
    print("산업분석 읽는 중...")
    ws_industry_vals = find_worksheet(spreadsheet, '산업 이해 및 기업 상황').get_all_values()
    analysis = read_analysis(ws_industry_vals)
    print(f"  섹션 {len(analysis)}개")

    # ── 4. 경쟁현황 (1회 일괄 호출) ─────────────────
    print("경쟁현황 읽는 중...")
    ws_comp_vals = find_worksheet(spreadsheet, '경쟁현황').get_all_values()
    competition = read_competition(ws_comp_vals)
    print(f"  경쟁사 {len(competition['경쟁사목록'])}개")

    # ── 5. 뉴스 (1회 일괄 호출) ──────────────────────
    print("뉴스 읽는 중...")
    ws_news_vals = find_worksheet(spreadsheet, '뉴스수집').get_all_values()
    news_items, investment_points = read_news(ws_news_vals)
    print(f"  뉴스 {len(news_items)}개 | 투자포인트 {len(investment_points)}개")

    # ── 6. 밸류에이션 데이터 (주식분析 산출값 시트) ─────────────────
    print("밸류에이션 데이터 읽는 중...")
    valuation_data = {}
    try:
        ws_calc = find_worksheet(spreadsheet, '주식분析 산출값')
        def _cell_val(addr):
            try:
                v = ws_calc.acell(addr).value
                return str(v).strip() if v is not None and str(v).strip() else None
            except Exception:
                return None
        mc   = _cell_val('J24')
        per  = _cell_val('J27')
        pbr  = _cell_val('N32')
        idea = _cell_val('S31')
        if mc:   valuation_data['market_cap'] = mc
        if per:  valuation_data['per']        = per
        if pbr:  valuation_data['pbr']        = pbr
        if idea: valuation_data['user_idea']  = idea
        print(f"  시가총액={mc} | PER={per} | PBR={pbr}")
    except Exception as e:
        print(f"  ⚠️ 밸류에이션 시트 읽기 실패 (무시): {e}")

    # ── 7. 피어 매핑 (로컬 파일) ─────────────────────
    peer_map = load_peer_mapping()
    peers    = peer_map.get(stock_code, {})

    # ── 8. EN 아티클 생성 ────────────────────────────
    print("\nEN 아티클 생성 중 (GPT 호출)...")
    en_article = generate_en_article(
        company_name           = company_name,
        stock_code             = stock_code,
        annual_metrics_by_year = annual_metrics_by_year,
        analysis               = analysis,
        competition            = competition,
        news_items             = news_items,
        investment_points      = investment_points,
        quarterly_by_year      = quarterly_by_year,
        related_posts          = [],
        peers                  = peers,
        valuation_data         = valuation_data,
    )

    # ── 8. WordPress 발행 ────────────────────────────
    print("\nWordPress EN 발행 중...")
    en_url = publish_post_en(
        article      = en_article,
        company_data = {
            'company_name':         company_name,
            'stock_code':           stock_code,
            'annual_financials':    en_article['annual_financials'],
            'quarterly_financials': en_article['quarterly_financials'],
        },
    )
    print(f"\n✅ EN 발행 완료: {en_url}")
    print(f"   슬러그      : {en_article.get('slug')}")
    print(f"   포커스 키워드: {en_article.get('focus_keyword')}")


if __name__ == '__main__':
    main()
