"""
wp_publisher.py — WordPress REST API 포스트 발행
표(HTML table) + Chart.js 차트 포함
"""

import base64
import json
import os
import re
import time
from datetime import datetime, timezone
import requests
from requests.auth import HTTPBasicAuth
from config import WP_URL, WP_USERNAME, WP_APP_PASSWORD

# WP_BASE_URL / WP_USER 환경변수 우선, 없으면 config 값으로 폴백
_WP_BASE_URL = os.getenv('WP_BASE_URL') or WP_URL
_WP_USER     = os.getenv('WP_USER') or WP_USERNAME

LOG_FILE = 'wp_publish_log.jsonl'

CATEGORY_NAME    = '기업분석'
EN_CATEGORY_NAME = 'Global Research'   # WordPress /en/ category for English articles

_KRW_USD_RATE_CACHE = None  # (rate, fetched_at_date)

def _get_krw_usd_rate() -> float:
    """Return approximate KRW per 1 USD. Fetches from frankfurter.app (free, no key).
    Caches result for the process lifetime. Falls back to 1,400 on failure."""
    global _KRW_USD_RATE_CACHE
    today = datetime.now().strftime('%Y-%m-%d')
    if _KRW_USD_RATE_CACHE and _KRW_USD_RATE_CACHE[1] == today:
        return _KRW_USD_RATE_CACHE[0]
    try:
        resp = requests.get('https://api.frankfurter.app/latest?from=USD&to=KRW', timeout=5)
        rate = float(resp.json()['rates']['KRW'])
        _KRW_USD_RATE_CACHE = (rate, today)
        print(f'  [환율] USD/KRW = {rate:.0f} (frankfurter.app)')
        return rate
    except Exception:
        fallback = 1400.0
        _KRW_USD_RATE_CACHE = (fallback, today)
        print(f'  [환율] 조회 실패 → 고정값 {fallback} 사용')
        return fallback

# =====================================================
# 가독성 강화: 중요 키워드 목록 (길이 내림차순 정렬 — 부분문자열 선매칭 방지)
# =====================================================
_BOLD_TERMS = sorted([
    '영업이익률', '영업이익', '매출액', '당기순이익', '순이익',
    '영업현금흐름', '잉여현금흐름', '매출총이익',
    '자기자본이익률', '배당수익률', '부채비율', '유동비율',
    '시장점유율', '경쟁우위', '핵심사업', '주요사업', '성장동력',
    'ROE', 'FCF', 'CAPEX', 'EBITDA', 'PER', 'PBR', 'EPS', 'BPS',
], key=len, reverse=True)

_UNDERLINE_TERMS = sorted([
    '투자 포인트', '핵심 리스크', '주요 리스크', '투자 리스크', '성장 전망',
    '투자포인트', '핵심리스크',
], key=len, reverse=True)

_BOLD_RE  = re.compile('(' + '|'.join(re.escape(t) for t in _BOLD_TERMS) + ')')
_ULINE_RE = re.compile('(' + '|'.join(re.escape(t) for t in _UNDERLINE_TERMS) + ')')


def _auth():
    return HTTPBasicAuth(_WP_USER, WP_APP_PASSWORD)


def _api(path):
    return f"{_WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/{path}"


# =====================================================
# 카테고리 / 태그
# =====================================================

def get_or_create_category(name):
    r = requests.get(
        _api('categories'),
        params={'search': name, 'per_page': 10},
        auth=_auth(), timeout=15,
    )
    r.raise_for_status()
    for item in r.json():
        if item.get('name') == name:
            return item['id']

    r = requests.post(_api('categories'), json={'name': name}, auth=_auth(), timeout=15)
    r.raise_for_status()
    return r.json()['id']


def get_or_create_tags(tag_names):
    tag_ids = []
    for name in tag_names:
        if not name:
            continue
        r = requests.get(
            _api('tags'),
            params={'search': name, 'per_page': 5},
            auth=_auth(), timeout=15,
        )
        r.raise_for_status()
        matched = next((x for x in r.json() if x.get('name') == name), None)
        if matched:
            tag_ids.append(matched['id'])
        else:
            r = requests.post(_api('tags'), json={'name': name}, auth=_auth(), timeout=15)
            if r.status_code in (200, 201):
                tag_ids.append(r.json()['id'])
    return tag_ids


# =====================================================
# 재무 테이블 HTML 생성
# =====================================================

def _fmt_eok(val):
    if val is None:
        return '-'
    try:
        return f"{float(val) / 1e8:,.1f}"
    except (TypeError, ValueError):
        return '-'


def _fmt_pct(val):
    if val is None:
        return '-'
    try:
        return f"{float(val) * 100:.1f}%"
    except (TypeError, ValueError):
        return '-'


def _fmt_usd_m(val):
    """원(₩) 단위 값 → USD million 문자열"""
    if val is None:
        return '-'
    try:
        return f"{float(val) / _get_krw_usd_rate() / 1e6:,.0f}"
    except (TypeError, ValueError):
        return '-'


def _fmt_q_usd(val_eok):
    """억원 단위 분기 데이터 → USD million 문자열"""
    if val_eok is None:
        return '-'
    try:
        return f"{float(val_eok) * 1e8 / _get_krw_usd_rate() / 1e6:,.0f}"
    except (TypeError, ValueError):
        return '-'


def _build_financial_table_html(annual_financials, lang='ko'):
    """연간 재무 데이터 → HTML 압축 테이블 (최신 4년, 모바일 최적화)"""
    if not annual_financials:
        return ''

    years = sorted(annual_financials.keys())[-4:]  # 최신 4년만

    if lang == 'en':
        rows_def = [
            ('매출액',           'Revenue',    _fmt_usd_m),
            ('영업이익',         'Op.Profit',  _fmt_usd_m),
            ('영업이익률',       'Op.Margin',  _fmt_pct),
            ('당기순이익',       'Net Income', _fmt_usd_m),
            ('영업활동현금흐름', 'OCF',        _fmt_usd_m),
            ('CAPEX',           'CAPEX',       _fmt_usd_m),
            ('ROE',             'ROE',         _fmt_pct),
        ]
        caption_text = '▶ Annual Financials (Unit: USD million, approx.)'
        item_label   = 'Item'
        yr_suffix    = ''
    else:
        rows_def = [
            ('매출액',           '매출액',    _fmt_eok),
            ('영업이익',         '영업이익',  _fmt_eok),
            ('영업이익률',       '영업이익률', _fmt_pct),
            ('당기순이익',       '순이익',    _fmt_eok),
            ('영업활동현금흐름', 'OCF',       _fmt_eok),
            ('CAPEX',           'CAPEX',      _fmt_eok),
            ('ROE',             'ROE',        _fmt_pct),
        ]
        caption_text = '▶ 연간 재무 실적 요약 (단위: 억원)'
        item_label   = '구분'
        yr_suffix    = '년'

    hdr_bg    = '#1a3a5c'
    style     = 'border-collapse:collapse;width:100%;font-size:12px;margin:20px 0;'
    th_style  = f'background:{hdr_bg};color:#fff;padding:6px 8px;text-align:center;border:1px solid #ddd;white-space:nowrap;'
    td_style  = 'padding:6px 8px;text-align:right;border:1px solid #ddd;white-space:nowrap;'
    td0_style = 'padding:6px 8px;text-align:left;border:1px solid #ddd;font-weight:bold;background:#f5f8fc;white-space:nowrap;'
    tr_even   = 'background:#f9f9f9;'

    header_cells = ''.join(f'<th style="{th_style}">{y}{yr_suffix}</th>' for y in years)
    thead = f'<thead><tr><th style="{th_style}">{item_label}</th>{header_cells}</tr></thead>'

    tbody_rows = []
    for idx, (key, label, fmt_fn) in enumerate(rows_def):
        row_bg = tr_even if idx % 2 == 1 else ''
        cells = ''.join(
            f'<td style="{td_style}">{fmt_fn(annual_financials.get(y, {}).get(key))}</td>'
            for y in years
        )
        tbody_rows.append(
            f'<tr style="{row_bg}">'
            f'<td style="{td0_style}">{label}</td>'
            f'{cells}'
            f'</tr>'
        )
    tbody = f'<tbody>{"".join(tbody_rows)}</tbody>'

    cap_style = f'caption-side:top;text-align:left;font-weight:bold;font-size:14px;margin-bottom:8px;color:{hdr_bg};'
    return (
        f'<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;">'
        f'<table style="{style}">'
        f'<caption style="{cap_style}">{caption_text}</caption>'
        f'{thead}{tbody}'
        f'</table>'
        f'</div>'
    )


# =====================================================
# 재무건전성 미니 지표 카드 (ROE + FCF)
# =====================================================

def _build_health_indicators_html(annual_financials):
    """ROE(%) + FCF(= OCF - CAPEX, 억원) 3~5년 추이 미니 카드"""
    if not annual_financials:
        return ''
    years = sorted(annual_financials.keys())

    def _roe(m):
        v = m.get('ROE')
        if v is None:
            return None
        try:
            return float(v) * 100
        except (TypeError, ValueError):
            return None

    def _fcf(m):
        ocf  = m.get('영업활동현금흐름')
        capex = m.get('CAPEX')
        if ocf is None or capex is None:
            return None
        try:
            return (float(ocf) - float(capex)) / 1e8
        except (TypeError, ValueError):
            return None

    def _color(val, positive_good=True):
        if val is None:
            return '#6b7280'
        return '#16a34a' if (val >= 0) == positive_good else '#dc2626'

    def _fmt_roe(val):
        return f'{val:.1f}%' if val is not None else '-'

    def _fmt_fcf(val):
        if val is None:
            return '-'
        return f'{val:+,.0f}'

    roe_vals = [_roe(annual_financials.get(y, {})) for y in years]
    fcf_vals = [_fcf(annual_financials.get(y, {})) for y in years]

    # 모두 None이면 카드 생성 안 함
    if all(v is None for v in roe_vals) and all(v is None for v in fcf_vals):
        return ''

    th_s = 'padding:6px 12px;text-align:center;font-size:12px;color:#fff;background:#374151;border:1px solid #d1d5db;'
    td0_s = 'padding:6px 12px;font-size:12px;font-weight:600;color:#374151;background:#f9fafb;border:1px solid #d1d5db;'
    td_s  = 'padding:6px 12px;text-align:right;font-size:13px;font-weight:700;border:1px solid #d1d5db;'

    year_headers = ''.join(f'<th style="{th_s}">{y}년</th>' for y in years)

    roe_cells = ''.join(
        f'<td style="{td_s}color:{_color(v)};">{_fmt_roe(v)}</td>'
        for v in roe_vals
    )
    fcf_cells = ''.join(
        f'<td style="{td_s}color:{_color(v)};">{_fmt_fcf(v)}</td>'
        for v in fcf_vals
    )

    return (
        '<div style="margin:16px 0 24px 0;overflow-x:auto;">'
        '<p style="font-weight:700;font-size:13px;color:#374151;margin:0 0 6px 0;">'
        '▶ 재무건전성 지표</p>'
        f'<table style="border-collapse:collapse;font-size:13px;">'
        f'<thead><tr><th style="{th_s}">지표</th>{year_headers}</tr></thead>'
        f'<tbody>'
        f'<tr><td style="{td0_s}">ROE (%)</td>{roe_cells}</tr>'
        f'<tr><td style="{td0_s}">FCF (억원)</td>{fcf_cells}</tr>'
        f'</tbody></table>'
        '<p style="font-size:11px;color:#9ca3af;margin:4px 0 0 0;">'
        'FCF = 영업활동현금흐름 − CAPEX. 양수(초록)·음수(빨강).</p>'
        '</div>'
    )


# =====================================================
# 분기 실적 테이블 HTML 생성
# =====================================================

def _fmt_q(val, is_pct=False):
    """분기 데이터 전용 포맷 — 이미 억원/% 변환된 값을 그대로 출력"""
    if val is None:
        return '-'
    try:
        v = float(val)
        return f"{v:.1f}%" if is_pct else f"{v:,.1f}"
    except (TypeError, ValueError):
        return '-'


def _build_quarterly_table_html(quarterly_financials, lang='ko'):
    """분기 실적 → HTML 압축 테이블 (최신 6분기, 모바일 최적화)"""
    if not quarterly_financials:
        return ''

    items = quarterly_financials[:8]  # 최신 8분기 (차트와 동일)

    if lang == 'en':
        headers      = ['Quarter', 'Revenue', 'Op.Profit', 'Op.Margin', 'Net Income']
        caption_text = '▶ Quarterly Financials (Unit: USD million, approx.)'
    else:
        headers      = ['분기', '매출액', '영업이익', '영업이익률', '순이익']
        caption_text = '▶ 최근 분기 실적 (최신순, 단위: 억원)'

    hdr_bg    = '#2c5f8a'
    style     = 'border-collapse:collapse;width:100%;font-size:12px;margin:20px 0;'
    th_style  = f'background:{hdr_bg};color:#fff;padding:6px 8px;text-align:center;border:1px solid #ddd;white-space:nowrap;'
    td_style  = 'padding:6px 8px;text-align:right;border:1px solid #ddd;white-space:nowrap;'
    td0_style = 'padding:6px 8px;text-align:center;border:1px solid #ddd;font-weight:bold;background:#f0f5fa;white-space:nowrap;'
    tr_even   = 'background:#f9f9f9;'

    thead = (
        '<thead><tr>'
        + ''.join(f'<th style="{th_style}">{h}</th>' for h in headers)
        + '</tr></thead>'
    )

    tbody_rows = []
    for idx, q in enumerate(items):
        row_bg = tr_even if idx % 2 == 1 else ''
        분기   = q.get('분기', '-')
        opm    = _fmt_q(q.get('영업이익률pct'), is_pct=True)
        if lang == 'en':
            rev = _fmt_q_usd(q.get('매출액억원'))
            op  = _fmt_q_usd(q.get('영업이익억원'))
            ni  = _fmt_q_usd(q.get('당기순이익억원'))
        else:
            rev = _fmt_q(q.get('매출액억원'))
            op  = _fmt_q(q.get('영업이익억원'))
            ni  = _fmt_q(q.get('당기순이익억원'))
        tbody_rows.append(
            f'<tr style="{row_bg}">'
            f'<td style="{td0_style}">{분기}</td>'
            f'<td style="{td_style}">{rev}</td>'
            f'<td style="{td_style}">{op}</td>'
            f'<td style="{td_style}">{opm}</td>'
            f'<td style="{td_style}">{ni}</td>'
            f'</tr>'
        )
    tbody = f'<tbody>{"".join(tbody_rows)}</tbody>'

    cap_style = f'caption-side:top;text-align:left;font-weight:bold;font-size:14px;margin-bottom:8px;color:{hdr_bg};'
    return (
        f'<div style="overflow-x:auto;-webkit-overflow-scrolling:touch;">'
        f'<table style="{style}">'
        f'<caption style="{cap_style}">{caption_text}</caption>'
        f'{thead}{tbody}'
        f'</table>'
        f'</div>'
    )


# =====================================================
# 모바일 대응: 감싸지지 않은 <table> 래핑
# =====================================================

def _wrap_tables_responsive(html):
    """overflow-x:auto div 로 감싸지지 않은 모든 <table>...</table> 을 래핑 (모바일 가로 스크롤)"""
    WRAP_OPEN = '<div style="overflow-x:auto;">'

    def _replacer(m):
        prefix = html[:m.start()].rstrip()
        if prefix.endswith(WRAP_OPEN):
            return m.group(0)
        return f'{WRAP_OPEN}{m.group(0)}</div>'

    return re.sub(r'<table[\s\S]*?</table>', _replacer, html, flags=re.IGNORECASE)


# =====================================================
# SVG 차트 생성 (JS 불필요 — 보안 플러그인 우회)
# =====================================================

def _build_svg_chart(annual_financials, company_name='', lang='ko'):
    """
    순수 SVG로 매출액(진파랑 막대) + 영업이익(하늘색 막대) + 영업이익률(빨간 꺾은선) 차트 생성.
    JavaScript 불필요 → WordPress 보안 플러그인 영향 없음.
    """
    if not annual_financials:
        return ''

    years = sorted(annual_financials.keys())
    n     = len(years)
    if n == 0:
        return ''

    def to_eok(v):
        try:
            return float(v) / 1e8 if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def to_usd_m(v):
        try:
            return float(v) / _get_krw_usd_rate() / 1e6 if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    def to_pct(v):
        try:
            return float(v) * 100 if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    to_bar     = to_usd_m if lang == 'en' else to_eok
    revenues   = [to_bar(annual_financials[y].get('매출액'))   for y in years]
    op_profits = [to_bar(annual_financials[y].get('영업이익')) for y in years]
    op_margins = [to_pct(annual_financials[y].get('영업이익률')) for y in years]

    # 캔버스 설정
    W, H          = 640, 300
    pad_l, pad_r  = 72, 65
    pad_t, pad_b  = 40, 48
    cw = W - pad_l - pad_r   # 차트 너비
    ch = H - pad_t - pad_b   # 차트 높이

    max_bar = max(revenues + op_profits + [1]) * 1.15
    max_pct = max(op_margins + [1]) * 1.3

    bar_group_w = cw / n
    bw          = bar_group_w * 0.32   # 막대 하나 너비

    elems = []

    # 배경
    elems.append(f'<rect x="{pad_l}" y="{pad_t}" width="{cw}" height="{ch}" fill="#fafafa" rx="4"/>')

    # 가로 그리드 (5개)
    for i in range(6):
        frac = i / 5
        gy   = pad_t + ch * (1 - frac)
        val  = max_bar * frac
        elems.append(f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{pad_l+cw}" y2="{gy:.1f}" stroke="#e0e0e0" stroke-width="1"/>')
        elems.append(
            f'<text x="{pad_l-6}" y="{gy+4:.1f}" text-anchor="end" '
            f'font-size="10" fill="#888">{val:,.0f}</text>'
        )
        # 우측 축 (%)
        pval = max_pct * frac
        elems.append(
            f'<text x="{pad_l+cw+6}" y="{gy+4:.1f}" text-anchor="start" '
            f'font-size="10" fill="#c0392b">{pval:.1f}%</text>'
        )

    # 막대 + 연도 레이블
    for i, year in enumerate(years):
        xc  = pad_l + (i + 0.5) * bar_group_w

        # 매출액 막대
        rev = revenues[i]
        rh  = (rev / max_bar) * ch if max_bar > 0 else 0
        rx  = xc - bw - 2
        ry  = pad_t + ch - rh
        elems.append(f'<rect x="{rx:.1f}" y="{ry:.1f}" width="{bw:.1f}" height="{rh:.1f}" fill="#1a3a5c" rx="2"/>')
        if rev > 0:
            elems.append(
                f'<text x="{rx + bw/2:.1f}" y="{ry - 3:.1f}" text-anchor="middle" '
                f'font-size="9" fill="#1a3a5c">{rev:,.0f}</text>'
            )

        # 영업이익 막대
        op  = op_profits[i]
        oh  = (op / max_bar) * ch if max_bar > 0 and op > 0 else 0
        ox  = xc + 2
        oy  = pad_t + ch - oh
        elems.append(f'<rect x="{ox:.1f}" y="{oy:.1f}" width="{bw:.1f}" height="{oh:.1f}" fill="#3498db" rx="2"/>')
        if op > 0:
            elems.append(
                f'<text x="{ox + bw/2:.1f}" y="{oy - 3:.1f}" text-anchor="middle" '
                f'font-size="9" fill="#2980b9">{op:,.0f}</text>'
            )

        # 연도 레이블
        elems.append(
            f'<text x="{xc:.1f}" y="{H - 10}" text-anchor="middle" '
            f'font-size="11" fill="#444">{year}</text>'
        )

    # 영업이익률 꺾은선
    margin_pts = []
    for i, m in enumerate(op_margins):
        xc = pad_l + (i + 0.5) * bar_group_w
        my = pad_t + ch * (1 - m / max_pct) if max_pct > 0 else pad_t + ch
        margin_pts.append((xc, my, m))

    if len(margin_pts) > 1:
        polyline = ' '.join(f'{x:.1f},{y:.1f}' for x, y, _ in margin_pts)
        elems.append(
            f'<polyline points="{polyline}" fill="none" stroke="#e74c3c" '
            f'stroke-width="2.5" stroke-linejoin="round"/>'
        )
    for xc, my, m in margin_pts:
        elems.append(f'<circle cx="{xc:.1f}" cy="{my:.1f}" r="4" fill="#e74c3c" stroke="#fff" stroke-width="1.5"/>')
        elems.append(
            f'<text x="{xc:.1f}" y="{my - 8:.1f}" text-anchor="middle" '
            f'font-size="9" fill="#c0392b">{m:.1f}%</text>'
        )

    # 축 테두리
    elems.append(
        f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+ch}" stroke="#bbb" stroke-width="1.5"/>'
    )
    elems.append(
        f'<line x1="{pad_l}" y1="{pad_t+ch}" x2="{pad_l+cw}" y2="{pad_t+ch}" stroke="#bbb" stroke-width="1.5"/>'
    )

    # 범례
    if lang == 'en':
        rev_label   = 'Revenue (USD M)'
        op_label    = 'Op. Profit (USD M)'
        opm_label   = 'Op. Margin (%)'
        alt_text    = f"{company_name} Annual Revenue & Operating Profit Trend" if company_name else "Annual Financial Chart"
        chart_title = '▶ Revenue & Operating Profit Trend (USD M, approx.)'
    else:
        rev_label   = '매출액(억원)'
        op_label    = '영업이익(억원)'
        opm_label   = '영업이익률(%)'
        alt_text    = f"{company_name} 연간 매출·영업이익·영업이익률 추이" if company_name else "연간 재무 실적 차트"
        chart_title = '▶ 매출액·영업이익 추이 및 영업이익률'

    ly = 14
    elems += [
        f'<rect x="{pad_l}" y="{ly}" width="12" height="12" fill="#1a3a5c" rx="2"/>',
        f'<text x="{pad_l+15}" y="{ly+10}" font-size="11" fill="#333">{rev_label}</text>',
        f'<rect x="{pad_l+95}" y="{ly}" width="12" height="12" fill="#3498db" rx="2"/>',
        f'<text x="{pad_l+110}" y="{ly+10}" font-size="11" fill="#333">{op_label}</text>',
        f'<line x1="{pad_l+205}" y1="{ly+6}" x2="{pad_l+218}" y2="{ly+6}" stroke="#e74c3c" stroke-width="2.5"/>',
        f'<circle cx="{pad_l+211}" cy="{ly+6}" r="3.5" fill="#e74c3c"/>',
        f'<text x="{pad_l+222}" y="{ly+10}" font-size="11" fill="#c0392b">{opm_label}</text>',
    ]

    svg_inner = '\n  '.join(elems)
    svg_str = (
        f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'\n  {svg_inner}\n'
        f'</svg>'
    )
    svg_b64 = base64.b64encode(svg_str.encode('utf-8')).decode('ascii')
    return (
        f'<div style="margin:24px 0;">'
        f'<p style="font-weight:bold;font-size:15px;color:#1a3a5c;margin-bottom:8px;">'
        f'{chart_title}</p>'
        f'<img src="data:image/svg+xml;base64,{svg_b64}" '
        f'style="max-width:660px;width:100%;display:block;" alt="{alt_text}"/>'
        f'</div>'
    )


# =====================================================
# 분기 SVG 차트 생성
# =====================================================

def _build_quarterly_svg_chart(quarterly_financials, company_name='', lang='ko'):
    """
    분기 실적 데이터로 SVG 차트 생성 (매출액·영업이익 막대 + 영업이익률 꺾은선).
    quarterly_financials는 최신순 리스트 → 시간순으로 역전하여 표시.
    값은 이미 억원/% 단위.
    """
    if not quarterly_financials:
        return ''

    items = list(reversed(quarterly_financials))  # 시간순 정렬
    n = len(items)

    def safe_f(v):
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    labels = [q.get('분기', '-') for q in items]
    if lang == 'en':
        # 억원 → USD M (1억원 = 1e8 KRW, ÷ KRW/USD ÷ 1e6)
        _rate = _get_krw_usd_rate()
        revenues   = [safe_f(q.get('매출액억원'))   * 1e8 / _rate / 1e6 for q in items]
        op_profits = [safe_f(q.get('영업이익억원')) * 1e8 / _rate / 1e6 for q in items]
    else:
        revenues   = [safe_f(q.get('매출액억원'))   for q in items]
        op_profits = [safe_f(q.get('영업이익억원')) for q in items]
    op_margins = [safe_f(q.get('영업이익률pct')) for q in items]  # 이미 %

    W, H         = 640, 300
    pad_l, pad_r = 72, 65
    pad_t, pad_b = 40, 50
    cw = W - pad_l - pad_r
    ch = H - pad_t - pad_b

    max_bar = max(revenues + op_profits + [1]) * 1.15
    max_pct = max(abs(m) for m in op_margins + [1]) * 1.3

    bar_group_w = cw / n
    bw = bar_group_w * 0.30

    elems = []

    # 배경
    elems.append(f'<rect x="{pad_l}" y="{pad_t}" width="{cw}" height="{ch}" fill="#fafafa" rx="4"/>')

    # 그리드 (5개)
    for i in range(6):
        frac = i / 5
        gy   = pad_t + ch * (1 - frac)
        val  = max_bar * frac
        elems.append(f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{pad_l+cw}" y2="{gy:.1f}" stroke="#e0e0e0" stroke-width="1"/>')
        elems.append(f'<text x="{pad_l-6}" y="{gy+4:.1f}" text-anchor="end" font-size="10" fill="#888">{val:,.0f}</text>')
        pval = max_pct * frac
        elems.append(f'<text x="{pad_l+cw+6}" y="{gy+4:.1f}" text-anchor="start" font-size="10" fill="#c0392b">{pval:.1f}%</text>')

    # 막대 + 분기 레이블
    for i, label in enumerate(labels):
        xc = pad_l + (i + 0.5) * bar_group_w

        # 매출액 막대
        rev = revenues[i]
        rh  = (rev / max_bar) * ch if max_bar > 0 else 0
        rx  = xc - bw - 2
        ry  = pad_t + ch - rh
        elems.append(f'<rect x="{rx:.1f}" y="{ry:.1f}" width="{bw:.1f}" height="{rh:.1f}" fill="#1a3a5c" rx="2"/>')

        # 영업이익 막대 (양수만)
        op = op_profits[i]
        if op > 0:
            oh = (op / max_bar) * ch if max_bar > 0 else 0
            ox = xc + 2
            oy = pad_t + ch - oh
            elems.append(f'<rect x="{ox:.1f}" y="{oy:.1f}" width="{bw:.1f}" height="{oh:.1f}" fill="#3498db" rx="2"/>')

        # 분기 레이블 (짧게 표시: 2023Q1 → 23Q1)
        short_label = label[2:] if len(label) >= 6 else label
        elems.append(
            f'<text x="{xc:.1f}" y="{H-10}" text-anchor="middle" '
            f'font-size="9" fill="#444">{short_label}</text>'
        )

    # 영업이익률 꺾은선
    margin_pts = []
    for i, m in enumerate(op_margins):
        xc = pad_l + (i + 0.5) * bar_group_w
        my = pad_t + ch * (1 - m / max_pct) if max_pct > 0 else pad_t + ch
        margin_pts.append((xc, my, m))

    if len(margin_pts) > 1:
        polyline = ' '.join(f'{x:.1f},{y:.1f}' for x, y, _ in margin_pts)
        elems.append(f'<polyline points="{polyline}" fill="none" stroke="#e74c3c" stroke-width="2.5" stroke-linejoin="round"/>')
    for xc, my, m in margin_pts:
        elems.append(f'<circle cx="{xc:.1f}" cy="{my:.1f}" r="4" fill="#e74c3c" stroke="#fff" stroke-width="1.5"/>')
        elems.append(f'<text x="{xc:.1f}" y="{my-8:.1f}" text-anchor="middle" font-size="9" fill="#c0392b">{m:.1f}%</text>')

    # 축
    elems.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t+ch}" stroke="#bbb" stroke-width="1.5"/>')
    elems.append(f'<line x1="{pad_l}" y1="{pad_t+ch}" x2="{pad_l+cw}" y2="{pad_t+ch}" stroke="#bbb" stroke-width="1.5"/>')

    # 범례
    if lang == 'en':
        rev_label   = 'Revenue (USD M)'
        op_label    = 'Op. Profit (USD M)'
        opm_label   = 'Op. Margin (%)'
        alt_text    = f"{company_name} Quarterly Revenue & Operating Profit Trend" if company_name else "Quarterly Financial Chart"
        chart_title = '▶ Quarterly Revenue & Operating Profit Trend (USD M, approx.)'
    else:
        rev_label   = '매출액(억원)'
        op_label    = '영업이익(억원)'
        opm_label   = '영업이익률(%)'
        alt_text    = f"{company_name} 분기별 매출·영업이익·영업이익률 추이" if company_name else "분기 실적 차트"
        chart_title = '▶ 분기별 매출액·영업이익 추이 및 영업이익률'

    ly = 14
    elems += [
        f'<rect x="{pad_l}" y="{ly}" width="12" height="12" fill="#1a3a5c" rx="2"/>',
        f'<text x="{pad_l+15}" y="{ly+10}" font-size="11" fill="#333">{rev_label}</text>',
        f'<rect x="{pad_l+95}" y="{ly}" width="12" height="12" fill="#3498db" rx="2"/>',
        f'<text x="{pad_l+110}" y="{ly+10}" font-size="11" fill="#333">{op_label}</text>',
        f'<line x1="{pad_l+205}" y1="{ly+6}" x2="{pad_l+218}" y2="{ly+6}" stroke="#e74c3c" stroke-width="2.5"/>',
        f'<circle cx="{pad_l+211}" cy="{ly+6}" r="3.5" fill="#e74c3c"/>',
        f'<text x="{pad_l+222}" y="{ly+10}" font-size="11" fill="#c0392b">{opm_label}</text>',
    ]

    svg_inner = '\n  '.join(elems)
    svg_str = (
        f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'\n  {svg_inner}\n'
        f'</svg>'
    )
    svg_b64 = base64.b64encode(svg_str.encode('utf-8')).decode('ascii')
    return (
        f'<div style="margin:24px 0;">'
        f'<p style="font-weight:bold;font-size:15px;color:#2c5f8a;margin-bottom:8px;">'
        f'{chart_title}</p>'
        f'<img src="data:image/svg+xml;base64,{svg_b64}" '
        f'style="max-width:660px;width:100%;display:block;" alt="{alt_text}"/>'
        f'</div>'
    )


# =====================================================
# HTML 콘텐츠에 재무 테이블/차트 주입 (GPT HTML 출력용)
# =====================================================
# 목차 앵커 자동 연결
# =====================================================

def _slugify_heading(text):
    """H2 텍스트에서 앵커용 id 문자열 생성 (한글 포함 그대로 사용)"""
    text = re.sub(r'<[^>]+>', '', text).strip()
    text = re.sub(r'[^\w\s\-가-힣ㄱ-ㅎㅏ-ㅣ]', '', text)
    text = re.sub(r'\s+', '-', text)
    return text[:60] or 'section'


def _inject_anchors(html):
    """H2에 id 부여 + 첫 번째 목차 ul의 li에 앵커 href 연결"""
    # ── H2 id 부여 ──────────────────────────────────
    h2_pattern = re.compile(r'<h2([^>]*)>(.*?)</h2>', re.DOTALL)
    slugs = []   # [(h2_text, slug), ...]
    used = set()

    def add_id(m):
        attrs, inner = m.group(1), m.group(2)
        raw = re.sub(r'<[^>]+>', '', inner).strip()
        base = _slugify_heading(raw)
        slug, n = base, 1
        while slug in used:
            slug = f'{base}-{n}'; n += 1
        used.add(slug)
        slugs.append((raw, slug))
        if 'id=' in attrs:
            attrs = re.sub(r'\bid=["\'][^"\']*["\']', f'id="{slug}"', attrs)
        else:
            attrs = f' id="{slug}"' + attrs
        return f'<h2{attrs}>{inner}</h2>'

    html = h2_pattern.sub(add_id, html)

    # ── 특수 앵커: '투자 결론 요약' div (H2 없이 div로 생성됨) ─────
    # 프롬프트 구조상 <div style="background:#f0f5fa..."><p>투자 결론 요약</p>... 형태
    _INVEST_CONCLUSION_SLUG = '투자-결론-요약'
    if '투자 결론 요약' in html and f'id="{_INVEST_CONCLUSION_SLUG}"' not in html:
        # 바로 앞 <div 태그에 id 삽입 (투자 결론 요약 p를 포함하는 div)
        html = re.sub(
            r'<div([^>]*)>(?=\s*<p[^>]*>[^<]*투자 결론 요약)',
            rf'<div id="{_INVEST_CONCLUSION_SLUG}"\1>',
            html, count=1
        )
        slugs.append(('투자 결론 요약', _INVEST_CONCLUSION_SLUG))

    if not slugs:
        return html

    # ── 목차 ul li에 href 연결 ───────────────────────
    def linkify_li(li_m):
        inner = li_m.group(1)
        # GPT가 <a href="#..."> 를 이미 생성해도 href가 잘못될 수 있으므로
        # 항상 텍스트 추출 → 퍼지 매칭 → 올바른 href로 교체
        li_text = re.sub(r'<[^>]+>', '', inner).strip()
        best_slug, best_score = None, 0.0
        for h2_text, slug in slugs:
            if not li_text or not h2_text:
                continue
            score = (len(set(li_text) & set(h2_text)) /
                     max(len(set(li_text) | set(h2_text)), 1))
            if score > best_score:
                best_score, best_slug = score, slug
        if best_slug and best_score >= 0.50:
            # 기존 <a> 래퍼 제거 후 새 href로 교체 (강조 태그 등 inner HTML 보존)
            inner_stripped = re.sub(r'</?a[^>]*>', '', inner)
            return f'<li><a href="#{best_slug}">{inner_stripped}</a></li>'
        return li_m.group(0)

    first_ul = re.search(r'<ul>(.*?)</ul>', html, re.DOTALL)
    if first_ul:
        new_ul = re.sub(r'<li>(.*?)</li>', linkify_li, first_ul.group(1), flags=re.DOTALL)
        html = html[:first_ul.start(1)] + new_ul + html[first_ul.end(1):]

    return html


def _enhance_readability(html):
    """
    <p>, <li> 내 중요 키워드를 자동 강조:
      - _UNDERLINE_TERMS : <u><strong> (밑줄 + 굵게)
      - _BOLD_TERMS      : <strong> (굵게)
    표·제목(h2/h3)·이미 강조된 태그(<strong>/<a>/<u>) 안쪽은 건드리지 않음.
    """

    def _fmt_text(text):
        """순수 텍스트 노드(태그 없음)에 강조 적용"""
        # 1) 밑줄+굵게+색(레드) 먼저 (단일 패스 regex → 부분문자열 중복 방지)
        text = _ULINE_RE.sub(
            r'<u><strong style="color:#c0392b">\1</strong></u>', text
        )
        # 2) 이제 text 안에 태그가 생길 수 있으므로 분리 후 굵게+색(네이비) 적용
        parts = re.split(r'(<[^>]+>)', text)
        depth, out = 0, []
        for part in parts:
            if part.startswith('<'):
                if re.match(r'<(strong|u)\b', part, re.I):
                    depth += 1
                elif re.match(r'</(strong|u)>', part, re.I):
                    depth = max(0, depth - 1)
                out.append(part)
            elif depth > 0:
                out.append(part)          # 이미 강조 안쪽 → 패스
            else:
                out.append(_BOLD_RE.sub(
                    r'<strong style="color:#1a3a5c">\1</strong>', part
                ))
        return ''.join(out)

    def _process(m):
        open_tag, content, close_tag = m.group(1), m.group(2), m.group(3)
        # content 내 기존 태그(a, strong, em 등) 안쪽은 건드리지 않음
        parts = re.split(r'(<[^>]+>)', content)
        depth, result = 0, []
        for part in parts:
            if part.startswith('<'):
                if re.match(r'<(strong|b|u|em|a)\b', part, re.I):
                    depth += 1
                elif re.match(r'</(strong|b|u|em|a)>', part, re.I):
                    depth = max(0, depth - 1)
                result.append(part)
            elif depth > 0:
                result.append(part)
            else:
                result.append(_fmt_text(part))
        return open_tag + ''.join(result) + close_tag

    html = re.sub(r'(<p[^>]*>)(.*?)(</p>)',   _process, html, flags=re.DOTALL)
    html = re.sub(r'(<li[^>]*>)(.*?)(</li>)', _process, html, flags=re.DOTALL)
    return html


# =====================================================

def _inject_visuals_html(html_content, annual_financials, company_name, quarterly_financials=None):
    """
    GPT가 생성한 HTML 본문의 '최근 실적' H2 바로 뒤에
    연간 재무 테이블 + SVG 차트 + 분기 SVG 차트 + 분기 실적 테이블을 삽입합니다.
    """
    import re
    table_html           = _build_financial_table_html(annual_financials)
    chart_html           = _build_svg_chart(annual_financials, company_name)
    health_html          = _build_health_indicators_html(annual_financials)
    quarterly_chart_html = _build_quarterly_svg_chart(quarterly_financials or [], company_name)
    quarterly_html       = _build_quarterly_table_html(quarterly_financials or [])
    visuals = table_html + chart_html + health_html + quarterly_chart_html + quarterly_html

    if not visuals:
        return html_content

    # <h2>최근 실적...</h2> 태그 바로 뒤에 삽입
    pattern = r'(<h2>[^<]*실적[^<]*</h2>)'
    result = re.sub(pattern, r'\1' + visuals, html_content, count=1, flags=re.IGNORECASE)
    return result


# =====================================================
# 마크다운 → WordPress HTML 변환 (구형 호환용)
# =====================================================

def _md_to_html(text, annual_financials, company_name):
    """
    마크다운을 HTML로 변환하면서
    '## 2. 재무 실적 분석' 섹션 뒤에 테이블 + SVG 차트 삽입
    """
    table_html = _build_financial_table_html(annual_financials)
    chart_html = _build_svg_chart(annual_financials, company_name)
    visuals    = table_html + chart_html

    lines    = text.split('\n')
    html_parts = []
    in_list  = False
    in_blockquote = False

    for line in lines:
        stripped = line.strip()

        # 제목 (h2 / h3)
        if stripped.startswith('### '):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append(f'<h3>{stripped[4:]}</h3>')
            continue

        if stripped.startswith('## '):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            heading_text = stripped[3:]
            html_parts.append(f'<h2>{heading_text}</h2>')
            # 재무 실적 분석 섹션 바로 아래에 테이블 + 차트 삽입
            if '재무 실적' in heading_text and visuals:
                html_parts.append(visuals)
            continue

        # 리스트
        if stripped.startswith('- ') or stripped.startswith('* '):
            if not in_list:
                html_parts.append('<ul>')
                in_list = True
            html_parts.append(f'<li>{stripped[2:]}</li>')
            continue

        # 빈 줄
        if stripped == '':
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            continue

        # 구분선
        if stripped in ('---', '***', '___'):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append('<hr>')
            continue

        # 인용문 (※ 면책 조항)
        if stripped.startswith('※'):
            if in_list:
                html_parts.append('</ul>')
                in_list = False
            html_parts.append(
                f'<p style="font-size:12px;color:#888;margin-top:24px;">{stripped}</p>'
            )
            continue

        # 일반 단락
        if in_list:
            html_parts.append('</ul>')
            in_list = False
        html_parts.append(f'<p>{stripped}</p>')

    if in_list:
        html_parts.append('</ul>')

    return '\n'.join(html_parts)


# =====================================================
# 내부링크: 기존 발행 글 조회
# =====================================================

def get_related_posts(category_name, exclude_title='', max_count=5):
    """
    동일 카테고리 내 발행된 포스트 목록 조회 (내부링크용).
    반환: [{'title': str, 'link': str}, ...]
    실패 시 빈 리스트 반환 (graceful degradation).
    """
    try:
        # 카테고리 ID 조회
        r = requests.get(
            _api('categories'),
            params={'search': category_name, 'per_page': 10},
            auth=_auth(), timeout=10,
        )
        r.raise_for_status()
        category_id = None
        for item in r.json():
            if item.get('name') == category_name:
                category_id = item['id']
                break
        if category_id is None:
            return []

        # 발행된 포스트 조회
        r = requests.get(
            _api('posts'),
            params={
                'categories': category_id,
                'status':     'publish',
                'per_page':   max_count + 1,
                '_fields':    'title,link',
            },
            auth=_auth(), timeout=10,
        )
        r.raise_for_status()

        related = []
        for post in r.json():
            raw_title = post.get('title', {})
            title = raw_title.get('rendered', '') if isinstance(raw_title, dict) else str(raw_title)
            link  = post.get('link', '')
            # 현재 발행 대상 기업은 제외
            if exclude_title and exclude_title in title:
                continue
            if title and link:
                related.append({'title': title, 'link': link})
            if len(related) >= max_count:
                break

        print(f"  내부링크 후보 {len(related)}개 조회 완료")
        return related

    except Exception as e:
        print(f"  내부링크 조회 실패 (무시): {e}")
        return []


# =====================================================
# 중복 발행 방지
# =====================================================

def find_existing_post(company_name):
    """
    동일 기업명이 제목에 포함된 draft/published 포스트 조회.
    반환: (post_id, post_url, status) 또는 (None, None, None)
    """
    try:
        for status in ('draft', 'publish'):
            r = requests.get(
                _api('posts'),
                params={
                    'search':   company_name,
                    'status':   status,
                    'per_page': 5,
                    '_fields':  'id,title,link,status',
                },
                auth=_auth(), timeout=10,
            )
            r.raise_for_status()
            for post in r.json():
                raw = post.get('title', {})
                title = raw.get('rendered', '') if isinstance(raw, dict) else str(raw)
                if company_name in title:
                    return post['id'], post.get('link', ''), post.get('status', status)
    except Exception as e:
        print(f"  [중복체크] 조회 실패 (무시): {e}")
    return None, None, None


# =====================================================
# FAQ Schema JSON-LD
# =====================================================

def _build_faq_schema_ld(faq_json_str):
    """FAQ_JSON 문자열에서 Schema.org FAQPage JSON-LD 스크립트 블록 생성"""
    if not faq_json_str:
        return ''
    try:
        items = json.loads(faq_json_str)
        if not isinstance(items, list) or not items:
            return ''
    except (json.JSONDecodeError, ValueError):
        return ''
    entities = []
    for item in items:
        q = str(item.get('question', '')).strip()
        a = str(item.get('answer', '')).strip()
        if q and a:
            entities.append({
                '@type': 'Question',
                'name': q,
                'acceptedAnswer': {'@type': 'Answer', 'text': a},
            })
    if not entities:
        return ''
    schema = {
        '@context': 'https://schema.org',
        '@type': 'FAQPage',
        'mainEntity': entities,
    }
    return json.dumps(schema, ensure_ascii=False, indent=2)


# =====================================================
# JSONL 로그
# =====================================================

def _log_jsonl(record: dict):
    """wp_publish_log.jsonl 에 줄 단위 기록 (타임스탬프 자동 포함)"""
    record.setdefault('timestamp', datetime.now(timezone.utc).isoformat())
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f'  [로그] JSONL 기록 실패: {e}')


# =====================================================
# 공통 HTTP 요청 헬퍼
# =====================================================

def wp_request(method, path, json_body=None, params=None):
    """
    WordPress REST API 공통 요청 헬퍼.
    인증: _WP_BASE_URL / _WP_USER / WP_APP_PASSWORD (Application Password Basic Auth).
    에러 케이스별 명시적 메시지를 예외로 발생시킨다.
    """
    url  = f"{_WP_BASE_URL.rstrip('/')}/wp-json/wp/v2/{path.lstrip('/')}"
    auth = HTTPBasicAuth(_WP_USER, WP_APP_PASSWORD)
    try:
        resp = requests.request(
            method.upper(), url,
            json=json_body, params=params,
            auth=auth, timeout=30,
        )
    except requests.exceptions.Timeout:
        raise RuntimeError(f'[WP] 요청 타임아웃 (30s): {method.upper()} {url}')
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f'[WP] 연결 실패 — WP_BASE_URL 주소를 확인하세요: {e}')

    if resp.status_code in (401, 403):
        raise PermissionError(
            f'[WP] 인증/권한 오류 ({resp.status_code}): '
            'WP_USER 또는 WP_APP_PASSWORD(Application Password)를 확인하세요.'
        )
    if resp.status_code == 400:
        raise ValueError(
            f'[WP] 잘못된 요청 (400): 전송 필드를 확인하세요. '
            f'응답: {resp.text[:300]}'
        )
    resp.raise_for_status()
    return resp


# =====================================================
# SEO/AEO 품질 체크
# =====================================================

def _check_seo_quality(content, focus_keyword):
    """
    SEO/AEO 최소 품질 점검.
      1) focus_keyword 문구가 본문에 없으면 도입부 첫 </ul> 뒤(없으면 </h1> 뒤)에 1문장 자동 삽입.
      2) href= 링크가 0개이면 경고 로그만 출력 (강제 삽입 안 함).
    반환: (수정된 content, 경고 메시지 리스트)
    """
    warnings_list = []
    current_year  = datetime.now().year

    if focus_keyword and focus_keyword not in content:
        sentence    = (
            f'<p>{focus_keyword}을(를) {current_year}년 기준으로 '
            f'데이터 기반 점검합니다.</p>'
        )
        first_ul_end = content.find('</ul>')
        h1_match     = re.search(r'</h1>', content)
        if first_ul_end != -1:
            pos     = first_ul_end + len('</ul>')
            content = content[:pos] + '\n' + sentence + content[pos:]
            print(f'  [SEO품질] focus_keyword 미포함 → 도입부 ul 뒤 삽입: "{focus_keyword}"')
        elif h1_match:
            pos     = h1_match.end()
            content = content[:pos] + '\n' + sentence + content[pos:]
            print(f'  [SEO품질] focus_keyword 미포함 → H1 뒤 삽입: "{focus_keyword}"')

    if not re.search(r'href=', content, re.IGNORECASE):
        msg = '  [SEO품질] ⚠️ 본문에 href 링크가 없습니다. 내부/외부 링크 추가를 권장합니다.'
        print(msg)
        warnings_list.append(msg)

    return content, warnings_list


# =====================================================
# Rank Math 메타 검증
# =====================================================

def verify_rank_math_meta(post_id, expected_meta, warn_keys=None):
    """
    GET /wp-json/wp/v2/posts/{id}?context=edit 로 Rank Math 메타 3종 검증.
    GET 1회로 warn_keys(post_meta_extra 키 목록) 존재 여부도 함께 확인.

    Args:
        expected_meta : Rank Math 3종 기대값 dict — 불일치 시 False 반환
        warn_keys     : post_meta_extra 키 목록(list) — 없을 경우 경고만 출력, 반환값 영향 없음
    반환: True(모두 일치) / False(불일치 또는 키 없음)
    """
    try:
        resp = wp_request('GET', f'posts/{post_id}', params={'context': 'edit'})
    except Exception as e:
        print(f'  [메타검증] GET 실패: {e}')
        return False

    meta = resp.json().get('meta', {})

    # Rank Math 3종 strict 검증
    ok = True
    for key, expected_val in expected_meta.items():
        actual_val = meta.get(key)
        if actual_val != expected_val:
            print(f'  [메타검증] 불일치 — {key}')
            print(f'    기대: {str(expected_val)[:80]}')
            print(f'    실제: {str(actual_val)[:80]}')
            ok = False

    if ok:
        print('  [메타검증] Rank Math 메타 3종 일치 ✓')

    # post_meta_extra 키 존재 warn-only (show_in_rest 미등록 감지용)
    for key in (warn_keys or []):
        if key not in meta:
            print(f'  [메타검증] 경고: "{key}" 저장 후 조회 안됨 '
                  '(register_post_meta show_in_rest 확인 필요)')

    return ok


# =====================================================
# 포스트 Upsert (생성 또는 업데이트)
# =====================================================

def _nonempty(v):
    """None·빈 문자열·공백 문자열이면 None 반환, 아니면 strip() 된 값 반환."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return v  # 문자열 외 타입(int 등)은 그대로 통과


def _rank_str(v):
    """Rank Math 메타 전용: None이면 None, 그 외 str() 변환 후 strip·빈값 걸러냄.
    숫자/리스트 등 비문자열이 실수로 들어와도 WP가 기대하는 string으로 변환."""
    if v is None:
        return None
    return _nonempty(str(v))


def upsert_post(post_payload):
    """
    WordPress REST API로 포스트 생성(POST) 또는 업데이트(PUT).
    Rank Math 메타 주입 + 검증 (불일치 시 PUT 재시도 1회) 포함.
    JSONL 로그 자동 기록.

    post_payload 키:
        post_id         : Optional[int]  — 있으면 업데이트, None 이면 신규 생성
        title           : str
        content         : str            — HTML 본문
        slug            : str
        status          : "draft" | "publish"  (기본 "draft")
        categories      : List[int]      — 카테고리 ID 리스트
        tags            : List[int]      — 태그 ID 리스트
        seo_title       : str
        meta_description: str
        focus_keyword   : str

    반환: (post_id: int, link: str)
    """
    post_id   = post_payload.get('post_id')
    # Rank Math 3종: str() 캐스팅 + strip + 빈값 → None (기존 WP 값 덮어쓰기 방지)
    seo_title = _rank_str(post_payload.get('seo_title'))
    meta_desc = _rank_str(post_payload.get('meta_description'))
    focus_kw  = _rank_str(post_payload.get('focus_keyword'))  # None 가능

    # SEO 품질 체크: 본문 문자열 처리이므로 focus_kw or '' 로 빈 문자열 보장
    content, _ = _check_seo_quality(post_payload.get('content', ''), focus_kw or '')

    # 값이 있는 meta 키만 전송 (None·빈 문자열·공백 문자열은 전송 제외)
    meta_body = {}
    if seo_title:
        meta_body['_rank_math_title']         = seo_title
    if meta_desc:
        meta_body['_rank_math_description']   = meta_desc
    if focus_kw:
        meta_body['_rank_math_focus_keyword'] = focus_kw
    # 추가 meta (FAQ Schema 등) 병합 — Rank Math 검증 대상에는 포함하지 않음
    meta_body.update(post_payload.get('post_meta_extra', {}))

    body = {
        'title':      post_payload.get('title', ''),
        'content':    content,
        'slug':       post_payload.get('slug', ''),
        'status':     post_payload.get('status', 'draft'),
        'categories': post_payload.get('categories', []),
        'tags':       post_payload.get('tags', []),
    }
    if meta_body:
        body['meta'] = meta_body

    if post_id:
        print(f'  [upsert] 포스트 업데이트 (ID={post_id})...')
        resp   = wp_request('PUT', f'posts/{post_id}', json_body=body)
        action = 'update'
    else:
        print('  [upsert] 포스트 신규 생성 중...')
        resp   = wp_request('POST', 'posts', json_body=body)
        action = 'create'

    data     = resp.json()
    new_id   = data.get('id')
    new_link = data.get('link', '')
    print(f'  [upsert] {action} 완료: ID={new_id}, URL={new_link}')

    # Rank Math 메타 검증 (실제로 전송한 값만 비교, _nonempty 통과값 재사용)
    expected = {}
    if seo_title:
        expected['_rank_math_title']         = seo_title
    if meta_desc:
        expected['_rank_math_description']   = meta_desc
    if focus_kw:
        expected['_rank_math_focus_keyword'] = focus_kw
    extra_keys = list(post_payload.get('post_meta_extra', {}).keys())
    # GET 1회로 Rank Math 검증 + extra meta 존재 warn-only 동시 처리
    meta_ok = verify_rank_math_meta(new_id, expected, warn_keys=extra_keys) if expected else True
    if not meta_ok:
        print('  [upsert] 메타 불일치 → PUT 재시도 (1회)...')
        time.sleep(2)
        try:
            wp_request('PUT', f'posts/{new_id}', json_body={'meta': expected})
            # 재시도: Rank Math 3종만 재검증 (extra warn은 이미 첫 검증에서 출력됨)
            meta_ok = verify_rank_math_meta(new_id, expected)
        except Exception as e:
            print(f'  [upsert] 재시도 실패: {e}')
            meta_ok = False

    log_rec = {
        'action':        action,
        'post_id':       new_id,
        'status_code':   resp.status_code,
        'ok':            meta_ok,
        'link':          new_link,
        'slug':          post_payload.get('slug', ''),
        'seo_title':     seo_title or '',
        'focus_keyword': focus_kw,
        'error':         None if meta_ok else 'rank_math_meta_mismatch',
    }
    _log_jsonl(log_rec)

    if not meta_ok:
        raise RuntimeError(
            f'[WP] Rank Math 메타 재시도 후에도 불일치 (post_id={new_id}). '
            'wp_publish_log.jsonl 을 확인하세요.'
        )

    return new_id, new_link


# =====================================================
# 발행
# =====================================================

def publish_post(title, content, company_data, seo_data=None):
    """
    WordPress에 임시저장(draft) 포스트 생성.
    content: HTML 또는 마크다운 문자열 (메타 태그 블록은 content_generator에서 이미 제거됨)
    company_data: 재무 데이터 포함 dict
    seo_data: {'seo_title': str, 'meta_description': str, 'focus_keyword': str,
               'slug': str, 'tags': list[str]}
    반환: post_url (str)
    """
    if seo_data is None:
        seo_data = {}

    company_name        = company_data.get('company_name', '')
    stock_code          = company_data.get('stock_code', '')
    annual_financials   = company_data.get('annual_financials', {})
    quarterly_financials = company_data.get('quarterly_financials', [])

    print("  WP 카테고리/태그 준비 중...")
    category_id = get_or_create_category(CATEGORY_NAME)
    # seo_data에 tags 목록이 있으면 그것을 사용, 없으면 기본 태그 생성
    seo_tags = seo_data.get('tags', [])
    if seo_tags:
        tag_names = [t.strip() for t in seo_tags if t.strip()]
    else:
        tag_names = [t for t in [company_name, stock_code, '주식분석', '투자분석'] if t]
    tag_ids     = get_or_create_tags(tag_names)

    # HTML 출력(신형)이면 직접 주입, 마크다운(구형)이면 변환
    is_html = '<h2>' in content or content.strip().startswith('<')
    if is_html:
        q_count = len(quarterly_financials)
        print(f"  HTML 본문 → 재무 테이블/차트 + 분기 테이블({q_count}분기) 주입 중...")
        wp_content = _inject_visuals_html(content, annual_financials, company_name, quarterly_financials)
    else:
        print("  마크다운 → HTML 변환 + 테이블/차트 삽입 중...")
        wp_content = _md_to_html(content, annual_financials, company_name)

    # 목차 앵커 연결 (H2 id 부여 + 목차 li href)
    wp_content = _inject_anchors(wp_content)

    # 모바일 대응: 모든 <table> overflow-x:auto 래핑
    wp_content = _wrap_tables_responsive(wp_content)

    # 가독성 강화: 중요 키워드 굵게 / 밑줄+굵게 처리
    wp_content = _enhance_readability(wp_content)

    # SEO 필드
    faq_json_str     = seo_data.get('faq_json', '')
    meta_description = seo_data.get('meta_description', '')
    focus_keyword    = seo_data.get('focus_keyword', '')
    slug             = seo_data.get('slug', '')

    # SEO/AEO 품질 체크 (focus_keyword 미포함 시 자동 삽입, href 없으면 경고)
    if focus_keyword:
        wp_content, _ = _check_seo_quality(wp_content, focus_keyword)

    # NinjaFirewall Rule 115 대응: content 내 모든 <script> 블록 제거
    # FAQ Schema JSON-LD는 post_meta_extra(_faq_schema_json)로 따로 전송됨
    wp_content = re.sub(r'<script[^>]*>.*?</script>', '', wp_content,
                        flags=re.IGNORECASE | re.DOTALL)

    # "(추정)" 텍스트 제거 (괄호 포함 다양한 형태 대응)
    wp_content = re.sub(r'\(추정[^)]*\)', '', wp_content)
    wp_content = re.sub(r'（추정[^）]*）', '', wp_content)  # 전각 괄호

    seo_title       = seo_data.get('seo_title', '')
    # FAQ Schema JSON-LD: NinjaFirewall이 content 내 <script> 차단 → post meta에 저장
    # functions.php에서 register_post_meta + wp_head 훅으로 출력 필요
    faq_schema_json = _build_faq_schema_ld(faq_json_str)
    if faq_schema_json:
        print("  FAQ Schema JSON-LD → post meta 저장")

    # 중복 체크: 기존 draft/published 포스트가 있으면 UPDATE, 없으면 CREATE
    existing_id, _, existing_status = find_existing_post(company_name)
    if existing_id:
        print(f"  ⚠️ 기존 {existing_status} 포스트 발견 (ID={existing_id}) → 업데이트")

    # upsert_post에 위임 — REST 호출 / Rank Math 검증 / JSONL 로그를 한 곳에서 처리
    post_payload = {
        'post_id':          existing_id,        # None 이면 신규 생성
        'title':            title,
        'content':          wp_content,
        'slug':             slug,
        'status':           'draft',
        'categories':       [category_id],
        'tags':             tag_ids,
        'seo_title':        seo_title,
        'meta_description': meta_description,
        'focus_keyword':    focus_keyword,
        'post_meta_extra':  {'_faq_schema_json': faq_schema_json} if faq_schema_json else {},
    }
    _, post_url = upsert_post(post_payload)

    if slug:
        print(f"  슬러그: {slug}")
    if meta_description:
        print(f"  메타디스크립션: {meta_description[:60]}...")
    return post_url


# =====================================================
# 영어 발행 파이프라인 (EN — Global Research 카테고리)
# =====================================================

def _inject_charts_en(html, annual_financials, company_name, quarterly_financials=None):
    """
    영어 아티클 본문의 <h2>Revenue & Margin Snapshot</h2> 바로 다음에
    재무 테이블 + SVG 차트를 주입한다.
    H2를 찾지 못하면 원본 html 그대로 반환 (warn-only).
    """
    # 환율 1회만 API 호출 → 이후 _fmt_usd_m/_fmt_q_usd 모두 캐시 사용
    _get_krw_usd_rate()

    marker = re.search(
        r'(<h2[^>]*>[^<]*Revenue[^<]*Margin[^<]*Snapshot[^<]*</h2>)',
        html, re.IGNORECASE
    )
    if not marker:
        print('  [EN차트] "Revenue & Margin Snapshot" H2 미발견 — 차트 주입 건너뜀')
        return html

    insert_pos = marker.end()

    annual_svg  = _build_svg_chart(annual_financials, company_name, lang='en') if annual_financials else ''
    quarterly_svg = (
        _build_quarterly_svg_chart(quarterly_financials, company_name, lang='en')
        if quarterly_financials else ''
    )

    chart_block = '\n'
    if annual_svg:
        chart_block += (
            '<div class="chart-wrap" style="margin:16px 0;">'
            '<p><strong>Revenue trend (annual)</strong></p>'
            f'{annual_svg}'
            '</div>\n'
        )
    if quarterly_svg:
        chart_block += (
            '<div class="chart-wrap" style="margin:16px 0;">'
            '<p><strong>Quarterly revenue & operating margin</strong></p>'
            f'{quarterly_svg}'
            '</div>\n'
        )

    # 재무 테이블 (EN 라벨, USD 단위)
    table_block = ''
    if annual_financials:
        table_block += _build_financial_table_html(annual_financials, lang='en')
    if quarterly_financials:
        table_block += _build_quarterly_table_html(quarterly_financials, lang='en')

    if not chart_block.strip() and not table_block.strip():
        return html

    return html[:insert_pos] + table_block + chart_block + html[insert_pos:]


def publish_post_en(article: dict, company_data: dict) -> str:
    """
    영어 아티클을 WordPress 'Global Research' 카테고리에 draft 발행.

    article     : generate_en_article() 반환 dict
    company_data: {'company_name': str, 'stock_code': str,
                   'annual_financials': dict, 'quarterly_financials': list}
    반환: post_url (str)
    """
    company_name        = company_data.get('company_name', '')
    annual_financials   = company_data.get('annual_financials', {})
    quarterly_financials = company_data.get('quarterly_financials', [])

    print('  [EN] 카테고리/태그 준비 중...')
    en_cat_id = get_or_create_category(EN_CATEGORY_NAME)
    tag_ids   = get_or_create_tags(article.get('tags', []))

    # SVG 차트 주입 (영문 H2 기준)
    content = _inject_charts_en(
        article.get('content', ''),
        annual_financials,
        company_name,
        quarterly_financials,
    )

    # TOC 앵커 연결 (기존 함수 재사용)
    content = _inject_anchors(content)
    # _enhance_readability 는 KO 금융 키워드 기반 → EN에는 미적용

    # 모바일 대응: 모든 <table> overflow-x:auto 래핑
    content = _wrap_tables_responsive(content)

    # NinjaFirewall Rule 115 대응: <script> 블록 제거
    content = re.sub(r'<script[^>]*>.*?</script>', '', content,
                     flags=re.IGNORECASE | re.DOTALL)

    # "(추정)" 텍스트 제거
    content = re.sub(r'\(추정[^)]*\)', '', content)
    content = re.sub(r'（추정[^）]*）', '', content)  # 전각 괄호

    # FAQ Schema JSON-LD
    faq_schema_json = _build_faq_schema_ld(article.get('faq_json', ''))
    if faq_schema_json:
        print('  [EN] FAQ Schema JSON-LD → post meta 저장')

    # 중복 체크 — EN은 slug 기반 (KO 회사명 검색과 분리)
    existing_id = None
    slug = article.get('slug', '')
    if slug:
        try:
            r = wp_request('GET', 'posts', params={'slug': slug, 'status': 'any'})
            hits = r.json()
            if isinstance(hits, list) and hits:
                existing_id = hits[0].get('id')
                print(f'  [EN] 기존 포스트 발견 (ID={existing_id}) → 업데이트')
        except Exception as e:
            print(f'  [EN] 중복 체크 GET 실패 (무시): {e}')

    post_payload = {
        'post_id':          existing_id,
        'title':            article.get('title', ''),
        'content':          content,
        'slug':             slug,
        'status':           'draft',
        'categories':       [en_cat_id],
        'tags':             tag_ids,
        'seo_title':        article.get('seo_title', ''),
        'meta_description': article.get('meta_description', ''),
        'focus_keyword':    article.get('focus_keyword', ''),
        'post_meta_extra':  {'_faq_schema_json': faq_schema_json} if faq_schema_json else {},
    }
    _, post_url = upsert_post(post_payload)
    return post_url


# =====================================================
# 실행 예시
# =====================================================

if __name__ == '__main__':
    """
    generate_wp_article() 결과를 받아 upsert_post()로 업로드하는 최소 예시.
    실제 사용 시 아래 더미 article 변수를 generate_wp_article() 호출로 교체하세요.

    환경변수 필수:
        WP_BASE_URL   (또는 WP_URL)         예: https://hanalpha.com
        WP_USER       (또는 WP_USERNAME)    예: admin
        WP_APP_PASSWORD                     워드프레스 Application Password
    """
    from wp_content_generator import generate_wp_article  # noqa: F401

    # ── 실제 사용 예시 (주석 해제 후 데이터 입력) ──────────────────────────
    # article = generate_wp_article(
    #     company_name='삼성전자',
    #     stock_code='005930',
    #     annual_metrics_by_year=[(2022, {...}), (2023, {...}), (2024, {...})],
    #     analysis={'산업 개요': '...', '투자 관점 핵심 리스크': '...'},
    #     competition={'경쟁사목록': [{'기업명': 'SK하이닉스', ...}]},
    #     news_items=[{'title': '최신 뉴스', 'pubDate': '2025-01-01'}],
    #     investment_points=[{'번호': 1, '투자포인트': 'HBM 수요 증가'}],
    # )

    # ── 테스트용 더미 article (API 호출 없이 구조 확인용) ──────────────────
    article = {
        'title':            '테스트 기업 주식 분석 (2025년) — 실적·산업·리스크 점검',
        'content':          (
            '<h1>테스트 기업 주식 분석</h1>'
            '<ul><li>핵심 결론: 테스트</li></ul>'
            '<p>본문 내용입니다.</p>'
        ),
        'seo_title':        '테스트 기업 주식 분석: 실적·산업·리스크 점검',
        'meta_description': '테스트 기업(000000)의 사업 구조와 실적 흐름을 점검합니다.',
        'slug':             '000000-stock-analysis-2025-01',
        'focus_keyword':    '테스트 기업 주식 분석',
        'category':         '기업분석',
        'tags':             ['테스트 기업', '000000', '주식분석'],
        'faq_json':         '',
        'annual_financials':    {},
        'quarterly_financials': [],
    }

    # 카테고리/태그 ID 확보
    cat_id  = get_or_create_category(article.get('category', '기업분석'))
    tag_ids = get_or_create_tags(article.get('tags', []))

    post_payload = {
        'post_id':          None,       # 업데이트 시 기존 포스트 int ID 입력
        'title':            article['title'],
        'content':          article['content'],
        'slug':             article['slug'],
        'status':           'draft',    # 즉시 발행: 'publish'
        'categories':       [cat_id],
        'tags':             tag_ids,
        'seo_title':        article['seo_title'],
        'meta_description': article['meta_description'],
        'focus_keyword':    article['focus_keyword'],
    }

    new_id, post_url = upsert_post(post_payload)
    print(f'\n발행 완료')
    print(f'  post_id : {new_id}')
    print(f'  URL     : {post_url}')
    print(f'  로그    : {LOG_FILE}')
