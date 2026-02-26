"""
wp_publisher.py — WordPress REST API 포스트 발행
표(HTML table) + Chart.js 차트 포함
"""

import base64
import json
import re
import requests
from requests.auth import HTTPBasicAuth
from config import WP_URL, WP_USERNAME, WP_APP_PASSWORD

CATEGORY_NAME = '기업분석'


def _auth():
    return HTTPBasicAuth(WP_USERNAME, WP_APP_PASSWORD)


def _api(path):
    return f"{WP_URL.rstrip('/')}/wp-json/wp/v2/{path}"


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


def _build_financial_table_html(annual_financials):
    """연간 재무 데이터 → HTML 테이블"""
    if not annual_financials:
        return ''

    years = sorted(annual_financials.keys())

    rows_def = [
        ('매출액',           '매출액 (억원)',      _fmt_eok),
        ('영업이익',         '영업이익 (억원)',    _fmt_eok),
        ('영업이익률',       '영업이익률',         _fmt_pct),
        ('당기순이익',       '당기순이익 (억원)',  _fmt_eok),
        ('영업활동현금흐름', 'OCF (억원)',         _fmt_eok),
        ('CAPEX',           'CAPEX (억원)',        _fmt_eok),
        ('ROE',             'ROE',                 _fmt_pct),
    ]

    style = (
        'border-collapse:collapse;width:100%;font-size:14px;margin:20px 0;'
    )
    th_style = (
        'background:#1a3a5c;color:#fff;padding:10px 14px;'
        'text-align:center;border:1px solid #ddd;white-space:nowrap;'
    )
    td_style  = 'padding:9px 14px;text-align:right;border:1px solid #ddd;'
    td0_style = 'padding:9px 14px;text-align:left;border:1px solid #ddd;font-weight:bold;background:#f5f8fc;'
    tr_even   = 'background:#f9f9f9;'

    # 헤더
    header_cells = ''.join(f'<th style="{th_style}">{y}년</th>' for y in years)
    thead = f'<thead><tr><th style="{th_style}">구분</th>{header_cells}</tr></thead>'

    # 바디
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

    caption_style = 'caption-side:top;text-align:left;font-weight:bold;font-size:15px;margin-bottom:8px;color:#1a3a5c;'

    return (
        f'<table style="{style}">'
        f'<caption style="{caption_style}">▶ 연간 재무 실적 요약 (단위: 억원)</caption>'
        f'{thead}{tbody}'
        f'</table>'
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
        '<div style="margin:16px 0 24px 0;">'
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


def _build_quarterly_table_html(quarterly_financials):
    """분기 실적 데이터 → HTML 테이블 (최신순)"""
    if not quarterly_financials:
        return ''

    style = 'border-collapse:collapse;width:100%;font-size:14px;margin:20px 0;'
    th_style = (
        'background:#2c5f8a;color:#fff;padding:9px 14px;'
        'text-align:center;border:1px solid #ddd;white-space:nowrap;'
    )
    td_style  = 'padding:8px 14px;text-align:right;border:1px solid #ddd;'
    td0_style = 'padding:8px 14px;text-align:center;border:1px solid #ddd;font-weight:bold;background:#f0f5fa;'
    tr_even   = 'background:#f9f9f9;'

    thead = (
        f'<thead><tr>'
        f'<th style="{th_style}">분기</th>'
        f'<th style="{th_style}">매출액 (억원)</th>'
        f'<th style="{th_style}">영업이익 (억원)</th>'
        f'<th style="{th_style}">영업이익률</th>'
        f'<th style="{th_style}">당기순이익 (억원)</th>'
        f'</tr></thead>'
    )

    tbody_rows = []
    for idx, q in enumerate(quarterly_financials):
        row_bg = tr_even if idx % 2 == 1 else ''
        분기 = q.get('분기', '-')
        tbody_rows.append(
            f'<tr style="{row_bg}">'
            f'<td style="{td0_style}">{분기}</td>'
            f'<td style="{td_style}">{_fmt_q(q.get("매출액억원"))}</td>'
            f'<td style="{td_style}">{_fmt_q(q.get("영업이익억원"))}</td>'
            f'<td style="{td_style}">{_fmt_q(q.get("영업이익률pct"), is_pct=True)}</td>'
            f'<td style="{td_style}">{_fmt_q(q.get("당기순이익억원"))}</td>'
            f'</tr>'
        )
    tbody = f'<tbody>{"".join(tbody_rows)}</tbody>'

    caption_style = 'caption-side:top;text-align:left;font-weight:bold;font-size:15px;margin-bottom:8px;color:#2c5f8a;'

    return (
        f'<table style="{style}">'
        f'<caption style="{caption_style}">▶ 최근 분기 실적 (최신순, 단위: 억원)</caption>'
        f'{thead}{tbody}'
        f'</table>'
    )


# =====================================================
# SVG 차트 생성 (JS 불필요 — 보안 플러그인 우회)
# =====================================================

def _build_svg_chart(annual_financials, company_name=''):
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

    def to_pct(v):
        try:
            return float(v) * 100 if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    revenues   = [to_eok(annual_financials[y].get('매출액'))   for y in years]
    op_profits = [to_eok(annual_financials[y].get('영업이익')) for y in years]
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
    ly = 14
    elems += [
        f'<rect x="{pad_l}" y="{ly}" width="12" height="12" fill="#1a3a5c" rx="2"/>',
        f'<text x="{pad_l+15}" y="{ly+10}" font-size="11" fill="#333">매출액(억원)</text>',
        f'<rect x="{pad_l+95}" y="{ly}" width="12" height="12" fill="#3498db" rx="2"/>',
        f'<text x="{pad_l+110}" y="{ly+10}" font-size="11" fill="#333">영업이익(억원)</text>',
        f'<line x1="{pad_l+205}" y1="{ly+6}" x2="{pad_l+218}" y2="{ly+6}" stroke="#e74c3c" stroke-width="2.5"/>',
        f'<circle cx="{pad_l+211}" cy="{ly+6}" r="3.5" fill="#e74c3c"/>',
        f'<text x="{pad_l+222}" y="{ly+10}" font-size="11" fill="#c0392b">영업이익률(%)</text>',
    ]

    svg_inner = '\n  '.join(elems)
    svg_str = (
        f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'\n  {svg_inner}\n'
        f'</svg>'
    )
    svg_b64  = base64.b64encode(svg_str.encode('utf-8')).decode('ascii')
    alt_text = f"{company_name} 연간 매출·영업이익·영업이익률 추이" if company_name else "연간 재무 실적 차트"
    return (
        f'<div style="margin:24px 0;">'
        f'<p style="font-weight:bold;font-size:15px;color:#1a3a5c;margin-bottom:8px;">'
        f'▶ 매출액·영업이익 추이 및 영업이익률</p>'
        f'<img src="data:image/svg+xml;base64,{svg_b64}" '
        f'style="max-width:660px;width:100%;display:block;" alt="{alt_text}"/>'
        f'</div>'
    )


# =====================================================
# 분기 SVG 차트 생성
# =====================================================

def _build_quarterly_svg_chart(quarterly_financials, company_name=''):
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

    labels     = [q.get('분기', '-') for q in items]
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
    ly = 14
    elems += [
        f'<rect x="{pad_l}" y="{ly}" width="12" height="12" fill="#1a3a5c" rx="2"/>',
        f'<text x="{pad_l+15}" y="{ly+10}" font-size="11" fill="#333">매출액(억원)</text>',
        f'<rect x="{pad_l+95}" y="{ly}" width="12" height="12" fill="#3498db" rx="2"/>',
        f'<text x="{pad_l+110}" y="{ly+10}" font-size="11" fill="#333">영업이익(억원)</text>',
        f'<line x1="{pad_l+205}" y1="{ly+6}" x2="{pad_l+218}" y2="{ly+6}" stroke="#e74c3c" stroke-width="2.5"/>',
        f'<circle cx="{pad_l+211}" cy="{ly+6}" r="3.5" fill="#e74c3c"/>',
        f'<text x="{pad_l+222}" y="{ly+10}" font-size="11" fill="#c0392b">영업이익률(%)</text>',
    ]

    svg_inner = '\n  '.join(elems)
    svg_str = (
        f'<svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'\n  {svg_inner}\n'
        f'</svg>'
    )
    svg_b64  = base64.b64encode(svg_str.encode('utf-8')).decode('ascii')
    alt_text = f"{company_name} 분기별 매출·영업이익·영업이익률 추이" if company_name else "분기 실적 차트"
    return (
        f'<div style="margin:24px 0;">'
        f'<p style="font-weight:bold;font-size:15px;color:#2c5f8a;margin-bottom:8px;">'
        f'▶ 분기별 매출액·영업이익 추이 및 영업이익률</p>'
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
    if not slugs:
        return html

    # ── 목차 ul li에 href 연결 ───────────────────────
    def linkify_li(li_m):
        inner = li_m.group(1)
        if '<a ' in inner:          # 이미 링크 있으면 스킵
            return li_m.group(0)
        li_text = re.sub(r'<[^>]+>', '', inner).strip()
        best_slug, best_score = None, 0.0
        for h2_text, slug in slugs:
            if not li_text or not h2_text:
                continue
            score = (len(set(li_text) & set(h2_text)) /
                     max(len(set(li_text) | set(h2_text)), 1))
            if score > best_score:
                best_score, best_slug = score, slug
        if best_slug and best_score >= 0.35:
            return f'<li><a href="#{best_slug}">{inner}</a></li>'
        return li_m.group(0)

    first_ul = re.search(r'<ul>(.*?)</ul>', html, re.DOTALL)
    if first_ul:
        new_ul = re.sub(r'<li>(.*?)</li>', linkify_li, first_ul.group(1), flags=re.DOTALL)
        html = html[:first_ul.start(1)] + new_ul + html[first_ul.end(1):]

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
    return f'<script type="application/ld+json">\n{json.dumps(schema, ensure_ascii=False, indent=2)}\n</script>\n'


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

    # FAQ Schema JSON-LD 본문 앞 주입
    faq_json_str = seo_data.get('faq_json', '')
    schema_ld = _build_faq_schema_ld(faq_json_str)
    if schema_ld:
        wp_content = schema_ld + wp_content
        print("  FAQ Schema JSON-LD 주입 완료")

    # SEO 필드
    meta_description = seo_data.get('meta_description', '')
    focus_keyword    = seo_data.get('focus_keyword', '')
    slug             = seo_data.get('slug', '')

    post_body = {
        'title':      title,
        'content':    wp_content,
        'status':     'draft',
        'categories': [category_id],
        'tags':       tag_ids,
    }
    if slug:
        post_body['slug'] = slug
    seo_title = seo_data.get('seo_title', '')
    if meta_description or focus_keyword or seo_title:
        post_body['meta'] = {
            'rank_math_title':          seo_title,
            'rank_math_description':    meta_description,
            'rank_math_focus_keyword':  focus_keyword,
        }

    # 중복 체크: 기존 draft/published 포스트가 있으면 UPDATE, 없으면 CREATE
    existing_id, existing_url, existing_status = find_existing_post(company_name)
    if existing_id:
        print(f"  ⚠️ 기존 {existing_status} 포스트 발견 (ID={existing_id}) → 업데이트")
        r = requests.patch(
            _api(f'posts/{existing_id}'),
            json=post_body,
            auth=_auth(),
            timeout=30,
        )
    else:
        print("  WP 포스트 생성 중 (임시저장 + SEO)...")
        r = requests.post(
            _api('posts'),
            json=post_body,
            auth=_auth(),
            timeout=30,
        )
    r.raise_for_status()

    data     = r.json()
    post_id  = data.get('id')
    post_url = data.get('link', '')

    action = "업데이트" if existing_id else "생성"
    print(f"  WP 포스트 {action} 완료: ID={post_id}, URL={post_url}")
    if slug:
        print(f"  슬러그: {slug}")
    if meta_description:
        print(f"  메타디스크립션: {meta_description[:60]}...")
    return post_url
