"""
wp_content_generator.py — 기업분석 GPT 원본 결과로 WordPress 투자 분석글 생성
SEO+AEO 최적화 HTML 출력, Rank Math 메타 포함
"""

import re
import json
from datetime import datetime
from html import unescape
from openai import OpenAI
from config import OPENAI_API_KEY

ARTICLE_MODEL = 'gpt-4o-mini'
QUARTERLY_MAX = 8

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


# =====================================================
# 포맷 헬퍼
# =====================================================

def _clean_html(text):
    text = re.sub(r'<[^>]+>', '', text or '')
    return unescape(text).strip()


def _to_eok(val):
    try:
        return round(float(val) / 1e8, 1) if val is not None else None
    except (TypeError, ValueError):
        return None


def _to_pct(val):
    try:
        return round(float(val) * 100, 1) if val is not None else None
    except (TypeError, ValueError):
        return None


# =====================================================
# 데이터 변환 어댑터 (기업분석 형식 → 표준 JSON)
# =====================================================

def _annual_to_dict(annual_metrics_by_year):
    return {year: metrics for year, metrics in (annual_metrics_by_year or [])}


def _quarterly_to_list(quarterly_by_year, max_count=QUARTERLY_MAX):
    """
    {year: {1: metrics, 2: metrics, 3: metrics, 4: metrics}}
    → [{"분기": "2024Q3", "매출액억원": ..., ...}] 최신순
    """
    if not quarterly_by_year:
        return []
    items = []
    for year in sorted(quarterly_by_year.keys(), reverse=True):
        quarters = quarterly_by_year[year] or {}
        for q_num in sorted(quarters.keys(), reverse=True):
            m = quarters[q_num] or {}
            if not any(m.get(k) is not None for k in ['매출액', '영업이익', '당기순이익']):
                continue
            items.append({
                '분기':          f"{year}Q{q_num}",
                '매출액억원':    _to_eok(m.get('매출액')),
                '영업이익억원':  _to_eok(m.get('영업이익')),
                '영업이익률pct': _to_pct(m.get('영업이익률')),
                '당기순이익억원': _to_eok(m.get('당기순이익')),
            })
            if len(items) >= max_count:
                return items
    return items


def _build_industry_text(analysis):
    if not analysis:
        return ""
    key_sections = [
        "산업 개요", "산업 현재 업황", "기업의 해자(경쟁우위)",
        "주요 제품", "기업 상황 (재무 중심)", "매출 구조 및 이익 변동 요인",
        "최신 기술 트렌드", "투자 관점 핵심 리스크",
    ]
    parts = []
    for key in key_sections:
        val = (analysis.get(key) or '').strip()
        if val:
            parts.append(f"[{key}] {val[:600]}")
    return "\n".join(parts)


def _build_competition_summary(competition):
    if not competition:
        return ""
    competitors = competition.get('경쟁사목록', [])
    lines = []
    for c in competitors[:5]:
        name     = c.get('기업명', '')
        strength = (c.get('강점') or '')[:100]
        risk     = (c.get('약점/리스크') or '')[:80]
        line = name
        if strength: line += f" | 강점: {strength}"
        if risk:     line += f" | 리스크: {risk}"
        lines.append(line)
    return "\n".join(lines)


def _build_news_summary(news_items, investment_points):
    if not news_items:
        return ""
    ip_map = {}
    if isinstance(investment_points, list):
        for ip in investment_points:
            if isinstance(ip, dict):
                idx   = ip.get('번호') or ip.get('index')
                point = ip.get('투자포인트') or ip.get('point', '')
                if idx is not None:
                    try:
                        ip_map[int(idx)] = point
                    except (ValueError, TypeError):
                        pass
    lines = []
    for i, item in enumerate(news_items[:12]):
        title = _clean_html(item.get('title', ''))
        date  = (item.get('pubDate') or '')[:10]
        point = ip_map.get(i + 1, '')
        line  = f"[{date}] {title}"
        if point:
            line += f" → {point}"
        lines.append(line)
    return "\n".join(lines)


def _extract_thesis_bullets(investment_points, max_n=5):
    bullets = []
    if isinstance(investment_points, list):
        for ip in investment_points:
            if isinstance(ip, dict):
                point = ip.get('투자포인트') or ip.get('point', '')
                if point and point not in bullets:
                    bullets.append(point)
    return bullets[:max_n]


def _extract_risk_bullets(analysis, max_n=5):
    bullets = []
    if not analysis:
        return bullets
    risk_text = analysis.get('투자 관점 핵심 리스크', '')
    for line in risk_text.split('\n'):
        line = line.strip().lstrip('-').lstrip('•').strip()
        if line and line not in bullets:
            bullets.append(line)
    return bullets[:max_n]


# =====================================================
# 표준 입력 JSON 빌더
# =====================================================

def _build_input_json(company_name, stock_code, annual_financials, quarterly_list,
                      analysis, competition, news_items, investment_points,
                      market=None, related_posts=None):
    today_str = datetime.now().strftime('%Y-%m-%d')

    # 연간 재무: 원단위 → 억원/% 변환 (GPT가 읽기 쉽게)
    fin_clean = {}
    for year, m in annual_financials.items():
        fin_clean[str(year)] = {
            '매출액억원':     _to_eok(m.get('매출액')),
            '영업이익억원':   _to_eok(m.get('영업이익')),
            '영업이익률pct':  _to_pct(m.get('영업이익률')),
            '당기순이익억원': _to_eok(m.get('당기순이익')),
            'OCF억원':        _to_eok(m.get('영업활동현금흐름')),
            'CAPEX억원':      _to_eok(m.get('CAPEX')),
            'ROEpct':         _to_pct(m.get('ROE')),
        }

    internal_links = [{"title": p['title'], "url": p['link']} for p in (related_posts or [])]

    return {
        "company_name": company_name,
        "ticker":       stock_code,
        "market":       market or "KOSPI/KOSDAQ",
        "date":         today_str,
        "key_data": {
            "annual_financials":    fin_clean,
            "quarterly_financials": quarterly_list or [],
            "industry_analysis":    _build_industry_text(analysis),
            "competition":          _build_competition_summary(competition),
            "recent_news":          _build_news_summary(news_items, investment_points),
            "internal_links":       internal_links,
        },
        "thesis_bullets": _extract_thesis_bullets(investment_points),
        "risk_bullets":   _extract_risk_bullets(analysis),
        "sources":        [],
    }


# =====================================================
# 프롬프트 빌더
# =====================================================

def build_prompt(input_json):
    company_name = input_json['company_name']
    ticker       = input_json['ticker']
    today        = input_json['date']
    current_year  = datetime.now().year
    current_month = datetime.now().strftime('%m')

    slug_base = f"{ticker}-stock-analysis-{current_year}-{current_month}"

    internal_links_section = ""
    links = input_json['key_data'].get('internal_links', [])
    if links:
        link_lines = "\n".join(f'- {l["title"]}: {l["url"]}' for l in links)
        internal_links_section = f"""
내부링크 후보 (본문에 2~3개 자연스럽게 삽입, HTML <a href="..."> 형식):
{link_lines}
"""

    data_json_str = json.dumps(input_json, ensure_ascii=False, indent=2)

    return f"""당신은 HanAlpha의 데이터 기반 한국 주식 리서치 에디터다.

아래 JSON 데이터를 바탕으로 기업분석 글을 작성하라.

입력 데이터:
{data_json_str}
{internal_links_section}
────────────────────────────
[작성 규칙]

1. 감정적 표현, 투자 권유("추천", "매수", "폭등", "대박" 등) 금지.
2. 입력 JSON에 없는 수치는 임의로 확정하지 말고 조건부 표현을 사용한다.
3. 기업명({company_name})은 본문에 최소 6회 이상 자연스럽게 등장해야 한다.
4. 산업 관련 핵심 키워드는 최소 3회 이상 등장해야 한다.
5. 표는 최소 2개 이상 포함한다.
6. 글 길이는 최소 1500자 이상이다.
7. 리스크는 반드시 3개 이상 작성한다.
8. 오늘 날짜 기준({today})으로 현재 시제로 작성한다. 과도한 미래 단정 금지.
9. 출력은 반드시 HTML 본문 + 메타 태그 블록 순서로 작성한다.
10. 별도의 설명 없이 완성된 본문만 출력한다.

────────────────────────────
[본문 필수 구조]

<h1>{company_name} 주식 분석: 실적·산업·투자 체크리스트 점검</h1>

도입부 요약 3줄:
- 1문장 핵심 결론
- 1문장 핵심 근거
- 1문장 리스크

간단한 목차 리스트 (ul/li 형식)

<h2>{company_name}은 어떤 사업을 하는 기업인가?</h2>
첫 문단: 2문장 요약(결론 먼저)
- 사업 구조, 주요 매출원, 최근 실적 흐름

<h2>최근 실적은 개선되고 있는가?</h2>
첫 문단: 2문장 요약(결론 먼저)
- 매출/영업이익 추이, 재무 안정성, 현금흐름
- 주의: 연간 재무 테이블·차트·분기 실적 표는 이 H2 아래에 자동 삽입됩니다. 수치 해석·의미 부여 중심으로 서술하세요.

<h2>산업 사이클은 어디에 위치해 있는가?</h2>
첫 문단: 2문장 요약(결론 먼저)
- 산업 수요, 경쟁 구조, 정책/거시 영향

<h2>투자 체크리스트로 보면 매력적인가?</h2>
첫 문단: 2문장 요약(결론 먼저)
HTML table 형식:
| 항목 | 현재 상태 | 체크 포인트 |
최소 4개 이상

<h2>리스크 요인은 무엇인가?</h2>
첫 문단: 2문장 요약(결론 먼저)
리스크 최소 3개 (ul/li 형식)

<h2>결론</h2>
구조적 평가 + 향후 관찰 포인트 3~5개

<h2>자주 묻는 질문(FAQ)</h2>
FAQ 6~8개, 각 답변 2~3문장 (dl/dt/dd 형식 또는 h3/p 형식)

면책 고지 (마지막 줄):
<p class="disclaimer">※ 본 글은 공개된 재무 데이터를 기반으로 작성한 참고 자료이며, 투자 권유가 아닙니다. 투자 결정은 반드시 본인의 판단과 책임 하에 이루어져야 합니다.</p>

────────────────────────────
[SEO 메타 태그 블록 — 반드시 본문 아래 별도 출력]

<SEO_TITLE>
{company_name} 주식 분석: 실적·산업·리스크 점검
</SEO_TITLE>

<SEO_DESCRIPTION>
(155자 이내) {company_name}({ticker})의 사업 구조와 실적 흐름, 산업 사이클 영향을 점검합니다. 투자 체크리스트와 핵심 리스크를 데이터 기반으로 정리했습니다.
</SEO_DESCRIPTION>

<SLUG>
{slug_base}
</SLUG>

<FOCUS_KEYWORD>
{company_name} 주식 분석
</FOCUS_KEYWORD>

<CATEGORY>
기업분석
</CATEGORY>

<TAGS>
{company_name}, {ticker}, 주식분석, 재무분석, 리스크, 투자체크리스트
</TAGS>

<FAQ_JSON>
[
  {{"question":"{company_name}은 어떤 사업을 하나요?","answer":"..."}},
  {{"question":"최근 실적은 개선되고 있나요?","answer":"..."}}
]
(FAQ 섹션과 동일한 내용을 JSON 형식으로 출력)
</FAQ_JSON>

<QA_CHECKLIST>
- H2 질문형 사용 여부
- 표 2개 이상 포함 여부
- 리스크 3개 이상 포함 여부
- SEO_DESCRIPTION 155자 이내 여부
- 투자 권유 표현 없음 여부
</QA_CHECKLIST>
"""


# =====================================================
# SEO 태그 파서
# =====================================================

def _parse_tag(text, tag_name):
    """<TAG_NAME>...</TAG_NAME> 에서 내용 추출"""
    pattern = rf'<{tag_name}>(.*?)</{tag_name}>'
    match = re.search(pattern, text, re.DOTALL)
    return match.group(1).strip() if match else ''


def _remove_all_meta_blocks(text):
    """본문에서 모든 메타 태그 블록 제거"""
    tags = ['SEO_TITLE', 'SEO_DESCRIPTION', 'SLUG', 'FOCUS_KEYWORD',
            'CATEGORY', 'TAGS', 'FAQ_JSON', 'QA_CHECKLIST']
    for tag in tags:
        text = re.sub(rf'\s*<{tag}>.*?</{tag}>\s*', '', text, flags=re.DOTALL)
    return text.strip()


def _fallback_meta_description(company_name, stock_code, content):
    """SEO_DESCRIPTION이 없을 때 자동 생성"""
    for line in content.split('\n'):
        stripped = re.sub(r'<[^>]+>', '', line).strip()
        if len(stripped) >= 30 and not stripped.startswith('※'):
            return stripped[:150]
    return f"{company_name}({stock_code}) 기업분석: 실적·산업·리스크를 데이터 기반으로 점검합니다."


# =====================================================
# 글 생성 (메인 함수)
# =====================================================

def generate_wp_article(company_name, stock_code, annual_metrics_by_year,
                        analysis, competition, news_items, investment_points,
                        quarterly_by_year=None, related_posts=None, market=None):
    """
    기업분석 GPT 원본 결과로 WordPress 투자 분석글 생성 (SEO+AEO 최적화, HTML 출력).

    파라미터:
        annual_metrics_by_year : [(year, metrics_dict), ...] 튜플 리스트
        analysis               : {"산업 개요": "...", ...} 딕셔너리
        competition            : {"경쟁사목록": [{...}, ...]} 딕셔너리
        news_items             : [{"title": "...", "pubDate": "...", ...}, ...] 리스트
        investment_points      : [{"번호": 1, "투자포인트": "..."}, ...] 리스트
        quarterly_by_year      : {year: {1: metrics, 2: metrics, 3: metrics, 4: metrics}}
        related_posts          : [{"title": str, "link": str}, ...] 내부링크 후보
        market                 : "KOSPI" | "KOSDAQ" | None (자동 처리)

    반환: {
        'title':             str,
        'content':           str,   # HTML 본문만 (메타 블록 제거됨)
        'seo_title':         str,
        'meta_description':  str,
        'slug':              str,
        'focus_keyword':     str,
        'category':          str,
        'tags':              list[str],
        'faq_json':          str,   # FAQ_JSON 원문 (Schema 마크업 활용 가능)
        'annual_financials': dict,  # {year: metrics} — wp_publisher 테이블/차트용
        'quarterly_financials': list,  # [{"분기": ..., ...}] — wp_publisher 분기표용
    }
    """
    print(f"  [WP글생성] {company_name} 분석글 작성 중 (SEO+AEO)...")

    # 데이터 변환
    annual_financials = _annual_to_dict(annual_metrics_by_year)
    quarterly_list    = _quarterly_to_list(quarterly_by_year or {})

    # 표준 입력 JSON 빌드
    input_json = _build_input_json(
        company_name, stock_code, annual_financials, quarterly_list,
        analysis, competition, news_items, investment_points,
        market=market, related_posts=related_posts,
    )

    prompt = build_prompt(input_json)
    client = _get_client()

    response = client.chat.completions.create(
        model=ARTICLE_MODEL,
        messages=[
            {
                'role': 'system',
                'content': (
                    '당신은 HanAlpha의 데이터 기반 한국 주식 리서치 에디터입니다. '
                    '구조적이고 데이터 중심의 기업분석 글을 HTML 형식으로 작성합니다. '
                    '감정적 표현, 투자 권유 표현 없이 객관적으로 서술합니다. '
                    '반드시 지정된 H2 질문형 구조와 SEO 메타 태그 블록을 모두 포함하세요.'
                )
            },
            {'role': 'user', 'content': prompt}
        ],
        max_tokens=10000,
        temperature=0.4,
    )

    full_text = response.choices[0].message.content.strip()

    # SEO 태그 파싱
    seo_title        = _parse_tag(full_text, 'SEO_TITLE')
    meta_description = _parse_tag(full_text, 'SEO_DESCRIPTION')
    slug             = _parse_tag(full_text, 'SLUG')
    focus_keyword    = _parse_tag(full_text, 'FOCUS_KEYWORD')
    category         = _parse_tag(full_text, 'CATEGORY') or '기업분석'
    tags_raw         = _parse_tag(full_text, 'TAGS')
    faq_json         = _parse_tag(full_text, 'FAQ_JSON')

    # 본문에서 메타 블록 제거
    content = _remove_all_meta_blocks(full_text)

    # 태그 리스트 파싱
    tags = [t.strip() for t in tags_raw.split(',') if t.strip()] if tags_raw else [
        company_name, stock_code, '주식분석', '투자분석'
    ]

    # 자동 생성 (GPT가 출력하지 않은 경우)
    current_year  = datetime.now().year
    current_month = datetime.now().strftime('%m')
    if not seo_title:
        seo_title = f"{company_name} 주식 분석: 실적·산업·리스크 점검"
    if not meta_description:
        meta_description = _fallback_meta_description(company_name, stock_code, content)
        print(f"  [WP글생성] 메타디스크립션 폴백 생성: {len(meta_description)}자")
    if not slug:
        slug = f"{stock_code}-stock-analysis-{current_year}-{current_month}"
    if not focus_keyword:
        focus_keyword = f"{company_name} 주식 분석"

    # 포스트 제목 (WP title, Rank Math title과 별도)
    title = f"{company_name} 주식 분석 ({current_year}년) — 실적·산업·리스크 체크리스트"

    print(f"  [WP글생성] 완료: 제목='{title}', 본문={len(content)}자")
    print(f"  [WP글생성] SEO — 키워드='{focus_keyword}', 슬러그='{slug}', 메타={len(meta_description)}자")

    return {
        'title':               title,
        'content':             content,
        'seo_title':           seo_title,
        'meta_description':    meta_description,
        'slug':                slug,
        'focus_keyword':       focus_keyword,
        'category':            category,
        'tags':                tags,
        'faq_json':            faq_json,
        'annual_financials':   annual_financials,
        'quarterly_financials': quarterly_list,
    }
