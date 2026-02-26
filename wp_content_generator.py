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

ARTICLE_MODEL = 'gpt-5-mini'
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

    return f"""이전 SEO/구조/점검 지시를 모두 무시하고 아래 규칙을 최신 기준으로 적용한다.

당신은 HanAlpha의 데이터 기반 한국 주식 리서치 에디터다.

목표:
- 한국 상장 기업 분석 콘텐츠를 SEO + AEO + YMYL 기준으로 작성한다.
- 구조는 항상 동일하게 유지한다.
- 감정적 표현, 투자 권유, 확정적 미래 예측을 금지한다.
- 입력 JSON에 없는 수치는 생성하지 않는다.
- 금융 콘텐츠 특성상 단정 표현 대신 조건부 표현을 사용한다.
- 출력은 반드시 HTML 본문 + 메타 블록 + 점검 블록을 포함한다.
- 메타 블록은 본문과 분리하여 맨 아래에만 출력한다.

────────────────────────────
[입력 데이터]

{data_json_str}
{internal_links_section}
JSON에 없는 정보는 단정하지 말 것.
수치 인용 시 sources 기반일 경우 "(출처 유형)"으로 표시.

────────────────────────────
[본문 작성 규칙]

1. H1 제목 포함
2. H2는 반드시 질문형
3. 각 H2 바로 아래 2문장 요약 (결론 → 근거 구조)
4. 기업명({company_name})은 본문에 최소 6회 자연 등장
5. 산업 키워드 최소 3회 등장
6. 표 최소 2개 이상
7. 글 길이 1800자 이상
8. 리스크 최소 3~5개
9. 투자 권유 표현 금지 ("추천", "매수", "폭등", "대박" 등)
10. 조건부 표현 사용 ("가능성", "변수", "관찰 필요")
11. 오늘 날짜 기준({today})으로 현재 시제 작성. 단정적 미래 예측 금지.
12. 별도의 설명 없이 완성된 본문만 출력한다.

────────────────────────────
[본문 필수 구조]

<h1>{company_name} 주식 분석: 실적·산업·리스크 점검</h1>

도입부 3줄 요약:
- 핵심 결론
- 핵심 근거 2~3개 요약
- 가장 큰 리스크

관찰 포인트 3가지 bullet

목차 리스트 (ul/li 형식)

<h2>{company_name}은 어떤 사업 구조를 가지고 있는가?</h2>
2문장 요약 (결론 → 근거 구조)
- 사업 구조
- 매출 구성
- 최근 실적 흐름

표 1: 사업 구조·매출 구성 요약

<h2>최근 실적은 구조적으로 개선되고 있는가?</h2>
2문장 요약 (결론 → 근거 구조)
- 주의: 연간 재무 테이블·SVG 차트·분기 실적 표는 이 H2 바로 아래에 자동 삽입됩니다. 수치를 직접 나열하거나 연도별·분기별 실적을 텍스트로 요약하지 마세요.
- 재무 안정성, 현금흐름 특이사항, 이익 품질 등 정성적 분석만 서술하세요.

표 2: 재무 체크 항목 (수치 나열 금지, 정성적 체크리스트로 작성)

<h2>해당 산업 사이클은 어디에 위치해 있는가?</h2>
2문장 요약 (결론 → 근거 구조)
- 산업 수요
- 경쟁 구조
- 정책/거시 변수

<h2>투자 체크리스트로 보면 어떤 구간인가?</h2>
2문장 요약 (결론 → 근거 구조)

표:
| 항목 | 현재 상태 | 관찰 포인트 |
최소 4개 이상

<h2>핵심 리스크는 무엇인가?</h2>
2문장 요약 (결론 → 근거 구조)
리스크 3~5개 (ul/li 형식)
각 리스크는 "발생 시 영향" 포함

<h2>결론: 구조적 관점에서의 현재 위치</h2>
- 중립적 평가
- 다음 분기 관찰 포인트 3~5개

<h2>자주 묻는 질문(FAQ)</h2>
FAQ 6~8개. 아래 HTML 구조를 그대로 사용하라 (스타일 속성 유지, 내용만 교체):

<div style="border:1px solid #dce8f5;border-radius:8px;padding:16px 20px;margin:10px 0;background:#f7fbff;">
<p style="font-weight:bold;color:#1a3a5c;margin:0 0 8px 0;font-size:15px;">Q. [질문]</p>
<p style="color:#333;margin:0;font-size:14px;line-height:1.75;">A. [답변 2~3문장. 조건/변수 1개 포함]</p>
</div>

위 div 블록을 FAQ 개수만큼 반복 출력한다.

면책 고지 (마지막 줄):
<p class="disclaimer">※ 본 글은 공개된 재무 데이터를 기반으로 작성한 참고 자료이며, 투자 권유가 아닙니다. 투자 결정은 반드시 본인의 판단과 책임 하에 이루어져야 합니다.</p>

────────────────────────────
[출처 관리 블록 — 반드시 본문 아래 출력]

<SOURCE_NOTES>
- 사용한 출처 유형 정리 (공시/IR/뉴스/통계 등)
- 확인 필요 데이터 2~3개 명시
</SOURCE_NOTES>

────────────────────────────
[내부링크 확장 블록 — 반드시 본문 아래 출력]

<INTERNAL_LINKS>
- 동일 산업 관련 글 5개 추천
- 경쟁사 비교 글 5개 추천
- 초보자 개념 글 3개 추천
</INTERNAL_LINKS>

────────────────────────────
[SEO 메타 블록 — 반드시 본문 아래 출력]

<SEO_TITLE>
{company_name} 주식 분석: 실적·산업·리스크 점검
</SEO_TITLE>

<SEO_DESCRIPTION>
(110~155자) {company_name}({ticker})의 사업 구조와 실적 흐름, 산업 사이클 위치를 점검합니다. 투자 체크리스트와 핵심 리스크를 데이터 기반으로 정리했습니다.
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

<SERP_SNIPPET_TEST>
SEO_DESCRIPTION을 한 줄로 재출력하여 잘림 여부 확인
</SERP_SNIPPET_TEST>

────────────────────────────
[자체 점검 및 자동 수정 단계 – 필수 수행]

본문과 메타 작성 후 아래 점검을 수행한다:

1. 구조 점검
- H2 질문형 여부
- 표 2개 이상
- 리스크 3개 이상
- FAQ 6개 이상

2. SEO 점검
- SEO_TITLE 길이 적합
- SEO_DESCRIPTION 110~155자
- 키워드 과다 반복 없음

3. AEO 점검
- 각 H2 요약이 결론부터 시작
- FAQ 답변 2~3문장

4. 금융 안전 점검
- 투자 권유 표현 없음
- 단정적 미래 예측 없음
- 조건부 표현 포함 여부

5. 논리 일관성 점검
- 도입부와 결론 일치
- 새 주장 결론에 추가되지 않음

미흡한 부분이 있으면 자동 수정 후 최종본만 출력한다.

<SELF_AUDIT_RESULT>
- 구조 적합 여부: 적합 / 수정함
- SEO 적합 여부: 적합 / 수정함
- 금융 안전성 적합 여부: 적합 / 수정함
</SELF_AUDIT_RESULT>
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
    tags = [
        'SEO_TITLE', 'SEO_DESCRIPTION', 'SLUG', 'FOCUS_KEYWORD',
        'CATEGORY', 'TAGS', 'FAQ_JSON',
        'SOURCE_NOTES', 'INTERNAL_LINKS', 'SERP_SNIPPET_TEST',
        'SELF_AUDIT_RESULT', 'QA_CHECKLIST',
    ]
    for tag in tags:
        text = re.sub(rf'\s*<{tag}>.*?</{tag}>\s*', '', text, flags=re.DOTALL)
    return text.strip()


def _extract_faq_from_html(html):
    """기존 FAQ HTML에서 Q/A 쌍 역추출 (FAQ_JSON 없을 때 fallback)"""
    items = []
    # 패턴 1: <dt>...</dt><dd>...</dd>
    for q, a in re.findall(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html, re.DOTALL):
        q_text = re.sub(r'^Q[\.\:\s]+', '', re.sub(r'<[^>]+>', '', q).strip()).strip()
        a_text = re.sub(r'^A[\.\:\s]+', '', re.sub(r'<[^>]+>', '', a).strip()).strip()
        if q_text and a_text:
            items.append({'question': q_text, 'answer': a_text})
    if items:
        return items
    # 패턴 2: Q./A. p 태그 패턴
    pending_q = None
    for chunk in re.split(r'(?=<p[^>]*>)', html):
        text = re.sub(r'<[^>]+>', '', chunk).strip()
        if re.match(r'^Q[\.\:\s]', text):
            pending_q = re.sub(r'^Q[\.\:\s]+', '', text).strip()
        elif re.match(r'^A[\.\:\s]', text) and pending_q:
            items.append({'question': pending_q, 'answer': re.sub(r'^A[\.\:\s]+', '', text).strip()})
            pending_q = None
    return items


def _build_faq_cards_html(items):
    """Q/A 쌍 리스트에서 카드 스타일 HTML 생성"""
    if not items:
        return ''
    cards = []
    for item in items:
        q = str(item.get('question', '')).strip()
        a = str(item.get('answer', '')).strip()
        if not q or not a:
            continue
        cards.append(
            '<div style="border-left:4px solid #2563eb;border-radius:0 8px 8px 0;'
            'padding:14px 18px;margin:12px 0;background:#f8faff;'
            'box-shadow:0 1px 3px rgba(0,0,0,0.06);">'
            '<p style="font-weight:700;color:#1e3a6e;margin:0 0 6px 0;font-size:15px;">'
            '<span style="background:#2563eb;color:#fff;border-radius:4px;'
            'padding:1px 8px;font-size:12px;font-weight:700;margin-right:8px;">Q</span>'
            f'{q}</p>'
            f'<p style="color:#374151;margin:0;font-size:14px;line-height:1.8;'
            f'padding-left:28px;">{a}</p>'
            '</div>'
        )
    return '\n'.join(cards)


def _inject_faq_cards(content, faq_json_str):
    """본문 FAQ 섹션을 카드 HTML로 교체 (FAQ_JSON 우선, HTML 역추출 fallback)"""
    # H2 찾기: 자주 묻는 질문 / FAQ / Q&A 변형 모두 커버
    h2_match = re.search(
        r'<h2[^>]*>[^<]*(자주\s*묻는\s*질문|FAQ|Q&amp;A|Q&A)[^<]*</h2>',
        content, re.IGNORECASE
    )
    if not h2_match:
        print('  [FAQ주입] FAQ H2 없음 — 건너뜀')
        return content

    faq_start = h2_match.end()
    tail = content[faq_start:]
    next_h2 = re.search(r'<h2', tail, re.IGNORECASE)
    disclaimer = re.search(r'<p[^>]*class=["\']disclaimer["\']', tail, re.IGNORECASE)
    ends = []
    if next_h2:
        ends.append(next_h2.start())
    if disclaimer:
        ends.append(disclaimer.start())
    faq_end = faq_start + min(ends) if ends else len(content)
    faq_section_html = content[faq_start:faq_end]

    # FAQ 아이템 파싱: FAQ_JSON 우선 → HTML 역추출 fallback
    items = []
    if faq_json_str:
        try:
            parsed = json.loads(faq_json_str)
            if isinstance(parsed, list):
                items = parsed
        except (json.JSONDecodeError, ValueError):
            pass
    if not items:
        items = _extract_faq_from_html(faq_section_html)
        if items:
            print(f'  [FAQ주입] FAQ_JSON 없음 → HTML 역추출 fallback ({len(items)}개)')
        else:
            print('  [FAQ주입] FAQ 추출 실패 — 원본 유지')
            return content

    cards_html = _build_faq_cards_html(items)
    if not cards_html:
        return content

    print(f'  [FAQ주입] 카드 {len(items)}개 주입 완료')
    return content[:faq_start] + '\n' + cards_html + '\n' + content[faq_end:]


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
        max_tokens=16000,
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

    # FAQ 섹션을 카드 HTML로 교체 (FAQ_JSON 우선, HTML 역추출 fallback)
    content = _inject_faq_cards(content, faq_json)

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
    title = f"{company_name} 주식 분석 ({current_year}년) — 실적·산업·리스크 점검"

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
