"""
naver_content_generator.py — 네이버 블로그용 SEO 최적화 요약 포스트 생성

네이버 노출 최적화 원칙:
- 종목명 3~5회 자연 반복
- "주가 전망" 키워드 포함
- 종목코드 포함
- 문장형 도입부 + 마무리 질문형 문장
- 체류시간 유도용 구조
"""

import os
import json

from openai import OpenAI
from config import OPENAI_API_KEY

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


def _to_eok(val):
    try:
        v = float(val) / 1e8  # 억원 단위
        if v >= 10000:
            return f"{round(v / 10000, 1)}조원"
        return f"{round(v):,}억원"
    except Exception:
        return None


def _to_pct(val):
    try:
        return f"{round(float(val) * 100, 1)}%"
    except Exception:
        return None


def _generate_intro_and_closing(company_name, stock_code, period_key, biz_summary):
    """
    도입부 2~3문장 + 마무리 질문형 문장 GPT 생성.
    실패 시 기본 템플릿 반환.
    """
    prompt = f"""다음 정보를 바탕으로 네이버 블로그용 텍스트를 작성하세요.

기업명: {company_name} (종목코드: {stock_code})
분석기간: {period_key}
기업 상황 요약: {str(biz_summary)[:400]}

요청:
1. 도입부(2~3문장): "{company_name} 주가 전망"을 핵심 키워드로, {company_name}을 2회 이상 자연스럽게 포함. 투자자가 관심 가질 핵심 이슈를 제기. 딱딱하지 않게 구어체로.
2. 마무리_질문(1문장): 독자 체류시간 유도용 의견 유도 질문. {company_name} 포함. "~어떻게 보시나요?" 형식.

반드시 JSON:
{{"도입부": "...", "마무리_질문": "..."}}"""

    try:
        client = _get_client()
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=500,
        )
        result = json.loads(response.choices[0].message.content)
        intro = result.get('도입부', '').strip()
        closing = result.get('마무리_질문', '').strip()
        return intro, closing
    except Exception as e:
        print(f"  [네이버] 도입부 생성 실패 (기본 템플릿 사용): {e}")
        return (
            f"{company_name} 주가 전망에 대한 투자자들의 관심이 높아지고 있습니다.\n"
            f"{company_name}의 최근 실적과 핵심 투자 포인트를 {period_key} 기준으로 정리했습니다.",
            f"여러분은 {company_name}의 향후 실적 전망을 어떻게 보시나요?"
        )


def generate_naver_post(
    company_name: str,
    stock_code: str,
    period_key: str,
    annual_metrics_by_year: list,
    analysis: dict,
    investment_points: list,
    wp_url: str,
) -> str:
    """
    네이버 블로그용 SEO 최적화 요약 포스트 생성.
    반환: 완성된 텍스트 (붙여넣기용 plain text)
    """
    analysis = analysis or {}

    # ── 1. 재무 지표 (최신 2개 연도) ─────────────────────────────
    fin_lines = []
    latest_year, latest_m = None, {}
    if annual_metrics_by_year:
        recent = sorted(annual_metrics_by_year, key=lambda x: x[0], reverse=True)[:2]
        latest_year, latest_m = recent[0]
        for year, m in reversed(recent):
            parts = [f"{year}년"]
            rev = _to_eok(m.get('매출액'))
            op  = _to_eok(m.get('영업이익'))
            opm = _to_pct(m.get('영업이익률'))
            roe = _to_pct(m.get('ROE'))
            if rev: parts.append(f"매출 {rev}")
            if op:  parts.append(f"영업이익 {op}")
            if opm: parts.append(f"영업이익률 {opm}")
            if roe: parts.append(f"ROE {roe}")
            if len(parts) > 1:
                fin_lines.append(" | ".join(parts))

    # ── 2. 투자 포인트 상위 3개 ───────────────────────────────────
    points = []
    for item in (investment_points or [])[:3]:
        if isinstance(item, dict):
            text = str(item.get('투자포인트') or item.get('포인트') or '').strip()
        else:
            text = str(item).strip()
        if text:
            points.append(text)

    # ── 3. 리스크 요인 (analysis에서 추출) ───────────────────────
    risk_raw = analysis.get('투자 관점 핵심 리스크', '')
    risk_lines = [
        line.lstrip('•·-– ').strip()
        for line in str(risk_raw).split('\n')
        if line.strip() and not line.strip().startswith('[')
    ][:3]

    # ── 4. GPT 도입부 + 마무리 질문 생성 ─────────────────────────
    biz_summary = analysis.get('기업 상황 (재무 중심)') or analysis.get('산업 상황') or ''
    intro, closing_q = _generate_intro_and_closing(
        company_name, stock_code, period_key, biz_summary
    )

    # ── 5. 해시태그 ───────────────────────────────────────────────
    tags = [company_name, stock_code, '주가전망', '기업분석', '주식분석', '투자']
    hashtag_str = ' '.join(f'#{t}' for t in tags)

    # ── 6. 템플릿 조립 ────────────────────────────────────────────
    L = []

    L.append(f"📊 {company_name} 주가 전망 | {period_key} 기업분석 요약 ({stock_code})")
    L.append("")
    L.append(intro)
    L.append("")

    if fin_lines:
        L.append(f"✅ {company_name} 핵심 재무 지표")
        L.append("")
        for fl in fin_lines:
            L.append(f"  {fl}")
        L.append("")

    if points:
        L.append(f"💡 {company_name} 핵심 투자 포인트")
        L.append("")
        emojis = ['1️⃣', '2️⃣', '3️⃣']
        for i, p in enumerate(points):
            prefix = emojis[i] if i < len(emojis) else '•'
            L.append(f"{prefix} {p}")
        L.append("")

    if risk_lines:
        L.append("⚠️ 리스크 요인")
        L.append("")
        for r in risk_lines:
            L.append(f"  • {r}")
        L.append("")

    # 마무리 문장에 종목명 한 번 더 자연스럽게
    L.append(f"{company_name} 주가 전망은 향후 실적 흐름에 따라 달라질 수 있습니다.")
    L.append(closing_q)
    L.append("")
    L.append(f"🔗 {company_name} 전체 산업·재무·경쟁사 분석 보기")
    L.append(f"👉 {wp_url}")
    L.append("")
    L.append(hashtag_str)

    return "\n".join(L)


def save_naver_post(content: str, stock_code: str, period_key: str) -> str:
    """naver_posts/ 폴더에 txt 파일로 저장. 저장 경로 반환."""
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naver_posts')
    os.makedirs(folder, exist_ok=True)
    filename = f"{stock_code}_{period_key.replace('-', '')}.txt"
    path = os.path.join(folder, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    return path
