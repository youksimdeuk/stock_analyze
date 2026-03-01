"""
wp_en_content_generator.py
English retail-authority investment research article generator.
SSOT: same dataset as Korean pipeline (annual_metrics_by_year, analysis, competition, etc.)
Output: same dict structure as generate_wp_article() for seamless integration.
"""

import json
import os
import re
from datetime import datetime
from html import unescape

from openai import OpenAI
from config import OPENAI_API_KEY

ARTICLE_MODEL  = 'gpt-5-mini'
QUARTERLY_MAX  = 8
PEER_MAP_FILE  = os.path.join(os.path.dirname(__file__), 'peer_mapping.json')

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


# =====================================================
# Peer mapping
# =====================================================

def load_peer_mapping() -> dict:
    """Load peer_mapping.json. Returns {} if file missing or invalid."""
    try:
        with open(PEER_MAP_FILE, encoding='utf-8') as f:
            data = json.load(f)
        # strip meta keys starting with _
        return {k: v for k, v in data.items() if not k.startswith('_')}
    except Exception:
        return {}


# =====================================================
# Data helpers (mirrors KO generator, English units)
# =====================================================

def _annual_to_dict(annual_metrics_by_year):
    return {year: metrics for year, metrics in (annual_metrics_by_year or [])}


def _to_100m(val):
    """Convert raw KRW → KRW 100M (억원). Returns None if unavailable."""
    try:
        return round(float(val) / 1e8, 1) if val is not None else None
    except (TypeError, ValueError):
        return None


def _to_pct(val):
    try:
        return round(float(val) * 100, 1) if val is not None else None
    except (TypeError, ValueError):
        return None


def _quarterly_to_list(quarterly_by_year):
    """
    {year: {1: metrics, 2: metrics, ...}} →
    [{"분기": "2024Q4", "매출액억원": ..., ...}, ...] newest first, max QUARTERLY_MAX.
    """
    rows = []
    for year in sorted((quarterly_by_year or {}).keys(), reverse=True):
        for q in [4, 3, 2, 1]:
            m = (quarterly_by_year[year] or {}).get(q)
            if not m:
                continue
            rows.append({
                '분기':         f'{year}Q{q}',
                '매출액억원':    _to_100m(m.get('매출액')),
                '영업이익억원':  _to_100m(m.get('영업이익')),
                '영업이익률pct': _to_pct(m.get('영업이익률')),
                '당기순이익억원': _to_100m(m.get('당기순이익')),
            })
            if len(rows) >= QUARTERLY_MAX:
                return rows
    return rows


def _build_financials_summary(annual_dict: dict) -> str:
    """Readable summary for GPT (English units: KRW 100M)."""
    lines = []
    for year in sorted(annual_dict.keys()):
        m  = annual_dict[year]
        rev = _to_100m(m.get('매출액'))
        op  = _to_100m(m.get('영업이익'))
        opm = _to_pct(m.get('영업이익률'))
        ni  = _to_100m(m.get('당기순이익'))
        roe = _to_pct(m.get('ROE'))
        parts = [f"{year}:"]
        if rev  is not None: parts.append(f"Revenue {rev} KRW100M")
        if op   is not None: parts.append(f"OpProfit {op} KRW100M")
        if opm  is not None: parts.append(f"OpMargin {opm}%")
        if ni   is not None: parts.append(f"NetIncome {ni} KRW100M")
        if roe  is not None: parts.append(f"ROE {roe}%")
        lines.append("  " + " | ".join(parts))
    return "\n".join(lines) or "(no annual data)"


def _build_competition_en(competition: dict, max_peers: int = 6) -> str:
    """Format competition dict for English GPT prompt (max_peers entries)."""
    peers = (competition or {}).get('경쟁사목록', [])
    lines = []
    for p in peers[:max_peers]:
        name = p.get('기업명', '')
        country = p.get('국가', '')
        strengths = p.get('강점', '')
        risks = p.get('약점/리스크', '')
        rank = p.get('순위(국내/글로벌)', '')
        lines.append(f"  {name} ({country}) | Rank: {rank} | Strength: {strengths[:80]} | Risk: {risks[:80]}")
    return "\n".join(lines) or "(no competitor data)"


def _build_news_en(news_items: list, investment_points: list, max_items: int = 10) -> str:
    """Format recent news + investment points for English GPT prompt."""
    ip_map = {}
    for ip in (investment_points or []):
        num = str(ip.get('번호', ''))
        pt  = ip.get('투자포인트', '')
        if num and pt:
            ip_map[num] = pt

    lines = []
    for i, item in enumerate(news_items[:max_items], 1):
        title = item.get('title', '')
        date  = item.get('pubDate', '')[:10] if item.get('pubDate') else ''
        point = ip_map.get(str(i), '')
        line  = f"  [{date}] {title[:100]}"
        if point:
            line += f" → {point[:80]}"
        lines.append(line)
    return "\n".join(lines) or "(no recent news)"


def _build_peers_section(peers: dict) -> str:
    """Format peer_mapping peers for GPT valuation section."""
    if not peers:
        return "No peer mapping available for this ticker."
    lines = []
    dom = peers.get('domestic_peers', [])
    glob = peers.get('global_peers', [])
    if dom:
        lines.append("Domestic peers: " + ", ".join(
            f"{p['name']} ({p['ticker']})" for p in dom
        ))
    if glob:
        lines.append("Global peers: " + ", ".join(
            f"{p['name']} ({p['ticker']})" for p in glob
        ))
    return "\n".join(lines)


def _build_industry_en(analysis: dict) -> str:
    """Extract key industry analysis fields in English-friendly format."""
    keys_order = [
        '산업 개요', '산업 현재 업황', '기업의 해자(경쟁우위)',
        '투자 관점 핵심 리스크', '최신 기술 트렌드',
    ]
    lines = []
    for key in keys_order:
        val = (analysis or {}).get(key, '')
        if val:
            label = {
                '산업 개요': 'Industry Overview',
                '산업 현재 업황': 'Current Industry Conditions',
                '기업의 해자(경쟁우위)': 'Competitive Moat',
                '투자 관점 핵심 리스크': 'Key Investment Risks',
                '최신 기술 트렌드': 'Technology Trends',
            }.get(key, key)
            lines.append(f"[{label}]\n{str(val)[:400]}")
    return "\n\n".join(lines) or "(no industry analysis)"


# =====================================================
# GPT Prompt
# =====================================================

def _build_valuation_section(valuation_data: dict) -> str:
    """Format user valuation data (시가총액·PER·PBR·투자아이디어) for EN GPT prompt."""
    if not valuation_data:
        return ''
    parts = []
    mc  = valuation_data.get('market_cap')
    per = valuation_data.get('per')
    pbr = valuation_data.get('pbr')
    if mc:  parts.append(f'Market Cap: {mc} KRW100M')
    if per: parts.append(f'Current PER: {per}x')
    if pbr: parts.append(f'Current PBR: {pbr}x')
    idea = valuation_data.get('user_idea', '')
    result = ' | '.join(parts) if parts else ''
    if idea:
        result += f'\nAnalyst Note (user-written): {idea}'
    return result


def _build_en_input_json(company_name, stock_code, annual_dict,
                         quarterly_list, analysis, competition,
                         news_items, investment_points, peers,
                         related_posts, valuation_data=None):
    today = datetime.now().strftime('%Y-%m-%d')
    year_month = datetime.now().strftime('%Y-%m')
    data = {
        'company_name':       company_name,
        'ticker':             stock_code,
        'date':               today,
        'slug_suffix':        year_month,
        'unit_note':          'All financial values in KRW 100M (= 1억원). 1 KRW100M ≈ USD 70K at 1400 KRW/USD.',
        'annual_financials':  _build_financials_summary(annual_dict),
        'quarterly_snapshot': quarterly_list[:4],
        'industry_analysis':  _build_industry_en(analysis),
        'competition':        _build_competition_en(competition),
        'peers':              _build_peers_section(peers),
        'recent_news':        _build_news_en(news_items, investment_points),
        'internal_links':     [{'title': p['title'], 'url': p['link']} for p in (related_posts or [])[:3]],
    }
    val_section = _build_valuation_section(valuation_data or {})
    if val_section:
        data['current_valuation'] = val_section
    return data


def _build_en_prompt(input_data: dict) -> str:
    company  = input_data['company_name']
    ticker   = input_data['ticker']
    slug_sfx = input_data.get('slug_suffix', datetime.now().strftime('%Y-%m'))

    return f"""You are a professional equity research analyst writing an English-language investment research article for global individual investors on a Korean stock. Write in a professional, calm, research-driven tone. No hype, no investment advice, no target price, no buy/sell recommendation.

=== INPUT DATA ===
{json.dumps(input_data, ensure_ascii=False, indent=2)}
=================

=== CONTENT RULES ===
1. Write ONLY in English.
2. Total body content: minimum 1,800 characters.
3. H1 must contain the focus keyword: "{company} stock analysis".
4. Mention the company name within the first 100 words.
5. Include 5–7 FAQ items optimized for Google featured snippets.
6. Valuation section: use multiple-based approach only (PER if profitable, PSR if volatile).
   - If "current_valuation" field is provided in input data, use those PER/PBR/Market Cap values directly.
   - Compare current multiple with domestic peers AND global peers using peer data provided.
   - If no peer data: state "Comparable peer data not currently available for this coverage."
   - If "Analyst Note" is present in current_valuation, weave it into the valuation narrative naturally.
   - Never mention DCF, target price, or buy/sell language.
7. Units: state clearly "KRW 100M" for revenue-scale numbers; explain the unit once in the article.
8. Key Risks section: bullet format, minimum 3 risks.
9. What to Watch Next: clear catalyst list.
10. No keyword stuffing.
11. Include <!-- related_posts --> comment placeholder after Valuation section.

=== REQUIRED HTML STRUCTURE ===
<h1>{company} Stock Analysis [YEAR]: [subtitle with key theme]</h1>
<p><strong>Executive Summary:</strong></p>
<ul>
  <li>[Key finding 1]</li>
  <li>[Key finding 2]</li>
  <li>[Key finding 3]</li>
</ul>
<h2>Why This Company Matters</h2>
[paragraph]
<h2>Business Model Explained</h2>
[paragraph]
<h2>Revenue & Margin Snapshot</h2>
[paragraph introducing charts — charts will be injected here automatically]
<h2>Recent Quarterly Performance</h2>
[paragraph + table if possible]
<h2>Industry Context & Competitive Position</h2>
[paragraph]
<h2>Balance Sheet & Financial Stability</h2>
[paragraph]
<h2>Valuation Perspective</h2>
[4 paragraphs: current multiple → peer comparison → structural interpretation → scenario framing]
<!-- related_posts -->
<h2>Key Risks</h2>
<ul>
  <li>[Risk 1]</li>
  ...
</ul>
<h2>What to Watch Next</h2>
<ul>
  <li>[Catalyst 1]</li>
  ...
</ul>
<h2>FAQ</h2>
[5–7 Q&A items styled as dt/dd pairs]

=== REQUIRED META OUTPUT BLOCKS ===
After the HTML body, output these blocks EXACTLY:

<SEO_TITLE>{company} Stock Analysis [YEAR] | Revenue, Margin & Valuation</SEO_TITLE>
<SEO_DESCRIPTION>Under 155 characters. Include: {company} + sector + key metric + main risk.</SEO_DESCRIPTION>
<SLUG>{ticker.lower()}-stock-analysis-{slug_sfx}</SLUG>
<FOCUS_KEYWORD>{company} stock analysis</FOCUS_KEYWORD>
<TAGS>Korea stock, {ticker}, {company}, stock analysis, Korean equity</TAGS>
<FAQ_JSON>[{{"question": "...", "answer": "..."}}]</FAQ_JSON>
"""


# =====================================================
# Output parsers (mirror KO generator approach)
# =====================================================

def _parse_tag(text, tag_name):
    pattern = rf'<{tag_name}>(.*?)</{tag_name}>'
    m = re.search(pattern, text, re.DOTALL)
    return m.group(1).strip() if m else ''


def _remove_all_meta_blocks(text):
    tags = [
        'SEO_TITLE', 'SEO_DESCRIPTION', 'SLUG', 'FOCUS_KEYWORD',
        'CATEGORY', 'TAGS', 'FAQ_JSON',
        'SOURCE_NOTES', 'INTERNAL_LINKS', 'SELF_AUDIT_RESULT',
    ]
    for tag in tags:
        text = re.sub(rf'\s*<{tag}>.*?</{tag}>\s*', '', text, flags=re.DOTALL)
    return text.strip()


def _extract_faq_from_html_en(html):
    """Parse FAQ from dt/dd pairs (English article format)."""
    items = []
    for q, a in re.findall(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', html, re.DOTALL):
        q_text = re.sub(r'<[^>]+>', '', q).strip()
        a_text = re.sub(r'<[^>]+>', '', a).strip()
        if q_text and a_text:
            items.append({'question': q_text, 'answer': a_text})
    return items


def _inject_faq_cards_en(content, faq_json_str):
    """Replace EN FAQ section with styled card HTML."""
    h2_match = re.search(r'<h2[^>]*>[^<]*FAQ[^<]*</h2>', content, re.IGNORECASE)
    if not h2_match:
        return content

    faq_start = h2_match.end()
    tail = content[faq_start:]
    next_h2 = re.search(r'<h2', tail, re.IGNORECASE)
    faq_end = faq_start + next_h2.start() if next_h2 else len(content)
    faq_section_html = content[faq_start:faq_end]

    items = []
    if faq_json_str:
        try:
            parsed = json.loads(faq_json_str)
            if isinstance(parsed, list):
                items = parsed
        except (json.JSONDecodeError, ValueError):
            pass
    if not items:
        items = _extract_faq_from_html_en(faq_section_html)

    if not items:
        return content

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

    if not cards:
        return content
    print(f'  [EN FAQ] {len(cards)} FAQ cards injected')
    return content[:faq_start] + '\n' + '\n'.join(cards) + '\n' + content[faq_end:]


# =====================================================
# Main: generate_en_article
# =====================================================

def generate_en_article(
    company_name,
    stock_code,
    annual_metrics_by_year,
    analysis,
    competition,
    news_items,
    investment_points,
    quarterly_by_year=None,
    related_posts=None,
    peers=None,
    valuation_data=None,   # {'market_cap': str, 'per': str, 'pbr': str, 'user_idea': str}
) -> dict:
    """
    Generate an English retail-authority investment research article.

    Args:
        company_name, stock_code: Company identifiers
        annual_metrics_by_year : list of (year, metrics_dict) — raw KRW values
        analysis               : dict from generate_industry_analysis()
        competition            : dict with '경쟁사목록' key
        news_items             : list of news dicts
        investment_points      : list of investment thesis dicts
        quarterly_by_year      : {year: {1..4: metrics}} optional
        related_posts          : [{'title': str, 'link': str}] optional
        peers                  : {'domestic_peers': [...], 'global_peers': [...]} optional

    Returns:
        dict with same keys as generate_wp_article() for publisher compatibility:
        title, content, seo_title, meta_description, slug, focus_keyword,
        tags, faq_json, annual_financials, quarterly_financials
    """
    annual_dict    = _annual_to_dict(annual_metrics_by_year)
    quarterly_list = _quarterly_to_list(quarterly_by_year)

    input_data = _build_en_input_json(
        company_name, stock_code, annual_dict, quarterly_list,
        analysis, competition, news_items, investment_points,
        peers or {}, related_posts,
        valuation_data=valuation_data,
    )
    prompt = _build_en_prompt(input_data)

    print(f'  [EN] Generating English article for {company_name} ({stock_code})...')
    client   = _get_client()
    response = client.chat.completions.create(
        model=ARTICLE_MODEL,
        messages=[
            {'role': 'system', 'content': 'You are a professional equity research analyst. English only. No investment advice.'},
            {'role': 'user',   'content': prompt},
        ],
        max_completion_tokens=14000,
    )
    raw = response.choices[0].message.content or ''
    print(f'  [EN] GPT response: {len(raw)} chars')

    # Parse meta blocks
    seo_title        = _parse_tag(raw, 'SEO_TITLE')
    meta_description = _parse_tag(raw, 'SEO_DESCRIPTION')
    slug             = _parse_tag(raw, 'SLUG')
    focus_keyword    = _parse_tag(raw, 'FOCUS_KEYWORD')
    tags_raw         = _parse_tag(raw, 'TAGS')
    faq_json_str     = _parse_tag(raw, 'FAQ_JSON')

    tags = [t.strip() for t in tags_raw.split(',') if t.strip()] if tags_raw else [
        f'{company_name} stock', stock_code, 'Korea stock', 'stock analysis'
    ]

    # Strip meta blocks from HTML body
    content = _remove_all_meta_blocks(raw)

    # Inject styled FAQ cards
    content = _inject_faq_cards_en(content, faq_json_str)

    # Fallbacks
    year = datetime.now().strftime('%Y')
    if not seo_title:
        seo_title = f'{company_name} Stock Analysis {year} | Korea Equity Research'
    if not slug:
        slug = f'{stock_code}-stock-analysis-{datetime.now().strftime("%Y-%m")}'
    if not focus_keyword:
        focus_keyword = f'{company_name} stock analysis'
    if not meta_description:
        meta_description = (
            f'{company_name} ({stock_code}) stock analysis {year}: '
            f'revenue trend, margin outlook, and key risks for global investors.'
        )[:155]

    print(f'  [EN] slug={slug} | focus_keyword={focus_keyword}')

    return {
        'title':                f'{company_name} Stock Analysis {year}: Revenue, Margin & Valuation Outlook',
        'content':              content,
        'seo_title':            seo_title,
        'meta_description':     meta_description,
        'slug':                 slug,
        'focus_keyword':        focus_keyword,
        'tags':                 tags,
        'faq_json':             faq_json_str,
        # Pass-through for chart injection in publish_post_en()
        'annual_financials':    annual_dict,
        'quarterly_financials': quarterly_list,
    }
