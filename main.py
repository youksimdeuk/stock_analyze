"""
기업분석 자동화 스크립트
- DART API: 재무 데이터 수집
- 네이버 뉴스 API: 뉴스 수집
- OpenAI GPT-5-mini: 내용 분석 및 요약
- Google Sheets API: 스프레드시트 자동 입력
"""

import os
import io
import re
import json
import sys
import time
import zipfile

# Windows cp949 터미널에서 유니코드(이모지 등) 출력 오류 방지
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
from openai import OpenAI
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from config import (
    DART_API_KEY, OPENAI_API_KEY,
    NAVER_CLIENT_ID, NAVER_CLIENT_SECRET,
    GOOGLE_CREDENTIALS_PATH, GOOGLE_TOKEN_PATH,
    GOOGLE_CREDENTIALS_JSON, GOOGLE_TOKEN_JSON
)

# =====================================================
# 시트 구조 매핑 (주식분석 값 입력)
# =====================================================

# 연간 데이터: 행2에 연도(A=2015~), 데이터는 짝수행
ANNUAL_YEAR_START = 2015
ANNUAL_DATA_ROWS = {
    '매출액':           4,
    '매출원가':         6,
    '판관비':           8,
    '영업이익':         10,
    '당기순이익':       12,
    '영업이익률':       14,
    'CAPEX':           16,
    '자본총계':         18,
    '영업활동현금흐름': 20,
    'ROE':             22,
}

# 분기 데이터 섹션별 구조
QUARTERLY_SECTIONS = [
    {
        'header_row': 24,
        'years': [2020, 2021],
        'data_rows': {
            '매출액': 26, '매출원가': 28, '판관비': 30,
            '영업이익': 32, '당기순이익': 34, '영업이익률': 36,
            'CAPEX': 38, '자본총계': 40, '영업활동현금흐름': 42, 'ROE': 44
        }
    },
    {
        'header_row': 47,
        'years': [2022, 2023],
        'data_rows': {
            '매출액': 49, '매출원가': 51, '판관비': 53,
            '영업이익': 55, '당기순이익': 57, '영업이익률': 59,
            'CAPEX': 61, '자본총계': 63, '영업활동현금흐름': 65, 'ROE': 67
        }
    },
    {
        'header_row': 70,
        'years': [2024, 2025],
        'data_rows': {
            '매출액': 72, '매출원가': 74, '판관비': 76,
            '영업이익': 78, '당기순이익': 80, '영업이익률': 82,
            'CAPEX': 84, '자본총계': 86, '영업활동현금흐름': 88, 'ROE': 90
        }
    },
    {
        'header_row': 93,
        'years': [2026, 2027],
        'data_rows': {
            '매출액': 95, '매출원가': 97, '판관비': 99,
            '영업이익': 101, '당기순이익': 103, '영업이익률': 105,
            'CAPEX': 107, '자본총계': 109, '영업활동현금흐름': 111, 'ROE': 113
        }
    },
]

REPRT_CODES = {
    'Q1': '11013',   # 1분기보고서
    'H1': '11012',   # 반기보고서
    'Q3': '11014',   # 3분기보고서
    'FY': '11011',   # 사업보고서(연간)
}

SHEET_ALIASES = {
    'corp_map': ['corp_map', 'corp map', 'corp-map', 'corpcode', 'corp_code'],
    '주식분석 값 입력': ['주식분석 값 입력', '주식분석값입력', '주식 분석 값 입력', '주식분석 값입력'],
    '뉴스수집': ['뉴스수집', '뉴스 수집'],
    '산업 이해 및 기업 상황': ['산업 이해 및 기업 상황', '산업이해 및 기업상황', '산업 이해', '기업 상황'],
    '경쟁현황': ['경쟁현황', '경쟁 현황'],
    '현재가 구하기': ['현재가 구하기', '현재가구하기'],
    '마스터': ['마스터', 'master', '마스터시트', 'master sheet'],
}

# =====================================================
# Google Sheets 인증
# =====================================================

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_google_client():
    if GOOGLE_CREDENTIALS_JSON:
        with open(GOOGLE_CREDENTIALS_PATH, 'w', encoding='utf-8') as f:
            f.write(GOOGLE_CREDENTIALS_JSON)
    if GOOGLE_TOKEN_JSON:
        with open(GOOGLE_TOKEN_PATH, 'w', encoding='utf-8') as f:
            f.write(GOOGLE_TOKEN_JSON)

    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        raise FileNotFoundError(
            f"Google credentials 파일이 없습니다: {GOOGLE_CREDENTIALS_PATH}. "
            "GOOGLE_CREDENTIALS_PATH 또는 GOOGLE_CREDENTIALS_JSON을 설정하세요."
        )

    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(GOOGLE_TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GOOGLE_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(GOOGLE_TOKEN_PATH, 'w') as f:
            f.write(creds.to_json())
    return gspread.authorize(creds)


def validate_runtime_config():
    required = {
        'DART_API_KEY': DART_API_KEY,
        'OPENAI_API_KEY': OPENAI_API_KEY,
        'NAVER_CLIENT_ID': NAVER_CLIENT_ID,
        'NAVER_CLIENT_SECRET': NAVER_CLIENT_SECRET,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "필수 환경변수가 비어 있습니다: " + ", ".join(missing)
        )

# =====================================================
# DART API
# =====================================================

def get_dart_disclosures(corp_code, count=20):
    """최근 공시 목록 조회"""
    url = "https://opendart.fss.or.kr/api/list.json"
    now = datetime.now()
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': corp_code,
        'bgn_de': f"{now.year - 5}0101",
        'end_de': now.strftime('%Y%m%d'),
        'page_no': '1',
        'page_count': str(count),
        'sort': 'date',
        'sort_mth': 'desc',
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get('status') == '000':
            return data.get('list', [])
        print(f"  [경고] 공시 조회 실패: status={data.get('status')} message={data.get('message')}")
    except Exception:
        pass
    return []

def _clean_xml_text(chunk):
    """XML/HTML 태그 제거 후 정규화된 텍스트 반환"""
    t = re.sub(r'<[^>]+>', ' ', chunk)
    t = re.sub(r'&[a-zA-Z#0-9]+;', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()


def _extract_dart_sections(raw, max_chars):
    """DART XML에서 TITLE 태그 기반으로 섹션 추출.
    우선순위: II. 사업의 내용 → I. 회사의 개요 → 나머지
    TITLE 태그가 없으면 None 반환 (폴백 처리 필요).
    """
    title_matches = list(re.finditer(r'<TITLE[^>]*>([^<]+)</TITLE>', raw))
    if not title_matches:
        return None

    title_positions = [(m.start(), m.group(1).strip()) for m in title_matches]

    def get_section(start_kw, end_kw):
        start_pos = next((pos for pos, t in title_positions if start_kw in t), -1)
        if start_pos < 0:
            return ''
        end_pos = next((pos for pos, t in title_positions if end_kw in t and pos > start_pos), len(raw))
        return _clean_xml_text(raw[start_pos:end_pos])

    sec_business = get_section('II. 사업의 내용', 'III. 재무에 관한 사항')
    sec_overview  = get_section('I. 회사의 개요',  'II. 사업의 내용')

    # 사업의 내용 우선, 남은 공간에 회사의 개요 추가
    business_limit = int(max_chars * 0.8)
    business_part  = sec_business[:business_limit]
    remaining      = max_chars - len(business_part)
    overview_part  = sec_overview[:remaining] if remaining > 0 else ''

    combined = (business_part + ' ' + overview_part).strip()
    return combined[:max_chars] if combined else None


def get_annual_report_text(corp_code, max_chars=20000):
    """최근 사업보고서 원문 텍스트 추출.
    DART XML의 TITLE 태그로 '사업의 내용' 섹션을 우선 추출하고,
    구조 인식 불가 시 파일 순서 기반 폴백.
    """
    try:
        disclosures = get_dart_disclosures(corp_code, count=50)
        annual = next((d for d in disclosures if '사업보고서' in d.get('report_nm', '')), None)
        if not annual:
            print("  [원문] 공시 목록에서 사업보고서를 찾지 못했습니다.")
            return ''
        rcept_no = annual.get('rcept_no', '')
        if not rcept_no:
            return ''

        print(f"  [원문] 사업보고서 다운로드 중: {annual.get('rcept_dt')} {annual.get('report_nm')}")
        r = requests.get(
            "https://opendart.fss.or.kr/api/document.xml",
            params={'crtfc_key': DART_API_KEY, 'rcept_no': rcept_no},
            timeout=30
        )
        if r.status_code != 200:
            print(f"  [원문] document.xml HTTP 오류: {r.status_code}")
            return ''

        try:
            zf = zipfile.ZipFile(io.BytesIO(r.content))
        except Exception:
            print(f"  [원문] ZIP 파싱 실패 (응답이 ZIP이 아님, size={len(r.content)})")
            return ''

        all_files = zf.namelist()

        # 가장 큰 파일 = 본문 전체가 담긴 메인 파일 (infolist로 메타데이터만 비교)
        info_map = {info.filename: info.file_size for info in zf.infolist()}
        xml_files = [n for n in all_files if n.lower().endswith('.xml') and 'xbrl' not in n.lower()]
        htm_files = [n for n in all_files if any(n.lower().endswith(e) for e in ['.htm', '.html'])]

        # ── 1순위: XML 메인 파일에서 TITLE 태그 기반 섹션 추출 ──
        if xml_files:
            main_file = max(xml_files, key=lambda n: info_map.get(n, 0))
            raw = zf.read(main_file).decode('utf-8', errors='ignore')
            result = _extract_dart_sections(raw, max_chars)
            if result:
                print(f"  [원문] 추출 완료: {len(result)}자 (섹션 기반)")
                return result

        # ── 2순위: HTM 파일들을 순서대로 합산 (폴백) ──
        target_files = htm_files if htm_files else xml_files
        texts = []
        for name in target_files:
            try:
                raw = zf.read(name).decode('utf-8', errors='ignore')
                clean = _clean_xml_text(raw)
                if len(clean) > 300:
                    texts.append(clean[:7000])
            except Exception:
                continue

        result = (' '.join(texts))[:max_chars]
        print(f"  [원문] 추출 완료: {len(result)}자 (파일 순서 기반, {len(texts)}개)")
        return result
    except Exception as e:
        print(f"  [원문] 예외 발생: {e}")
        return ''

def get_corp_info(stock_code):
    """종목코드로 DART corp_code, 기업명 조회"""
    stock_code = str(stock_code).zfill(6)
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {'crtfc_key': DART_API_KEY, 'stock_code': stock_code}
    response_data = None
    try:
        r = requests.get(url, params=params, timeout=10)
        response_data = r.json()
        if response_data.get('status') == '000':
            return response_data.get('corp_code'), response_data.get('corp_name')
    except Exception as e:
        print(f"  [오류] corp_info 조회 실패: {e}")

    if response_data:
        print(
            f"  [경고] company.json 실패: status={response_data.get('status')} "
            f"message={response_data.get('message')}"
        )

    # company.json 실패 시 corpCode.xml 원본에서 폴백 조회
    corp_code, corp_name = get_corp_info_from_master(stock_code)
    if corp_code:
        print(f"  [폴백성공] corpCode.xml에서 corp_code 조회: {corp_code}")
        return corp_code, corp_name

    return None, None


def get_corp_info_from_master(stock_code):
    """DART corpCode.xml(전체 목록)에서 종목코드로 corp_code 조회"""
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {'crtfc_key': DART_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"  [오류] corpCode.xml 다운로드 실패: HTTP {r.status_code}")
            return None, None

        zf = zipfile.ZipFile(io.BytesIO(r.content))
        xml_name = next((name for name in zf.namelist() if name.lower().endswith('.xml')), None)
        if not xml_name:
            print("  [오류] corpCode.xml 내부 XML 파일을 찾지 못했습니다.")
            return None, None

        xml_text = zf.read(xml_name).decode('utf-8', errors='ignore')
        root = ET.fromstring(xml_text)
        target = str(stock_code).zfill(6)
        for node in root.findall('list'):
            sc = (node.findtext('stock_code') or '').strip()
            if sc == target:
                return (node.findtext('corp_code') or '').strip(), (node.findtext('corp_name') or '').strip()

        print(f"  [오류] corpCode.xml에서 stock_code={target}를 찾지 못했습니다.")
    except Exception as e:
        print(f"  [오류] corpCode.xml 파싱 실패: {e}")
    return None, None

def get_financial_statements(corp_code, year, reprt_code):
    """DART 재무제표 조회 (연결 우선, 없으면 개별)"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    for fs_div in ['CFS', 'OFS']:
        params = {
            'crtfc_key': DART_API_KEY,
            'corp_code': corp_code,
            'bsns_year': str(year),
            'reprt_code': reprt_code,
            'fs_div': fs_div
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            if data.get('status') == '000' and data.get('list'):
                return data['list'], fs_div
        except Exception as e:
            print(f"  [오류] 재무제표 조회 실패 ({year}/{reprt_code}/{fs_div}): {e}")
    return [], None

def get_fin_data(corp_code, year, reprt_code, fs_div, sj_div):
    """DART 재무제표 단일 섹션 조회 (sj_div: BS/IS/CF)"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': corp_code,
        'bsns_year': str(year),
        'reprt_code': reprt_code,
        'fs_div': fs_div,
        'sj_div': sj_div,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get('status') == '000' and data.get('list'):
            return data['list']
    except Exception:
        pass
    return []

def extract_bs_price_data(rows):
    """BS에서 현재가 구하기 시트용 데이터 추출"""
    assets = liab = cash_eq = st_fin = nci_equity = None
    long_debt_sum = pref_equity_sum = 0
    long_debt_hit = pref_hit = False

    ASSET_IDS = {'ifrsfullassets', 'ifrsassets'}
    LIAB_IDS  = {'ifrsfullliabilities', 'ifrsliabilities'}
    CASH_IDS  = {'ifrsfullcashandcashequivalents'}
    NCI_IDS   = {'ifrsfullnoncontrollinginterests'}

    LONG_DEBT_KEYS = ['장기차입금', '사채', '리스부채', '전환사채', '신주인수권부사채']
    ST_FIN_KEYS    = ['단기금융상품', '단기금융자산', '기타유동금융자산', '유동금융자산', '단기투자자산']
    NCI_KEYS       = ['비지배주주지분', '비지배주주', '비지배']
    PREF_KEYS      = ['우선주자본금', '우선주자본', '우선주관련', '우선주']

    for item in rows:
        aid = (item.get('account_id') or '').lower().replace('-', '').replace('_', '').replace(' ', '')
        anm = (item.get('account_nm') or '').replace(' ', '')
        try:
            v = int((item.get('thstrm_amount') or '0').replace(',', '').strip())
        except Exception:
            continue

        if assets is None and aid in ASSET_IDS:    assets = v
        if liab   is None and aid in LIAB_IDS:     liab   = v
        if cash_eq is None and aid in CASH_IDS:    cash_eq = v
        if st_fin is None and any(k in anm for k in ST_FIN_KEYS):  st_fin = v
        if any(k in anm for k in LONG_DEBT_KEYS):
            long_debt_sum += v; long_debt_hit = True
        if nci_equity is None and (aid in NCI_IDS or any(k in anm for k in NCI_KEYS)):
            nci_equity = v
        if any(k in anm for k in PREF_KEYS):
            pref_equity_sum += v; pref_hit = True

    cash_like = (cash_eq or 0) + (st_fin or 0) if (cash_eq is not None or st_fin is not None) else None
    return {
        'assets':      assets,
        'liab':        liab,
        'cash_like':   cash_like,
        'long_debt':   long_debt_sum if long_debt_hit else None,
        'nci_equity':  nci_equity,
        'pref_equity': pref_equity_sum if pref_hit else None,
    }

def detect_latest_bs(corp_code):
    """최신 BS가 있는 보고서 탐색 (3Q > H1 > Q1 > FY 순)"""
    reprt_priority = ['11014', '11012', '11013', '11011']
    current_year = datetime.now().year
    for year in range(current_year, 2014, -1):
        for reprt_code in reprt_priority:
            for fs_div in ['CFS', 'OFS']:
                rows = get_fin_data(corp_code, year, reprt_code, fs_div, 'BS')
                if rows:
                    bs = extract_bs_price_data(rows)
                    if bs['assets'] is not None or bs['liab'] is not None:
                        return year, reprt_code, fs_div, bs
                time.sleep(0.2)
    return None, None, None, None

def get_stock_shares(corp_code, year, reprt_code):
    """DART stockTotqySttus: 주식수 조회"""
    url = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': corp_code,
        'bsns_year': str(year),
        'reprt_code': reprt_code,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        if data.get('status') == '000' and data.get('list'):
            return data['list']
    except Exception:
        pass
    return []

def parse_share_counts(share_list):
    """주식수 파싱: 보통주 우선, fallback"""
    def to_int(v):
        try:
            return int(str(v or '0').replace(',', ''))
        except Exception:
            return None

    fb_issued = fb_treasury = fb_float = None
    for item in share_list:
        se = str(item.get('se', '')).strip()
        i = to_int(item.get('istc_totqy'))
        t = to_int(item.get('tesstk_co'))
        f = to_int(item.get('distb_stock_co'))
        if i is None and t is None and f is None:
            continue
        if fb_issued   is None and i is not None: fb_issued   = i
        if fb_treasury is None and t is not None: fb_treasury = t
        if fb_float    is None and f is not None: fb_float    = f
        if '보통' in se:
            return i, t, f
    return fb_issued, fb_treasury, fb_float

def fetch_latest_shares(corp_code, latest_year, latest_reprt):
    """최신 보고서 기준 주식수 조회 (없으면 이전 연도로 순차 탐색)"""
    reprt_priority = ['11014', '11012', '11013', '11011']
    tries = [(latest_year, latest_reprt)]
    for r in reprt_priority:
        if r != latest_reprt:
            tries.append((latest_year, r))
    for y in range(latest_year - 1, max(2014, latest_year - 5), -1):
        for r in reprt_priority:
            tries.append((y, r))

    for year, reprt in tries:
        share_list = get_stock_shares(corp_code, year, reprt)
        if share_list:
            issued, treasury, float_s = parse_share_counts(share_list)
            if issued is not None:
                return issued, treasury, float_s
        time.sleep(0.2)
    return None, None, None

def write_price_sheet(ws, bs, issued, treasury, float_shares):
    """현재가 구하기 시트에 데이터 쓰기"""
    updates = []
    def add(cell, val):
        if val is not None:
            updates.append({'range': cell, 'values': [[val]]})

    add('A14', bs.get('assets'))
    add('B14', bs.get('liab'))
    add('A16', bs.get('nci_equity'))
    add('B16', bs.get('pref_equity'))
    add('A20', bs.get('cash_like'))
    add('B20', bs.get('long_debt'))
    add('D5',  issued)
    add('D6',  treasury)

    # 유통주식수 = 발행 - 자기주식
    if issued is not None:
        add('D17', issued - (treasury or 0))
    elif float_shares is not None:
        add('D17', float_shares)

    if updates:
        ws.batch_update(updates)

def parse_dart_int(value):
    text = str(value or '').strip()
    if not text or text in {'-', '--', 'N/A', '—', '–'}:
        return None
    neg = False
    if text.startswith('△'):
        neg = True
        text = text[1:]
    text = text.replace(',', '')
    if text.startswith('(') and text.endswith(')'):
        neg = True
        text = text[1:-1]
    try:
        n = int(float(text))
        return -n if neg else n
    except Exception:
        return None


def pick_numeric_amount(item):
    """DART 누적금액(thstrm_add_amount) 우선, 없으면 thstrm_amount"""
    v = parse_dart_int(item.get('thstrm_add_amount'))
    if v is not None:
        return v
    return parse_dart_int(item.get('thstrm_amount'))


def normalize_account_id(value):
    return (value or '').lower().replace('-', '').replace('_', '').replace(' ', '')


def normalize_account_name(value):
    return re.sub(r'[^0-9A-Za-z가-힣]', '', str(value or '')).lower().strip()


def pick_is_core_from_rows(fin_list):
    """GAS 로직 기반 IS/CIS 핵심값 추출"""
    out = {'rev': None, 'cogs': None, 'sga': None, 'op': None, 'ni': None}
    selling = None
    admin = None

    revenue_ids = {
        'ifrsfullrevenue', 'ifrsrevenue', 'ifrsfullrevenuefromcontractswithcustomers'
    }
    cogs_ids = {'ifrsfullcostofsales', 'ifrscostofsales'}
    sga_ids = {
        'darttotalsellinggeneraladministrativeexpenses',
        'ifrsfullsellinggeneralandadministrativeexpense'
    }
    op_ids = {
        'dartoperatingincomeloss',
        'ifrsfulloperatingprofitloss',
        'ifrsoperatingprofitloss',
        'ifrsfullprofitlossfromoperatingactivities',
    }
    ni_ids = {
        'ifrsfullprofitloss',
        'ifrsprofitloss',
        'ifrsfullprofitlossattributabletoownersofparent',
    }

    # 1) account_id 우선
    for item in fin_list:
        sj = (item.get('sj_div') or '').upper()
        if sj not in {'IS', 'CIS'}:
            continue
        aid = normalize_account_id(item.get('account_id'))
        val = pick_numeric_amount(item)
        if val is None:
            continue

        if out['rev'] is None and aid in revenue_ids:
            out['rev'] = val
        if out['cogs'] is None and aid in cogs_ids:
            out['cogs'] = val
        if out['sga'] is None and aid in sga_ids:
            out['sga'] = val
        if out['op'] is None and aid in op_ids:
            out['op'] = val
        if out['ni'] is None and aid in ni_ids:
            out['ni'] = val

    # 2) account_nm 보조
    for item in fin_list:
        sj = (item.get('sj_div') or '').upper()
        if sj not in {'IS', 'CIS'}:
            continue
        nm = normalize_account_name(item.get('account_nm'))
        val = pick_numeric_amount(item)
        if val is None:
            continue

        if out['rev'] is None:
            if (
                nm == '매출액'
                or '수익매출액' in nm
                or nm == '수익'
                or '영업수익' in nm
                or '고객과의계약' in nm
            ):
                out['rev'] = val

        if out['cogs'] is None:
            # '영업비용'은 오인 가능성이 커서 제외
            if '매출원가' in nm or ('원가' in nm and '판매' not in nm and '관리' not in nm):
                out['cogs'] = val

        if out['sga'] is None:
            if any(k in nm for k in ['판매비와관리비', '판매비및관리비', '판관비']):
                out['sga'] = val

        if selling is None and '판매비' in nm:
            selling = val
        if admin is None and '관리비' in nm:
            admin = val

        if out['op'] is None and any(k in nm for k in ['영업이익', '영업손익', '영업손실']):
            out['op'] = val

        if out['ni'] is None and any(k in nm for k in ['당기순이익', '당기순손익', '연결당기순이익', '지배기업소유주']):
            out['ni'] = val

    if out['sga'] is None and selling is not None and admin is not None:
        out['sga'] = selling + admin

    return out


def find_amount(fin_list, keywords, sj_div=None, account_ids=None):
    """재무제표 항목에서 키워드로 금액 추출"""
    target_ids = {normalize_account_id(x) for x in (account_ids or [])}
    normalized_keywords = [(kw or '').replace(' ', '') for kw in keywords]

    allowed_sj = None
    if sj_div:
        allowed_sj = {sj_div.upper()}
        if sj_div.upper() == 'IS':
            allowed_sj.add('CIS')  # 포괄손익계산서 사용 기업 대응

    for item in fin_list:
        item_sj = (item.get('sj_div') or '').upper()
        if allowed_sj and item_sj not in allowed_sj:
            continue
        account_nm = (item.get('account_nm') or '').replace(' ', '')
        account_id = normalize_account_id(item.get('account_id'))
        if target_ids and account_id in target_ids:
            val = pick_numeric_amount(item)
            if val is not None:
                return val
        for kw in normalized_keywords:
            if kw and kw in account_nm:
                val = pick_numeric_amount(item)
                if val is not None:
                    return val
    return None

def parse_metrics(fin_list):
    """핵심 재무 지표 파싱"""
    m = {}
    is_core = pick_is_core_from_rows(fin_list)
    m['매출액'] = is_core.get('rev')
    m['매출원가'] = is_core.get('cogs')
    m['판관비'] = is_core.get('sga')
    m['영업이익'] = is_core.get('op')
    m['당기순이익'] = is_core.get('ni')
    m['자본총계'] = find_amount(fin_list,
        ['자본총계'], 'BS',
        account_ids=['ifrsfullequity', 'ifrsequity'])

    capex = find_amount(fin_list,
        ['유형자산의 취득', '유형자산취득', '유형자산의취득'], 'CF')
    m['CAPEX'] = abs(capex) if capex is not None else None

    m['영업활동현금흐름'] = find_amount(fin_list,
        ['영업활동으로 인한 현금흐름', '영업활동현금흐름',
         '영업활동으로인한현금흐름', '영업활동으로 인한현금흐름'], 'CF',
        account_ids=['ifrsfullcashflowsfromusedinoperatingactivities', 'ifrscashflowsfromusedinoperatingactivities'])

    # 계산 지표
    if m.get('매출액') is not None and m.get('매출액') != 0 and m.get('영업이익') is not None:
        m['영업이익률'] = m['영업이익'] / m['매출액']
    else:
        m['영업이익률'] = None

    if m.get('자본총계') is not None and m.get('자본총계') != 0 and m.get('당기순이익') is not None:
        m['ROE'] = m['당기순이익'] / m['자본총계']
    else:
        m['ROE'] = None

    return m

def calc_quarter(annual, prev_cum):
    """단일 분기값 계산: annual(누적) - prev_cum(직전 누적)"""
    if annual is None:
        return None
    if prev_cum is None:
        return annual
    return annual - prev_cum


def calc_quarter_q4_safe(fy_cum, q3_cum):
    """Q4 단일값 계산(보수적): FY-Q3, 단 FY<Q3이면 FY를 그대로 사용"""
    if fy_cum is None:
        return None
    if q3_cum is None:
        return fy_cum
    return fy_cum - q3_cum if fy_cum >= q3_cum else fy_cum


def detect_fs_sj_by_quarter_logic(corp_code, year):
    """연도별로 분기/반기/3Q/연간 순서로 유효한 fs_div, sj_div 조합 탐색"""
    reprt_choices = [REPRT_CODES['Q1'], REPRT_CODES['H1'], REPRT_CODES['Q3'], REPRT_CODES['FY']]
    for reprt_code in reprt_choices:
        for fs_div in ['CFS', 'OFS']:
            for sj_div in ['IS', 'CIS']:
                rows = get_fin_data(corp_code, year, reprt_code, fs_div, sj_div)
                if not rows:
                    continue
                metrics = parse_metrics(rows)
                if any(metrics.get(k) is not None for k in ['매출액', '영업이익', '당기순이익']):
                    return fs_div, sj_div
    return 'CFS', 'IS'


def fetch_report_metrics(corp_code, year, reprt_code, fs_div, sj_div):
    """동일 fs/sj 기준으로 IS+CIS/BS/CF를 모아 누적 지표 파싱"""
    rows = []
    if sj_div in ('IS', 'CIS'):
        rows.extend(get_fin_data(corp_code, year, reprt_code, fs_div, sj_div))
    rows.extend(get_fin_data(corp_code, year, reprt_code, fs_div, 'BS'))
    rows.extend(get_fin_data(corp_code, year, reprt_code, fs_div, 'CF'))
    return parse_metrics(rows) if rows else {}


def fetch_equity_end(corp_code, year, reprt_code, fs_div):
    """BS 기준 시점 자본총계"""
    bs_rows = get_fin_data(corp_code, year, reprt_code, fs_div, 'BS')
    if not bs_rows:
        return None
    return parse_metrics(bs_rows).get('자본총계')

def get_quarterly_metrics(corp_code, year):
    """특정 연도의 분기별 재무지표 딕셔너리 반환
    반환: {1: metrics, 2: metrics, 3: metrics, 4: metrics}
    """
    quarters = {}

    fs_div, sj_div = detect_fs_sj_by_quarter_logic(corp_code, year)

    # 동일 fs/sj 기준으로 Q1, H1, 9M, FY 누적값 조회
    fin = {}
    for q, code in REPRT_CODES.items():
        fin[q] = fetch_report_metrics(corp_code, year, code, fs_div, sj_div)
        time.sleep(0.2)

    keys = ['매출액', '매출원가', '판관비', '영업이익', '당기순이익',
            '자본총계', 'CAPEX', '영업활동현금흐름']

    # Q1: 직접 사용
    q1 = {k: fin['Q1'].get(k) for k in keys}

    # Q2 = H1 - Q1
    q2 = {k: calc_quarter(fin['H1'].get(k), q1.get(k)) for k in keys}

    # Q3 = 9M - H1
    q3 = {k: calc_quarter(fin['Q3'].get(k), fin['H1'].get(k)) for k in keys}

    # Q4 = FY - 9M (보수적 처리)
    q4 = {k: calc_quarter(fin['FY'].get(k), fin['Q3'].get(k)) for k in keys}
    for k in ['매출액', '매출원가', '판관비', '영업이익', '당기순이익', 'CAPEX', '영업활동현금흐름']:
        q4[k] = calc_quarter_q4_safe(fin['FY'].get(k), fin['Q3'].get(k))

    # BS 항목(자본총계)은 각 시점 잔액 그대로 사용
    q1['자본총계'] = fin['Q1'].get('자본총계')
    q2['자본총계'] = fin['H1'].get('자본총계')
    q3['자본총계'] = fin['Q3'].get('자본총계')
    q4['자본총계'] = fin['FY'].get('자본총계')

    # 비율 계산 (ROE는 평균자본 기반 연율화)
    prev_year_eq = fetch_equity_end(corp_code, year - 1, REPRT_CODES['FY'], fs_div)
    eq_end = [q1.get('자본총계'), q2.get('자본총계'), q3.get('자본총계'), q4.get('자본총계')]
    q_list = [q1, q2, q3, q4]

    for idx, q_data in enumerate(q_list):
        rev = q_data.get('매출액')
        op = q_data.get('영업이익')
        if rev is not None and rev != 0 and op is not None:
            q_data['영업이익률'] = op / rev
        else:
            q_data['영업이익률'] = None

        ni = q_data.get('당기순이익')
        eq_curr = eq_end[idx]
        eq_prev = prev_year_eq if idx == 0 else eq_end[idx - 1]
        if ni is not None and eq_curr not in (None, 0) and eq_prev not in (None, 0):
            avg_eq = (eq_curr + eq_prev) / 2
            q_data['ROE'] = (ni * 4 / avg_eq) if avg_eq != 0 else None
        else:
            q_data['ROE'] = None

    quarters[1] = q1
    quarters[2] = q2
    quarters[3] = q3
    quarters[4] = q4
    return quarters

# =====================================================
# 네이버 뉴스 API
# =====================================================

def get_naver_news(company_name, display=100):
    """네이버 뉴스 API로 기업 관련 뉴스 수집"""
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET
    }
    params = {'query': company_name, 'display': display, 'sort': 'date'}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        return r.json().get('items', [])
    except Exception as e:
        print(f"  [오류] 네이버 뉴스 조회 실패: {e}")
        return []

def clean_html(text):
    return (text or '').replace('<b>', '').replace('</b>', '').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"')

# 주가/시세 관련 제외 키워드
STOCK_PRICE_KEYWORDS = [
    '주가', '목표주가', '상한가', '하한가', '종가', '시세',
    '주가 상승', '주가 하락', '주가 급등', '주가 급락',
]

DISCLOSURE_NEWS_KEYWORDS = [
    '공시', '전자공시', 'dart', '수시공시', '정정공시', '주요사항보고서'
]

MIN_NEWS_COUNT = 20
NEWS_LOOKBACK_DAYS = 365 * 5

HEADER_BG = {'red': 0.12, 'green': 0.29, 'blue': 0.52}
HIGHLIGHT_BG = {'red': 0.91, 'green': 0.96, 'blue': 1.0}


def parse_news_date(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ['%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d']:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return parsedate_to_datetime(value).replace(tzinfo=None)
    except Exception:
        return None


def is_within_last_five_years(dt):
    if not dt:
        return False
    return dt >= (datetime.now() - timedelta(days=NEWS_LOOKBACK_DAYS))


def is_disclosure_news(item):
    title = clean_html(item.get('title', '')).lower()
    desc = clean_html(item.get('description', '')).lower()
    link = (item.get('originallink') or item.get('link') or '').lower()
    return (
        any(kw in title for kw in DISCLOSURE_NEWS_KEYWORDS)
        or any(kw in desc for kw in DISCLOSURE_NEWS_KEYWORDS)
        or 'dart.fss.or.kr' in link
    )


def normalize_news_item(item, source=''):
    title = clean_html(item.get('title', '')).strip()
    desc = clean_html(item.get('description', '')).strip()
    link = item.get('originallink') or item.get('link') or ''
    dt = parse_news_date(item.get('pubDate'))
    return {
        'title': title,
        'description': desc,
        'link': link.strip(),
        'pubDate': dt.strftime('%Y-%m-%d') if dt else '',
        'published_dt': dt,
        'source': source or item.get('source', ''),
    }


def get_google_news_rss(query, max_items=100):
    url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=ko&gl=KR&ceid=KR:ko"
    items = []
    try:
        r = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        root = ET.fromstring(r.content)
        for node in root.findall('.//item')[:max_items]:
            items.append({
                'title': node.findtext('title', default=''),
                'description': re.sub(r'<[^>]+>', ' ', node.findtext('description', default='')),
                'link': node.findtext('link', default=''),
                'pubDate': node.findtext('pubDate', default=''),
                'source': 'google_news',
            })
    except Exception as e:
        print(f"  [오류] Google News RSS 조회 실패 ({query}): {e}")
    return items


def collect_news_items(company_name, min_count=MIN_NEWS_COUNT):
    merged = []
    merged.extend([normalize_news_item(x, 'naver') for x in get_naver_news(company_name, display=100)])
    merged.extend([normalize_news_item(x, 'google_news_ko') for x in get_google_news_rss(f"{company_name} 기업", max_items=100)])
    merged.extend([normalize_news_item(x, 'google_news_en') for x in get_google_news_rss(f"{company_name} company earnings", max_items=100)])

    dedup = {}
    for item in merged:
        key = (item.get('link') or '') or f"{item.get('title')}::{item.get('pubDate')}"
        if key in dedup:
            continue
        dedup[key] = item

    items = list(dedup.values())
    items = [x for x in items if is_within_last_five_years(x.get('published_dt'))]
    items = [x for x in items if not is_disclosure_news(x)]
    items = filter_stock_price_news(items)
    items.sort(key=lambda x: x.get('published_dt') or datetime.min, reverse=True)

    if len(items) < min_count:
        print(f"  [경고] 뉴스가 {len(items)}건으로 최소 기준({min_count})보다 적습니다.")
    return items


def to_hyperlink_formula(url, label='원문보기'):
    if not url:
        return ''
    safe_url = str(url).replace('"', '""')
    safe_label = str(label).replace('"', '""')
    return f'=HYPERLINK("{safe_url}", "{safe_label}")'


def extract_urls(value):
    urls = []
    if isinstance(value, list):
        candidates = [str(x).strip() for x in value if str(x).strip()]
    else:
        text = str(value or '').strip()
        if not text:
            return []
        candidates = [line.strip() for line in text.splitlines() if line.strip()]

    for line in candidates:
        found = re.findall(r'https?://[^\s,\)\]]+', line)
        if found:
            urls.extend(found)
        elif line.startswith('http://') or line.startswith('https://'):
            urls.append(line)

    dedup = []
    seen = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        dedup.append(u)
    return dedup


def to_multiline_hyperlink_formula(value, label_prefix='원문'):
    urls = extract_urls(value)
    if not urls:
        return ''
    parts = []
    for i, url in enumerate(urls, start=1):
        safe_url = str(url).replace('"', '""')
        safe_label = f"{label_prefix}{i}".replace('"', '""')
        parts.append(f'HYPERLINK("{safe_url}", "{safe_label}")')
    return "=" + "&CHAR(10)&".join(parts)


def fmt_eok(value):
    if value is None:
        return ''
    return f"{value/1e8:.1f}억원"


def fmt_pct(value):
    if value is None:
        return ''
    return f"{value*100:.2f}%"


def build_financial_context_text(annual_metrics_by_year, quarterly_by_year):
    lines = []
    if annual_metrics_by_year:
        lines.append("연간 지표:")
        for year, m in annual_metrics_by_year:
            lines.append(
                f"- {year}: 매출 {fmt_eok(m.get('매출액'))}, 매출원가 {fmt_eok(m.get('매출원가'))}, "
                f"영업이익 {fmt_eok(m.get('영업이익'))}, 당기순이익 {fmt_eok(m.get('당기순이익'))}, "
                f"OPM {fmt_pct(m.get('영업이익률'))}, OCF {fmt_eok(m.get('영업활동현금흐름'))}, "
                f"CAPEX {fmt_eok(m.get('CAPEX'))}, ROE {fmt_pct(m.get('ROE'))}"
            )

    if quarterly_by_year:
        lines.append("분기 지표:")
        for year in sorted(quarterly_by_year.keys()):
            qset = quarterly_by_year.get(year) or {}
            q_lines = []
            for q in [1, 2, 3, 4]:
                m = qset.get(q, {})
                if not m:
                    continue
                rev = m.get('매출액')
                op = m.get('영업이익')
                if rev is None and op is None:
                    continue
                q_lines.append(
                    f"Q{q} 매출 {fmt_eok(rev)}, 영업이익 {fmt_eok(op)}, OPM {fmt_pct(m.get('영업이익률'))}"
                )
            if q_lines:
                lines.append(f"- {year}: " + " | ".join(q_lines))
    return "\n".join(lines)


def format_korean_date(value):
    dt = parse_news_date(value)
    if not dt:
        return str(value or '')
    return f"{dt.year}년 {dt.month}월 {dt.day}일"


def normalize_sheet_title(name):
    return re.sub(r'[\s_-]+', '', str(name or '').strip()).lower()


def find_worksheet(spreadsheet, canonical_name, create_if_missing=False, rows=2000, cols=26):
    aliases = SHEET_ALIASES.get(canonical_name, [canonical_name])
    target_norms = {normalize_sheet_title(x) for x in aliases}

    for ws in spreadsheet.worksheets():
        if normalize_sheet_title(ws.title) in target_norms:
            return ws

    if create_if_missing:
        ws = spreadsheet.add_worksheet(title=canonical_name, rows=rows, cols=cols)
        if canonical_name == '뉴스수집':
            ws.update(
                values=[['날짜', '핵심요약', '원본링크(하이퍼링크)', '투자포인트', '비고']],
                range_name='A1:E1',
                value_input_option='USER_ENTERED'
            )
        elif canonical_name == '산업 이해 및 기업 상황':
            ws.update(
                values=[['항목', '내용', '근거 링크']],
                range_name='A2:C2',
                value_input_option='USER_ENTERED'
            )
        elif canonical_name == '경쟁현황':
            ws.update(
                values=[[
                    '기업명', '최근 3년 매출', '최근 3년 영업이익', '시장점유율(%)', '순위(국내/글로벌)',
                    '주요 제품(매출액/비중)', '강점', '약점/리스크', 'CAPEX/증설',
                    '최근 3년 기업활동 뉴스', '뉴스 원본 링크', '투자 고민 포인트', '비고'
                ]],
                range_name='A1:M1',
                value_input_option='USER_ENTERED'
            )
        print(f"  [안내] '{canonical_name}' 시트가 없어 새로 생성했습니다.")
        return ws

    raise gspread.WorksheetNotFound(canonical_name)


def to_multiline_numbered(values):
    if isinstance(values, list):
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        return "\n".join([f"{i + 1}. {v}" for i, v in enumerate(cleaned)])
    text = str(values or '').strip()
    return text if text else '[자료 없음]'


def apply_batch_format(ws, requests):
    if not requests:
        return
    ws.spreadsheet.batch_update({'requests': requests})


def apply_news_sheet_format(ws, row_count):
    requests = [
        {
            'repeatCell': {
                'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 5},
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': HEADER_BG,
                        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                        'horizontalAlignment': 'CENTER',
                        'wrapStrategy': 'WRAP',
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)',
            }
        }
    ]
    if row_count > 0:
        requests.append(
            {
                'repeatCell': {
                    'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': row_count + 1, 'startColumnIndex': 0, 'endColumnIndex': 5},
                    'cell': {
                        'userEnteredFormat': {
                            'textFormat': {'bold': True},
                            'wrapStrategy': 'WRAP',
                            'verticalAlignment': 'TOP',
                        }
                    },
                    'fields': 'userEnteredFormat(textFormat,wrapStrategy,verticalAlignment)',
                }
            }
        )
    apply_batch_format(ws, requests)


def apply_competition_sheet_format(ws, row_count):
    requests = [
        {
            'repeatCell': {
                'range': {'sheetId': ws.id, 'startRowIndex': 0, 'endRowIndex': 1, 'startColumnIndex': 0, 'endColumnIndex': 13},
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': HEADER_BG,
                        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                        'horizontalAlignment': 'CENTER',
                        'wrapStrategy': 'WRAP',
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)',
            }
        }
    ]
    if row_count > 0:
        requests.extend([
            {
                'repeatCell': {
                    'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': row_count + 1, 'startColumnIndex': 0, 'endColumnIndex': 8},
                    'cell': {'userEnteredFormat': {'horizontalAlignment': 'CENTER', 'textFormat': {'bold': True}, 'wrapStrategy': 'WRAP'}},
                    'fields': 'userEnteredFormat(horizontalAlignment,textFormat,wrapStrategy)',
                }
            },
            {
                'repeatCell': {
                    'range': {'sheetId': ws.id, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': 13},
                    'cell': {'userEnteredFormat': {'backgroundColor': HIGHLIGHT_BG}},
                    'fields': 'userEnteredFormat(backgroundColor)',
                }
            }
        ])
    apply_batch_format(ws, requests)

def filter_stock_price_news(news_items):
    """주가/시세 관련 뉴스 제목 필터링"""
    filtered = []
    for item in news_items:
        title = clean_html(item.get('title', ''))
        if any(kw in title for kw in STOCK_PRICE_KEYWORDS):
            continue
        filtered.append(item)
    return filtered

# =====================================================
# 증권사 리포트 수집 (네이버 증권)
# =====================================================

def fetch_naver_research_reports(company_name, count=3, max_chars_per_report=5000):
    """네이버 증권 기업 리포트 PDF 최신 {count}개 다운로드 및 텍스트 추출
    여러 페이지를 순회하며 종목명으로 클라이언트 필터링 (최대 10페이지 = 약 250개 리포트).
    반환: (report_items list, combined_text str)
    """
    try:
        import pdfplumber
    except ImportError:
        print("  [경고] pdfplumber 미설치. pip install pdfplumber 실행 후 재시도하세요.")
        return [], ''

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://finance.naver.com/',
    }

    # 네이버 company_list는 서버 측 종목 필터링 미지원 → 페이지별 순회 후 클라이언트 필터링
    norm_name = re.sub(r'[^가-힣A-Za-z0-9]', '', company_name).lower()
    row_pattern = re.compile(r'<tr>(.*?)</tr>', re.DOTALL)
    entries = []
    max_pages = 10  # 최대 250개 최근 리포트 검색

    for page in range(1, max_pages + 1):
        try:
            url = f'https://finance.naver.com/research/company_list.naver?page={page}'
            r = requests.get(url, headers=headers, timeout=15)
            html = r.content.decode('euc-kr', errors='ignore')
        except Exception as e:
            print(f"  [경고] 네이버 리포트 목록 조회 실패 (page={page}): {e}")
            break

        for row in row_pattern.findall(html):
            pdf_m = re.search(r'href="(https://stock\.pstatic\.net/stock-research/[^"]+\.pdf)"', row)
            if not pdf_m:
                continue
            # 행의 종목명 추출 후 대상 기업 필터링
            stock_m = re.search(r'class="stock_item">([^<]+)</a>', row)
            if not stock_m:
                continue
            row_name = re.sub(r'[^가-힣A-Za-z0-9]', '', stock_m.group(1)).lower()
            if norm_name not in row_name and row_name not in norm_name:
                continue
            title_m = re.search(r'href="company_read\.naver[^"]*">([^<]+)</a>', row)
            firm_m  = re.search(r'</td>\s*<td>([^<\s][^<]*)</td>\s*<td class="file">', row)
            date_m  = re.search(r'<td class="date"[^>]*>([^<]+)</td>', row)
            entries.append({
                'url':   pdf_m.group(1),
                'title': title_m.group(1).strip() if title_m else '리포트',
                'firm':  firm_m.group(1).strip()  if firm_m  else '',
                'date':  date_m.group(1).strip()  if date_m  else '',
            })
            if len(entries) >= count:
                break
        if len(entries) >= count:
            break

    if not entries:
        print(f"  [경고] 최근 리포트 없음 ({company_name}) - GPT 지식 기반으로 분석")
        return [], ''

    report_items   = []
    combined_texts = []

    for entry in entries:
        try:
            pdf_r = requests.get(entry['url'], headers=headers, timeout=30)
            if pdf_r.status_code != 200:
                continue
            with pdfplumber.open(io.BytesIO(pdf_r.content)) as pdf:
                text = '\n'.join(
                    p.extract_text() or '' for p in pdf.pages[:15]
                )[:max_chars_per_report]
            if not text.strip():
                print(f"  [경고] 이미지 PDF (텍스트 추출 불가): {entry['title'][:40]}")
                continue
            firm_str      = f" ({entry['firm']})" if entry['firm'] else ''
            display_title = f"[증권리포트{firm_str}] {entry['title']}"
            report_items.append({
                'title':        display_title,
                'description':  text[:300],
                'link':         entry['url'],
                'pubDate':      entry['date'],
                'published_dt': parse_news_date(entry['date']),
                'source':       '증권사리포트',
            })
            combined_texts.append(f"【{display_title} | {entry['date']}】\n{text}")
            print(f"  ✅ 리포트: {entry['title'][:40]} ({len(text)}자)")
        except Exception as e:
            print(f"  [경고] PDF 처리 실패 ({entry['title'][:30]}): {e}")

    return report_items, '\n\n'.join(combined_texts)


# =====================================================
# OpenAI 분석
# =====================================================

openai_client = OpenAI(api_key=OPENAI_API_KEY, timeout=90.0, max_retries=2)
OPENAI_MODEL_PRIMARY = 'gpt-5-mini'
OPENAI_MODEL_FALLBACK = 'gpt-5-nano'


def extract_message_text(message):
    if not message:
        return ''

    content = getattr(message, 'content', '')
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                text = item.get('text') or item.get('content') or ''
                if text:
                    parts.append(str(text))
                continue
            text = getattr(item, 'text', None)
            if text:
                parts.append(str(text))
        return "\n".join(parts).strip()

    return str(content or '').strip()


def parse_json_from_chat_response(response):
    try:
        message = response.choices[0].message
    except Exception:
        return {}

    text = extract_message_text(message)
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    # 모델이 JSON 앞뒤로 설명 문구를 붙이는 경우 대비
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end > start:
        try:
            parsed = json.loads(text[start:end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _build_token_kwargs(model, n):
    """모델별 토큰 파라미터 이름 결정"""
    # gpt-5-mini 계열은 max_completion_tokens, 나머지는 max_tokens
    if 'gpt-5' in model:
        return {'max_completion_tokens': n}
    return {'max_tokens': n}


def call_openai_json(prompt, max_completion_tokens, task_label='OpenAI'):
    models = [OPENAI_MODEL_PRIMARY, OPENAI_MODEL_FALLBACK]
    last_err = None

    for model in models:
        for use_json_format in (True, False):
            mode_label = "json_format" if use_json_format else "text_format"
            for attempt in range(1, 4):
                try:
                    kwargs = {
                        'model': model,
                        'messages': [{"role": "user", "content": prompt}],
                        **_build_token_kwargs(model, max_completion_tokens),
                    }
                    if use_json_format:
                        kwargs['response_format'] = {"type": "json_object"}
                    response = openai_client.chat.completions.create(**kwargs)
                    result = parse_json_from_chat_response(response)
                    if result:
                        if model != OPENAI_MODEL_PRIMARY:
                            print(f"  [안내] {task_label}: 폴백 모델({model}) 사용")
                        if not use_json_format:
                            print(f"  [안내] {task_label}: {mode_label} 모드로 복구")
                        return result
                    finish = getattr(getattr(response, 'choices', [None])[0], 'finish_reason', 'unknown') if response.choices else 'unknown'
                    raise ValueError(f"빈 JSON 응답 (finish_reason={finish})")
                except Exception as e:
                    last_err = e
                    msg = str(e)
                    err_type = type(e).__name__
                    print(f"  [재시도] {task_label} {model} {mode_label} {attempt}/3 실패: {err_type}: {msg}")
                    if attempt < 3:
                        time.sleep(1.0 * attempt)
                        continue
        # primary 모델 실패 시 fallback 모델로 진행
    raise last_err if last_err else RuntimeError(f"{task_label} 호출 실패")


def generate_industry_analysis(company_name, stock_code, news_items, financial_summary, report_text='', disclosure_titles='', financial_detail='', research_text=''):
    """산업 이해 및 기업 상황 시트 내용 생성"""
    news_lines = []
    for item in news_items[:50]:
        title = clean_html(item.get('title', ''))
        desc = clean_html(item.get('description', ''))[:180]
        link = item.get('link') or item.get('originallink') or ''
        dt = item.get('pubDate', '')
        news_lines.append(f"- ({dt}) [{title}] {desc} | 링크: {link if link else '[링크 없음]'}")
    news_text = "\n".join(news_lines)

    prompt = f"""당신은 한국 주식 투자 리서치 전문가입니다.
아래 제공된 자료를 최대한 활용하고, 부족한 부분은 학습 지식으로 보완하되 "(추정)" 표기하세요.
모든 금액은 반드시 KRW(원화)로만 표기하세요. 해외 통화가 원문에 있으면 KRW 환산값을 우선 표기하고 환산 기준을 함께 표기하세요.

■ 기업명: {company_name} (종목코드: {stock_code})

■ 증권사 리포트 (최신 3개 본문):
{research_text if research_text else '[리포트 없음]'}

■ DART 사업보고서 원문:
{report_text if report_text else '[사업보고서 원문 없음]'}

■ 최근 공시 목록:
{disclosure_titles if disclosure_titles else '[공시 없음]'}

■ DART 재무데이터 (실제 수치):
{financial_summary if financial_summary else '[재무 데이터 없음]'}

■ 재무 상세 요약:
{financial_detail if financial_detail else '[재무 상세 없음]'}

■ 최근 뉴스:
{news_text if news_text else '[뉴스 없음]'}

위 자료만 근거로 각 항목을 bullet point(•) 형식으로 작성하세요.

━━━ 항목별 작성 지침 ━━━

【산업 개요】
- 이 기업이 속한 산업의 대분류(반도체/2차전지/바이오 등) → 세부 분야 명시
- 글로벌·국내 시장 규모(조원/억달러 단위, 연도 포함) 및 성장률
- 산업 성장 핵심 동인: 수요 드라이버, 정책 방향, 기술 전환 흐름
- 사업보고서 또는 뉴스의 문구를 인용하여 근거 제시

【산업 구조 및 특징】
- 밸류체인 구조: 업스트림(소재/부품) → 미드스트림(제조) → 다운스트림(수요처)
- 진입장벽: 기술/인증/자본/규제 장벽 (수치나 구체적 예시 포함)
- 고객 집중도, 공급자 교섭력, 대체재 위협 등 구조적 특징

【산업 현재 업황】
- 현재 업황 사이클 판단: 호황/침체/회복 + 근거(재고/가동률/수주/스프레드 수치)
- 주요 수요처(고객 산업)의 투자 증가/축소 동향
- 원재료 가격 방향, 제품 판가 스프레드 변화
- 업계 전반 CAPEX 증설/감산 방향

【기업의 해자(경쟁우위)】
- 원가 우위: 규모·공정 효율 등 구체적 수치 근거
- 기술 우위: 보유 특허/기술명 + 단계(개발중/양산준비/양산중/상용화)
- 고객 잠금효과: 장기계약/전환비용/인증장벽 (계약 기간·금액 포함 가능 시)
- 규모의 경제 또는 네트워크 효과 (해당 시)

【주요 제품】
- 사업보고서에 기재된 제품/서비스 목록 그대로 나열
- 반드시 형식 지킬 것: "제품명 (매출액 OOO억원 / 전체 매출의 OO%)"
- 매출 비중 불명 시 "[비중 자료 없음]" 표기

【주요 제품 설명】
- 각 주요 제품의 기능·용도·적용 산업 설명
- 반드시 형식 지킬 것: "제품명: 기능/용도 (매출액 OOO억원 / OO%)"

【주요 원재료 및 원가 구조】
- 사업보고서 기재 주요 원재료명과 매입액/비중
- 반드시 형식 지킬 것: "원재료명 (매입액 OOO억원 / 매출원가의 OO%)"
- 원가 변동성 요인(가격 변동, 환율, 조달 리스크) 포함

【주요 고객 구조】
- 사업보고서/뉴스에서 확인된 고객사 + 매출액/비중
- 반드시 형식 지킬 것: "고객사명 (매출 OOO억원 / 비중 OO%)"
- 고객 집중도 리스크 여부 판단 (상위 1·3사 비중 포함 가능 시)

【기업 상황 (재무 중심)】
- 자본구조: 자기자본비율, 부채비율 수치
- 차입금: 단기/장기 차입금 규모 (억원 단위)
- 현금: 현금 및 현금성 자산 규모
- 유동성: 유동비율 또는 운전자본 상황
- 종합 안정성 판단: "안정 / 보통 / 주의" 중 하나 + 근거

【매출 구조 및 이익 변동 요인】
- 최근 연도별 매출 증감 추이와 주요 원인
- 영업이익·순이익 변동 요인 분해: 가격/물량/환율/원가 중 무엇이 주요인인지
- 일회성 요인 여부 명시 (자산매각, 충당금 등)

【최신 기술 트렌드】
- 산업 내 주요 기술 변화 트렌드 (AI 도입, 신소재, 공정 전환 등)
- 해당 기업의 기술 단계 명시: "개발중 / 양산준비 / 양산중 / 매출발생"
- 뉴스 또는 공시 근거 인용

【투자 관점 핵심 리스크】
- 업황 리스크: 수요 감소, 판가 하락, 재고 증가
- 기업 특유 리스크: 고객 집중, 기술 경쟁 열위, 부채 부담
- 규제·정책 리스크: 환경규제, 무역분쟁, 정부 정책 변화
- 뉴스 또는 공시 근거 포함

━━━ 출력 규칙 ━━━
- 각 항목 최소 3개 bullet(•) 이상 작성 (자료가 있는 경우)
- 모든 bullet은 구체적 수치(금액/비율/연도) 또는 출처 근거 포함
- "지속적인 성장", "다양한 제품" 같은 모호한 일반론 절대 금지
- 제공 자료에 없는 내용은 "[자료 없음]" 표기 (추측 금지)
- 반드시 순수 JSON 형식으로만 반환 (마크다운 코드블록 사용 금지):

{{
  "산업 개요": "• ...\n• ...\n• ...",
  "산업 구조 및 특징": "• ...\n• ...\n• ...",
  "산업 현재 업황": "• ...\n• ...\n• ...",
  "기업의 해자(경쟁우위)": "• ...\n• ...\n• ...",
  "주요 제품": "• ...\n• ...",
  "주요 제품 설명": "• ...\n• ...",
  "주요 원재료 및 원가 구조": "• ...\n• ...",
  "주요 고객 구조": "• ...\n• ...",
  "기업 상황 (재무 중심)": "• ...\n• ...\n• ...",
  "매출 구조 및 이익 변동 요인": "• ...\n• ...\n• ...",
  "최신 기술 트렌드": "• ...\n• ...",
  "투자 관점 핵심 리스크": "• ...\n• ...\n• ..."
}}"""

    try:
        return call_openai_json(prompt, max_completion_tokens=6000, task_label='산업분석')
    except Exception as e:
        print(f"  [오류] OpenAI 분석 생성 실패: {e}")
        return {}

def generate_competition_analysis(company_name, stock_code, news_items, financial_summary, report_text='', disclosure_titles='', competitor_financials='', research_text=''):
    """경쟁현황 시트 내용 생성"""
    news_text = "\n".join([
        f"- {clean_html(item.get('title',''))}: {clean_html(item.get('description',''))[:120]}"
        for item in news_items[:40]
    ])

    prompt = f"""당신은 한국 주식 투자 리서치 전문가입니다.
아래 제공된 자료를 우선 활용하고, 자료에 없는 내용은 당신의 학습 지식을 적극 활용하여 채우세요.
단, 학습 지식 사용 시 "(추정)" 또는 "(일반적)" 표기를 붙이세요.
모든 금액은 KRW(원화)로 표기하세요. 해외기업 데이터는 KRW 환산값과 환산기준(연도/환율)을 함께 적으세요.
분석 대상 기업({company_name})은 반드시 경쟁사목록의 첫 번째 항목으로 넣으세요.

■ 기업명: {company_name} (종목코드: {stock_code})

■ 증권사 리포트 (최신 3개 본문):
{research_text if research_text else '[리포트 없음]'}

■ DART 사업보고서 원문 (경쟁현황 참고):
{report_text if report_text else '[사업보고서 원문 없음]'}

■ 최근 공시 목록:
{disclosure_titles if disclosure_titles else '[공시 없음]'}

■ 분석 대상 기업 DART 재무데이터:
{financial_summary if financial_summary else '[재무 데이터 없음]'}

■ 경쟁사 DART 재무데이터 (직접 수집):
{competitor_financials if competitor_financials else '[경쟁사 재무 데이터 없음 - 학습 지식으로 보완하세요]'}

■ 최근 뉴스:
{news_text if news_text else '[뉴스 없음]'}

위 자료와 학습 지식을 종합하여 분석 대상 기업과 경쟁사를 포함하여 JSON으로 반환하세요.
- 경쟁사는 사업보고서/뉴스에서 언급된 기업 + 마스터 시트 기업 모두 포함하세요.
- DART 재무 수치는 그대로 사용하고, 없는 항목은 학습 지식으로 적극 채우되 "(추정)" 표기하세요.
- "[자료 없음]" 표기는 절대 사용하지 마세요. 정말 모르면 빈 문자열("")로 두세요.
- 뉴스 원본 링크: URL만 줄바꿈 문자열 (하이퍼링크 문구 금지)

{{
  "경쟁사목록": [
    {{
      "기업명": "{company_name}",
      "구분": "분석대상",
      "최근3년매출액": "2022: OOO억원\\n2023: OOO억원\\n2024: OOO억원",
      "최근3년영업이익": "2022: OOO억원\\n2023: OOO억원\\n2024: OOO억원",
      "시장점유율(%)": "OO%(추정)",
      "순위(국내/글로벌)": "국내 O위 / 글로벌 O위(추정)",
      "주요 제품(매출액/비중)": "제품명 (매출액 OOO억원 / OO%)",
      "강점": "내용",
      "약점/리스크": "내용",
      "CAPEX/증설": "내용",
      "최근3년 기업활동 뉴스": "1. YYYY-MM-DD 뉴스요약\\n2. ...",
      "뉴스 원본 링크": "https://...",
      "투자 고민 포인트": "• 체크포인트",
      "비고": "환산기준/추가메모"
    }},
    {{
      "기업명": "경쟁사명",
      "구분": "경쟁사",
      "최근3년매출액": "2022: OOO억원\\n2023: OOO억원\\n2024: OOO억원",
      "최근3년영업이익": "2022: OOO억원\\n2023: OOO억원\\n2024: OOO억원",
      "시장점유율(%)": "OO%(추정)",
      "순위(국내/글로벌)": "국내 O위 / 글로벌 O위(추정)",
      "주요 제품(매출액/비중)": "제품명 (비중 OO%)",
      "강점": "내용",
      "약점/리스크": "내용",
      "CAPEX/증설": "내용",
      "최근3년 기업활동 뉴스": "1. YYYY-MM-DD 뉴스요약\\n2. ...",
      "뉴스 원본 링크": "",
      "투자 고민 포인트": "• 체크포인트",
      "비고": "환산기준/추가메모"
    }}
  ]
}}"""

    try:
        return call_openai_json(prompt, max_completion_tokens=6000, task_label='경쟁분석')
    except Exception as e:
        print(f"  [오류] OpenAI 경쟁분석 생성 실패: {e}")
        return {}

def generate_news_investment_points(news_items, company_name):
    """뉴스별 투자 포인트 생성"""
    if not news_items:
        return []

    news_list = [
        f"{i+1}. [{clean_html(item.get('title',''))}] {clean_html(item.get('description',''))[:150]}"
        for i, item in enumerate(news_items[:20])
    ]
    news_text = "\n".join(news_list)

    prompt = f"""{company_name} 관련 뉴스 목록입니다.
각 뉴스에 대해 투자자 관점의 핵심 포인트를 한 줄로 작성해주세요.

뉴스:
{news_text}

반드시 JSON 형식으로 반환하세요:
{{"포인트": ["뉴스1 투자포인트", "뉴스2 투자포인트", ...]}}"""

    try:
        result = call_openai_json(prompt, max_completion_tokens=1500, task_label='투자포인트')
        points = result.get('포인트')
        if not isinstance(points, list):
            points = next((v for v in result.values() if isinstance(v, list)), [])
        return [str(x).strip() for x in points if str(x).strip()]
    except Exception as e:
        print(f"  [오류] 투자포인트 생성 실패: {e}")
        return []


def build_disclosure_links(disclosures):
    links = []
    for d in disclosures:
        rcept_no = d.get('rcept_no')
        if not rcept_no:
            continue
        links.append(f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}")
    return links

# =====================================================
# Google Sheets 쓰기
# =====================================================

def write_annual_data(ws, year, metrics):
    """연간 재무 데이터를 주식분석 값 입력 시트에 쓰기"""
    col = year - ANNUAL_YEAR_START + 1
    updates = []
    for metric, row in ANNUAL_DATA_ROWS.items():
        val = metrics.get(metric)
        if val is not None:
            updates.append({'range': gspread.utils.rowcol_to_a1(row, col), 'values': [[val]]})
    if updates:
        ws.batch_update(updates)

def write_quarterly_data(ws, year, quarter_metrics):
    """분기별 재무 데이터 쓰기"""
    # 해당 연도의 섹션 찾기
    section = None
    for s in QUARTERLY_SECTIONS:
        if year in s['years']:
            section = s
            break
    if not section:
        return

    year_idx = section['years'].index(year)
    base_col = year_idx * 4  # Q1=0, Q2=1, Q3=2, Q4=3 offset

    updates = []
    for q in range(1, 5):
        m = quarter_metrics.get(q, {})
        col = base_col + q  # 1-indexed
        for metric, row in section['data_rows'].items():
            val = m.get(metric)
            if val is not None:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row, col),
                    'values': [[val]]
                })

    if updates:
        ws.batch_update(updates)

def write_news_data(ws, news_items, investment_points):
    """뉴스수집 시트에 데이터 쓰기"""
    rows = []
    for i, item in enumerate(news_items):
        pub_date = format_korean_date(item.get('pubDate', ''))
        title = clean_html(item.get('title', ''))
        desc = clean_html(item.get('description', ''))[:300]
        link = item.get('originallink') or item.get('link') or item.get('url') or ''
        point = investment_points[i] if i < len(investment_points) else ''
        summary = f"{title}\n{desc}".strip()
        rows.append([pub_date, summary, to_hyperlink_formula(link, '원문링크') if link else '', point or '', ''])

    if rows:
        ws.batch_clear(['A2:E2000'])
        ws.update(values=rows, range_name=f'A2:E{1 + len(rows)}', value_input_option='USER_ENTERED')
        apply_news_sheet_format(ws, len(rows))

def write_industry_analysis(ws, analysis, source_links):
    """산업 이해 및 기업 상황 시트에 데이터 쓰기"""
    sections = [
        '산업 개요', '산업 구조 및 특징', '산업 현재 업황', '기업의 해자(경쟁우위)',
        '주요 제품', '주요 제품 설명', '주요 원재료 및 원가 구조',
        '주요 고객 구조', '기업 상황 (재무 중심)', '매출 구조 및 이익 변동 요인',
        '최신 기술 트렌드', '투자 관점 핵심 리스크'
    ]
    rows = []
    for i, section in enumerate(sections):
        content = strip_no_data(analysis.get(section) or '')
        link = source_links[i] if i < len(source_links) else ''
        rows.append([section, content, to_hyperlink_formula(link, '근거링크') if link else ''])
    ws.batch_clear(['A3:C100'])
    ws.update(values=rows, range_name='A3:C14', value_input_option='USER_ENTERED')
    apply_batch_format(ws, [{
        'repeatCell': {
            'range': {'sheetId': ws.id, 'startRowIndex': 2, 'endRowIndex': 14, 'startColumnIndex': 0, 'endColumnIndex': 3},
            'cell': {'userEnteredFormat': {'wrapStrategy': 'WRAP', 'verticalAlignment': 'TOP'}},
            'fields': 'userEnteredFormat(wrapStrategy,verticalAlignment)',
        }
    }])

NO_DATA_PATTERNS = {'[자료 없음]', '[원문 링크 없음]', '[링크 없음]', '[비고 없음]', '[내용 없음]'}

# =====================================================
# 경쟁사 재무 수집
# =====================================================

def fetch_competitor_annual_summary(corp_code, name, current_year):
    """경쟁사 최근 3년 IS 재무 요약 (매출/영업이익)"""
    year_lines = []
    for year in range(current_year - 2, current_year + 1):
        metrics = {}
        found = False
        for fs_div in ['CFS', 'OFS']:
            for sj_div in ['IS', 'CIS']:
                rows = get_fin_data(corp_code, year, REPRT_CODES['FY'], fs_div, sj_div)
                if rows:
                    m = parse_metrics(rows)
                    if m.get('매출액') is not None or m.get('영업이익') is not None:
                        metrics = m
                        found = True
                        break
                time.sleep(0.1)
            if found:
                break
        rev = metrics.get('매출액')
        op = metrics.get('영업이익')
        opm = metrics.get('영업이익률')
        if rev is not None or op is not None:
            line = f"  {year}: 매출 {rev/1e8:.0f}억원" if rev is not None else f"  {year}:"
            if op is not None:
                line += f", 영업이익 {op/1e8:.0f}억원"
            if opm is not None:
                line += f", OPM {opm*100:.1f}%"
            year_lines.append(line)
        time.sleep(0.2)
    if not year_lines:
        return ''
    return f"[{name}]\n" + '\n'.join(year_lines)


def fetch_competitor_financials(master_rows, exclude_name, current_year):
    """마스터 시트(A=기업명, B=종목코드) 기반 경쟁사 재무 요약 수집"""
    if not master_rows or len(master_rows) < 2:
        return ''
    summaries = []
    for row in master_rows[1:]:  # 1행 헤더 제외
        if len(row) < 2:
            continue
        name = str(row[0]).strip()
        stock_code = str(row[1]).strip()
        if not name or not stock_code or name == exclude_name:
            continue
        stock_code = stock_code.zfill(6)
        corp_code, _ = get_corp_info(stock_code)
        if not corp_code:
            print(f"  [경쟁사] {name}({stock_code}): corp_code 조회 실패, 건너뜀")
            continue
        print(f"  [경쟁사] {name} 재무 수집 중...")
        summary = fetch_competitor_annual_summary(corp_code, name, current_year)
        if summary:
            summaries.append(summary)
        time.sleep(0.1)
    return '\n\n'.join(summaries)

def strip_no_data(v):
    """GPT가 반환한 '[자료 없음]' 계열 문자열을 빈 문자열로 치환"""
    if not isinstance(v, str):
        return v
    stripped = v.strip()
    return '' if stripped in NO_DATA_PATTERNS else stripped


def write_competition_data(ws, competition, company_name):
    """경쟁현황 시트에 데이터 쓰기"""
    competitors = competition.get('경쟁사목록', [])
    if not competitors:
        return

    sorted_competitors = sorted(
        competitors,
        key=lambda x: 0 if str(x.get('기업명', '')).strip() == company_name else 1
    )

    rows = []
    for c in sorted_competitors:
        rows.append([
            strip_no_data(c.get('기업명') or ''),
            strip_no_data(c.get('최근3년매출액') or ''),
            strip_no_data(c.get('최근3년영업이익') or ''),
            strip_no_data(c.get('시장점유율(%)') or ''),
            strip_no_data(c.get('순위(국내/글로벌)') or ''),
            strip_no_data(c.get('주요 제품(매출액/비중)') or ''),
            strip_no_data(c.get('강점') or ''),
            strip_no_data(c.get('약점/리스크') or ''),
            strip_no_data(c.get('CAPEX/증설') or ''),
            strip_no_data(to_multiline_numbered(c.get('최근3년 기업활동 뉴스'))),
            to_multiline_hyperlink_formula(c.get('뉴스 원본 링크'), '원문'),
            strip_no_data(c.get('투자 고민 포인트') or ''),
            strip_no_data(c.get('비고') or ''),
        ])

    ws.batch_clear(['A2:M2000'])
    ws.update(values=rows, range_name=f'A2:M{1 + len(rows)}', value_input_option='USER_ENTERED')
    apply_competition_sheet_format(ws, len(rows))

# =====================================================
# 메인 실행
# =====================================================

def run_analysis(spreadsheet):
    """spreadsheet: gspread Spreadsheet 객체"""
    print("=" * 50)
    print(f"기업분석 자동화 시작: {spreadsheet.title}")
    print("=" * 50)

    # corp_map 읽기
    ws_corp_map = find_worksheet(spreadsheet, 'corp_map')
    corp_data = ws_corp_map.get_all_values()
    if len(corp_data) < 2:
        raise RuntimeError("corp_map 시트 2행(A2:C2)에 종목코드/고유번호/기업명을 입력해주세요.")
    row2 = (corp_data[1] + ['', '', ''])[:3]
    stock_code = str(row2[0]).strip().zfill(6) if row2[0] else None
    corp_code = str(row2[1]).strip() if row2[1] else None
    company_name = str(row2[2]).strip() if row2[2] else None
    if not company_name:
        company_name = str(spreadsheet.title).replace('-기업분석', '').strip()
    if not stock_code:
        raise RuntimeError("corp_map!A2 종목코드(6자리)가 비어 있습니다.")

    print(f"\n분석 대상: {company_name} (종목코드: {stock_code})")

    # corp_code 없으면 DART에서 조회
    if not corp_code and stock_code:
        print("  DART에서 corp_code 조회 중...")
        corp_code, fetched_name = get_corp_info(stock_code)
        if corp_code:
            ws_corp_map.update_cell(2, 2, corp_code)
            if not company_name and fetched_name:
                company_name = fetched_name
                ws_corp_map.update_cell(2, 3, company_name)
            print(f"  ✅ corp_code: {corp_code}")
        else:
            print("  ❌ corp_code 조회 실패. 직접 입력 필요")
            return False

    # ===== 연간 재무 데이터 =====
    print("\n[1/7] 연간 재무 데이터 수집 중...")
    ws_stock = find_worksheet(spreadsheet, '주식분석 값 입력')
    current_year = datetime.now().year
    financial_summary_parts = []
    annual_metrics_by_year = []
    quarterly_by_year = {}

    # fs_div/sj_div 최신 연도 기준으로 1번만 탐색 후 전 연도 재사용
    print("  재무제표 형식 탐색 중...", end=' ')
    fs_div, sj_div = detect_fs_sj_by_quarter_logic(corp_code, current_year)
    alt_fs_div = 'OFS' if fs_div == 'CFS' else 'CFS'
    print(f"{fs_div}/{sj_div}")

    for year in range(ANNUAL_YEAR_START, current_year + 1):
        print(f"  {year}년 조회 중...", end=' ')
        metrics = fetch_report_metrics(corp_code, year, REPRT_CODES['FY'], fs_div, sj_div)
        has_any = any(metrics.get(k) is not None for k in ['매출액', '영업이익', '당기순이익', '매출원가', '판관비'])
        # 데이터 없으면 반대 fs_div로 폴백 (과거 연도 보고 형식 변경 대응)
        if not has_any:
            metrics = fetch_report_metrics(corp_code, year, REPRT_CODES['FY'], alt_fs_div, sj_div)
            has_any = any(metrics.get(k) is not None for k in ['매출액', '영업이익', '당기순이익', '매출원가', '판관비'])
        if has_any:
            write_annual_data(ws_stock, year, metrics)
            annual_metrics_by_year.append((year, metrics))
            rev = metrics.get('매출액')
            op = metrics.get('영업이익')
            if rev is not None and op is not None:
                financial_summary_parts.append(
                    f"{year}년: 매출 {rev/1e8:.0f}억원, 영업이익 {op/1e8:.0f}억원"
                )
            if rev is None:
                print("⚠️ 매출 추출 실패")
            else:
                print(f"✅ 매출: {rev/1e8:.0f}억")
        else:
            print("데이터 없음")
        time.sleep(0.1)

    financial_summary = "\n".join(financial_summary_parts)

    # ===== 분기 재무 데이터 =====
    print("\n[2/7] 분기별 재무 데이터 수집 중...")
    for year in range(2020, current_year + 1):
        print(f"  {year}년 분기 데이터 조회 중...")
        quarterly = get_quarterly_metrics(corp_code, year)
        quarterly_by_year[year] = quarterly
        write_quarterly_data(ws_stock, year, quarterly)
        print(f"  ✅ {year}년 분기 완료")
        time.sleep(0.3)

    # ===== 뉴스 수집 =====
    print(f"\n[3/8] 뉴스 수집 중... ({company_name})")
    news_items = collect_news_items(company_name, min_count=MIN_NEWS_COUNT)
    print(f"  ✅ {len(news_items)}개 뉴스 수집 (국내+해외, 5년 이내)")

    # ===== 증권사 리포트 수집 =====
    print(f"\n[4/8] 증권사 리포트 수집 중... ({company_name})")
    research_items, research_text = fetch_naver_research_reports(company_name, count=3)
    if research_items:
        print(f"  ✅ 리포트 {len(research_items)}개 수집 완료")
        news_items = research_items + news_items  # 뉴스수집 시트 상단에 리포트 추가
    else:
        print("  ⚠️ 증권사 리포트 수집 실패, 건너뜀")

    # 투자 포인트 생성
    print("  투자 포인트 생성 중...")
    investment_points = generate_news_investment_points(news_items, company_name)

    ws_news = find_worksheet(spreadsheet, '뉴스수집', create_if_missing=True)
    write_news_data(ws_news, news_items, investment_points)
    print("  ✅ 뉴스수집 시트 입력 완료")

    # ===== DART 공시 및 사업보고서 원문 수집 =====
    print(f"\n[5/8] DART 공시 및 사업보고서 원문 수집 중...")
    disclosures = get_dart_disclosures(corp_code, count=20)
    disclosure_titles = "\n".join([
        f"- {d.get('rcept_dt','')} [{d.get('report_nm','')}]"
        for d in disclosures
    ])
    disclosure_links = build_disclosure_links(disclosures)
    print(f"  ✅ 공시 {len(disclosures)}건 수집")

    print("  사업보고서 원문 다운로드 중... (시간이 걸릴 수 있음)")
    report_text = get_annual_report_text(corp_code, max_chars=20000)
    print(f"  ✅ 사업보고서 원문 {len(report_text)}자 추출" if report_text else "  ⚠️ 사업보고서 원문 없음")

    # ===== 산업/기업 분석 =====
    print("\n[6/8] 산업 및 기업 분석 생성 중...")
    analysis = generate_industry_analysis(
        company_name, stock_code, news_items, financial_summary,
        report_text=report_text, disclosure_titles=disclosure_titles,
        financial_detail=build_financial_context_text(annual_metrics_by_year, quarterly_by_year),
        research_text=research_text,
    )
    ws_industry = find_worksheet(spreadsheet, '산업 이해 및 기업 상황', create_if_missing=True)
    source_links = [item.get('link') or item.get('originallink') for item in news_items[:12]]
    source_links = [x for x in source_links if x] + disclosure_links[:12]
    write_industry_analysis(ws_industry, analysis, source_links)
    print("  ✅ 산업 이해 및 기업 상황 시트 입력 완료")

    # ===== 경쟁사 재무 수집 (마스터 시트) =====
    print("\n[7/8] 경쟁현황 분석 생성 중...")
    competitor_financials = ''
    try:
        ws_master = find_worksheet(spreadsheet, '마스터')
        master_rows = ws_master.get_all_values()
        print(f"  마스터 시트 {len(master_rows) - 1}개 기업 재무 수집 중...")
        competitor_financials = fetch_competitor_financials(master_rows, company_name, current_year)
        print(f"  ✅ 경쟁사 재무 수집 완료")
    except Exception as e:
        print(f"  ⚠️ 마스터 시트 없음 또는 오류 ({e}), 경쟁사 재무 없이 진행")

    competition = generate_competition_analysis(
        company_name, stock_code, news_items, financial_summary,
        report_text=report_text[:10000], disclosure_titles=disclosure_titles,
        competitor_financials=competitor_financials,
        research_text=research_text,
    )
    ws_competition = find_worksheet(spreadsheet, '경쟁현황', create_if_missing=True)
    write_competition_data(ws_competition, competition, company_name)
    print("  ✅ 경쟁현황 시트 입력 완료")

    # ===== 현재가 구하기 시트 =====
    print("\n[8/8] 현재가 구하기 시트 데이터 수집 중...")
    try:
        ws_price = find_worksheet(spreadsheet, '현재가 구하기')
        year_bs, reprt_bs, fs_div_bs, bs_data = detect_latest_bs(corp_code)
        if bs_data:
            issued, treasury, float_s = fetch_latest_shares(corp_code, year_bs, reprt_bs)
            write_price_sheet(ws_price, bs_data, issued, treasury, float_s)
            print(f"  ✅ 현재가 구하기 시트 입력 완료 (BS기준: {year_bs}년 {reprt_bs})")
        else:
            print("  ⚠️ BS 데이터를 찾지 못했습니다.")
    except Exception as e:
        print(f"  ⚠️ 현재가 구하기 시트 오류 (시트 없거나 데이터 없음): {e}")

    print("\n" + "=" * 50)
    print(f"✅ {company_name} 분석 완료!")
    print("=" * 50)
    return True


# =====================================================
# 자동 탐색 및 스케줄러
# =====================================================

def is_already_analyzed(spreadsheet):
    """뉴스수집 시트 A2가 비어있으면 미분석으로 판단"""
    try:
        ws = find_worksheet(spreadsheet, '뉴스수집')
        val = ws.acell('A2').value
        return bool(val and val.strip())
    except Exception:
        return False

def find_analysis_spreadsheets(gc):
    """'-기업분석'으로 끝나는 구글 스프레드시트 목록 반환"""
    files = gc.list_spreadsheet_files()
    return [f for f in files if f['name'].endswith('-기업분석')]


def dedupe_analysis_files(files):
    """동일 파일(id) 중복 및 동일 이름(name) 중복 제거"""
    unique = []
    seen_ids = set()
    seen_names = set()
    skipped = []

    for f in files:
        file_id = str(f.get('id') or '').strip()
        name = str(f.get('name') or '').strip()
        if not file_id:
            skipped.append((name or '[이름없음]', 'id 없음'))
            continue
        if file_id in seen_ids:
            skipped.append((name or '[이름없음]', 'id 중복'))
            continue
        seen_ids.add(file_id)

        # 동일 기업명 파일이 여러 개면 첫 번째 1개만 처리
        if name in seen_names:
            skipped.append((name or '[이름없음]', '이름 중복'))
            continue
        seen_names.add(name)

        unique.append(f)

    return unique, skipped

def run_all_pending(gc):
    """미분석 스프레드시트 모두 처리"""
    target_id = (os.getenv('TARGET_SPREADSHEET_ID') or '').strip()
    force_reanalyze = (os.getenv('FORCE_REANALYZE') or '').strip().lower() in {'1', 'true', 'yes', 'y'}

    summary = {
        'found': 0,
        'processed': 0,
        'skipped': 0,
        'failed': 0,
    }

    if target_id:
        print(f"\n[직접실행] TARGET_SPREADSHEET_ID 지정됨: {target_id}")
        try:
            spreadsheet = gc.open_by_key(target_id)
            summary['found'] = 1
            if is_already_analyzed(spreadsheet) and not force_reanalyze:
                print(f"  [{spreadsheet.title}] 이미 분석됨. 건너뜀. (FORCE_REANALYZE 미지정)")
                summary['skipped'] += 1
                return summary
            ok = run_analysis(spreadsheet)
            if ok:
                summary['processed'] += 1
            else:
                summary['failed'] += 1
            return summary
        except Exception as e:
            print(f"  [직접실행 오류] {e}")
            summary['failed'] += 1
            return summary

    print(f"\n[스캔] '-기업분석' 시트 검색 중...")
    all_files = find_analysis_spreadsheets(gc)
    if not all_files:
        print("  '-기업분석'으로 끝나는 시트가 없습니다.")
        return summary

    files, skipped_dupes = dedupe_analysis_files(all_files)
    summary['found'] = len(files)
    print(f"  총 {len(all_files)}개 발견 / 중복제거 후 {len(files)}개 처리: {[f['name'] for f in files]}")
    if skipped_dupes:
        skipped_desc = ", ".join([f"{name}({reason})" for name, reason in skipped_dupes])
        print(f"  [중복건너뜀] {skipped_desc}")

    for f in files:
        try:
            spreadsheet = gc.open_by_key(f['id'])
            if is_already_analyzed(spreadsheet) and not force_reanalyze:
                print(f"  [{f['name']}] 이미 분석됨. 건너뜀. (FORCE_REANALYZE 미지정)")
                summary['skipped'] += 1
                continue
            print(f"\n  [{f['name']}] 분석 시작!")
            ok = run_analysis(spreadsheet)
            if ok:
                summary['processed'] += 1
            else:
                summary['failed'] += 1
        except Exception as e:
            print(f"  [{f['name']}] 오류: {e}")
            summary['failed'] += 1

    print(
        f"\n[요약] found={summary['found']}, processed={summary['processed']}, "
        f"skipped={summary['skipped']}, failed={summary['failed']}"
    )
    return summary


if __name__ == "__main__":
    try:
        validate_runtime_config()
        print("구글 계정 인증 중...")
        gc = get_google_client()
        result = run_all_pending(gc)
        if result.get('failed', 0) > 0:
            raise RuntimeError(f"분석 실패 건수: {result['failed']}")
        if result.get('processed', 0) == 0:
            print("[알림] 처리된 시트가 없습니다. TARGET_SPREADSHEET_ID 또는 FORCE_REANALYZE를 확인하세요.")
    except Exception as e:
        print(f"[실행중단] 설정 오류: {e}")
        raise
