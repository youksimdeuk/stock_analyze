# 챕터 01 — 데이터 호출 최소화 규칙

## 원칙
API, DB, Google Sheets 등 외부 데이터는 **한 번만 호출**하고 변수에 저장 후 재사용한다.

## 규칙 상세

### 1. 일괄 호출 원칙
- 동일 데이터를 두 번 이상 호출하는 코드를 짜지 않는다.
- 함수 안에서 호출한 데이터는 반환값으로 넘기거나, 상위 함수에서 호출 후 인자로 전달한다.

### 2. 루프 내 호출 최소화
- 반복 호출이 불가피한 경우(연도별 루프, 분기별 루프 등)에만 허용한다.
- 루프 안에서도 **공통 파라미터(fs_div, sj_div 등)는 루프 밖에서 1회만 탐색**해 재사용한다.

### 3. 캐싱 패턴
```python
# 좋은 예: 한 번 호출 후 재사용
data = fetch_data(corp_code)
result_a = process_a(data)
result_b = process_b(data)

# 나쁜 예: 같은 데이터를 두 번 호출
result_a = process_a(fetch_data(corp_code))
result_b = process_b(fetch_data(corp_code))  # 중복 호출!
```

### 4. Google Sheets 호출
- 시트 데이터는 `spreadsheet.values().get()` 또는 `batchGet()`으로 1회 일괄 조회한다.
- 행 단위 반복 조회 금지.

## 체크리스트
- [ ] 같은 API 엔드포인트를 두 번 이상 호출하는 코드가 없는가?
- [ ] 루프 밖으로 뺄 수 있는 호출이 루프 안에 있지 않은가?
- [ ] 공통 파라미터를 매번 새로 계산하지 않는가?
