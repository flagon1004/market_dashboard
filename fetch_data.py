"""
마켓 인사이트 대시보드 — 최종 데이터 수집 스크립트
====================================================

■ 노션 페이지 구조 (인라인 DB 4개, 순서 중요)

  [DB 1] Daily Market Log      ← 하루 1행
    날짜        : title         "2026-02-20"  ← Title 컬럼, 날짜 필터 기준
    시장 강도   : select        "강세" / "약세" / "보합"
    AI 3줄 요약 : rich_text     줄바꿈(\n)으로 3줄 구분

  [DB 2] 주도 종목 분석         ← 하루 1행
    종목명      : title         오늘 날짜 입력 "2026-02-20"  ← 날짜 필터 기준
    주도 섹터   : rich_text     "반도체 | +3.42%\nAI | +2.81%\n..."
    특이 종목   : rich_text     "SK하이닉스 | HBM 뉴스 | 185,400 | +5.23%\n..."

  [DB 3] 핵심 뉴스 아카이브     ← 날짜별 1행
    뉴스 제목   : title         "2026-02-20 핵심 뉴스 요약"  ← 날짜 contains 필터
    AI 뉴스 요약: rich_text     여러 뉴스를 줄바꿈(\n)으로 구분

  [DB 4] 추천종목               ← 하루 3~5행, 종목 1개 = 1행
    날짜        : title         "2026-02-20"  ← Title 컬럼, 날짜 필터 기준
    종목명      : rich_text     "삼성전자"
    추천등급    : select        "A+" / "A" / "B+" / "B" / "C"
    투자기간    : select        "단기" / "중기" / "장기"
    추천사유    : rich_text     자유 텍스트

■ 금융 지수 (Yahoo Finance — 외부 라이브러리 불필요)
  KOSPI / KOSDAQ / S&P500 / NASDAQ
  원달러 / 미국채10년 / WTI / 금

■ 환경변수
  NOTION_TOKEN   : secret_xxxxxxxxxxxx
  NOTION_PAGE_ID : 노션 페이지 ID (하이픈 포함/불포함 모두 OK)

■ 실행
  python fetch_data.py

■ 출력
  data.json  →  dashboard.html 이 읽는 파일
"""

import os
import json
import datetime
import urllib.request
import urllib.error

# ══════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════
NOTION_TOKEN   = os.environ.get("NOTION_TOKEN", "")
NOTION_PAGE_ID = os.environ.get("NOTION_PAGE_ID", "").replace("-", "")

DATE_FORMAT = "%Y-%m-%d"   # 노션에 입력하는 날짜 형식과 반드시 일치

# ══════════════════════════════════════════════════════
# Notion API 공통 헬퍼
# ══════════════════════════════════════════════════════
def _h():
    """매 호출마다 최신 토큰 반영"""
    return {
        "Authorization" : f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type"  : "application/json",
    }

def n_get(path):
    req = urllib.request.Request(f"https://api.notion.com/v1{path}", headers=_h())
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

def n_post(path, body):
    req = urllib.request.Request(
        f"https://api.notion.com/v1{path}",
        data=json.dumps(body).encode(),
        headers=_h(), method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())

# ══════════════════════════════════════════════════════
# 속성값 추출
# ══════════════════════════════════════════════════════
def get_prop(row, col, ptype):
    """row['properties'][col] → 문자열 또는 숫자"""
    p = row.get("properties", {}).get(col)
    if not p:
        return "" if ptype != "number" else None
    val = p.get(ptype)
    if ptype in ("title", "rich_text"):
        return "".join(t.get("plain_text", "") for t in (val or []))
    if ptype == "select":
        return val.get("name", "") if isinstance(val, dict) else ""
    if ptype == "number":
        return val  # float or None
    return ""

def safe_float(s):
    try:
        return float(str(s).replace(",", "").replace("+", "").replace("%", "").strip())
    except Exception:
        return 0.0

# ══════════════════════════════════════════════════════
# 페이지 내 child_database 블록 목록 수집
# ══════════════════════════════════════════════════════
def get_child_dbs(page_id):
    dbs, cursor = [], None
    while True:
        url = f"/blocks/{page_id}/children?page_size=100"
        if cursor:
            url += f"&start_cursor={cursor}"
        data = n_get(url)
        for b in data.get("results", []):
            if b.get("type") == "child_database":
                dbs.append({
                    "id"   : b["id"].replace("-", ""),
                    "title": b.get("child_database", {}).get("title", ""),
                })
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return dbs

# ══════════════════════════════════════════════════════
# DB 쿼리 공통
# ══════════════════════════════════════════════════════
def query_by_date(db_id, date_col, today_str, limit=100):
    """title 컬럼이 오늘 날짜인 행 전체 반환 (페이지네이션 처리)"""
    body = {
        "filter"   : {"property": date_col, "title": {"equals": today_str}},
        "page_size": limit,
    }
    rows, cursor = [], None
    while True:
        if cursor:
            body["start_cursor"] = cursor
        data = n_post(f"/databases/{db_id}/query", body)
        rows.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return rows

def query_latest(db_id, limit=10):
    """날짜 필터 없이 최신 생성순 (뉴스 폴백용)"""
    body = {
        "page_size": limit,
        "sorts"    : [{"timestamp": "created_time", "direction": "descending"}],
    }
    return n_post(f"/databases/{db_id}/query", body).get("results", [])

# ══════════════════════════════════════════════════════
# DB 1 — 시장 요약
# ══════════════════════════════════════════════════════
def parse_db1(db_id, today_str):
    # DB1: Daily Market Log
    # 컬럼: 날짜(title) / 시장 강도(select) / AI 3줄 요약(rich_text)
    rows = query_by_date(db_id, "날짜", today_str)
    if not rows:
        print(f"  [DB1] ⚠ 오늘({today_str}) 행 없음 → 노션에 오늘 날짜 행을 추가하세요")
        return {"summary": [], "market_strength": ""}

    row      = rows[0]
    strength = get_prop(row, "시장 강도", "select")
    raw      = get_prop(row, "AI 3줄 요약", "rich_text")

    # 줄바꿈 분리 후 번호/기호 정리, 최대 3줄
    lines = []
    for line in raw.replace("\r", "").split("\n"):
        line = line.strip().lstrip("0123456789.-•·) ")
        if line:
            lines.append(line)

    summary = lines[:3]
    print(f"  [DB1] 강도={strength or '—'}, 요약={len(summary)}줄")
    return {"summary": summary, "market_strength": strength}

# ══════════════════════════════════════════════════════
# DB 2 — 주도 섹터 & 특이 종목
# ══════════════════════════════════════════════════════
def parse_pipe(text):
    """줄바꿈 + 파이프(|) 구조 → [[col1, col2, ...], ...]"""
    result = []
    for line in text.replace("\r", "").split("\n"):
        if "|" in line:
            result.append([p.strip() for p in line.split("|")])
    return result

def parse_db2(db_id, today_str):
    # DB2: 주도 종목 분석
    # 컬럼: 종목명(title) / 주도 섹터(rich_text) / 특이 종목(rich_text)
    # ※ Title 컬럼(종목명)에 오늘 날짜를 입력해서 행을 구분
    rows = query_by_date(db_id, "종목명", today_str)
    if not rows:
        print(f"  [DB2] ⚠ 오늘({today_str}) 행 없음 → 종목명 컬럼에 오늘 날짜를 입력하세요")
        return {"sectors": [], "stocks": []}

    row     = rows[0]
    sec_raw = get_prop(row, "주도 섹터", "rich_text")
    stk_raw = get_prop(row, "특이 종목", "rich_text")

    # ── 섹터: "섹터명 | +3.42%"
    sectors = []
    for parts in parse_pipe(sec_raw):
        if len(parts) >= 2 and parts[0]:
            chg = parts[1]
            if chg and not chg.startswith(("+", "-")):
                chg = f"+{chg}"
            sectors.append({
                "name"  : parts[0],
                "change": chg,
                "value" : safe_float(chg),
            })

    # ── 특이 종목: "종목명 | 사유 | 가격 | 등락률"
    stocks = []
    for parts in parse_pipe(stk_raw):
        if len(parts) >= 4 and parts[0]:
            chg = parts[3]
            stocks.append({
                "name"  : parts[0],
                "reason": parts[1],
                "price" : parts[2],
                "change": chg,
                "pos"   : not chg.strip().startswith("-"),
            })

    print(f"  [DB2] 섹터={len(sectors)}개, 종목={len(stocks)}개")
    return {"sectors": sectors, "stocks": stocks}

# ══════════════════════════════════════════════════════
# DB 3 — 뉴스
# ══════════════════════════════════════════════════════
def created_to_kst(iso_str):
    """ISO8601 UTC → KST HH:MM 문자열"""
    try:
        dt = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return (dt + datetime.timedelta(hours=9)).strftime("%H:%M")
    except Exception:
        return ""

def parse_db3(db_id, today_str):
    # DB3: 핵심 뉴스 아카이브
    # 실제 구조: 날짜별 1행
    #   뉴스 제목(title)    : "2026-02-20 핵심 뉴스 요약"  (날짜 필터 기준)
    #   AI 뉴스 요약(text)  : 여러 뉴스를 줄바꿈으로 구분

    # 오늘 날짜가 포함된 title 행 검색
    # title이 정확히 오늘 날짜와 일치하지 않으므로 contains 방식 사용
    body = {
        "filter": {
            "property": "뉴스 제목",
            "title": {"contains": today_str}
        },
        "page_size": 10,
    }
    rows = []
    try:
        data = n_post(f"/databases/{db_id}/query", body)
        rows = data.get("results", [])
        print(f"  [DB3] 날짜 필터({today_str}) → {len(rows)}행 조회")
    except Exception as e:
        print(f"  [DB3] ❌ 뉴스 조회 실패: {e}")
        return {"news": []}

    if not rows:
        print(f"  [DB3] ⚠ 오늘({today_str}) 행 없음")
        return {"news": []}

    row     = rows[0]
    raw     = get_prop(row, "AI 뉴스 요약", "rich_text").strip()

    if not raw:
        print(f"  [DB3] ⚠ AI 뉴스 요약 내용 없음")
        return {"news": []}

    # 줄바꿈으로 개별 뉴스 분리, 번호/기호 제거
    news = []
    for line in raw.replace("\r", "").split("\n"):
        line = line.strip().lstrip("0123456789.-•·) ")
        if line:
            news.append({
                "time"    : "",       # 시간 정보 없음
                "headline": line,
                "summary" : "",
                "tag"     : "",
            })

    print(f"  [DB3] 뉴스={len(news)}건")
    return {"news": news}

# ══════════════════════════════════════════════════════
# DB 4 — 추천 종목
#
# 노션 DB 컬럼 구조 (확정):
#   날짜     : title   ← 반드시 Title(제목) 타입  예) "2026-02-19"  ← 행 구분 키
#   종목명   : text    예) "삼성전자"
#   추천등급 : select  A+ / A / B+ / B / C
#   투자기간 : select  단기 / 중기 / 장기
#   추천이유 : text    자유 텍스트
#
# ※ 날짜(Title)가 오늘 날짜와 일치하는 행 전체를 수집 (3~5행)
# ══════════════════════════════════════════════════════
def parse_db4(db_id, today_str):
    # 날짜가 Title 타입 → query_by_date 로 필터
    rows = query_by_date(db_id, "날짜", today_str)

    if not rows:
        print(f"  [DB4] ⚠ 오늘({today_str}) 추천 종목 행 없음")
        return {"recommendations": []}

    recs = []
    for i, row in enumerate(rows[:5]):   # 최대 5종목
        name      = get_prop(row, "종목명", "rich_text")
        grade     = get_prop(row, "추천등급", "select")
        timeframe = get_prop(row, "투자기간", "select")
        reason    = get_prop(row, "추천사유", "rich_text")

        if not name:
            continue

        recs.append({
            "name"     : name,
            "grade"    : grade or "B",
            "timeframe": timeframe or "—",
            "reason"   : reason,
            "featured" : i == 0,   # 첫 번째 행 = TOP PICK 강조
        })

    print(f"  [DB4] 추천종목={len(recs)}개")
    return {"recommendations": recs}


# ══════════════════════════════════════════════════════
# 노션 전체 수집 (DB 1~4)
# ══════════════════════════════════════════════════════
def fetch_notion():
    today_str = datetime.date.today().strftime(DATE_FORMAT)
    print(f"  조회 날짜 : {today_str}")

    dbs = get_child_dbs(NOTION_PAGE_ID)
    print(f"  발견된 DB : {len(dbs)}개")
    for i, db in enumerate(dbs, 1):
        print(f"    [{i}] '{db['title']}'  ({db['id'][:8]}...)")

    if len(dbs) < 4:
        missing = 4 - len(dbs)
        print(f"  ⚠ DB {missing}개가 부족합니다. (현재 {len(dbs)}개 감지)")
        print("    → 추천 종목 DB가 아직 없으면 샘플 데이터로 표시됩니다.")

    result = {}

    # DB 1 — 시장 요약
    if len(dbs) >= 1:
        result.update(parse_db1(dbs[0]["id"], today_str))
    else:
        result.update({"summary": [], "market_strength": ""})

    # DB 2 — 섹터 & 특이 종목
    if len(dbs) >= 2:
        result.update(parse_db2(dbs[1]["id"], today_str))
    else:
        result.update({"sectors": [], "stocks": []})

    # DB 3 — 뉴스
    if len(dbs) >= 3:
        result.update(parse_db3(dbs[2]["id"], today_str))
    else:
        result.update({"news": []})

    # DB 4 — 추천 종목
    if len(dbs) >= 4:
        result.update(parse_db4(dbs[3]["id"], today_str))
    else:
        print("  [DB4] 아직 미생성 → 샘플 추천 종목 사용")
        result.update({"recommendations": _sample_rec()})

    return result

# ══════════════════════════════════════════════════════
# 금융 지수 — Yahoo Finance
# ══════════════════════════════════════════════════════
def fetch_yahoo(symbol, dec=2):
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?interval=1d&range=2d"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            meta = json.loads(r.read().decode())["chart"]["result"][0]["meta"]
        price  = meta.get("regularMarketPrice", 0)
        prev   = meta.get("chartPreviousClose") or meta.get("previousClose") or price
        change = price - prev
        pct    = (change / prev * 100) if prev else 0
        return {
            "price" : round(price, dec),
            "change": round(change, dec),
            "pct"   : round(pct, 2),
            "pos"   : change >= 0,
        }
    except Exception as e:
        print(f"  [Yahoo] {symbol} 실패: {e}")
        return {"price": 0, "change": 0, "pct": 0, "pos": True}

def fetch_indices():
    TARGETS = [
        ("kospi",  "^KS11",  0),   # 코스피
        ("kosdaq", "^KQ11",  0),   # 코스닥
        ("sp500",  "^GSPC",  0),   # S&P 500
        ("nasdaq", "^IXIC",  0),   # 나스닥
        ("usdkrw", "KRW=X",  1),   # 원달러 환율
        ("us10y",  "^TNX",   3),   # 미국채 10년물
        ("wti",    "CL=F",   2),   # WTI 유가
        ("gold",   "GC=F",   0),   # 금 시세
    ]
    indices = {}
    for key, sym, dec in TARGETS:
        v = fetch_yahoo(sym, dec)
        indices[key] = v
        arrow = "▲" if v["pos"] else "▼"
        print(f"    {key:10s}: {str(v['price']):>12s}  {arrow} {abs(v['pct']):.2f}%")
    return indices

# ══════════════════════════════════════════════════════
# 샘플 데이터 (노션 미연결 / 오늘 데이터 없을 때)
# ══════════════════════════════════════════════════════
def _sample_rec():
    return [
        {
            "name": "삼성전자",
            "grade": "A+", "timeframe": "중기",
            "reason": "HBM 전환 가속 및 파운드리 수주 회복 사이클 진입. 저점 분할매수 구간.",
            "featured": True,
        },
        {
            "name": "SK하이닉스",
            "grade": "A", "timeframe": "단기",
            "reason": "HBM3E 독점 공급 지위 유지. 고수익성 구조 지속.",
            "featured": False,
        },
        {
            "name": "NAVER",
            "grade": "B+", "timeframe": "장기",
            "reason": "HyperCLOVA X B2B 전환 가속. AI 수익화 초입 구간.",
            "featured": False,
        },
    ]

SAMPLE_NOTION = {
    "summary": [
        "미 연준 금리 동결 시사에 코스피·나스닥 동반 상승. 외국인 순매수 전환이 지수 방어에 기여했다.",
        "AI 반도체 섹터 급등. 엔비디아 실적 서프라이즈로 SK하이닉스·삼성전자 HBM 관련주 강세.",
        "원/달러 환율 1,310원대 안정. 수출주 밸류에이션 개선 기대감이 코스피 안착을 이끌었다.",
    ],
    "market_strength": "강세",
    "sectors": [
        {"name":"반도체",       "change":"+3.42%","value": 3.42},
        {"name":"AI·소프트웨어","change":"+2.81%","value": 2.81},
        {"name":"2차전지",      "change":"+2.20%","value": 2.20},
        {"name":"바이오",       "change":"+1.58%","value": 1.58},
        {"name":"금융",         "change":"-0.84%","value":-0.84},
        {"name":"건설·부동산",  "change":"-1.43%","value":-1.43},
    ],
    "stocks": [
        {"name":"SK하이닉스",  "reason":"HBM3E 양산 확대","price":"185,400","change":"+5.23%","pos":True},
        {"name":"한미반도체",  "reason":"TC본더 수주 체결","price":"94,800", "change":"+8.71%","pos":True},
        {"name":"삼성SDI",    "reason":"유럽 수요 둔화",  "price":"312,000","change":"-3.12%","pos":False},
        {"name":"카카오페이", "reason":"해외결제 서비스 확대","price":"24,650","change":"+4.46%","pos":True},
    ],
    "news": [
        {"time":"09:02","headline":"연준 1월 의사록 — 금리인하 서두를 필요 없다 기조 재확인","summary":"","tag":"통화정책"},
        {"time":"10:15","headline":"엔비디아 블랙웰 GPU 공급난 — 국내 HBM 기업 반사이익 기대","summary":"","tag":"반도체"},
        {"time":"11:33","headline":"정부 밸류업 2차 프로그램 발표 — 자사주 소각 의무화 논의","summary":"","tag":"정책"},
        {"time":"13:47","headline":"BYD 국내 출시 재연기 — 국산 완성차·배터리주 단기 수혜","summary":"","tag":"자동차"},
    ],
    "recommendations": _sample_rec(),
}

# ══════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════
def main():
    DIV = "=" * 58
    kst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

    print(DIV)
    print("  마켓 인사이트 — 데이터 수집 시작")
    print(f"  실행 시각: {kst.strftime('%Y-%m-%d %H:%M:%S')} KST")
    print(DIV)

    output = {
        "updated_at" : datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "updated_kst": kst.strftime("%Y-%m-%d %H:%M KST"),
    }

    # ── 1. 노션 데이터
    print("\n[1] 노션 데이터 수집")
    print("-" * 42)
    if NOTION_TOKEN and NOTION_PAGE_ID:
        try:
            output.update(fetch_notion())
        except Exception as e:
            print(f"  ❌ 노션 수집 실패: {e}")
            print("  → 샘플 데이터로 대체합니다.")
            output.update(SAMPLE_NOTION)
    else:
        print("  ⚠  NOTION_TOKEN / NOTION_PAGE_ID 미설정")
        print("  → 샘플 데이터로 대체합니다.")
        output.update(SAMPLE_NOTION)

    # ── 2. 금융 지수
    print("\n[2] 금융 지수 수집 (Yahoo Finance)")
    print("-" * 42)
    output["indices"] = fetch_indices()

    # ── 저장
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ data.json 저장 완료")
    print(f"   {out_path}")
    print(DIV)

if __name__ == "__main__":
    main()
