"""
AMPIS 과거 뉴스 수집기 v3
────────────────────────────────────────────────────────────
【사용 방법】

▶ 방법 A — 네이버 공식 검색 API (가장 쉽고 안정적, 무료)
  1. https://developers.naver.com 접속 → 로그인 → [Application] → [등록]
  2. 애플리케이션 이름 자유 입력, 사용 API에서 '검색' 선택 → 등록
  3. 발급된 Client ID / Client Secret을 아래에 붙여넣기
  4. python historical_crawler_v3.py 실행
  ※ 하루 25,000건 무료, 한 번에 100건씩 가져올 수 있음

▶ 방법 B — BigKinds API (5년치 과거 데이터에 가장 완벽)
  1. https://www.bigkinds.or.kr 접속 → 회원가입 → 마이페이지 → API키 발급
  2. 아래 BIGKINDS_KEY 에 붙여넣기

▶ 둘 다 없으면 → 세션 기반 크롤링 자동 시도
────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import hashlib, os, re, sqlite3, time, random, json, html
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ══════════════════════════════════════════════════════
#   ★ 여기에 API 키를 입력하세요 ★
# ══════════════════════════════════════════════════════
NAVER_CLIENT_ID     = os.environ.get("NAVER_CLIENT_ID",     "")   # 네이버 API Client ID
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")   # 네이버 API Client Secret
BIGKINDS_KEY        = os.environ.get("BIGKINDS_API_KEY",    "")   # BigKinds API 키
# ══════════════════════════════════════════════════════

DB_PATH = "/tmp/ampis.db" if os.environ.get("RENDER") else "ampis.db"

FINANCE_KEYWORDS = [
    "프로젝트파이낸싱", "PF대출", "브릿지론", "메자닌",
    "물류센터 개발", "데이터센터 PF", "자산운용 출자",
    "공제회 출자", "연기금 출자", "NPL 부실채권",
    "리츠 REITs", "인프라 펀드", "신재생에너지 PF",
    "사모펀드 PEF 인수", "딜클로징",
]

MEDIA_DOMAINS = {
    "hankyung.com":   "한국경제",
    "mk.co.kr":       "매일경제",
    "edaily.co.kr":   "이데일리",
    "mt.co.kr":       "머니투데이",
    "einfomax.co.kr": "연합인포맥스",
    "thebell.co.kr":  "더벨",
    "fnnews.com":     "파이낸셜뉴스",
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

# ─── DB ──────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL, title TEXT NOT NULL,
            url TEXT UNIQUE, summary TEXT, published TEXT,
            title_hash TEXT UNIQUE, is_parsed INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id INTEGER REFERENCES raw_news(id),
            name TEXT, type TEXT, re_sub TEXT, size TEXT,
            fi TEXT, si TEXT, ci TEXT, amount TEXT,
            structure TEXT, collateral TEXT, exit_plan TEXT,
            features TEXT, source_url TEXT, source_name TEXT,
            published TEXT, status TEXT DEFAULT '검토',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.commit(); conn.close()

def title_hash(t: str) -> str:
    return hashlib.md5(re.sub(r"[^\w가-힣]","",t).lower().encode()).hexdigest()

def save(source, title, url, summary, published) -> int:
    title = html.unescape(re.sub(r"<[^>]+>", "", title)).strip()
    if not title or len(title) < 5:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO raw_news (source,title,url,summary,published,title_hash) VALUES (?,?,?,?,?,?)",
            (source, title, (url or "")[:500], (summary or "")[:400],
             published, title_hash(title))
        )
        changed = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
        return changed
    finally:
        conn.close()

def db_count():
    conn = sqlite3.connect(DB_PATH)
    n = conn.execute("SELECT COUNT(*) FROM raw_news").fetchone()[0]
    conn.close()
    return n


# ══════════════════════════════════════════════════════════════
# 방법 A: 네이버 공식 검색 API
# ══════════════════════════════════════════════════════════════
def crawl_naver_api(keyword: str, years_back: int = 5) -> int:
    """
    네이버 공식 뉴스 검색 API 사용.
    한 번에 100건, start 최대 1000 → 키워드당 최대 1,000건.
    날짜 필터는 sort=date로 최신순 정렬 후 기간 밖이면 중단.
    """
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return -1  # 키 없음

    cutoff = datetime.now() - timedelta(days=365 * years_back)
    total_saved = 0

    for start in range(1, 1001, 100):   # 1, 101, 201 ... 901
        url = (
            "https://openapi.naver.com/v1/search/news.json"
            f"?query={quote(keyword)}&display=100&start={start}&sort=date"
        )
        try:
            r = requests.get(url, headers={
                **HEADERS,
                "X-Naver-Client-Id":     NAVER_CLIENT_ID,
                "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
            }, timeout=10)
            if r.status_code != 200:
                print(f"  [API 오류] {r.status_code}: {r.text[:100]}")
                break
            items = r.json().get("items", [])
        except Exception as e:
            print(f"  [네이버 API 오류] {e}")
            break

        if not items:
            break

        batch_saved = 0
        stop_flag = False
        for item in items:
            # 발행일 파싱
            try:
                pub_dt = parsedate_to_datetime(item.get("pubDate",""))
                pub_str = pub_dt.strftime("%Y-%m-%d %H:%M")
                if pub_dt.replace(tzinfo=None) < cutoff:
                    stop_flag = True
                    continue   # 기간 밖 → 스킵
            except Exception:
                pub_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            orig_url = item.get("originallink") or item.get("link","")
            source = next((nm for dom, nm in MEDIA_DOMAINS.items()
                           if dom in orig_url), "기타")
            n = save(source,
                     item.get("title",""),
                     orig_url,
                     item.get("description",""),
                     pub_str)
            batch_saved += n

        total_saved += batch_saved
        print(f"  [네이버 API] '{keyword}' start={start}: {len(items)}건 / {batch_saved}건 저장")

        if stop_flag:
            print(f"  → 수집 기간({years_back}년) 초과 → 다음 키워드로")
            break
        time.sleep(random.uniform(0.3, 0.6))

    return total_saved


def run_naver_api(years_back: int = 5) -> int:
    print("\n" + "─"*50)
    print("▶ 방법 A: 네이버 공식 API 수집 시작")
    print("─"*50)
    grand = 0
    for kw in FINANCE_KEYWORDS:
        n = crawl_naver_api(kw, years_back)
        if n == -1:
            print("  [!] 네이버 API 키 없음 → 다음 방법 시도")
            return -1
        grand += n
        print(f"  키워드 '{kw}': 누적 {n}건 저장")
        time.sleep(random.uniform(0.5, 1.0))
    print(f"\n✅ 네이버 API 완료: {grand}건 저장 / DB 총 {db_count()}건")
    return grand


# ══════════════════════════════════════════════════════════════
# 방법 B: BigKinds API (과거 5년 전수 데이터)
# ══════════════════════════════════════════════════════════════
BIGKINDS_PROVIDERS = {
    "한국경제": "02100311",
    "매일경제": "02100271",
    "이데일리": "02100601",
    "머니투데이": "02100681",
}

def crawl_bigkinds(keyword: str, start_date: str, end_date: str) -> int:
    if not BIGKINDS_KEY:
        return -1
    total_saved = 0
    page_size = 100

    for provider_name, provider_code in BIGKINDS_PROVIDERS.items():
        offset = 0
        while True:
            payload = {
                "access_key": BIGKINDS_KEY,
                "argument": {
                    "query": keyword,
                    "published_at": {"from": start_date, "until": end_date},
                    "provider": [provider_code],
                    "return_from": offset,
                    "return_size": page_size,
                    "sort": {"date": "desc"},
                    "fields": ["title","content","published_at",
                               "provider","provider_link_page"],
                }
            }
            try:
                r = requests.post("https://tools.kinds.or.kr/search/news",
                                  json=payload, timeout=15)
                data = r.json()
            except Exception as e:
                print(f"  [BigKinds 오류] {e}")
                break

            docs = data.get("return_object", {}).get("documents", [])
            if not docs:
                break

            batch = 0
            for d in docs:
                pub = (d.get("published_at") or "")[:10]
                n = save(provider_name,
                         d.get("title",""),
                         d.get("provider_link_page",""),
                         (d.get("content") or "")[:300],
                         pub + " 00:00")
                batch += n
            total_saved += batch

            total_hits = data.get("return_object",{}).get("total_hits", 0)
            print(f"  [BigKinds] {provider_name} / '{keyword}' offset={offset}: {len(docs)}건 / {batch}건 저장 (전체 {total_hits}건)")

            offset += page_size
            if offset >= total_hits or len(docs) < page_size:
                break
            time.sleep(0.5)

    return total_saved


def run_bigkinds(years_back: int = 5) -> int:
    print("\n" + "─"*50)
    print("▶ 방법 B: BigKinds API 수집 시작")
    print("─"*50)
    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=365*years_back)).strftime("%Y-%m-%d")
    grand = 0
    for kw in FINANCE_KEYWORDS:
        n = crawl_bigkinds(kw, start, end)
        if n == -1:
            print("  [!] BigKinds API 키 없음 → 다음 방법 시도")
            return -1
        grand += n
        time.sleep(random.uniform(0.3, 0.7))
    print(f"\n✅ BigKinds 완료: {grand}건 저장 / DB 총 {db_count()}건")
    return grand


# ══════════════════════════════════════════════════════════════
# 방법 C: 세션 기반 크롤링 (키 없을 때 최후 수단)
# ══════════════════════════════════════════════════════════════
def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    try:
        sess.get("https://www.naver.com", timeout=10, verify=False)
        time.sleep(random.uniform(1.5, 2.5))
        sess.get("https://news.naver.com", timeout=10, verify=False)
        time.sleep(random.uniform(1.0, 2.0))
    except Exception:
        pass
    return sess


def parse_items(soup: BeautifulSoup) -> list[dict]:
    """현재 Naver 뉴스 HTML에서 기사 추출 (다중 셀렉터 시도)"""
    items = []

    # 셀렉터 우선순위 (최신 → 구버전)
    for sel in [
        "ul.list_news li.bx",
        "ul.list_news > li",
        "li[id^='sp_nws']",
        "div.news_area",
        "div[class*='news_wrap']",
    ]:
        found = soup.select(sel)
        if found:
            for node in found:
                link = (node.select_one("a.news_tit") or
                        node.select_one("a[class*='tit']") or
                        node.select_one("a[class*='news']"))
                if not link:
                    continue
                title = link.get_text(strip=True)
                url   = link.get("href","")
                dsc   = node.select_one("div.news_dsc, a.news_dsc, div[class*='dsc']")
                summ  = dsc.get_text(strip=True) if dsc else ""
                pub   = datetime.now().strftime("%Y-%m-%d %H:%M")
                for span in node.select("span.info, span[class*='date'], span[class*='time']"):
                    m = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", span.get_text())
                    if m:
                        pub = f"{m.group(1)}-{m.group(2)}-{m.group(3)} 00:00"
                        break
                items.append({"title": title, "url": url, "summary": summ, "published": pub})
            break  # 첫 번째 매칭 셀렉터 사용

    # Fallback: 경제 언론 URL 포함 a 태그 직접 수집
    if not items:
        for a in soup.find_all("a", href=True):
            href  = a.get("href","")
            title = a.get_text(strip=True)
            if any(d in href for d in MEDIA_DOMAINS) and len(title) > 10:
                items.append({"title": title, "url": href,
                              "summary": "", "published": datetime.now().strftime("%Y-%m-%d %H:%M")})
    return items


def crawl_session(keyword: str, years_back: int = 5,
                  max_pages: int = 30) -> int:
    """세션 쿠키 방식 Naver 크롤링"""
    sess = make_session()
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=365 * years_back)
    ds, de   = start_dt.strftime("%Y.%m.%d"), end_dt.strftime("%Y.%m.%d")
    nso_f, nso_t = start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")
    total_saved = 0

    for pg in range(max_pages):
        start = pg * 10 + 1
        url = (
            "https://search.naver.com/search.naver"
            f"?where=news&query={quote(keyword)}"
            "&sm=tab_opt&sort=1&pd=3"
            f"&ds={ds}&de={de}"
            f"&nso=so:dd,p:from{nso_f}to{nso_t},a:all"
            f"&start={start}"
        )
        try:
            time.sleep(random.uniform(2.5, 4.0))
            r = sess.get(url, timeout=15, verify=False)

            if r.status_code != 200 or len(r.text) < 1000:
                print(f"  [차단] status={r.status_code}, len={len(r.text)} → 중단")
                print("  → 스마트폰 핫스팟으로 연결 후 재시도하거나 API 키를 사용하세요.")
                break

            soup  = BeautifulSoup(r.text, "html.parser")
            arts  = parse_items(soup)

            if not arts:
                print(f"  [Page {pg+1}] 파싱 결과 없음 → 종료")
                break

            pg_saved = 0
            for art in arts:
                src = next((nm for dom, nm in MEDIA_DOMAINS.items()
                            if dom in art["url"]), None)
                if src:
                    pg_saved += save(src, art["title"], art["url"],
                                     art["summary"], art["published"])
            total_saved += pg_saved
            print(f"  [Page {pg+1}] {len(arts)}건 파싱 / {pg_saved}건 저장")
        except Exception as e:
            print(f"  [오류] {e}")
            break
    return total_saved


def run_session(years_back: int = 5) -> int:
    print("\n" + "─"*50)
    print("▶ 방법 C: 세션 크롤링 시작 (API 키 없을 때)")
    print("─"*50)
    grand = 0
    for kw in FINANCE_KEYWORDS:
        print(f"\n🔍 키워드: '{kw}'")
        n = crawl_session(kw, years_back)
        grand += n
        print(f"  → {n}건 저장")
        time.sleep(random.uniform(5, 10))
    print(f"\n✅ 세션 수집 완료: {grand}건 저장 / DB 총 {db_count()}건")
    return grand


# ══════════════════════════════════════════════════════════════
# 자동 실행: 키 유무에 따라 최적 방법 선택
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 55)
    print("📰 AMPIS 과거 뉴스 수집기 v3")
    print("=" * 55)
    init_db()

    # 초기 DB 상태
    print(f"현재 DB: {db_count()}건")

    # 방법 A: 네이버 공식 API
    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
        print("\n✅ 네이버 API 키 감지 → 방법 A 실행")
        run_naver_api(years_back=5)

    # 방법 B: BigKinds
    elif BIGKINDS_KEY:
        print("\n✅ BigKinds API 키 감지 → 방법 B 실행")
        run_bigkinds(years_back=5)

    # 방법 C: 세션 크롤링
    else:
        print("\n⚠️  API 키 없음 → 방법 C (세션 크롤링) 시도")
        print("   API 키를 발급받으면 훨씬 많은 데이터를 안정적으로 수집할 수 있습니다.")
        print("   → 네이버 API: https://developers.naver.com")
        print("   → BigKinds:   https://www.bigkinds.or.kr\n")
        run_session(years_back=5)

    print(f"\n🎉 최종 DB 저장 건수: {db_count()}건")
