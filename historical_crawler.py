"""
AMPIS 과거 5개년 뉴스 수집 전용 모듈 (시장 전체 트렌드 수집용 고도화 버전)
- 특정 분석 회사에 국한하지 않고, PF/대체투자 핵심 키워드로 검색하여 시장의 모든 관련 뉴스를 아카이빙합니다.
- 네이버 뉴스 검색 엔진을 통해 5대 경제 매체의 5개년 역사적 데이터를 대량 수집합니다.
"""

import hashlib
import os
import re
import sqlite3
import time
import random
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 시장 전체의 딜(Deal) 뉴스를 수집하기 위한 금융/투자 핵심 키워드 조합
HISTORICAL_KEYWORDS = [
    "프로젝트파이낸싱", "PF 대출", "브릿지론", "메자닌 투자", 
    "물류센터 개발", "데이터센터 PF", "자산운용사 출자", "공제회 출자", 
    "연기금 출자", "부실채권 NPL", "리츠 REITs", "인프라 펀드", 
    "신재생에너지 PF", "지분 인수합병", "사모펀드 PEF"
]

MEDIA_DOMAINS = {
    "hankyung.com": "한국경제",
    "mk.co.kr": "매일경제",
    "edaily.co.kr": "이데일리",
    "mt.co.kr": "머니투데이",
    "einfomax.co.kr": "연합인포맥스"
}

DB_PATH = "/tmp/ampis.db" if os.environ.get("RENDER") else "ampis.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
}

def normalize_title(title: str) -> str:
    return re.sub(r"[^\w가-힣]", "", title).lower()

def get_title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode()).hexdigest()

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            source TEXT NOT NULL, 
            title TEXT NOT NULL, 
            url TEXT UNIQUE, 
            summary TEXT, 
            published TEXT, 
            title_hash TEXT UNIQUE, 
            is_parsed INTEGER DEFAULT 0, 
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()
    conn.close()

def crawl_historical_news(keyword: str, years_back: int = 5):
    init_db()
    
    end_date_dt = datetime.now()
    start_date_dt = end_date_dt - timedelta(days=365 * years_back)
    
    start_date_str = start_date_dt.strftime("%Y.%m.%d")
    end_date_str = end_date_dt.strftime("%Y.%m.%d")
    nso_start = start_date_dt.strftime("%Y%m%d")
    nso_end = end_date_dt.strftime("%Y%m%d")
    
    print(f"\n🔍 [키워드: {keyword}] {start_date_str} ~ {end_date_str} 기간 수집 시작")
    
    start_index = 1
    total_saved = 0
    
    while start_index < 4000:
        search_url = (
            f"https://search.naver.com/search.naver?where=news&query={keyword}"
            f"&sm=tab_pge&sort=1&photo=0&field=0&pd=3"
            f"&ds={start_date_str}&de={end_date_str}"
            f"&nso=so:dd,p:from{nso_start}to{nso_end},a:all"
            f"&start={start_index}"
        )
        
        try:
            time.sleep(random.uniform(0.8, 1.8))
            
            resp = requests.get(search_url, headers=HEADERS, timeout=10, verify=False)
            if resp.status_code != 200:
                print(f"  [!] HTTP {resp.status_code} 에러 발생. 중단합니다.")
                break
                
            soup = BeautifulSoup(resp.text, "html.parser")
            news_items = soup.select("ul.list_news > li.bx")
            
            if not news_items:
                print("  [✓] 검색 결과의 끝에 도달하여 루프를 종료합니다.")
                break
                
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            
            page_saved = 0
            page_duplicates = 0
            
            for item in news_items:
                link_tag = item.select_one("a.news_tit")
                if not link_tag: continue
                
                title = link_tag.get_text().strip()
                url = link_tag.get("href", "").strip()
                
                dsc_tag = item.select_one("div.news_dsc")
                summary = dsc_tag.get_text().strip() if dsc_tag else ""
                
                info_tags = item.select("span.info")
                pub_date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                for info in info_tags:
                    date_match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", info.get_text())
                    if date_match:
                        pub_date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)} 00:00"
                        break
                
                matched_source = None
                for domain, source_name in MEDIA_DOMAINS.items():
                    if domain in url:
                        matched_source = source_name
                        break
                        
                if not matched_source:
                    continue
                
                title_hash = get_title_hash(title)
                
                try:
                    cur.execute(
                        "INSERT INTO raw_news (source, title, url, summary, published, title_hash) VALUES (?, ?, ?, ?, ?, ?)",
                        (matched_source, title, url, summary[:500], pub_date_str, title_hash)
                    )
                    page_saved += 1
                except sqlite3.IntegrityError:
                    page_duplicates += 1
                    
            conn.commit()
            conn.close()
            total_saved += page_saved
            
            if page_saved == 0 and page_duplicates >= len(news_items):
                print("  [✓] 이미 수집된 기존 뉴스 데이터 영역과 완벽히 겹칩니다. 다음 키워드로 이동합니다.")
                break
                
            print(f"  → Page {start_index // 10 + 1} 처리 완료 (신규 저장: {page_saved}건 / 중복 제거: {page_duplicates}건)")
            start_index += 10
            
        except Exception as e:
            print(f"  [런타임 에러 발생] {e}")
            break
            
    print(f"🎉 [키워드: {keyword}] 수집 완료: 총 {total_saved}개의 금융 프로젝트 뉴스를 원천 확보했습니다.")

if __name__ == "__main__":
    print("=" * 60)
    print("📰 AMPIS 5개년 금융/대체투자 전수 조사 아카이빙 엔진")
    print("=" * 60)
    for keyword in HISTORICAL_KEYWORDS:
        crawl_historical_news(keyword, years_back=5)
    print("\n[완료] 대체투자 시장 전체의 역사적 원천 기사 저장이 완료되었습니다.")