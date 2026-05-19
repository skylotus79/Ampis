from __future__ import annotations
import re
import hashlib
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOURCES = [
    {
        "name": "한국경제",
        "rss_urls": [
            "https://rss.hankyung.com/feed/finance.xml",
            "https://rss.hankyung.com/feed/realestate.xml",
            "https://rss.hankyung.com/feed/economy.xml",
        ],
        "html_fallbacks": [
            {"url": "https://www.hankyung.com/economy", "patterns": ["/article/"], "base_url": "https://www.hankyung.com"},
            {"url": "https://www.hankyung.com/realestate", "patterns": ["/article/"], "base_url": "https://www.hankyung.com"}
        ],
    },
    {
        "name": "매일경제",
        "rss_urls": [
            "https://www.mk.co.kr/rss/30100041/",
            "https://www.mk.co.kr/rss/30200030/",
        ],
        "html_fallbacks": [
            {"url": "https://www.mk.co.kr/news/economy", "patterns": ["/news/"], "base_url": "https://www.mk.co.kr"},
        ],
    },
    {
        "name": "이데일리",
        "rss_urls": [],
        "html_fallbacks": [
            {"url": "https://www.edaily.co.kr/economy", "patterns": ["newsId="], "base_url": "https://www.edaily.co.kr"},
        ],
    },
    {
        "name": "머니투데이",
        "rss_urls": [],
        "html_fallbacks": [
            {"url": "https://www.mt.co.kr/stock", "patterns": ["/stock/", "/article/", "no="], "base_url": "https://www.mt.co.kr"},
            {"url": "https://www.mt.co.kr/economy", "patterns": ["/economy/", "/article/", "no="], "base_url": "https://www.mt.co.kr"}
        ],
    },
    {
        "name": "연합인포맥스",
        "rss_urls": [],
        "html_fallbacks": [
            {"url": "https://news.einfomax.co.kr/news/articleList.html?sc_section_code=S1N15", "patterns": ["articleView.html"], "base_url": "https://news.einfomax.co.kr"},
        ],
    },
]

FINANCE_KEYWORDS = ["PF","프로젝트파이낸싱","선순위","후순위","메자닌","브릿지론","부동산","오피스","물류센터","데이터센터","인프라","에너지","풍력","태양광","사모펀드","M&A","인수합병","NPL","부실채권","자산운용","펀드결성","리츠","REITs","IB","딜클로징","약정","조달","공제회","연기금","출자"]
DEDUP_THRESHOLD = 0.75
REQUEST_TIMEOUT = 10
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept-Language": "ko-KR,ko;q=0.9"}
DB_PATH = "ampis.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS raw_news (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, title TEXT NOT NULL, url TEXT UNIQUE, summary TEXT, published TEXT, title_hash TEXT UNIQUE, is_parsed INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now','localtime')))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, news_id INTEGER REFERENCES raw_news(id), name TEXT, type TEXT, re_sub TEXT, size TEXT, fi TEXT, si TEXT, ci TEXT, amount TEXT, structure TEXT, collateral TEXT, exit_plan TEXT, features TEXT, source_url TEXT, source_name TEXT, published TEXT, status TEXT DEFAULT '검토', created_at TEXT DEFAULT (datetime('now','localtime')))""")
    conn.commit(); conn.close()

def fetch_rss(url, source_name):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception:
        return []
    cleaned = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', resp.text)
    cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
    items = []
    try:
        root = ET.fromstring(cleaned.encode('utf-8'))
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            url_tag = (item.findtext("link") or "").strip()
            summary = re.sub(r"<[^>]+>", "", (item.findtext("description") or "")).strip()
            pub_raw = item.findtext("pubDate") or ""
            try:
                pub_dt = parsedate_to_datetime(pub_raw).strftime("%Y-%m-%d %H:%M")
            except:
                pub_dt = datetime.now().strftime("%Y-%m-%d %H:%M")
            if title and url_tag:
                items.append({"source": source_name, "title": title, "url": url_tag, "summary": summary[:500], "published": pub_dt})
    except:
        pass
    return items

def fetch_html_fallback(cfg, source_name):
    try:
        resp = requests.get(cfg["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception as e:
        print(f"    [HTML 폴백 실패] {source_name}: {e}")
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    for tag in soup.find_all("a"):
        title = " ".join(tag.get_text().split()).strip()
        href = tag.get("href", "").strip()
        if not title or not href or len(title) < 6:
            continue
        is_matched = any(pt in href for pt in cfg.get("patterns", []))
        is_article_id = bool(re.search(r'\d{8,}', href))
        if is_matched or is_article_id:
            if href.startswith("//"): href = "https:" + href
            elif not href.startswith("http"): href = cfg["base_url"] + ("" if href.startswith("/") else "/") + href
            if "news_list" in href or "articleList" in href: continue
            if href not in [i["url"] for i in items]:
                items.append({"source": source_name, "title": title, "url": href, "summary": "", "published": now})
        if len(items) >= 30: break
    return items

def is_finance_related(title, summary):
    text = (title + " " + summary).upper()
    return any(kw.upper() in text for kw in FINANCE_KEYWORDS)

def normalize_title(title):
    return re.sub(r"[^\w가-힣]", "", title).lower()

def title_hash(title):
    return hashlib.md5(normalize_title(title).encode()).hexdigest()

def is_similar(t1, t2, threshold=DEDUP_THRESHOLD):
    return SequenceMatcher(None, normalize_title(t1), normalize_title(t2)).ratio() >= threshold

def deduplicate(articles):
    seen_urls, seen_hashes, seen_titles, result = set(), set(), [], []
    for art in articles:
        if art["url"] and art["url"] in seen_urls: continue
        h = title_hash(art["title"])
        if h in seen_hashes: continue
        if any(is_similar(art["title"], t) for t in seen_titles): continue
        seen_urls.add(art["url"]); seen_hashes.add(h); seen_titles.append(art["title"])
        art["title_hash"] = h; result.append(art)
    return result

def save_to_db(articles):
    conn = sqlite3.connect(DB_PATH); cur = conn.cursor(); saved = 0
    for art in articles:
        try:
            cur.execute("INSERT OR IGNORE INTO raw_news (source,title,url,summary,published,title_hash) VALUES (?,?,?,?,?,?)",
                (art["source"],art["title"],art["url"],art["summary"],art["published"],art["title_hash"]))
            if cur.rowcount: saved += 1
        except: pass
    conn.commit(); conn.close()
    return saved

def collect_source(src):
    articles = []
    for rss_url in src.get("rss_urls", []):
        articles.extend(fetch_rss(rss_url, src["name"]))
    if not articles and src.get("html_fallbacks"):
        for cfg in src["html_fallbacks"]:
            articles.extend(fetch_html_fallback(cfg, src["name"]))
    return articles

def run_crawler():
    init_db()
    all_articles, by_source = [], {}
    for src in SOURCES:
        items = collect_source(src)
        by_source[src["name"]] = len(items)
        all_articles.extend(items)
    fetched = len(all_articles)
    filtered = [a for a in all_articles if is_finance_related(a["title"], a["summary"])]
    deduped = deduplicate(filtered)
    saved = save_to_db(deduped)
    return {"fetched": fetched, "filtered": len(filtered), "deduped": len(deduped), "saved": saved, "by_source": by_source}

if __name__ == "__main__":
    result = run_crawler()
    print(f"수집:{result['fetched']} 필터:{result['filtered']} 중복제거:{result['deduped']} 저장:{result['saved']}")
    for s,c in result["by_source"].items(): print(f"  {s}: {c}건")
