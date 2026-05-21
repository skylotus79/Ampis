"""
AMPIS 12,500건 배치 AI 파싱 프로세서
────────────────────────────────────────────────
ampis.db에 쌓인 대량의 raw_news를 효율적으로 처리합니다.

【실행 방법】
  python batch_processor.py              # 전체 자동 처리
  python batch_processor.py --stats      # 현재 DB 통계만 출력
  python batch_processor.py --preview    # 샘플 10건 미리보기
  python batch_processor.py --batch 50   # 한 번에 50건씩 처리

【비용 추정 (Claude Sonnet)】
  - 기사당 입력: ~300 토큰, 출력: ~200 토큰
  - 건당 비용: 약 $0.002~0.004
  - 12,500건 전체: 약 $25~50
  - 권장: 하루 500건씩 나눠서 처리 (약 25일)
"""

from __future__ import annotations
import argparse, json, re, sqlite3, time, random, os
import anthropic

DB_PATH  = os.environ.get("DB_PATH", "ampis.db")
MODEL    = "claude-sonnet-4-20250514"

# ─── 1차 필터: AI 파싱 전 고가치 기사 선별 ─────────────────
# 이 키워드를 포함한 기사만 AI 파싱 → 비용 절감
HIGH_VALUE_KEYWORDS = [
    "PF", "프로젝트파이낸싱", "선순위", "후순위", "메자닌", "브릿지론",
    "딜클로징", "약정", "출자", "펀드결성", "투자집행", "조달",
    "리츠", "REITs", "물류센터", "데이터센터", "오피스", "주거",
    "인프라", "풍력", "태양광", "해상풍력",
    "NPL", "부실채권", "경매",
    "M&A", "인수합병", "LBO", "바이아웃",
    "사모펀드", "PEF", "PE펀드",
    "공제회", "연기금", "국민연금", "교직원공제회",
    "KB증권", "미래에셋", "한국투자", "NH투자", "신한투자",
    "삼성증권", "하나증권", "대신증권", "키움증권",
    "이지스", "마스턴", "코람코", "JLL", "CBRE",
    "현대건설", "삼성물산", "GS건설", "대우건설", "포스코",
]

SYSTEM_PROMPT = """당신은 금융 투자 뉴스에서 프로젝트 정보를 추출하는 전문 AI입니다.
한국의 부동산 PF, 인프라, 에너지, 기업금융, PE/PEF, M&A, NPL 등 자산운용 딜을 분석합니다.

뉴스 원문을 읽고 아래 JSON 형식으로만 응답하세요. 마크다운 없이 순수 JSON만 출력하세요.

{
  "is_project": true,
  "name": "프로젝트/딜 명칭",
  "type": "부동산|인프라|에너지|기업금융|PEF|M&A|NPL|기타",
  "re_sub": "오피스|주거|물류|리테일|데이터센터|호텔|null",
  "size": "총 규모",
  "fi": "금융투자자(FI)",
  "si": "전략적투자자(SI) 또는 null",
  "ci": "시공사(CI) 또는 null",
  "amount": "대출/투자 구조",
  "structure": "금융 구조 요약",
  "collateral": "담보·신용보강",
  "exit_plan": "Exit 전략",
  "features": "핵심 특징 1~2문장",
  "status": "진행|검토|완료"
}

is_project가 false이면 나머지 필드는 모두 null로 반환하세요."""


# ─── DB 유틸 ─────────────────────────────────────────────────
def get_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    total    = conn.execute("SELECT COUNT(*) FROM raw_news").fetchone()[0]
    unparsed = conn.execute("SELECT COUNT(*) FROM raw_news WHERE is_parsed=0").fetchone()[0]
    parsed   = conn.execute("SELECT COUNT(*) FROM raw_news WHERE is_parsed=1").fetchone()[0]
    projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    by_src   = dict(conn.execute(
        "SELECT source, COUNT(*) FROM raw_news GROUP BY source ORDER BY COUNT(*) DESC"
    ).fetchall())
    # 고가치 기사 수 추정
    kw_filter = " OR ".join([f"title LIKE '%{k}%' OR summary LIKE '%{k}%'"
                              for k in HIGH_VALUE_KEYWORDS[:15]])
    high_val = conn.execute(
        f"SELECT COUNT(*) FROM raw_news WHERE is_parsed=0 AND ({kw_filter})"
    ).fetchone()[0]
    conn.close()
    return {"total": total, "unparsed": unparsed, "parsed": parsed,
            "projects": projects, "by_source": by_src, "high_value": high_val}

def get_high_value_unparsed(limit: int) -> list[dict]:
    """고가치 키워드를 포함한 미파싱 기사 우선 반환"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    kw_filter = " OR ".join([f"title LIKE '%{k}%' OR summary LIKE '%{k}%'"
                              for k in HIGH_VALUE_KEYWORDS])
    rows = conn.execute(f"""
        SELECT id, source, title, url, summary, published
        FROM raw_news
        WHERE is_parsed = 0 AND ({kw_filter})
        ORDER BY published DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_all_unparsed(limit: int) -> list[dict]:
    """일반 미파싱 기사 반환 (고가치 소진 후)"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, source, title, url, summary, published
        FROM raw_news WHERE is_parsed=0 ORDER BY published DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def mark_parsed(news_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE raw_news SET is_parsed=1 WHERE id=?", (news_id,))
    conn.commit(); conn.close()

def save_project(news_id, data, source_url, source_name, published):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO projects
            (news_id,name,type,re_sub,size,fi,si,ci,
             amount,structure,collateral,exit_plan,features,
             source_url,source_name,published,status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (news_id,
          data.get("name"), data.get("type"),
          data.get("re_sub") if data.get("re_sub") != "null" else None,
          data.get("size"), data.get("fi"), data.get("si"), data.get("ci"),
          data.get("amount"), data.get("structure"), data.get("collateral"),
          data.get("exit_plan"), data.get("features"),
          source_url, source_name, published, data.get("status","검토")))
    conn.commit(); conn.close()


# ─── Claude 파싱 (Rate Limit 재시도 포함) ────────────────────
def parse_one(news: dict, client: anthropic.Anthropic, max_retries=5) -> dict | None:
    content = (f"제목: {news['title']}\n출처: {news['source']}\n"
               f"날짜: {news['published']}\n요약: {news['summary'] or '(없음)'}")
    base = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=800, system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": content}]
            )
            raw = re.sub(r"```(?:json)?|```", "", msg.content[0].text).strip()
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
        except anthropic.APIStatusError as e:
            if e.status_code == 429:
                wait = (base ** attempt) + random.uniform(1, 3)
                print(f"    ⏳ Rate Limit → {wait:.0f}초 대기 ({attempt}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"    ❌ API 오류 {e.status_code}")
                return None
        except Exception as e:
            print(f"    ❌ 오류: {e}")
            return None
    return None


# ─── 메인 배치 처리 ──────────────────────────────────────────
def run_batch(batch_size: int = 100, priority_only: bool = True,
              delay: float = 1.0) -> dict:
    """
    batch_size: 한 번에 처리할 기사 수
    priority_only: True=고가치 기사만, False=전체
    delay: 기사 간 대기 시간(초)
    """
    client = anthropic.Anthropic()
    news_list = (get_high_value_unparsed(batch_size) if priority_only
                 else get_all_unparsed(batch_size))

    if not news_list:
        print("📭 처리할 기사가 없습니다.")
        return {"processed": 0, "saved": 0, "skipped": 0}

    print(f"\n🚀 배치 처리 시작: {len(news_list)}건")
    processed = saved = skipped = 0
    start_time = time.time()

    for i, news in enumerate(news_list, 1):
        elapsed = time.time() - start_time
        eta = (elapsed / i) * (len(news_list) - i) if i > 1 else 0
        print(f"  [{i:4d}/{len(news_list)}] [{news['source']}] {news['title'][:45]}..."
              f"  (경과 {elapsed:.0f}s / 남은 {eta:.0f}s)")

        result = parse_one(news, client)
        mark_parsed(news["id"])
        processed += 1

        if result and result.get("is_project"):
            save_project(news["id"], result, news["url"],
                        news["source"], news["published"])
            saved += 1
            print(f"       ✅ {result.get('name')} [{result.get('type')}] {result.get('size','')}")
        else:
            skipped += 1

        time.sleep(delay)

    total_time = time.time() - start_time
    st = get_stats()
    print(f"\n{'='*50}")
    print(f"✅ 배치 완료: 처리 {processed}건 / 프로젝트 저장 {saved}건 / 스킵 {skipped}건")
    print(f"   소요 시간: {total_time:.0f}초 ({total_time/60:.1f}분)")
    print(f"   DB 현황: 뉴스 {st['total']}건 / 프로젝트 {st['projects']}건 / 남은 미파싱 {st['unparsed']}건")
    print(f"{'='*50}")
    return {"processed": processed, "saved": saved, "skipped": skipped}


def show_stats():
    st = get_stats()
    print("\n" + "="*55)
    print("📊 AMPIS DB 현황")
    print("="*55)
    print(f"  전체 뉴스     : {st['total']:,}건")
    print(f"  AI 파싱 완료  : {st['parsed']:,}건")
    print(f"  파싱 대기     : {st['unparsed']:,}건")
    print(f"    └ 고가치 기사: {st['high_value']:,}건 (우선 처리 권장)")
    print(f"  추출 프로젝트 : {st['projects']:,}건")
    print(f"\n  매체별 분포:")
    for src, cnt in st['by_source'].items():
        bar = "█" * min(cnt // 200, 30)
        print(f"    {src:<12}: {cnt:>5}건  {bar}")
    print()

    # 비용 추정
    remaining = st['unparsed']
    cost_low  = remaining * 0.002
    cost_high = remaining * 0.004
    days_500  = remaining // 500 + 1
    print(f"  💰 전체 파싱 예상 비용: ${cost_low:.0f}~${cost_high:.0f}")
    print(f"  📅 하루 500건 기준: 약 {days_500}일 소요")
    print(f"  ⚡ 권장 일일 배치: python batch_processor.py --batch 500")
    print("="*55)


def show_preview(n: int = 10):
    """고가치 기사 샘플 미리보기"""
    samples = get_high_value_unparsed(n)
    print(f"\n📋 고가치 미파싱 기사 샘플 ({len(samples)}건)")
    print("-"*60)
    for s in samples:
        print(f"  [{s['source']}] {s['published'][:10]}")
        print(f"  {s['title']}")
        if s['summary']:
            print(f"  → {s['summary'][:80]}...")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMPIS 배치 AI 파싱 프로세서")
    parser.add_argument("--stats",   action="store_true", help="DB 통계 출력")
    parser.add_argument("--preview", action="store_true", help="샘플 미리보기")
    parser.add_argument("--batch",   type=int, default=100, help="배치 크기 (기본 100)")
    parser.add_argument("--all",     action="store_true", help="고가치 외 일반 기사도 처리")
    parser.add_argument("--delay",   type=float, default=1.0, help="기사 간 대기시간(초)")
    args = parser.parse_args()

    show_stats()
    if args.stats:
        pass
    elif args.preview:
        show_preview(10)
    else:
        run_batch(
            batch_size=args.batch,
            priority_only=not args.all,
            delay=args.delay
        )
