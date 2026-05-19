"""
AMPIS AI 파서
- raw_news 테이블의 미파싱 뉴스를 Claude API로 구조화
- 결과를 projects 테이블에 저장
"""

import json
import re
import sqlite3
import time

import anthropic

DB_PATH = "ampis.db"
MODEL   = "claude-sonnet-4-20250514"

# Claude에게 전달할 시스템 프롬프트
SYSTEM_PROMPT = """
당신은 금융 투자 뉴스에서 프로젝트 정보를 추출하는 전문 AI입니다.
한국의 부동산 PF, 인프라, 에너지, 기업금융, PE/PEF, M&A, NPL 등 자산운용 딜을 분석합니다.

뉴스 원문을 읽고 아래 JSON 형식으로만 응답하세요.
마크다운 코드블록 없이 순수 JSON만 출력하세요.

{
  "is_project": true,          // 실제 투자·딜 뉴스인지 (단순 시황·의견은 false)
  "name": "프로젝트/딜 명칭",
  "type": "부동산|인프라|에너지|기업금융|PEF|M&A|NPL|기타",
  "re_sub": "오피스|주거|물류|리테일|데이터센터|호텔|null",  // type이 부동산일 때만
  "size": "총 규모 (예: 3,200억원)",
  "fi": "주요 금융투자자 (FI) 나열",
  "si": "전략적투자자 (SI) 또는 null",
  "ci": "시공사 (CI) 또는 null",
  "amount": "대출/투자 구조 (예: 선순위 2,400억 / 후순위 800억)",
  "structure": "금융 구조 요약 (예: PF 대출 + 메자닌)",
  "collateral": "담보·신용보강 (예: 토지 담보 + 준공보증)",
  "exit_plan": "Exit 전략 (예: 완공 후 리츠 편입)",
  "features": "핵심 특징 1~2문장",
  "status": "진행|검토|완료"
}

is_project가 false이면 나머지 필드는 모두 null로 반환하세요.
"""

def get_unparsed_news(limit: int = 20) -> list[dict]:
    """is_parsed = 0 인 뉴스 가져오기"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT id, source, title, url, summary, published
        FROM raw_news
        WHERE is_parsed = 0
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def mark_parsed(news_id: int):
    """파싱 완료 표시"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE raw_news SET is_parsed = 1 WHERE id = ?", (news_id,))
    conn.commit()
    conn.close()

def save_project(news_id: int, data: dict, source_url: str,
                 source_name: str, published: str):
    """파싱된 프로젝트 정보를 DB에 저장"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO projects
            (news_id, name, type, re_sub, size, fi, si, ci,
             amount, structure, collateral, exit_plan, features,
             source_url, source_name, published, status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        news_id,
        data.get("name"),
        data.get("type"),
        data.get("re_sub") if data.get("re_sub") != "null" else None,
        data.get("size"),
        data.get("fi"),
        data.get("si"),
        data.get("ci"),
        data.get("amount"),
        data.get("structure"),
        data.get("collateral"),
        data.get("exit_plan"),
        data.get("features"),
        source_url,
        source_name,
        published,
        data.get("status", "검토"),
    ))
    conn.commit()
    conn.close()

def parse_news_with_claude(news: dict, client: anthropic.Anthropic) -> dict | None:
    """
    단일 뉴스를 Claude로 파싱.
    반환: 파싱된 dict 또는 None (오류 시)
    """
    user_content = f"""
제목: {news['title']}
출처: {news['source']}
날짜: {news['published']}
요약: {news['summary'] or '(요약 없음)'}
URL: {news['url']}
"""
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )
        raw = msg.content[0].text.strip()

        # JSON 코드블록 있을 경우 제거
        raw = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()

        return json.loads(raw)

    except json.JSONDecodeError as e:
        print(f"  [!] JSON 파싱 오류 (news_id={news['id']}): {e}")
        return None
    except anthropic.APIError as e:
        print(f"  [!] Claude API 오류: {e}")
        return None

def run_parser(batch_size: int = 20, delay: float = 0.5) -> dict:
    """
    미파싱 뉴스를 일괄 처리.
    delay: API 호출 간격 (초) — rate limit 방지
    반환: {"processed": N, "projects_saved": N, "skipped": N}
    """
    client = anthropic.Anthropic()   # ANTHROPIC_API_KEY 환경변수 자동 사용

    news_list = get_unparsed_news(limit=batch_size)
    if not news_list:
        print("[파서] 처리할 뉴스 없음")
        return {"processed": 0, "projects_saved": 0, "skipped": 0}

    print(f"[파서] {len(news_list)}건 처리 시작")
    processed = projects_saved = skipped = 0

    for news in news_list:
        print(f"  → [{news['source']}] {news['title'][:45]}...")
        result = parse_news_with_claude(news, client)
        mark_parsed(news["id"])
        processed += 1

        if result is None:
            skipped += 1
        elif not result.get("is_project"):
            print(f"     투자 딜 아님 — 스킵")
            skipped += 1
        else:
            save_project(
                news_id     = news["id"],
                data        = result,
                source_url  = news["url"],
                source_name = news["source"],
                published   = news["published"],
            )
            projects_saved += 1
            print(f"     ✓ 저장: {result.get('name')} [{result.get('type')}] {result.get('size')}")

        time.sleep(delay)   # rate limit 방지

    print(f"\n[파서 완료] 처리:{processed} / 프로젝트저장:{projects_saved} / 스킵:{skipped}")
    return {"processed": processed, "projects_saved": projects_saved, "skipped": skipped}


# ── 단독 실행 테스트 ──────────────────────────────────
if __name__ == "__main__":
    # 테스트용 샘플 뉴스 삽입 (실제로는 crawler.py 실행 후 진행)
    import os
    if not os.path.exists(DB_PATH):
        from crawler import init_db
        init_db()

    conn = sqlite3.connect(DB_PATH)
    samples = [
        ("한국경제", "KB증권, 판교 물류센터 PF 3,200억 주관…선순위 2,400억 조달",
         "https://www.hankyung.com/sample1",
         "KB증권이 경기도 판교 소재 물류센터 PF 딜을 주관한다. 총 규모 3,200억원으로 선순위 2,400억원은 KB증권이, 후순위 800억원은 미래에셋이 담당한다. 현대건설이 시공을 맡으며 완공 후 리츠 편입 예정.",
         "2025-05-10 09:00"),
        ("매일경제", "국민연금, 해상풍력 인프라 8,500억 투자 검토",
         "https://www.mk.co.kr/sample2",
         "국민연금이 서해안 해상풍력 발전소 건설 프로젝트에 8,500억원 투자를 검토 중이다. 선순위 6,000억원, 후순위 2,500억원 구조로 교직원공제회도 참여한다. 두산에너빌리티가 EPC를 맡을 예정.",
         "2025-05-11 10:30"),
        ("이데일리", "IMM PE, K-바이오 LBO 딜 완료…1,200억 규모",
         "https://www.edaily.co.kr/sample3",
         "IMM프라이빗에쿼티가 국내 바이오 기업 지분 70%를 1,200억원에 인수했다. LBO 구조에 브릿지론을 활용했으며, 3년 내 코스닥 IPO를 목표로 하고 있다.",
         "2025-05-09 15:00"),
        ("한국경제", "오늘 코스피 1% 상승, 외국인 순매수",
         "https://www.hankyung.com/sample4",
         "코스피가 오늘 1% 상승했다. 외국인이 순매수 전환하면서 증시가 반등했다.",
         "2025-05-12 09:00"),   # 딜 뉴스 아님 → 스킵 예상
    ]
    for s in samples:
        conn.execute("""
            INSERT OR IGNORE INTO raw_news (source, title, url, summary, published, title_hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (*s, s[1][:32]))
    conn.commit()
    conn.close()
    print("[테스트] 샘플 뉴스 4건 삽입 완료\n")

    result = run_parser(batch_size=10, delay=1.0)
    print("\n── 결과 ──")
    print(result)
