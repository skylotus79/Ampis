"""
AMPIS Flask 메인 앱 — 전체 기능 통합 완성본
- Render 환경의 파일 리셋 방지를 위한 S3 오토 백업/복원 연동
- 과부하 락 해소를 위한 SQLite 동시성 제어 몽키 패치 탑재
- 날짜 필터링 및 Claude API 백오프 파싱 완벽 연동
"""
import json, os, re, sqlite3, threading, traceback
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler

# ── SQLite 멀티 프로세스 락 방지용 전역 몽키 패치 (Gunicorn 다중 워커 대응) ──
_original_connect = sqlite3.connect
def robust_connect(*args, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = 30.0
    return _original_connect(*args, **kwargs)
sqlite3.connect = robust_connect

# ── 모듈 로드 ──
import db_sync
from crawler import run_crawler
import crawler
import ai_parser

app = Flask(__name__)

# ── API 토큰 인증 (선택적: AMPIS_API_TOKEN 환경변수 설정 시 활성화) ──
AMPIS_API_TOKEN = os.environ.get("AMPIS_API_TOKEN", "")

def check_token():
    """AMPIS_API_TOKEN이 설정된 경우에만 인증 강제"""
    if not AMPIS_API_TOKEN:
        return None   # 토큰 미설정 시 인증 생략
    token = request.headers.get("X-API-Token", "")
    if token != AMPIS_API_TOKEN:
        return jsonify({"ok": False, "error": "인증 토큰이 올바르지 않습니다."}), 401
    return None

@app.context_processor
def inject_api_token():
    """모든 Jinja2 템플릿에 api_token 자동 주입"""
    return {"api_token": AMPIS_API_TOKEN}


# ── Render 배포 환경 검출 및 경로 세팅 ──
if os.environ.get("RENDER"):
    DB_PATH = "/tmp/ampis.db"
    SETTINGS = "/tmp/settings.json"
    crawler.DB_PATH = "/tmp/ampis.db"
    ai_parser.DB_PATH = "/tmp/ampis.db"
    db_sync.DB_PATH = "/tmp/ampis.db"
else:
    DB_PATH = "ampis.db"
    SETTINGS = "settings.json"

# ── 설정 ──────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "crawl_from":     (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
    "crawl_to":       "",
    "crawl_interval": 3,
    "max_days":       30,
    "auto_parse":     True,
    "dedup":          True,
    "sources": {"한국경제": True, "매일경제": True, "이데일리": True,
                "머니투데이": True, "연합인포맥스": True},
}

def load_settings():
    if os.path.exists(SETTINGS):
        try:
            with open(SETTINGS, "r", encoding="utf-8") as f:
                s = DEFAULT_SETTINGS.copy()
                s.update(json.load(f))
                return s
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()

def save_settings(data):
    try:
        with open(SETTINGS, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[설정 저장 실패] {e}")

# ── DB ────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def qdb(sql, args=(), one=False):
    conn = get_db()
    rows = [dict(r) for r in conn.execute(sql, args).fetchall()]
    conn.close()
    return rows[0] if (one and rows) else (None if one else rows)

def init_db():
    conn = get_db()
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
            news_id INTEGER, name TEXT, type TEXT, re_sub TEXT,
            size TEXT, fi TEXT, si TEXT, ci TEXT, amount TEXT,
            structure TEXT, collateral TEXT, exit_plan TEXT,
            features TEXT, source_url TEXT, source_name TEXT,
            published TEXT, status TEXT DEFAULT '검토',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
    """)
    conn.commit(); conn.close()

# ── 백그라운드 스케줄러 ───────────────────────────────
scheduler  = BackgroundScheduler()
crawl_lock = threading.Lock()

def scheduled_job():
    if not crawl_lock.acquire(blocking=False): return
    try:
        db_sync.restore_db()
        settings = load_settings()
        crawl_from = settings.get("crawl_from")
        crawl_to = settings.get("crawl_to")
        
        result = run_crawler(crawl_from, crawl_to)
        
        if result["saved"] > 0:
            db_sync.backup_db()
            if settings.get("auto_parse"):
                ai_parser.run_parser(batch_size=min(result["saved"], 15))
                db_sync.backup_db()
    finally:
        crawl_lock.release()

def restart_scheduler():
    hours = int(load_settings().get("crawl_interval", 3))
    try: scheduler.remove_job("auto_crawl")
    except Exception: pass
    scheduler.add_job(scheduled_job, "interval", hours=hours, id="auto_crawl")

db_sync.restore_db()
init_db()
restart_scheduler()
if not scheduler.running:
    scheduler.start()

@app.errorhandler(Exception)
def handle_exception(e):
    print("=" * 60)
    print("🚨 [AMPIS APP RUNTIME ERROR DECTECTED]")
    traceback.print_exc()
    print("=" * 60)
    
    return jsonify({
        "ok": False,
        "error_type": type(e).__name__,
        "message": str(e),
        "traceback": traceback.format_exc().split("\n")
    }), 500

# ── 페이지 라우트 ─────────────────────────────────────
@app.route("/")
def page_dashboard():    return render_template("dashboard.html")
@app.route("/projects")
def page_projects():     return render_template("projects.html")
@app.route("/companies")
def page_companies():    return render_template("companies.html")
@app.route("/news")
def page_news():         return render_template("news.html")
@app.route("/settings")
def page_settings():     return render_template("settings.html", settings=load_settings())
@app.route("/ai")
def page_ai():           return render_template("ai.html")
@app.route("/alerts")
def page_alerts():       return render_template("alerts.html")

# ── API: 통계 ─────────────────────────────────────────
@app.route("/api/stats")
def api_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    return jsonify({
        "total_news":     (qdb("SELECT COUNT(*) c FROM raw_news", one=True) or {}).get("c",0),
        "total_projects": (qdb("SELECT COUNT(*) c FROM projects", one=True) or {}).get("c",0),
        "unparsed":       (qdb("SELECT COUNT(*) c FROM raw_news WHERE is_parsed=0", one=True) or {}).get("c",0),
        "today_news":     (qdb("SELECT COUNT(*) c FROM raw_news WHERE created_at LIKE ?", (today+"%",), one=True) or {}).get("c",0),
        "by_type":   {r["type"]: r["c"] for r in qdb("SELECT type,COUNT(*) c FROM projects WHERE type IS NOT NULL GROUP BY type")},
        "by_source": {r["source"]: r["c"] for r in qdb("SELECT source,COUNT(*) c FROM raw_news GROUP BY source ORDER BY c DESC")},
        "by_month":  {r["month"]: r["c"] for r in qdb("SELECT substr(published,1,7) month,COUNT(*) c FROM projects WHERE published IS NOT NULL GROUP BY month ORDER BY month DESC LIMIT 12")},
    })

# ── API: 프로젝트 ─────────────────────────────────────
@app.route("/api/projects")
def api_projects():
    type_  = request.args.get("type","")
    re_sub = request.args.get("re_sub","")
    fi     = request.args.get("fi","")
    q      = request.args.get("q","")
    sort   = request.args.get("sort","date")
    limit  = int(request.args.get("limit",200))

    sql, args = "SELECT * FROM projects WHERE 1=1", []
    if type_:  sql += " AND type=?";  args.append(type_)
    if re_sub: sql += " AND re_sub=?"; args.append(re_sub)
    if fi:     sql += " AND (fi LIKE ? OR si LIKE ? OR ci LIKE ?)"; args += [f"%{fi}%"]*3
    if q:      sql += " AND (name LIKE ? OR fi LIKE ? OR features LIKE ?)"; args += [f"%{q}%"]*3
    sql += " ORDER BY " + ("created_at DESC" if sort=="date" else "id DESC")
    sql += f" LIMIT {limit}"
    return jsonify(qdb(sql, args))

@app.route("/api/projects/<int:pid>")
def api_project_detail(pid):
    row = qdb("SELECT * FROM projects WHERE id=?", (pid,), one=True)
    if not row: return jsonify({"error":"not found"}), 404
    row["news"] = qdb("SELECT title,url,source,published FROM raw_news WHERE id=?", (row.get("news_id"),), one=True)
    return jsonify(row)

# ── API: 회사 분석 ────────────────────────────────────
@app.route("/api/companies")
def api_companies():
    projects = qdb("SELECT id,name,type,size,fi,si,ci,published,status FROM projects")
    cmap = {}

    def add(raw, project, role):
        if not raw: return
        for co in re.split(r"[,/·\s]+(?=\S{2,})", raw):
            co = co.strip()
            if not co or len(co) < 2: continue
            if co not in cmap:
                cmap[co] = {"name": co, "projects": [], "types": {}, "roles": set()}
            e = cmap[co]
            ids = [p["id"] for p in e["projects"]]
            if project["id"] not in ids:
                e["projects"].append({k: project[k] for k in ("id","name","type","size","published","status")})
            t = project.get("type") or "기타"
            e["types"][t] = e["types"].get(t, 0) + 1
            e["roles"].add(role)

    for p in projects:
        add(p.get("fi"), p, "FI")
        add(p.get("si"), p, "SI")
        add(p.get("ci"), p, "CI")

    result = []
    for co, d in cmap.items():
        result.append({
            "name":          co,
            "project_count": len(d["projects"]),
            "top_type":      max(d["types"], key=d["types"].get) if d["types"] else None,
            "roles":         list(d["roles"]),
            "projects":      d["projects"],
        })
    result.sort(key=lambda x: x["project_count"], reverse=True)
    return jsonify(result)

@app.route("/api/companies/<company_name>")
def api_company_detail(company_name):
    rows = qdb("SELECT * FROM projects WHERE fi LIKE ? OR si LIKE ? OR ci LIKE ? ORDER BY created_at DESC",
               (f"%{company_name}%",)*3)
    return jsonify({"name": company_name, "projects": rows, "count": len(rows)})

# ── API: 뉴스 ─────────────────────────────────────────
@app.route("/api/news")
def api_news():
    source    = request.args.get("source","")
    date_from = request.args.get("from","")
    date_to   = request.args.get("to","")
    parsed    = request.args.get("parsed","")
    limit     = int(request.args.get("limit",200))

    sql, args = "SELECT * FROM raw_news WHERE 1=1", []
    if source:    sql += " AND source=?"; args.append(source)
    if date_from: sql += " AND published>=?"; args.append(date_from)
    if date_to:   sql += " AND published<=?"; args.append(date_to+" 23:59")
    if parsed in ("0","1"): sql += " AND is_parsed=?"; args.append(int(parsed))
    sql += " ORDER BY id DESC LIMIT ?"; args.append(limit)
    return jsonify(qdb(sql, args))

# ── API: 크롤링 / 파싱 / AI ──────────────────────────
@app.route("/api/crawl", methods=["POST"])
def api_crawl():
    if not crawl_lock.acquire(blocking=False):
        return jsonify({"ok": False, "message": "이미 크롤링 중"}), 409
    try:
        db_sync.restore_db()
        settings = load_settings()
        crawl_from = settings.get("crawl_from")
        crawl_to = settings.get("crawl_to")
        
        result = run_crawler(crawl_from, crawl_to)
        if result["saved"] > 0:
            db_sync.backup_db()
            
        return jsonify({"ok": True, **result})
    finally:
        crawl_lock.release()

@app.route("/api/parse", methods=["POST"])
def api_parse():
    body    = request.get_json(silent=True) or {}
    batch   = int(body.get("batch_size", 15))
    news_id = body.get("news_id")   # 특정 기사 1건만 파싱 시
    try:
        db_sync.restore_db()
        # 특정 news_id가 지정된 경우 해당 기사를 미파싱 상태로 강제 설정 후 파싱
        if news_id:
            conn = get_db()
            conn.execute("UPDATE raw_news SET is_parsed=0 WHERE id=?", (news_id,))
            conn.commit(); conn.close()
            batch = 1
        parse_result = ai_parser.run_parser(batch_size=batch)
        if parse_result["projects_saved"] > 0:
            db_sync.backup_db()
        return jsonify({"ok": True, **parse_result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/ai-chat", methods=["POST"])
def api_ai_chat():
    err = check_token()
    if err: return err
    import anthropic as _ant
    body = request.get_json(silent=True) or {}
    msg  = (body.get("message") or "").strip()
    if not msg: return jsonify({"error":"메시지 없음"}), 400

    recent = qdb("SELECT name,type,size,fi,structure,status,published FROM projects ORDER BY id DESC LIMIT 20")
    stats  = json.loads(api_stats().data)
    system = (f"당신은 AMPIS 자산운용 AI입니다. DB: 프로젝트 {stats['total_projects']}건, "
              f"유형별={stats['by_type']}, 최근프로젝트={json.dumps(recent,ensure_ascii=False)}. "
              f"금융 전문 용어를 정확히 사용해 한국어로 간결하게 답변하세요.")
    try:
        client = _ant.Anthropic()
        res    = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=1000,
                                        system=system, messages=[{"role":"user","content":msg}])
        return jsonify({"ok": True, "text": res.content[0].text})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/ai-extract", methods=["POST"])
def api_ai_extract():
    err = check_token()
    if err: return err
    import anthropic as _ant
    body = request.get_json(silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text: return jsonify({"error":"원문 없음"}), 400
    system = ('금융 뉴스에서 투자 프로젝트 정보를 추출하세요. 순수 JSON만 반환:\n'
              '{"is_project":true,"name":"","type":"부동산|인프라|에너지|기업금융|PEF|M&A|NPL|기타",'
              '"re_sub":"오피스|주거|물류|리테일|데이터센터|호텔|null","size":"","fi":"","si":"","ci":"",'
              '"amount":"","structure":"","collateral":"","exit_plan":"","features":"","status":"진행|검토|완료"}')
    try:
        client = _ant.Anthropic()
        res    = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=1000,
                                        system=system, messages=[{"role":"user","content":text}])
        raw  = re.sub(r"[\`]{3}(?:json)?|[\`]{3}", "", res.content[0].text).strip()
        return jsonify({"ok": True, "data": json.loads(raw)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    return jsonify(load_settings())

@app.route("/api/settings", methods=["POST"])
def api_settings_post():
    body = request.get_json(silent=True) or {}
    cur  = load_settings()
    cur.update(body)
    save_settings(cur)
    restart_scheduler()
    return jsonify({"ok": True, "settings": cur})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)