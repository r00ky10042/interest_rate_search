# -*- coding: utf-8 -*-
import os
import threading
from dotenv import load_dotenv
from flask import Flask, jsonify, request, render_template
import db
import scraper

load_dotenv()

SCRAPE_PASSWORD = os.environ.get("SCRAPE_PASSWORD", "")
print(f"[CONFIG] SCRAPE_PASSWORD={'설정됨' if SCRAPE_PASSWORD else '미설정'}")

app = Flask(__name__)
db.init_db()

# ── 스크래핑 상태 ───────────────────────────
_scrape_state = {"running": False, "done": 0, "total": 0, "log": []}


def _run_scrape():
    _scrape_state.update({"running": True, "done": 0, "total": 0, "log": []})
    log_id = db.log_scrape_start()

    def log(msg):
        _scrape_state["log"].append(msg)

    def progress(done, total):
        _scrape_state["done"] = done
        _scrape_state["total"] = total

    records = scraper.scrape_all(log_cb=log, progress_cb=progress)
    db.upsert_rates(records)
    db.log_scrape_done(log_id, len(records))
    _scrape_state["running"] = False
    log(f"완료! {len(records)}개 업데이트됨")


# ── Routes ──────────────────────────────────────
@app.route("/ads.txt")
def ads_txt():
    return "google.com, pub-5283512698876643, DIRECT, f08c47fec0942fa0", 200, {"Content-Type": "text/plain"}


@app.route("/")
def index():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr).split(",")[0].strip()
    db.record_visit(ip)
    return render_template("index.html")


@app.route("/api/rates")
def api_rates():
    import re
    r1       = request.args.get("r1") or None
    keyword  = request.args.get("q") or None
    only_m   = request.args.get("only_monthly", "false").lower() == "true"
    sort_col = request.args.get("sort", "monthly_12m")
    sort_dir = request.args.get("dir", "desc")

    rows = db.query_rates(r1=r1, keyword=keyword, only_monthly=only_m)

    def rate_val(s):
        if not s:
            return -1
        m = re.search(r"[\d.]+", str(s))
        return float(m.group()) if m else -1

    num_cols = {"monthly_12m", "maturity_12m"}
    reverse  = sort_dir == "desc"
    if sort_col in num_cols:
        rows.sort(key=lambda r: rate_val(r.get(sort_col)), reverse=reverse)
    else:
        rows.sort(key=lambda r: str(r.get(sort_col, "")), reverse=reverse)

    return jsonify({"data": rows, "total": len(rows)})


@app.route("/api/stats")
def api_stats():
    return jsonify(db.get_stats())


@app.route("/api/visits")
def api_visits():
    return jsonify(db.get_visit_stats())


# ── 스크래핑 (비밀번호 검증 후 실행) ──────────────
@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    pw = request.json.get("password") if request.is_json else None

    if not pw:
        return jsonify({"error": "비밀번호를 입력하세요"}), 400
    if not SCRAPE_PASSWORD:
        return jsonify({"error": "서버에 SCRAPE_PASSWORD가 설정되지 않았습니다"}), 500
    if pw != SCRAPE_PASSWORD:
        return jsonify({"error": "비밀번호가 올바르지 않습니다"}), 401

    if _scrape_state["running"]:
        return jsonify({"error": "이미 수집 중입니다"}), 409

    threading.Thread(target=_run_scrape, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/scrape/status")
def api_scrape_status():
    return jsonify(_scrape_state)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
