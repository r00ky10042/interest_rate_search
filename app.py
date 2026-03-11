# -*- coding: utf-8 -*-
import threading
from flask import Flask, jsonify, request, render_template
import db
import scraper

app = Flask(__name__)
db.init_db()

# 스크래핑 상태
_scrape_state = {"running": False, "done": 0, "total": 0, "log": []}


def _run_scrape():
    _scrape_state.update({"running": True, "done": 0, "total": 0, "log": []})
    log_id = db.log_scrape_start()

    def log(msg):
        _scrape_state["log"].append(msg)

    def progress(done, total):
        _scrape_state["done"] = done
        _scrape_state["total"] = total

    with db.get_conn() as conn:
        existing = {r[0] for r in conn.execute("SELECT gmgo_cd FROM rates").fetchall()}

    records = scraper.scrape_all(existing_codes=existing, log_cb=log, progress_cb=progress)
    db.upsert_rates(records)
    db.log_scrape_done(log_id, len(records))
    _scrape_state["running"] = False
    log(f"완료! {len(records)}개 업데이트됨")


# ── Routes ──────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/rates")
def api_rates():
    r1      = request.args.get("r1") or None
    keyword = request.args.get("q") or None
    only_m  = request.args.get("only_monthly", "false").lower() == "true"
    sort_col = request.args.get("sort", "monthly_12m")
    sort_dir = request.args.get("dir", "desc")

    rows = db.query_rates(r1=r1, keyword=keyword, only_monthly=only_m)

    def rate_val(s):
        import re
        if not s:
            return -1
        m = re.search(r"[\d.]+", str(s))
        return float(m.group()) if m else -1

    num_cols = {"monthly_12m", "maturity_12m"}
    reverse = sort_dir == "desc"
    if sort_col in num_cols:
        rows.sort(key=lambda r: rate_val(r.get(sort_col)), reverse=reverse)
    else:
        rows.sort(key=lambda r: str(r.get(sort_col, "")), reverse=reverse)

    return jsonify({"data": rows, "total": len(rows)})


@app.route("/api/stats")
def api_stats():
    return jsonify(db.get_stats())


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    if _scrape_state["running"]:
        return jsonify({"error": "이미 수집 중입니다"}), 409
    threading.Thread(target=_run_scrape, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/scrape/status")
def api_scrape_status():
    return jsonify(_scrape_state)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
