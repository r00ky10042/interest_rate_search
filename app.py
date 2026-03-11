# -*- coding: utf-8 -*-
import os
import random
import threading
import time
from dotenv import load_dotenv
from flask import Flask, jsonify, request, render_template
import db
import scraper

load_dotenv()

def _load_smtp_pass():
    """암호화된 비밀번호 복호화. 평문도 fallback으로 지원."""
    enc_key  = os.environ.get("ENCRYPT_KEY", "")
    enc_pass = os.environ.get("SMTP_PASS_ENC", "")
    if enc_key and enc_pass:
        try:
            from cryptography.fernet import Fernet
            return Fernet(enc_key.encode()).decrypt(enc_pass.encode()).decode()
        except Exception as e:
            print(f"[ERROR] 비밀번호 복호화 실패: {e}")
    return os.environ.get("SMTP_PASS", "")  # fallback: 평문

SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = _load_smtp_pass()
print(f"[CONFIG] SMTP_USER={'설정됨' if SMTP_USER else '미설정'}, SMTP_PASS={'설정됨' if SMTP_PASS else '미설정'}")
OTP_TO    = os.environ.get("OTP_TO", SMTP_USER)
OTP_TTL   = 300  # 5분

app = Flask(__name__)
db.init_db()

# ── OTP 저장소 ─────────────────────────────
_otp_store = {"code": None, "expires": 0, "last_sent": 0}
OTP_COOLDOWN = 180  # 3분

# ── 스크래핑 상태 ───────────────────────────
_scrape_state = {"running": False, "done": 0, "total": 0, "log": []}


def _send_otp(code):
    import resend
    resend.api_key = os.environ.get("RESEND_API_KEY", "")
    resend.Emails.send({
        "from": "onboarding@resend.dev",
        "to":   OTP_TO,
        "subject": f"[금리조회] 인증번호 {code}",
        "text": f"새마을금고 금리 재수집 인증번호입니다.\n\n인증번호: {code}\n\n유효시간: 5분",
    })


def _run_scrape():
    _scrape_state.update({"running": True, "done": 0, "total": 0, "log": []})
    log_id = db.log_scrape_start()

    def log(msg):
        _scrape_state["log"].append(msg)

    def progress(done, total):
        _scrape_state["done"] = done
        _scrape_state["total"] = total

    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT gmgo_cd FROM rates")
            existing = {r["gmgo_cd"] for r in cur.fetchall()}

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


# ── OTP 발송 ──────────────────────────────────
@app.route("/api/config/check")
def api_config_check():
    """환경변수 설정 상태 확인 (비밀번호 노출 없이)"""
    return jsonify({
        "SMTP_USER":     "설정됨" if SMTP_USER else "미설정",
        "SMTP_PASS":     "설정됨" if SMTP_PASS else "미설정",
        "ENCRYPT_KEY":   "설정됨" if os.environ.get("ENCRYPT_KEY") else "미설정",
        "SMTP_PASS_ENC": "설정됨" if os.environ.get("SMTP_PASS_ENC") else "미설정",
    })

@app.route("/api/otp/send", methods=["POST"])
def api_otp_send():
    if not SMTP_USER or not SMTP_PASS:
        return jsonify({"error": f"이메일 설정이 되지 않았습니다 (USER={'설정됨' if SMTP_USER else '미설정'}, PASS={'설정됨' if SMTP_PASS else '미설정'})"}), 500

    since_last = time.time() - _otp_store["last_sent"]
    if since_last < OTP_COOLDOWN:
        remain = int(OTP_COOLDOWN - since_last)
        return jsonify({"error": f"재발송은 {remain}초 후에 가능합니다"}), 429

    code = str(random.randint(100000, 999999))
    _otp_store["code"]      = code
    _otp_store["expires"]   = time.time() + OTP_TTL
    _otp_store["last_sent"] = time.time()

    try:
        _send_otp(code)
    except Exception as e:
        return jsonify({"error": f"메일 발송 실패: {e}"}), 500

    masked = OTP_TO[:3] + "***" + OTP_TO[OTP_TO.index("@"):]
    return jsonify({"message": f"{masked} 으로 인증번호를 발송했습니다"})


# ── 스크래핑 (OTP 검증 후 실행) ──────────────
@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    otp = request.json.get("otp") if request.is_json else None

    if not otp:
        return jsonify({"error": "인증번호를 입력하세요"}), 400
    if time.time() > _otp_store["expires"]:
        return jsonify({"error": "인증번호가 만료되었습니다. 다시 요청하세요"}), 401
    if otp != _otp_store["code"]:
        return jsonify({"error": "인증번호가 올바르지 않습니다"}), 401

    # 사용 후 즉시 무효화
    _otp_store["code"]    = None
    _otp_store["expires"] = 0

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
