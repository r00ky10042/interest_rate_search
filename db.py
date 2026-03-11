# -*- coding: utf-8 -*-
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "kfcc_rates.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rates (
                gmgo_cd     TEXT PRIMARY KEY,
                r1          TEXT,
                r2          TEXT,
                name        TEXT,
                div_nm      TEXT,
                addr        TEXT,
                has_monthly INTEGER DEFAULT 0,
                monthly_12m TEXT,
                maturity_12m TEXT,
                updated_at  TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                finished_at TEXT,
                total      INTEGER,
                status     TEXT
            )
        """)
        conn.commit()


def upsert_rates(records):
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO rates (gmgo_cd, r1, r2, name, div_nm, addr, has_monthly, monthly_12m, maturity_12m, updated_at)
            VALUES (:gmgo_cd, :r1, :r2, :name, :div_nm, :addr, :has_monthly, :monthly_12m, :maturity_12m, datetime('now','localtime'))
            ON CONFLICT(gmgo_cd) DO UPDATE SET
                r1=excluded.r1, r2=excluded.r2, name=excluded.name,
                div_nm=excluded.div_nm, addr=excluded.addr,
                has_monthly=excluded.has_monthly,
                monthly_12m=excluded.monthly_12m,
                maturity_12m=excluded.maturity_12m,
                updated_at=excluded.updated_at
        """, records)
        conn.commit()


def query_rates(r1=None, keyword=None, only_monthly=False):
    sql = "SELECT * FROM rates WHERE 1=1"
    params = []
    if r1:
        sql += " AND r1 = ?"
        params.append(r1)
    if keyword:
        sql += " AND (name LIKE ? OR r2 LIKE ? OR addr LIKE ?)"
        kw = f"%{keyword}%"
        params += [kw, kw, kw]
    if only_monthly:
        sql += " AND has_monthly = 1 AND monthly_12m IS NOT NULL AND monthly_12m NOT IN ('연0.0%','연0%')"
    sql += " ORDER BY CAST(REPLACE(REPLACE(monthly_12m,'연',''),'%','') AS REAL) DESC NULLS LAST"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_stats():
    with get_conn() as conn:
        total = conn.execute("SELECT COUNT(*) FROM rates").fetchone()[0]
        monthly = conn.execute(
            "SELECT COUNT(*) FROM rates WHERE has_monthly=1 AND monthly_12m NOT IN ('연0.0%','연0%')"
        ).fetchone()[0]
        last = conn.execute(
            "SELECT MAX(updated_at) FROM rates"
        ).fetchone()[0]
        last_scrape = conn.execute(
            "SELECT started_at, finished_at, status FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "total": total,
        "monthly_count": monthly,
        "last_updated": last,
        "last_scrape": dict(last_scrape) if last_scrape else None,
    }


def log_scrape_start():
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO scrape_log (started_at, status) VALUES (datetime('now','localtime'), 'running')"
        )
        conn.commit()
        return cur.lastrowid


def log_scrape_done(log_id, total):
    with get_conn() as conn:
        conn.execute(
            "UPDATE scrape_log SET finished_at=datetime('now','localtime'), total=?, status='done' WHERE id=?",
            (total, log_id)
        )
        conn.commit()
