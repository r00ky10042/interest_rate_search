# -*- coding: utf-8 -*-
import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rates (
                    gmgo_cd      TEXT PRIMARY KEY,
                    r1           TEXT,
                    r2           TEXT,
                    name         TEXT,
                    div_nm       TEXT,
                    addr         TEXT,
                    has_monthly  INTEGER DEFAULT 0,
                    monthly_12m  TEXT,
                    maturity_12m TEXT,
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scrape_log (
                    id          SERIAL PRIMARY KEY,
                    started_at  TIMESTAMP,
                    finished_at TIMESTAMP,
                    total       INTEGER,
                    status      TEXT
                )
            """)
        conn.commit()


def upsert_rates(records):
    if not records:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in records:
                cur.execute("""
                    INSERT INTO rates
                        (gmgo_cd, r1, r2, name, div_nm, addr, has_monthly, monthly_12m, maturity_12m, updated_at)
                    VALUES
                        (%(gmgo_cd)s, %(r1)s, %(r2)s, %(name)s, %(div_nm)s, %(addr)s,
                         %(has_monthly)s, %(monthly_12m)s, %(maturity_12m)s, NOW())
                    ON CONFLICT (gmgo_cd) DO UPDATE SET
                        r1=EXCLUDED.r1, r2=EXCLUDED.r2, name=EXCLUDED.name,
                        div_nm=EXCLUDED.div_nm, addr=EXCLUDED.addr,
                        has_monthly=EXCLUDED.has_monthly,
                        monthly_12m=EXCLUDED.monthly_12m,
                        maturity_12m=EXCLUDED.maturity_12m,
                        updated_at=NOW()
                """, r)
        conn.commit()


def query_rates(r1=None, keyword=None, only_monthly=False):
    sql = "SELECT * FROM rates WHERE 1=1"
    params = []
    if r1:
        sql += " AND r1 = %s"
        params.append(r1)
    if keyword:
        sql += " AND (name ILIKE %s OR r2 ILIKE %s OR addr ILIKE %s)"
        kw = f"%{keyword}%"
        params += [kw, kw, kw]
    if only_monthly:
        sql += " AND has_monthly = 1 AND monthly_12m IS NOT NULL AND monthly_12m NOT IN ('연0.0%','연0%')"
    sql += " ORDER BY CAST(REGEXP_REPLACE(COALESCE(monthly_12m,'0'), '[^0-9.]', '', 'g') AS NUMERIC) DESC NULLS LAST"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def get_stats():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM rates")
            total = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM rates WHERE has_monthly=1 AND monthly_12m NOT IN ('연0.0%','연0%')")
            monthly = cur.fetchone()["cnt"]

            cur.execute("SELECT MAX(updated_at) AS last FROM rates")
            last = cur.fetchone()["last"]

            cur.execute("SELECT started_at, finished_at, status FROM scrape_log ORDER BY id DESC LIMIT 1")
            last_scrape = cur.fetchone()

    return {
        "total":        total,
        "monthly_count": monthly,
        "last_updated": str(last) if last else None,
        "last_scrape":  dict(last_scrape) if last_scrape else None,
    }


def log_scrape_start():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO scrape_log (started_at, status) VALUES (NOW(), 'running') RETURNING id")
            log_id = cur.fetchone()["id"]
        conn.commit()
    return log_id


def log_scrape_done(log_id, total):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_log SET finished_at=NOW(), total=%s, status='done' WHERE id=%s",
                (total, log_id)
            )
        conn.commit()
