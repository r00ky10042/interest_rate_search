# -*- coding: utf-8 -*-
"""기존 kfcc_rates.json → SQLite DB 마이그레이션"""
import json, sys, os
sys.path.insert(0, os.path.dirname(__file__))
import db

SRC = "C:/Users/TM00000002/Desktop/새마을금고금리조회/kfcc_rates.json"

db.init_db()
with open(SRC, encoding="utf-8") as f:
    data = json.load(f)

records = [{
    "gmgo_cd":    r.get("gmgoCd", ""),
    "r1":         r.get("r1", ""),
    "r2":         r.get("r2", ""),
    "name":       r.get("name", ""),
    "div_nm":     r.get("divNm", ""),
    "addr":       r.get("addr", ""),
    "has_monthly":  int(r.get("hasMonthly", False)),
    "monthly_12m":  r.get("monthly12m"),
    "maturity_12m": r.get("maturity12m"),
} for r in data]

db.upsert_rates(records)
print(f"마이그레이션 완료: {len(records)}개")
