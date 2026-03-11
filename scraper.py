# -*- coding: utf-8 -*-
import re
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

REGIONS = [
    "서울", "인천", "경기", "강원", "충남", "충북", "대전", "세종",
    "경북", "경남", "대구", "부산", "울산", "전북", "전남", "광주", "제주"
]
CONCURRENCY = 100

_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.kfcc.co.kr/map/list.do",
})


def _fetch(path, data=None):
    try:
        url = f"https://www.kfcc.co.kr{path}"
        r = _session.post(url, data=data, timeout=15) if data else _session.get(url, timeout=15)
        r.encoding = "utf-8"
        return r.text
    except Exception:
        return ""


def _parse_banks(html):
    banks = []
    for row in re.findall(r'<tr[^>]*class="ac"[^>]*>([\s\S]*?)</tr>', html):
        spans = {m[0]: m[1].strip() for m in re.findall(r'title="([^"]+)">([^<]*)', row)}
        if not spans.get("gmgoCd"):
            continue
        addr = spans.get("addr", "")
        r2 = spans.get("r2") or (addr.split()[1] if len(addr.split()) > 1 else "")
        banks.append({
            "gmgoCd": spans["gmgoCd"],
            "name":   spans.get("gmgoNm") or spans.get("name", ""),
            "divNm":  spans.get("divNm", ""),
            "addr":   addr,
            "r1":     spans.get("r1", ""),
            "r2":     r2,
        })
    return banks


def _max_page(html):
    nums = [int(n) for n in re.findall(r"pageNo=(\d+)", html) if int(n) > 0]
    return max(nums) if nums else 1


def _parse_rate(html):
    m = re.search(r'id="divTmp1"([\s\S]*?)(?=id="divTmp2"|id="divTmp4"|</div>\s*</div>\s*</div>\s*</div>)', html)
    if not m:
        return None
    section = m.group(0)
    th = re.search(r"<thead>([\s\S]*?)</thead>", section)
    has_monthly = bool(th and "월지급식" in th.group(1))
    tb = re.search(r"<tbody>([\s\S]*?)</tbody>", section)
    if not tb:
        return None
    result = {"has_monthly": has_monthly, "monthly_12m": None, "maturity_12m": None}
    for row in re.findall(r"<tr>([\s\S]*?)</tr>", tb.group(1)):
        tds = [re.sub(r"<[^>]+>", "", td).strip()
               for td in re.findall(r"<td[^>]*>([\s\S]*?)</td>", row)]
        if not any("12개월" in t for t in tds):
            continue
        pi = next((i for i, t in enumerate(tds) if "개월" in t or "년" in t), -1)
        if pi >= 0:
            result["monthly_12m"]  = tds[pi+1] if has_monthly and pi+1 < len(tds) else None
            result["maturity_12m"] = tds[pi+2] if has_monthly and pi+2 < len(tds) else (tds[pi+1] if not has_monthly and pi+1 < len(tds) else None)
        break
    return result


def get_all_banks(log_cb=None):
    def fetch_page(r1, page):
        return _fetch(f"/map/list.do?r1={requests.utils.quote(r1)}&r2=&pageNo={page}")

    with ThreadPoolExecutor(max_workers=len(REGIONS)) as ex:
        first = {ex.submit(fetch_page, r1, 1): r1 for r1 in REGIONS}
        first_htmls = {fut.result(): REGIONS[list(first.keys()).index(f)] for f in first for fut in [f]}
        first_html_map = {}
        for fut, r1 in first.items():
            first_html_map[r1] = fut.result()

    extra_tasks = []
    for r1 in REGIONS:
        mp = _max_page(first_html_map[r1])
        if log_cb:
            log_cb(f"  {r1}: {mp}페이지")
        for p in range(2, mp + 1):
            extra_tasks.append((r1, p))

    seen, banks = set(), []
    all_htmls = list(first_html_map.values())

    if extra_tasks:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
            futs = [ex.submit(fetch_page, r1, p) for r1, p in extra_tasks]
            all_htmls += [f.result() for f in as_completed(futs)]

    for html in all_htmls:
        for b in _parse_banks(html):
            if b["gmgoCd"] not in seen:
                seen.add(b["gmgoCd"])
                banks.append(b)
    return banks


def scrape_all(existing_codes=None, log_cb=None, progress_cb=None):
    existing_codes = existing_codes or set()
    banks = get_all_banks(log_cb)
    new_banks = [b for b in banks if b["gmgoCd"] not in existing_codes]

    if log_cb:
        log_cb(f"총 {len(banks)}개 금고 | 신규 {len(new_banks)}개")

    done = [0]
    results = []

    def fetch_one(bank):
        html = _fetch("/map/goods_19.do", data={"OPEN_TRMID": bank["gmgoCd"], "gubuncode": "13"})
        rate = _parse_rate(html) if html and len(html) > 100 else None
        done[0] += 1
        if progress_cb:
            progress_cb(done[0], len(new_banks))
        return {
            "gmgo_cd":    bank["gmgoCd"],
            "r1":         bank["r1"],
            "r2":         bank["r2"],
            "name":       bank["name"],
            "div_nm":     bank["divNm"],
            "addr":       bank["addr"],
            "has_monthly":  int(rate["has_monthly"]) if rate else 0,
            "monthly_12m":  rate["monthly_12m"]  if rate else None,
            "maturity_12m": rate["maturity_12m"] if rate else None,
        }

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futs = [ex.submit(fetch_one, b) for b in new_banks]
        for f in as_completed(futs):
            results.append(f.result())

    return results
