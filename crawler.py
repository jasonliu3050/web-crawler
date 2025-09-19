# crawler.py
import csv
import os
import time
import datetime as dt
from typing import List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://ecshweb.pchome.com.tw/search/v3.3/all/results"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "zh-TW,zh;q=0.9"
}
OUT_DIR = "data"
DAILY_ALL_CSV = os.path.join(OUT_DIR, "daily.csv")  # 累積所有天
TODAY = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).date()  # 以台北時間為準
TODAY_SNAPSHOT = os.path.join(OUT_DIR, f"pchome_keywords_{TODAY.isoformat()}.csv")

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

def fetch_count(session: requests.Session, keyword: str) -> Tuple[int, str]:
    """回傳 (商品總數, 訊息)。若失敗，商品總數回 0 並帶訊息。"""
    try:
        r = session.get(BASE_URL, params={"q": keyword, "page": 1}, timeout=15)
        r.raise_for_status()
        j = r.json()
        total = int(j.get("totalRows", 0))
        return total, "ok"
    except Exception as e:
        return 0, f"error: {e}"

def read_keywords(path: str = "keywords.txt") -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        kws = [ln.strip() for ln in f if ln.strip()]
    # 去重、保持順序
    seen, result = set(), []
    for kw in kws:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result

def ensure_dir():
    os.makedirs(OUT_DIR, exist_ok=True)

def append_csv(path: str, rows: List[dict], fieldnames: List[str]):
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def write_csv(path: str, rows: List[dict], fieldnames: List[str]):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    ensure_dir()
    session = make_session()
    keywords = read_keywords()

    results = []
    for kw in keywords:
        count, msg = fetch_count(session, kw)
        results.append({
            "date": TODAY.isoformat(),
            "keyword": kw,
            "count": count,
            "status": msg
        })
        print(f"{kw:10s} → {count:6d}  ({msg})")
        time.sleep(1.2)  # 禮貌性延遲

    # 1) 累積總表
    append_csv(DAILY_ALL_CSV, results, fieldnames=["date", "keyword", "count", "status"])
    # 2) 今日快照
    write_csv(TODAY_SNAPSHOT, results, fieldnames=["date", "keyword", "count", "status"])

if __name__ == "__main__":
    main()
