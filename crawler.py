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
DAILY_ALL_CSV = os.path.join(OUT_DIR, "daily.csv")
TZ8 = dt.timezone(dt.timedelta(hours=8))
TODAY = dt.datetime.now(TZ8).date()
TODAY_SNAPSHOT = os.path.join(OUT_DIR, f"pchome_keywords_{TODAY.isoformat()}.csv")
TODAY_LOG = os.path.join(OUT_DIR, f"log_{TODAY.isoformat()}.txt")

DEFAULT_KEYWORDS = ["洋裝","連身裙","牛仔褲","短裙","雪紡","針織衫","襯衫","西裝外套","風衣","高腰褲"]

def log(msg: str):
    os.makedirs(OUT_DIR, exist_ok=True)
    ts = dt.datetime.now(TZ8).strftime("%Y-%m-%d %H:%M:%S")
    with open(TODAY_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

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
    try:
        r = session.get(BASE_URL, params={"q": keyword, "page": 1}, timeout=15)
        r.raise_for_status()
        j = r.json()
        total = int(j.get("totalRows", 0))
        return total, "ok"
    except Exception as e:
        return 0, f"error: {e}"

def read_keywords(path: str = "keywords.txt") -> List[str]:
    if not os.path.exists(path):
        log(f"keywords.txt 不存在，改用預設清單：{DEFAULT_KEYWORDS}")
        return DEFAULT_KEYWORDS[:]
    with open(path, "r", encoding="utf-8") as f:
        kws = [ln.strip() for ln in f if ln.strip()]
    # 去重並保序
    seen, result = set(), []
    for kw in kws:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result or DEFAULT_KEYWORDS[:]

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
    os.makedirs(OUT_DIR, exist_ok=True)
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
        log(f"{kw} → {count} ({msg})")
        time.sleep(1.0)

    append_csv(DAILY_ALL_CSV, results, fieldnames=["date", "keyword", "count", "status"])
    write_csv(TODAY_SNAPSHOT, results, fieldnames=["date", "keyword", "count", "status"])

    log(f"已寫入：{DAILY_ALL_CSV}")
    log(f"已寫入：{TODAY_SNAPSHOT}")
    log(f"Log 檔：{TODAY_LOG}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"致命錯誤：{e}")
        raise
