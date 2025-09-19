# crawler.py
import os
import csv
import time
import datetime as dt
from typing import List, Tuple, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # 無頭環境
import matplotlib.pyplot as plt

# ========= 基本參數 =========
BASE_URL = "https://ecshweb.pchome.com.tw/search/v3.3/all/results"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "zh-TW,zh;q=0.9"}
OUT_DIR = "data"
CHART_DIR = "charts"
DAILY_ALL_CSV = os.path.join(OUT_DIR, "daily.csv")
TZ8 = dt.timezone(dt.timedelta(hours=8))
TODAY = dt.datetime.now(TZ8).date()
TODAY_SNAPSHOT = os.path.join(OUT_DIR, f"pchome_keywords_{TODAY.isoformat()}.csv")
TODAY_LOG = os.path.join(OUT_DIR, f"log_{TODAY.isoformat()}.txt")
TODAY_TOP_RISERS = os.path.join(OUT_DIR, f"top_risers_{TODAY.isoformat()}.csv")

DEFAULT_KEYWORDS = ["洋裝","連身裙","牛仔褲","短裙","雪紡","針織衫","襯衫","西裝外套","風衣","高腰褲"]

# ========= 公用 =========
def log(msg: str):
    os.makedirs(OUT_DIR, exist_ok=True)
    ts = dt.datetime.now(TZ8).strftime("%Y-%m-%d %H:%M:%S")
    with open(TODAY_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(msg)

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

def read_keywords(path: str = "keywords.txt") -> List[str]:
    if not os.path.exists(path):
        log(f"keywords.txt 不存在，改用預設清單：{DEFAULT_KEYWORDS}")
        return DEFAULT_KEYWORDS[:]
    with open(path, "r", encoding="utf-8") as f:
        kws = [ln.strip() for ln in f if ln.strip()]
    # 去重保序
    seen, result = set(), []
    for kw in kws:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)
    return result or DEFAULT_KEYWORDS[:]

def fetch_count(session: requests.Session, keyword: str) -> Tuple[int, str]:
    try:
        r = session.get(BASE_URL, params={"q": keyword, "page": 1}, timeout=15)
        r.raise_for_status()
        j = r.json()
        total = int(j.get("totalRows", 0))
        return total, "ok"
    except Exception as e:
        return 0, f"error: {e}"

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

# ========= 分析與圖表 =========
def compute_growth(df: pd.DataFrame) -> pd.DataFrame:
    """
    針對「今天這一批 rows」計算：
    - 日增率：與昨天（date-1）相比
    - 週增率：與上週同日（date-7）相比
    若沒有比較基準，留空（NaN）。
    """
    df["date"] = pd.to_datetime(df["date"])
    # 載入歷史
    if os.path.exists(DAILY_ALL_CSV):
        hist = pd.read_csv(DAILY_ALL_CSV, encoding="utf-8-sig")
        if len(hist):
            hist["date"] = pd.to_datetime(hist["date"])
        else:
            hist = pd.DataFrame(columns=["date","keyword","count","status"])
    else:
        hist = pd.DataFrame(columns=["date","keyword","count","status"])

    out_rows = []
    for _, row in df.iterrows():
        kw = row["keyword"]
        d0 = row["date"].normalize()
        # 昨天
        d_y = d0 - pd.Timedelta(days=1)
        # 上週同日
        d_w = d0 - pd.Timedelta(days=7)

        base_y = hist.loc[(hist["keyword"]==kw) & (hist["date"]==d_y), "count"]
        base_w = hist.loc[(hist["keyword"]==kw) & (hist["date"]==d_w), "count"]

        count = int(row["count"])
        day_growth = (count - int(base_y.iloc[0]))/int(base_y.iloc[0])*100 if len(base_y) and int(base_y.iloc[0])>0 else None
        week_growth = (count - int(base_w.iloc[0]))/int(base_w.iloc[0])*100 if len(base_w) and int(base_w.iloc[0])>0 else None

        out = dict(row)
        out["day_growth_pct"] = round(day_growth, 2) if day_growth is not None else ""
        out["week_growth_pct"] = round(week_growth, 2) if week_growth is not None else ""
        out_rows.append(out)
    return pd.DataFrame(out_rows)

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(CHART_DIR, exist_ok=True)

def plot_charts():
    """
    讀取 data/daily.csv，把每個 keyword 畫一張 count 趨勢圖到 charts/keyword.png
    """
    if not os.path.exists(DAILY_ALL_CSV):
        return
    df = pd.read_csv(DAILY_ALL_CSV, encoding="utf-8-sig")
    if df.empty:
        return
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["keyword", "date"])

    for kw, g in df.groupby("keyword"):
        plt.figure(figsize=(7,4.2))
        plt.plot(g["date"], g["count"], marker="o")
        plt.title(f"{kw} — PChome 商品數趨勢")
        plt.xlabel("日期")
        plt.ylabel("商品數（熱度代理）")
        plt.grid(True, alpha=0.3)
        # 末點標註
        last = g.iloc[-1]
        plt.annotate(f"{int(last['count'])}", xy=(last["date"], last["count"]),
                     xytext=(5,5), textcoords="offset points")
        fname = os.path.join(CHART_DIR, f"{kw}.png")
        plt.tight_layout()
        plt.savefig(fname, dpi=150)
        plt.close()

def write_top_risers(today_rows: List[Dict]):
    """
    從今天的 rows（已含 week_growth_pct / day_growth_pct），抓出週增率前幾名。
    """
    df = pd.DataFrame(today_rows)
    # 轉數值（空字串轉 NaN）
    for col in ["day_growth_pct","week_growth_pct"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 以週增率排序，保底至少有值且 count 基礎 > 50 避免噪音
    base = df[(df["week_growth_pct"].notna()) & (df["count"] >= 50)].copy()
    if base.empty:
        # 若沒有週比，退而求其次看日比
        base = df[(df["day_growth_pct"].notna()) & (df["count"] >= 50)].copy()
        sort_col = "day_growth_pct"
    else:
        sort_col = "week_growth_pct"

    base = base.sort_values(sort_col, ascending=False)
    top = base.head(10)
    # 輸出
    cols = ["date","keyword","count","day_growth_pct","week_growth_pct","status"]
    top_rows = top[cols].to_dict(orient="records")
    write_csv(TODAY_TOP_RISERS, top_rows, fieldnames=cols)
    log(f"已寫入：{TODAY_TOP_RISERS}")

# ========= Main =========
def main():
    ensure_dirs()
    session = make_session()
    keywords = read_keywords()

    # 1) 今日抓取
    today_rows = []
    for kw in keywords:
        count, msg = fetch_count(session, kw)
        today_rows.append({
            "date": TODAY.isoformat(),
            "keyword": kw,
            "count": int(count),
            "status": msg
        })
        log(f"{kw} → {count} ({msg})")
        time.sleep(1.0)

    # 2) 寫當日快照
    write_csv(TODAY_SNAPSHOT, today_rows, fieldnames=["date","keyword","count","status"])
    log(f"已寫入：{TODAY_SNAPSHOT}")

    # 3) 計算增率（與昨天/上週同日）
    df_today = pd.DataFrame(today_rows)
    df_today_with_growth = compute_growth(df_today)

    # 4) 追加到總表（含增率）
    # 若是新檔，就包含增率欄位；舊檔也會沿用新欄位
    append_csv(
        DAILY_ALL_CSV,
        df_today_with_growth.to_dict(orient="records"),
        fieldnames=["date","keyword","count","status","day_growth_pct","week_growth_pct"],
    )
    log(f"已寫入：{DAILY_ALL_CSV}")

    # 5) 畫圖
    plot_charts()
    log(f"圖表已更新：{CHART_DIR}/*.png")

    # 6) Top 上升關鍵字快照
    write_top_risers(df_today_with_growth.to_dict(orient="records"))

    # 7) Log 提示
    log(f"Log 檔：{TODAY_LOG}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"致命錯誤：{e}")
        raise
