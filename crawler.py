# crawler.py
import os
import csv
import time
import datetime as dt
from typing import List, Tuple, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import matplotlib
matplotlib.use("Agg")
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

SCHEMA_V1 = ["date","keyword","count","status"]
SCHEMA_V2 = ["date","keyword","count","status","day_growth_pct","week_growth_pct"]  # 目標欄位

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

def safe_read_csv(path: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    更彈性的讀取：容忍壞行、不同欄位數。
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception as e:
        log(f"讀取 {path} 失敗：{e}，改用 engine='python'")
        df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip", engine="python")
    if usecols:
        # 缺少的欄位補上
        for c in usecols:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[usecols]
    return df

def upgrade_daily_schema_if_needed():
    """
    若 daily.csv 是舊版 4 欄，升級為新版 6 欄（補空值）後回寫。
    """
    if not os.path.exists(DAILY_ALL_CSV):
        return
    # 先用寬鬆方式讀
    df = safe_read_csv(DAILY_ALL_CSV)
    if df.empty:
        return
    cols = list(df.columns)
    # 判斷常見情況：完全符合 v1
    if cols == SCHEMA_V1:
        for c in ["day_growth_pct","week_growth_pct"]:
            df[c] = pd.NA
        df = df[SCHEMA_V2]
        write_csv(DAILY_ALL_CSV, df.to_dict(orient="records"), fieldnames=SCHEMA_V2)
        log("已升級 daily.csv schema：v1 → v2（補上 day_growth_pct / week_growth_pct）")
    # 若欄位包含我們要的就不動；若少其中一個也補齊
    else:
        changed = False
        for c in ["day_growth_pct","week_growth_pct"]:
            if c not in df.columns:
                df[c] = pd.NA
                changed = True
        if changed:
            # 重排欄位，盡量用 v2 順序，缺的擺最後
            ordered = [c for c in SCHEMA_V2 if c in df.columns] + [c for c in df.columns if c not in SCHEMA_V2]
            df = df[ordered]
            write_csv(DAILY_ALL_CSV, df.to_dict(orient="records"), fieldnames=list(df.columns))
            log("已修補 daily.csv 缺少欄位，完成對齊。")

# ========= 分析與圖表 =========
def compute_growth(df_today: pd.DataFrame) -> pd.DataFrame:
    """
    對今天資料計算：
    - day_growth_pct 與昨天相比
    - week_growth_pct 與上週同日相比
    """
    df_today["date"] = pd.to_datetime(df_today["date"])
    # 讀歷史（用彈性讀法）
    hist = safe_read_csv(DAILY_ALL_CSV)
    if not hist.empty:
        # 只取我們需要的欄位，避免舊檔雜訊
        for c in ["date","keyword","count"]:
            if c not in hist.columns:
                hist[c] = pd.NA
        hist = hist[["date","keyword","count"]].copy()
        hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
        hist = hist.dropna(subset=["date","keyword","count"])
    else:
        hist = pd.DataFrame(columns=["date","keyword","count"])

    out_rows = []
    for _, row in df_today.iterrows():
        kw = row["keyword"]
        d0 = row["date"].normalize()
        count = int(row["count"])

        d_y = d0 - pd.Timedelta(days=1)
        d_w = d0 - pd.Timedelta(days=7)

        base_y = hist.loc[(hist["keyword"]==kw) & (hist["date"]==d_y), "count"]
        base_w = hist.loc[(hist["keyword"]==kw) & (hist["date"]==d_w), "count"]

        def pct(curr: int, base_series: pd.Series) -> Optional[float]:
            if len(base_series)==0:
                return None
            try:
                base = int(base_series.iloc[0])
                if base <= 0:
                    return None
                return (curr - base) / base * 100.0
            except Exception:
                return None

        day_growth = pct(count, base_y)
        week_growth = pct(count, base_w)

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
    只取必要欄位來畫圖，避免欄位不一致導致失敗。
    """
    if not os.path.exists(DAILY_ALL_CSV):
        return
    df = safe_read_csv(DAILY_ALL_CSV)
    if df.empty:
        return

    # 確保欄位存在
    for c in ["date","keyword","count"]:
        if c not in df.columns:
            log("daily.csv 欄位不足，略過畫圖。")
            return

    df = df[["date","keyword","count"]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")
    df = df.dropna(subset=["date","keyword","count"])
    if df.empty:
        return

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
        try:
            plt.annotate(f"{int(last['count'])}", xy=(last["date"], last["count"]),
                         xytext=(5,5), textcoords="offset points")
        except Exception:
            pass
        fname = os.path.join(CHART_DIR, f"{kw}.png")
        plt.tight_layout()
        plt.savefig(fname, dpi=150)
        plt.close()

def write_top_risers(today_rows: List[Dict]):
    df = pd.DataFrame(today_rows)
    for col in ["day_growth_pct","week_growth_pct"]:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # 以週增率排序為主，沒有週比就用日比
    base = df[(df["week_growth_pct"].notna()) & (df["count"] >= 50)].copy()
    sort_col = "week_growth_pct"
    if base.empty:
        base = df[(df["day_growth_pct"].notna()) & (df["count"] >= 50)].copy()
        sort_col = "day_growth_pct"
    if base.empty:
        # 真的沒有比較基準，就用當日數量排序
        base = df.copy()
        sort_col = "count"

    top = base.sort_values(sort_col, ascending=False).head(10)
    cols = ["date","keyword","count","day_growth_pct","week_growth_pct","status"]
    for c in cols:
        if c not in top.columns:
            top[c] = pd.NA
    top_rows = top[cols].to_dict(orient="records")
    write_csv(TODAY_TOP_RISERS, top_rows, fieldnames=cols)
    log(f"已寫入：{TODAY_TOP_RISERS}")

# ========= Main =========
def main():
    ensure_dirs()
    # 先升級舊檔 schema（若需要）
    upgrade_daily_schema_if_needed()

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
    write_csv(TODAY_SNAPSHOT, today_rows, fieldnames=SCHEMA_V1)
    log(f"已寫入：{TODAY_SNAPSHOT}")

    # 3) 計算增率（與昨天/上週同日）
    df_today = pd.DataFrame(today_rows)
    df_today_with_growth = compute_growth(df_today)

    # 4) 追加到總表（含增率）
    append_csv(DAILY_ALL_CSV,
               df_today_with_growth.to_dict(orient="records"),
               fieldnames=SCHEMA_V2)
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
