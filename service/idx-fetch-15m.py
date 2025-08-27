# /service/idx-fetch-15m.py
# ============================================
# 15M APPEND FIX (cache_15m)
# - TZ Asia/Jakarta, sesi 09:00–15:50
# - Base UTUH, append pakai window lookback
# - Merge: drop-dup by Datetime (keep last)
# - Overwrite atomik (safe write)
# ============================================

from pathlib import Path
from datetime import datetime
import os
import pandas as pd
import yfinance as yf
import tempfile, shutil

# ---------- PATH ROOT AMAN ----------
ROOT = Path(__file__).resolve().parent.parent   # project root (folder yang berisi 'emiten/')
FOLDER = ROOT / "emiten" / "cache_15m"
FOLDER.mkdir(parents=True, exist_ok=True)

# ---------- CONFIG ----------
YF_PERIOD     = "60d"      # batas aman interval 15m di Yahoo
YF_INTERVAL   = "15m"
SESSION_START = "09:00"
SESSION_END   = "15:50"
LOOKBACK_MIN  = 15
DRY_RUN       = False
STANDARD_COLS = ["Open","High","Low","Close","Adj Close","Volume"]
# ----------------------------

def _atomic_write_csv(fp: Path, df: pd.DataFrame):
    """Tulis atomik di filesystem yang sama."""
    fp.parent.mkdir(parents=True, exist_ok=True)
    tmpfp = fp.with_suffix(fp.suffix + ".tmp")
    df.to_csv(tmpfp, index=False)
    os.replace(tmpfp, fp)

def _parse_jakarta(x: pd.Series) -> pd.Series:
    dt = pd.to_datetime(x, errors="coerce", utc=False)
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("Asia/Jakarta")
    else:
        dt = dt.dt.tz_convert("Asia/Jakarta")
    return dt

def _fetch_fresh_15m(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker, period=YF_PERIOD, interval=YF_INTERVAL,
        auto_adjust=False, threads=False, progress=False
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_COLS)

    # ratakan multiindex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # normalisasi kolom
    if "Price" in df.columns and "Close" in df.columns:
        df = df.drop(columns=["Price"])
    elif "Price" in df.columns and "Close" not in df.columns:
        df = df.rename(columns={"Price": "Close"})

    # pastikan tz → JKT
    idx = df.index
    if getattr(idx, "tz", None) is None:
        df.index = pd.DatetimeIndex(idx).tz_localize("UTC")
    df = df.tz_convert("Asia/Jakarta")

    # filter jam sesi
    df = df.between_time(SESSION_START, SESSION_END)

    # kolom wajib & tipe
    for c in STANDARD_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[STANDARD_COLS]
    for c in STANDARD_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # buang baris kosong total
    df = df.dropna(how="all", subset=["Open","High","Low","Close","Volume"])
    return df

def _read_last_dt(fp: Path):
    if not fp.exists() or fp.stat().st_size == 0:
        return None
    try:
        d = pd.read_csv(fp, usecols=["Datetime"])
        if d.empty:
            return None
        dt = _parse_jakarta(d["Datetime"])
        return dt.max()
    except Exception:
        return None

def _merge_append_write(ticker: str, out_csv: Path) -> dict:
    fresh = _fetch_fresh_15m(ticker)
    if fresh.empty:
        return {"ticker": ticker, "status": "no-fresh", "wrote": False}

    last_dt = _read_last_dt(out_csv)
    sess_today_start = pd.Timestamp(datetime.now().date(), tz="Asia/Jakarta") + pd.Timedelta(hours=9)

    # tentukan merge_start
    if (last_dt is None) or (last_dt < sess_today_start):
        merge_start = sess_today_start
    else:
        merge_start = last_dt - pd.Timedelta(minutes=LOOKBACK_MIN)

    fresh = fresh.loc[fresh.index >= merge_start].copy()

    # baca base lama
    if out_csv.exists() and out_csv.stat().st_size > 0:
        base = pd.read_csv(out_csv, low_memory=False)
        if base.empty or "Datetime" not in base.columns:
            base = pd.DataFrame(columns=["Datetime"] + STANDARD_COLS)
        else:
            base["Datetime"] = _parse_jakarta(base["Datetime"])
            for c in STANDARD_COLS:
                if c not in base.columns:
                    base[c] = pd.NA
            base = base[["Datetime"] + STANDARD_COLS]
    else:
        base = pd.DataFrame(columns=["Datetime"] + STANDARD_COLS)

    # siapkan fresh_out dengan 'Datetime' yang pasti ada
    fresh_out = fresh.reset_index()
    if "index" in fresh_out.columns and "Datetime" not in fresh_out.columns:
        fresh_out = fresh_out.rename(columns={"index": "Datetime"})
    elif "Datetime" not in fresh_out.columns:
        first_col = fresh_out.columns[0]
        fresh_out = fresh_out.rename(columns={first_col: "Datetime"})
    for c in STANDARD_COLS:
        if c not in fresh_out.columns:
            fresh_out[c] = pd.NA
    fresh_out = fresh_out[["Datetime"] + STANDARD_COLS]

    # merge & tulis
    frames = [x for x in (base, fresh_out) if not x.empty]
    if frames:
        merged = (
            pd.concat(frames, ignore_index=True)
              .drop_duplicates(subset=["Datetime"], keep="last")
              .sort_values("Datetime")
        )
    else:
        merged = pd.DataFrame(columns=["Datetime"] + STANDARD_COLS)

    if not DRY_RUN:
        _atomic_write_csv(out_csv, merged)

    return {
        "ticker": ticker,
        "status": "ok",
        "rows_base": len(base),
        "rows_fresh": len(fresh_out),
        "rows_out": len(merged),
        "min_out": merged["Datetime"].min() if not merged.empty else None,
        "max_out": merged["Datetime"].max() if not merged.empty else None,
        "merge_start": merge_start,
        "last_dt_before": last_dt,
        "wrote": not DRY_RUN,
        "file": str(out_csv),
    }

def _run_batch():
    for file in sorted(FOLDER.glob("*.csv")):
        tkr = file.stem.upper()
        res = _merge_append_write(tkr, file)
        if res.get("status") == "ok":
            print(
                f"✅ {tkr} | base={res['rows_base']} fresh={res['rows_fresh']} out={res['rows_out']} | "
                f"{res['min_out']} … {res['max_out']} | wrote={res['wrote']} | file={res['file']}"
            )
        else:
            print(f"⚠️  {tkr} | {res.get('status')} | wrote={res.get('wrote')} | file={file}")

if __name__ == "__main__":
    _run_batch()
