# /service/idx-fetch-5m.py
# ============================================
# 5M APPEND + SMART BACKFILL (cache_5m)
# - TZ Asia/Jakarta, sesi 09:00–15:50
# - First run: backfill penuh (coba beberapa period Yahoo)
# - Next runs: efisien (append dgn window lookback)
# - Merge: drop-dup by Datetime (keep last), overwrite atomik
# - Diagnostik ringkas per-ticker (VERBOSE)
# ============================================

from pathlib import Path
from datetime import datetime
import os
import pandas as pd
import yfinance as yf
import tempfile, shutil

# ---------- PATH ROOT AMAN ----------
ROOT = Path(__file__).resolve().parent.parent   # -> project root (folder yg berisi 'emiten/')
FOLDER = ROOT / "emiten" / "cache_5m"
FOLDER.mkdir(parents=True, exist_ok=True)

# ---------- CONFIG ----------
SESSION_START = "09:00"
SESSION_END   = "15:50"
LOOKBACK_MIN  = 15
DRY_RUN       = False
STANDARD_COLS = ["Open","High","Low","Close","Adj Close","Volume"]

# Smart backfill
MIN_UNIQUE_DATES_TARGET            = 5
PERIOD_CANDIDATES_5M               = ["60d", "30d", "1mo", "14d", "7d"]  # Yahoo 5m max ~60d
ALLOW_BACKFILL_IF_BASE_SINGLE_DAY  = True
VERBOSE = True
# ----------------------------

def _atomic_write_csv(fp: Path, df: pd.DataFrame):
    """Atomic overwrite di filesystem yg sama."""
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

def _unique_dates_from_index(dt_index) -> list:
    if dt_index is None or len(dt_index) == 0:
        return []
    idx = pd.DatetimeIndex(dt_index)
    if getattr(idx, "tz", None) is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert("Asia/Jakarta")
    return sorted(pd.Series(idx.date).unique().tolist())

def _fetch_fresh_5m_try(ticker: str, period: str) -> pd.DataFrame:
    df = yf.download(
        ticker, period=period, interval="5m",
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
    if getattr(df.index, "tz", None) is None:
        df.index = pd.DatetimeIndex(df.index).tz_localize("UTC")
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

def _fetch_fresh_5m(ticker: str) -> pd.DataFrame:
    best_df, best_days = pd.DataFrame(columns=STANDARD_COLS), 0
    for per in PERIOD_CANDIDATES_5M:
        df = _fetch_fresh_5m_try(ticker, per)
        days = len(_unique_dates_from_index(df.index)) if not df.empty else 0
        if VERBOSE:
            print(f"    [Yahoo] {ticker} period={per} → rows={len(df)} days={days}")
        if days > best_days:
            best_days, best_df = days, df
        if days >= MIN_UNIQUE_DATES_TARGET:
            break
    return best_df

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

def _read_base(out_csv: Path) -> pd.DataFrame:
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
    return base

def _merge_append_write(ticker: str, out_csv: Path) -> dict:
    fresh = _fetch_fresh_5m(ticker)
    if fresh.empty:
        return {"ticker": ticker, "status": "no-fresh", "wrote": False}

    base   = _read_base(out_csv)
    last_dt = _read_last_dt(out_csv)
    today   = datetime.now().date()
    sess_today_start = pd.Timestamp(today, tz="Asia/Jakarta") + pd.Timedelta(hours=9)

    # deteksi base: berapa tanggal unik
    if not base.empty:
        base_dates = sorted(pd.Series(base["Datetime"].dt.tz_convert("Asia/Jakarta").dt.date).unique().tolist())
        is_base_single_day = (len(base_dates) == 1)
    else:
        base_dates, is_base_single_day = [], False

    # tentukan merge_start
    if (last_dt is None) or (ALLOW_BACKFILL_IF_BASE_SINGLE_DAY and is_base_single_day):
        merge_start = None                         # backfill penuh (ambil semua dari Yahoo)
    elif last_dt < sess_today_start:
        merge_start = sess_today_start             # refill dari awal sesi hari ini
    else:
        merge_start = last_dt - pd.Timedelta(minutes=LOOKBACK_MIN)  # append dgn window

    if VERBOSE:
        fresh_days = _unique_dates_from_index(fresh.index)
        print(f"    [Diag] base_days={base_dates} | fresh_days={fresh_days} | last_dt={last_dt} | merge_start={merge_start}")

    # filter fresh jika perlu
    if merge_start is not None:
        fresh = fresh.loc[fresh.index >= merge_start].copy()

    # bentuk fresh_out (pastikan kolom 'Datetime' ada)
    fresh_idx = pd.DatetimeIndex(fresh.index)
    fresh_idx.name = "Datetime"
    fresh_out = fresh.copy()
    fresh_out.index = fresh_idx
    fresh_out = fresh_out.reset_index()
    for c in STANDARD_COLS:
        if c not in fresh_out.columns:
            fresh_out[c] = pd.NA
    fresh_out = fresh_out[["Datetime"] + STANDARD_COLS]

    # merge & tulis (overwrite atomik)
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
        "base_days": base_dates if base_dates else [],
        "fresh_days": _unique_dates_from_index(fresh.index),
    }

def _run_batch():
    files = sorted(FOLDER.glob("*.csv"))
    for file in files:
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
