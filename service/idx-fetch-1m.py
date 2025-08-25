# /service/idx-fetch-1m.py
from pathlib import Path
from datetime import datetime
import pandas as pd
import yfinance as yf
import os, tempfile, shutil

# ---------- PATH ROOT AMAN ----------
ROOT = Path(__file__).resolve().parent.parent
FOLDER = ROOT / "emiten" / "cache_1m"
FOLDER.mkdir(parents=True, exist_ok=True)

# ---------- CONFIG ----------
YF_PERIOD      = "5d"
YF_INTERVAL    = "1m"
SESSION_START  = "09:00"
SESSION_END    = "15:50"
LOOKBACK_MIN   = 5
DRY_RUN        = False
# ----------------------------


STANDARD_COLS = ["Open","High","Low","Close","Adj Close","Volume"]

def _atomic_write_csv(fp: Path, df: pd.DataFrame):
    """
    Tulis ke file secara (hampir) atomic di filesystem yg sama:
    - tulis ke <file>.tmp di direktori yg sama
    - os.replace() ganti file lama -> atomic di POSIX jika 1 filesystem
    """
    fp.parent.mkdir(parents=True, exist_ok=True)
    tmpfp = fp.with_suffix(fp.suffix + ".tmp")
    df.to_csv(tmpfp, index=False)
    os.replace(tmpfp, fp)

def _parse_jakarta(x: pd.Series) -> pd.Series:
    dt = pd.to_datetime(x, errors="coerce", utc=False)
    # jika naive → lokalize ke Asia/Jakarta; jika sudah ada tz → convert ke Asia/Jakarta
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("Asia/Jakarta")
    else:
        dt = dt.dt.tz_convert("Asia/Jakarta")
    return dt

def _fetch_fresh_1m(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker, period=YF_PERIOD, interval=YF_INTERVAL,
        auto_adjust=False, threads=False, progress=False
    )
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_COLS)

    # ratakan kolom multiindex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    if "Price" in df.columns and "Close" not in df.columns:
        df = df.rename(columns={"Price":"Close"})

    # pastikan index ber-tz dan di-convert ke Asia/Jakarta
    idx = df.index
    if getattr(idx, "tz", None) is None:
        df.index = pd.DatetimeIndex(idx).tz_localize("UTC")
    df = df.tz_convert("Asia/Jakarta")

    # filter jam sesi
    df = df.between_time(SESSION_START, SESSION_END)

    # lengkapkan kolom & cast numeric
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
    fresh = _fetch_fresh_1m(ticker)
    if fresh.empty:
        return {"ticker": ticker, "status": "no-fresh", "wrote": False}

    last_dt = _read_last_dt(out_csv)
    sess_today_start = pd.Timestamp(datetime.now().date(), tz="Asia/Jakarta") + pd.Timedelta(hours=9)

    # titik mulai merge
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

    # siapkan fresh_out dengan kolom Datetime
    fresh_out = fresh.reset_index()
    if "index" in fresh_out.columns and "Datetime" not in fresh_out.columns:
        fresh_out = fresh_out.rename(columns={"index":"Datetime"})
    elif "Datetime" not in fresh_out.columns:
        # fallback: apa pun nama kolom datetime, jadikan "Datetime"
        first_col = fresh_out.columns[0]
        fresh_out = fresh_out.rename(columns={first_col: "Datetime"})
    for c in STANDARD_COLS:
        if c not in fresh_out.columns:
            fresh_out[c] = pd.NA
    fresh_out = fresh_out[["Datetime"] + STANDARD_COLS]

    # gabung & dedup
    frames = [x for x in (base, fresh_out) if not x.empty]
    if frames:
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["Datetime"], keep="last").sort_values("Datetime")
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
    csv_files = sorted(FOLDER.glob("*.csv"))
    for fp in csv_files:
        print(f"📄 Running on {fp.name} ...")
        ticker = fp.stem.upper()
        res = _merge_append_write(ticker, fp)

        if res.get("status") == "ok":
            print(
                f"✅ {ticker} | base={res['rows_base']} fresh={res['rows_fresh']} out={res['rows_out']} | "
                f"{res['min_out']} … {res['max_out']} | wrote={res['wrote']} | file={res['file']}"
            )
        else:
            print(f"⚠️  {ticker} | {res.get('status')} | wrote={res.get('wrote')} | file={fp}")

if __name__ == "__main__":
    _run_batch()
