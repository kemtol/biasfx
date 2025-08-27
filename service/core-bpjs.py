# /service/core-bpjs.py — v2.6 (1m→5m→15m→daily fallback) + minimal patch cols
from pathlib import Path
from datetime import datetime, date
from collections import defaultdict, Counter
from typing import Optional, Dict, List, Tuple

import os, math, argparse, numpy as np, pandas as pd

ROOT         = Path(__file__).resolve().parents[1]
OUT_DIR      = ROOT / "rekomendasi"
SESSION_TZ   = "Asia/Jakarta"

DEFAULT_CUTOFF_STR     = "09:30"
DEFAULT_TOP_N          = 10
DEFAULT_BASELINE_DAYS  = 60
DEFAULT_PACE_MIN       = 1.2
DEFAULT_RETURN_MIN     = 0.01
DEFAULT_RETURN_MAX     = 0.40

def ensure_outdir():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- dir resolvers ----------
def _resolve_dir(cands: List[Path]) -> Path:
    for p in cands:
        if p and p.exists():
            return p
    return cands[0]

def resolve_1m_dir(cli: Optional[str]) -> Path:
    c = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_1M_DIR"): c.append(Path(os.getenv("INTRADAY_1M_DIR")))
    c += [ROOT/"emiten"/"cache_1m", ROOT/"intraday"/"1m", ROOT/"data"/"idx-1m", ROOT/"data"/"intraday-1m"]
    return _resolve_dir(c)

def resolve_5m_dir(cli: Optional[str]) -> Path:
    c = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_5M_DIR"): c.append(Path(os.getenv("INTRADAY_5M_DIR")))
    c += [ROOT/"emiten"/"cache_5m", ROOT/"intraday"/"5m", ROOT/"data"/"idx-5m", ROOT/"data"/"intraday-5m"]
    return _resolve_dir(c)

def resolve_15m_dir(cli: Optional[str]) -> Path:
    c = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_15M_DIR"): c.append(Path(os.getenv("INTRADAY_15M_DIR")))
    c += [ROOT/"emiten"/"cache_15m", ROOT/"intraday"/"15m", ROOT/"data"/"idx-15m", ROOT/"data"/"intraday-15m"]
    return _resolve_dir(c)

def resolve_daily_dir(cli: Optional[str]) -> Path:
    c = []
    if cli: c.append(Path(cli))
    if os.getenv("DAILY_DIR"): c.append(Path(os.getenv("DAILY_DIR")))
    c += [ROOT/"emiten"/"cache_daily", ROOT/"data"/"idx-daily", ROOT/"data"/"daily"]
    return _resolve_dir(c)

# ---------- IO helpers ----------
def read_intraday(folder: Path, ticker: str) -> Optional[pd.DataFrame]:
    fp = folder / f"{ticker}.csv"
    if not fp.exists(): return None
    try:
        df = pd.read_csv(fp, low_memory=False)
        if "Datetime" in df.columns:
            dt = pd.to_datetime(df["Datetime"], errors="coerce")
        elif "Date" in df.columns and "Time" in df.columns:
            dt = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"].astype(str), errors="coerce")
        elif "Date" in df.columns:
            dt = pd.to_datetime(df["Date"], errors="coerce")
        else:
            return None
        df["Datetime"] = dt
        df["Date"]     = df["Datetime"].dt.date
        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = pd.to_numeric(df["Adj Close"], errors="coerce")
        if "Volume" in df.columns:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
        return df.sort_values("Datetime").reset_index(drop=True)
    except Exception:
        return None

def read_daily_from(folder: Path, ticker: str) -> Optional[pd.DataFrame]:
    fp = folder / f"{ticker}.csv"
    if not fp.exists(): return None
    try:
        df = pd.read_csv(fp, low_memory=False)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
        elif "date" in df.columns:
            df["Date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        else:
            return None
        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = pd.to_numeric(df["Adj Close"], errors="coerce")
        if "Volume" in df.columns:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
        return df.sort_values("Date").reset_index(drop=True)
    except Exception:
        return None

# ---------- metrics ----------
def baseline_volumes_up_to_cutoff(df: pd.DataFrame, work_date: date, cutoff_time, n_days: int) -> List[float]:
    days = sorted([d for d in df["Datetime"].dt.date.unique() if d < work_date])[-n_days:]
    vols = []
    for d in days:
        m = (df["Datetime"].dt.date == d) & (df["Datetime"].dt.time <= cutoff_time)
        v = float(df.loc[m, "Volume"].sum())
        if v > 0: vols.append(v)
    return vols

def price_at_cutoff(df: pd.DataFrame, work_date: date, cutoff_time) -> Optional[float]:
    try:
        m = (df["Datetime"].dt.date == work_date) & (df["Datetime"].dt.time <= cutoff_time)
        sub = df.loc[m]
        if sub.empty: return None
        return float(sub["Close"].iloc[-1])
    except Exception:
        return None

def daily_return_until_cutoff(df: pd.DataFrame, work_date: date, cutoff_time) -> Optional[float]:
    try:
        days = sorted(df["Datetime"].dt.date.unique())
        if work_date not in days: return None
        idx = days.index(work_date)
        if idx == 0: return None
        prev_day = days[idx-1]
        prev_close = float(df.loc[df["Datetime"].dt.date == prev_day, "Close"].tail(1).iloc[0])
        m = (df["Datetime"].dt.date == work_date) & (df["Datetime"].dt.time <= cutoff_time)
        sub = df.loc[m]
        if sub.empty or prev_close <= 0: return None
        cut_close = float(sub["Close"].iloc[-1])
        return (cut_close/prev_close) - 1.0
    except Exception:
        return None

def vol_pace_robust(ticker: str, work_date: date, cutoff_time,
                    df_1m: Optional[pd.DataFrame],
                    df_5m: Optional[pd.DataFrame],
                    df_15m: Optional[pd.DataFrame],
                    folder_daily: Path,
                    baseline_days: int) -> float:
    # 1) 1m
    try:
        if df_1m is not None and not df_1m.empty:
            vols = baseline_volumes_up_to_cutoff(df_1m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(df_1m.loc[(df_1m["Datetime"].dt.date==work_date) &
                                        (df_1m["Datetime"].dt.time<=cutoff_time), "Volume"].sum())
                return max(0.0, today/base)
    except Exception:
        pass
    # 2) 5m
    try:
        if df_5m is not None and not df_5m.empty:
            vols = baseline_volumes_up_to_cutoff(df_5m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(df_5m.loc[(df_5m["Datetime"].dt.date==work_date) &
                                        (df_5m["Datetime"].dt.time<=cutoff_time), "Volume"].sum())
                return max(0.0, today/base)
    except Exception:
        pass
    # 3) 15m
    try:
        if df_15m is not None and not df_15m.empty:
            vols = baseline_volumes_up_to_cutoff(df_15m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(df_15m.loc[(df_15m["Datetime"].dt.date==work_date) &
                                         (df_15m["Datetime"].dt.time<=cutoff_time), "Volume"].sum())
                return max(0.0, today/base)
    except Exception:
        pass
    # 4) daily approx (konservatif)
    try:
        df_d = read_daily_from(folder_daily, ticker)
        if df_d is not None and not df_d.empty and "Volume" in df_d.columns:
            vols = df_d["Volume"].tail(baseline_days).to_numpy()
            base = float(np.median(vols)) if vols.size>0 else 0.0
            if base > 0:
                today_vol_approx = 0.75*base
                return max(0.0, today_vol_approx/base)
    except Exception:
        pass
    return 0.0

def score_row(price: float, pace: float) -> float:
    price_term = math.sqrt(max(price, 0.0))
    pace_term  = math.log1p(min(max(pace, 0.0), 50.0))
    return float(price_term * pace_term)

# ---------- engine ----------
def bpjs_candidates(target_date: date, cutoff_time,
                    baseline_days: int, pace_min: float,
                    ret_min: float, ret_max: float, top_n: int,
                    folder_1m: Path, folder_5m: Path, folder_15m: Path, folder_daily: Path,
                    diag: bool=True) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:

    SUMMARY = []
    drop = defaultdict(list) if diag else None

    universe = sorted(set(p.stem for p in folder_1m.glob("*.csv")) |
                      set(p.stem for p in folder_5m.glob("*.csv")) |
                      set(p.stem for p in folder_15m.glob("*.csv")))

    for ticker in universe:
        try:
            df1  = read_intraday(folder_1m,  ticker)
            df5  = read_intraday(folder_5m,  ticker)
            df15 = read_intraday(folder_15m, ticker)
            # sumber harga/return: 1m → 5m → 15m
            df_px = df1 if df1 is not None else (df5 if df5 is not None else df15)
            if df_px is None:
                if diag: drop[ticker].append("no_intraday")
                continue

            price_cut = price_at_cutoff(df_px, target_date, cutoff_time)
            if price_cut is None or price_cut <= 0:
                if diag: drop[ticker].append("no_price_at_cutoff")
                continue

            ret = daily_return_until_cutoff(df_px, target_date, cutoff_time)
            if ret is None or not (ret_min <= ret <= ret_max):
                if diag: drop[ticker].append("ret_out_of_range")
                continue

            pace = vol_pace_robust(ticker, target_date, cutoff_time, df1, df5, df15, folder_daily, baseline_days)
            if pace < pace_min:
                if diag: drop[ticker].append("pace_too_low")
                continue

            SUMMARY.append({
                "ticker": ticker,
                "price_at_cutoff": price_cut,
                "daily_return": ret,
                "vol_pace": pace,
                "score": score_row(price_cut, pace),
                "last": price_cut,
            })
        except Exception:
            if diag: drop[ticker].append("exception")
            continue

    if not SUMMARY:
        return pd.DataFrame(), (drop or {})

    df = (pd.DataFrame(SUMMARY)
            .sort_values("score", ascending=False)
            .reset_index(drop=True))
    return df, (drop or {})

# ---------- CLI ----------
def main():
    p = argparse.ArgumentParser("bpjs v2.6 core (1m→5m→15m→daily)")
    p.add_argument("--date", help="YYYY-MM-DD (default: today)")
    p.add_argument("--cutoff", action="append",
                   help='HH:MM; repeatable or comma-separated (e.g. --cutoff 09:30 --cutoff 14:15 or "09:30,14:15")')
    p.add_argument("--top", type=int, default=DEFAULT_TOP_N)
    p.add_argument("--baseline-days", type=int, default=DEFAULT_BASELINE_DAYS)
    p.add_argument("--pace-min", type=float, default=DEFAULT_PACE_MIN)
    p.add_argument("--ret-min", type=float, default=DEFAULT_RETURN_MIN)
    p.add_argument("--ret-max", type=float, default=DEFAULT_RETURN_MAX)
    p.add_argument("--min-price", "--minprice", dest="min_price", type=float, default=None)
    p.add_argument("--resolutions", "--fetchlist", dest="resolutions", default=None)
    p.add_argument("--src-1m",   dest="src_1m",   default=None)
    p.add_argument("--src-5m",   dest="src_5m",   default=None)
    p.add_argument("--src-15m",  dest="src_15m",  default=None)
    p.add_argument("--src-daily",dest="src_daily",default=None)
    args = p.parse_args()

    if args.resolutions:
        print(f"[INFO] Resolutions (audit only): {args.resolutions}")

    today = pd.Timestamp("today", tz=SESSION_TZ).normalize().date()
    target_date = date.fromisoformat(args.date) if args.date else today

    F1 = resolve_1m_dir(args.src_1m)
    F5 = resolve_5m_dir(args.src_5m)
    F15= resolve_15m_dir(args.src_15m)
    FD = resolve_daily_dir(args.src_daily)

    ensure_outdir()
    print(f"Hari ini: {today} | target_date: {target_date}")
    print(f"[INFO] 1m dir: {F1}  | files: {len(list(F1.glob('*.csv')))}")
    print(f"[INFO] 5m dir: {F5}  | files: {len(list(F5.glob('*.csv')))}")
    print(f"[INFO] 15m dir: {F15} | files: {len(list(F15.glob('*.csv')))}")
    print(f"[INFO] daily dir: {FD}")

    cutoffs = []
    if not args.cutoff:
        cutoffs = [DEFAULT_CUTOFF_STR]
    else:
        for item in args.cutoff:
            cutoffs += [x.strip() for x in item.split(",") if x.strip()]
    print(f"[INFO] Cutoffs: {', '.join(cutoffs)}")

    any_nonempty = False
    for cstr in cutoffs:
        t = datetime.strptime(cstr, "%H:%M").time()
        df, drop = bpjs_candidates(
            target_date=target_date, cutoff_time=t,
            baseline_days=args.baseline_days, pace_min=args.pace_min,
            ret_min=args.ret_min, ret_max=args.ret_max, top_n=args.top,
            folder_1m=F1, folder_5m=F5, folder_15m=F15, folder_daily=FD, diag=True
        )
        # min-price filter (opsional)
        if args.min_price is not None and not df.empty:
            mp = float(args.min_price)
            m = pd.Series(False, index=df.index)
            if "last" in df.columns:
                m |= pd.to_numeric(df["last"], errors="coerce") > mp
            if "price_at_cutoff" in df.columns:
                m |= pd.to_numeric(df["price_at_cutoff"], errors="coerce") > mp
            df = df.loc[m].reset_index(drop=True)
            print(f"[{cstr}] Min price > {mp} → remain={len(df)}")

        df = df.head(args.top) if not df.empty else df

        # ---- MINIMAL PATCH: pastikan kolom untuk Markov/konsistensi tersedia ----
        if not df.empty:
            if "closing_strength" not in df.columns and "daily_return" in df.columns:
                df["closing_strength"] = df["daily_return"]
            if "afternoon_power" not in df.columns and "score" in df.columns:
                df["afternoon_power"] = df["score"]

            # urutan kolom yang konsisten (opsional)
            preferred = [
                "ticker", "price_at_cutoff", "daily_return",
                "vol_pace", "score", "last",
                "closing_strength", "afternoon_power",
            ]
            df = df[[c for c in preferred if c in df.columns] +
                    [c for c in df.columns if c not in preferred]]
        # ------------------------------------------------------------------------

        hhmm = cstr.replace(":", "")
        outfile = OUT_DIR / f"bpjs_rekomendasi_{target_date}_{hhmm}.csv"
        if df.empty:
            print(f"[{cstr}] ❌ Tidak ada kandidat. (menyimpan file kosong)")
            df.to_csv(outfile, index=False)
        else:
            print(f"\n[{cstr}] [✓] TOP CANDIDATES")
            with pd.option_context("display.max_rows", args.top, "display.max_columns", None, "display.width", 160):
                print(df.head(args.top).to_string(index=False))
            df.to_csv(outfile, index=False)
            any_nonempty = True

            flat = [r for reasons in drop.values() for r in reasons]
            if flat:
                print("\n[DIAG] Alasan drop teratas:")
                for k, v in Counter(flat).most_common(8):
                    print(f"  - {k:>18}: {v}")

    if not any_nonempty:
        print("\n[INFO] Semua cutoff kosong. Cek data & filter.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise
