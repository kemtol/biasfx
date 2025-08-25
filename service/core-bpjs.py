# /service/core_bpjs.py
# ======================================================================
# bpjs v2.4-core — Intraday Spike Detector (IHSG) — CLI Service
# ----------------------------------------------------------------------
# - Core engine tanpa hardcode daftar ARA/evaluator
# - Otomatis pilih "latest trading day" <= TODAY jika data hari ini belum ada
# - TZ: Asia/Jakarta, cutoff configurable (default 09:30)
# - Filters: daily_return 1%–40%, vol_pace > 1.2x
# - Score v2.2: price_term * log1p(min(pace, 50))
# - vol_pace fallback: 1m → 5m → daily (daily dikoreksi faktor 0.75)
# - Diagnostics ringkas: ringkasan alasan drop (Counter)
# - Output: Top-N (default 10) + simpan CSV ke root/rekomendasi/
# ======================================================================

from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple, Dict
from collections import defaultdict, Counter
import argparse
import pandas as pd
import numpy as np
import sys

# ---------- PATH ROOT & FOLDERS ----------
ROOT = Path(__file__).resolve().parent.parent   # project root
FOLDER_1M    = ROOT / "emiten" / "cache_1m"
FOLDER_5M    = ROOT / "emiten" / "cache_5m"
FOLDER_DAILY = ROOT / "emiten" / "cache_daily"
OUT_DIR      = ROOT / "rekomendasi"
OUT_DIR.mkdir(exist_ok=True)

SESSION_TZ = "Asia/Jakarta"

# ================== DEFAULT CONFIG (bisa di-override CLI) ==================
DEFAULT_BASELINE_DAYS = 60
DEFAULT_PACE_MIN      = 1.2
DEFAULT_RETURN_MIN    = 0.01
DEFAULT_RETURN_MAX    = 0.40
DEFAULT_TOP_N         = 10
DEFAULT_CUTOFF_STR    = "09:30"   # ubah via --cutoff, mis. "14:15"
# ==========================================================================

def to_jkt(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    try:
        if s.dt.tz is None:
            return s.dt.tz_localize(SESSION_TZ)
        return s.dt.tz_convert(SESSION_TZ)
    except Exception:
        s = pd.to_datetime(s, errors="coerce", utc=True).dt.tz_convert(SESSION_TZ)
        return s

def pick_work_date(df_dt: pd.Series, today_date: date) -> Optional[date]:
    dlist = pd.Series(df_dt.dt.date.unique()).dropna().sort_values().tolist()
    if not dlist:
        return None
    for d in reversed(dlist):
        if d <= today_date:
            return d
    return None

def read_daily_flex(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, low_memory=False)
        # map date col
        for dc in ("Date", "date", "Datetime"):
            if dc in df.columns:
                df["Date"] = pd.to_datetime(df[dc], errors="coerce").dt.date
                break
        else:
            return None
        # map price/volume
        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = pd.to_numeric(df["Adj Close"], errors="coerce")
        if "Volume" in df.columns:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
        if "Close" not in df.columns or "Volume" not in df.columns:
            return None
        return df.dropna(subset=["Date", "Close", "Volume"]).sort_values("Date").reset_index(drop=True)
    except Exception:
        return None

def read_intraday(folder: Path, ticker: str) -> Optional[pd.DataFrame]:
    fp = folder / f"{ticker}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp, low_memory=False)
        if "Datetime" not in df.columns:
            return None
        df["Datetime"] = to_jkt(df["Datetime"])
        for c in ("Open", "High", "Low", "Close", "Volume"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["Datetime", "Close", "Volume"])
    except Exception:
        return None

def intraday_cut_volume(df_intraday: pd.DataFrame, work_date: date, cutoff_time) -> float:
    mask = (df_intraday["Datetime"].dt.date == work_date) & (df_intraday["Datetime"].dt.time <= cutoff_time)
    return float(df_intraday.loc[mask, "Volume"].sum())

def intraday_hist_cut_volumes(df_intraday: pd.DataFrame, work_date: date, n_days: int, cutoff_time) -> List[float]:
    days = sorted([d for d in df_intraday["Datetime"].dt.date.unique() if d < work_date])[-n_days:]
    vols = []
    for d in days:
        m = (df_intraday["Datetime"].dt.date == d) & (df_intraday["Datetime"].dt.time <= cutoff_time)
        v = float(df_intraday.loc[m, "Volume"].sum())
        if v > 0:
            vols.append(v)
    return vols

def vol_pace_robust(ticker: str, work_date: date, cutoff_time, vol_today_cut_1m: Optional[float],
                    df_1m: Optional[pd.DataFrame], baseline_days: int) -> float:
    """Return pace; fallback 1m → 5m → daily (daily dikoreksi 0.75 utk cutoff)."""
    # 1) 1m baseline
    try:
        if df_1m is not None:
            vol_today_cut = vol_today_cut_1m if vol_today_cut_1m is not None else intraday_cut_volume(df_1m, work_date, cutoff_time)
            vols_hist_1m = intraday_hist_cut_volumes(df_1m, work_date, baseline_days, cutoff_time)
            if len(vols_hist_1m) >= 10:
                base_1m = float(np.median(vols_hist_1m))
                if base_1m > 0:
                    return vol_today_cut / base_1m
    except Exception:
        pass
    # 2) 5m baseline
    try:
        df_5m = read_intraday(FOLDER_5M, ticker)
        if df_5m is not None:
            vol_today_cut_5m = intraday_cut_volume(df_5m, work_date, cutoff_time)
            vols_hist_5m = intraday_hist_cut_volumes(df_5m, work_date, baseline_days, cutoff_time)
            if len(vols_hist_5m) >= 10:
                base_5m = float(np.median(vols_hist_5m))
                if base_5m > 0:
                    return vol_today_cut_5m / base_5m
    except Exception:
        pass
    # 3) Daily baseline (coarser)
    try:
        df_daily = read_daily_flex(FOLDER_DAILY / f"{ticker}.csv")
        if df_daily is not None:
            hist_daily = df_daily[df_daily["Date"] < work_date].tail(baseline_days)
            if len(hist_daily) >= 20:
                base_daily = float(hist_daily["Volume"].median())
                if base_daily > 0:
                    vol_today_cut = vol_today_cut_1m
                    if vol_today_cut is None:
                        try:
                            if 'df_5m' in locals() and df_5m is not None:
                                vol_today_cut = intraday_cut_volume(df_5m, work_date, cutoff_time)
                        except Exception:
                            pass
                    if vol_today_cut is None:
                        return np.nan
                    return vol_today_cut / (base_daily * 0.75)
    except Exception:
        pass
    return np.nan

def detect_latest_intraday_date(folder_1m: Path, today_date: date) -> Optional[date]:
    """Scan ringan: cari tanggal kerja terbaru yang tersedia di 1m (<= today)."""
    latest = None
    for fp in folder_1m.glob("*.csv"):
        try:
            d = pd.read_csv(fp, usecols=["Datetime"], low_memory=False)
            dt = to_jkt(d["Datetime"])
            wd = pick_work_date(dt, today_date)
            if wd and (latest is None or wd > latest):
                latest = wd
        except Exception:
            continue
    return latest

def bpjs_candidates(target_date: date,
                    cutoff_time,
                    baseline_days: int,
                    pace_min: float,
                    ret_min: float,
                    ret_max: float,
                    top_n: int,
                    diag: bool = True) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    SUMMARY = []
    drop_reasons = defaultdict(list) if diag else None

    for fp in sorted(FOLDER_1M.glob("*.csv")):
        ticker = fp.stem
        try:
            df_1m = read_intraday(FOLDER_1M, ticker)
            if df_1m is None:
                if diag: drop_reasons[ticker].append("no_1m_file_or_parse_fail")
                continue

            work_date = pick_work_date(df_1m["Datetime"], target_date)
            if (work_date is None) or (work_date != target_date):
                if diag: drop_reasons[ticker].append(f"not_target_date:{work_date}")
                continue

            df_dwork = df_1m[(df_1m["Datetime"].dt.date == work_date)].copy().dropna(subset=["Close", "Volume"])
            if df_dwork.empty or df_dwork["Volume"].sum() == 0:
                if diag: drop_reasons[ticker].append("no_intraday_or_zero_vol")
                continue

            # prev close dari daily
            df_daily = read_daily_flex(FOLDER_DAILY / f"{ticker}.csv")
            prev_close = np.nan
            if df_daily is not None:
                prev_day = df_daily[df_daily["Date"] < work_date]
                if not prev_day.empty:
                    prev_close = pd.to_numeric(prev_day.iloc[-1]["Close"], errors="coerce")
            if pd.isna(prev_close) or prev_close <= 0:
                if diag: drop_reasons[ticker].append("no_prev_close_daily")
                continue

            # metrik dasar
            high_px = float(df_dwork['High'].max())
            low_px  = float(df_dwork['Low'].min())
            last_px = float(df_dwork['Close'].iloc[-1])
            daily_return = (last_px / prev_close) - 1.0
            if not (ret_min < daily_return < ret_max):
                if diag: drop_reasons[ticker].append("daily_return_out_of_range")
                continue

            vol_today_cut_1m = intraday_cut_volume(df_1m, work_date, cutoff_time)
            pace = vol_pace_robust(ticker, work_date, cutoff_time, vol_today_cut_1m, df_1m, baseline_days)
            if not (pd.notna(pace) and pace > pace_min):
                if diag: drop_reasons[ticker].append(f"pace_insufficient:{pace}")
                continue

            # metrik lanjutan
            daily_range = high_px - low_px
            closing_strength = (last_px - low_px) / daily_range if daily_range > 0 else 1.0

            start_time = df_dwork['Datetime'].min()
            first_5min = df_dwork[df_dwork['Datetime'] <= start_time + timedelta(minutes=5)]
            if not first_5min.empty and first_5min['Volume'].sum() > 0:
                stable_open = float((first_5min["Close"] * first_5min["Volume"]).sum() / first_5min["Volume"].sum())
            else:
                stable_open = float(df_dwork['Open'].iloc[0])
            afternoon_power = (last_px / stable_open) - 1.0 if stable_open > 0 else 0.0

            # skor v2.2
            price_term  = (1 + daily_return) * (1 + max(0.0, afternoon_power)) * closing_strength
            volume_term = np.log1p(min(pace, 50))
            score = price_term * volume_term

            # harga pada cutoff (opsional diagnostik)
            cut_mask = df_dwork["Datetime"].dt.time <= cutoff_time
            price_at_cutoff = float(df_dwork.loc[cut_mask, "Close"].iloc[-1]) if cut_mask.any() else np.nan

            SUMMARY.append({
                "ticker": ticker, "date": work_date, "score": score, "last": last_px,
                "daily_return": daily_return, "closing_strength": closing_strength,
                "afternoon_power": afternoon_power, "vol_pace": pace,
                "price_at_cutoff": price_at_cutoff
            })

        except Exception as e:
            if diag: drop_reasons[ticker].append(f"exception:{type(e).__name__}")
            continue

    cols = ["ticker","date","score","last","daily_return","closing_strength","afternoon_power","vol_pace","price_at_cutoff"]
    df_result = (pd.DataFrame(SUMMARY)[cols].sort_values("score", ascending=False).reset_index(drop=True)
                 if SUMMARY else pd.DataFrame(columns=cols))
    return df_result, (drop_reasons or {})

def format_table(df: pd.DataFrame, top_n: int) -> str:
    def fmt_pct(x):  return f"{x:,.2%}" if pd.notna(x) else "N/A"
    def fmt_x(x):    return f"{x:.2f}x"   if pd.notna(x) else "N/A"
    def fmt_f3(x):   return f"{x:.3f}"    if pd.notna(x) else "N/A"

    out = df.copy()
    if not out.empty:
        out.loc[:, "score"]            = out["score"].map(fmt_f3)
        out.loc[:, "daily_return"]     = out["daily_return"].map(fmt_pct)
        out.loc[:, "closing_strength"] = out["closing_strength"].map(fmt_pct)
        out.loc[:, "afternoon_power"]  = out["afternoon_power"].map(fmt_pct)
        out.loc[:, "vol_pace"]         = out["vol_pace"].map(fmt_x)
    return out.head(top_n).to_string(index=False)

def main():
    parser = argparse.ArgumentParser(description="bpjs v2.4-core — Intraday Spike Detector")
    parser.add_argument("--date", help="Override target date (YYYY-MM-DD). Default: auto latest <= today")
    # ↓↓↓ sekarang bisa multi --cutoff, atau 1 argumen dipisah koma
    parser.add_argument("--cutoff", action="append", help='Cutoff HH:MM. Bisa dipakai berulang: --cutoff 09:30 --cutoff 14:15. '
                                                          'Atau pisahkan dengan koma: "09:30,14:15"')
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N, help="Top-N to output & save (default 10)")
    parser.add_argument("--baseline-days", type=int, default=DEFAULT_BASELINE_DAYS, help="Baseline days (default 60)")
    parser.add_argument("--pace-min", type=float, default=DEFAULT_PACE_MIN, help="Min volume pace (default 1.2)")
    parser.add_argument("--ret-min", type=float, default=DEFAULT_RETURN_MIN, help="Min daily return (default 0.01)")
    parser.add_argument("--ret-max", type=float, default=DEFAULT_RETURN_MAX, help="Max daily return (default 0.40)")
    args = parser.parse_args()

    today = pd.Timestamp("today", tz=SESSION_TZ).normalize().date()

    # pilih target_date
    if args.date:
        target_date = date.fromisoformat(args.date)
    else:
        latest = detect_latest_intraday_date(FOLDER_1M, today)
        target_date = latest or today

    # siapkan daftar cutoff
    cutoffs: list[str]
    if not args.cutoff:
        cutoffs = [DEFAULT_CUTOFF_STR]           # default 1 cutoff (09:30)
    else:
        # flatten: support --cutoff a --cutoff b, atau "--cutoff a,b"
        cutoffs = []
        for item in args.cutoff:
            cutoffs.extend([x.strip() for x in item.split(",") if x.strip()])

    print(f"Hari ini: {today} | target_date: {target_date}")
    print(f"[INFO] 1m files: {len(list(FOLDER_1M.glob('*.csv')))} | root={ROOT}")
    print(f"[INFO] Cutoffs: {', '.join(cutoffs)}")

    # proses setiap cutoff dan simpan file per cutoff
    any_nonempty = False
    for cstr in cutoffs:
        cutoff_time = datetime.strptime(cstr, "%H:%M").time()
        df_result, drop_reasons = bpjs_candidates(
            target_date=target_date,
            cutoff_time=cutoff_time,
            baseline_days=args.baseline_days,
            pace_min=args.pace_min,
            ret_min=args.ret_min,
            ret_max=args.ret_max,
            top_n=args.top,
            diag=True,
        )

        hhmm = cstr.replace(":", "")
        outfile = OUT_DIR / f"bpjs_rekomendasi_{target_date}_{hhmm}.csv"

        if df_result.empty:
            print(f"[{cstr}] ❌ Tidak ada kandidat yang lolos. (menyimpan file kosong)")
            df_result.head(args.top).to_csv(outfile, index=False)
        else:
            print(f"\n[{cstr}] [✓] TOP CANDIDATES — bpjs v2.4-core")
            print(f"(work_date = {target_date}, cutoff = {cstr}, filters: return {int(args.ret_min*100)}–{int(args.ret_max*100)}%, pace > {args.pace_min}x)")
            print(format_table(df_result, args.top))
            df_result.head(args.top).to_csv(outfile, index=False)
            any_nonempty = True

            # Ringkasan alasan drop (Top 8)
            flat_reasons = [r for reasons in drop_reasons.values() for r in reasons]
            if flat_reasons:
                print("\n[DIAG] Alasan drop teratas:")
                for k, v in Counter(flat_reasons).most_common(8):
                    print(f"- {k}: {v}")

        print(f"[{cstr}] [✔] Disimpan ke: {outfile}")

    if not any_nonempty:
        print("\n[INFO] Semua cutoff menghasilkan tabel kosong. Cek data & filter.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
