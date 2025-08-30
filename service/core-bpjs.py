#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
BPJS Core — Intraday Markup Detector & Top‑Picks Generator (v3.1 - Fixed)
================================================================================

Tujuan
------
Mendeteksi **titik awal markup** pada timeframe 1‑menit dan menghasilkan kandidat
**BPJS (Beli Pagi Jual Sore)**/intraday picks yang siap dipublikasikan ke
frontend. Skrip ini membaca cache OHLCV (1m/5m/15m/daily), menghitung fitur
utama (ZVol minute‑of‑day, ΔVWAP%, persistensi N/M, proxy spread), menerapkan
**gating** (drop reason) dan memberi **skor** untuk pemeringkatan.

Arsitektur jalur data (ujung ke ujung)
--------------------------------------
1) **Fetcher/Cache** (di luar file ini):
   - Direktori: `emiten/cache_1m`, `emiten/cache_5m`, `emiten/cache_15m`, `emiten/cache_daily`
2) **Core (file ini)**:
   - Dibungkus oleh runner (`run_slot.sh` via `cron_wrap.sh`).
   - Menghasilkan file CSV per cutoff: `rekomendasi/bpjs_rekomendasi_YYYY-MM-DD_HHMM.csv`.
3) **Publisher** (di luar file ini):
   - `kv_sync.sh` → unggah CSV ke Cloudflare KV/R2.
4) **API** (Cloudflare Worker):
   - Endpoint `/api/reko/latest...` hanya **membaca** CSV/summary dari KV.
5) **Frontend**:
   - Render nilai (rekomendasi, probabilitas, skor) → **tidak menghitung** probabilitas.

Pipeline per cutoff (ringkas)
-----------------------------
- **Input**: OHLCV intraday + harian untuk `target_date` sampai `cutoff`.
- **Filter awal**: range return harian wajar, pace volume relatif (median historis),
  harga > min (opsional).
- **Fitur markup (baru)**: ZVol(1m) minute‑of‑day, ΔVWAP%, persistensi N/M menit,
  proxy micro spread.
- **Gating**: drop jika ZVol rendah, ΔVWAP% < ambang, persistensi < N, atau spread lebar.
- **Scoring**: skor dasar × booster halus dari ZVol/ΔVWAP/persistensi, penalti spread.
- **Output**: TOP N kandidat (CSV) + diagnostik (drop reasons pada stdout).

Cara pakai cepat
----------------
Normal (tulis CSV):
    python service/core-bpjs.py \
      --cutoff 15:50 --top 10 \
      --src-1m emiten/cache_1m --src-5m emiten/cache_5m \
      --src-15m emiten/cache_15m --src-daily emiten/cache_daily

Dry‑run (tanpa tulis CSV) & debug satu ticker:
    python service/core-bpjs.py \
      --cutoff 15:50 --dry-run --dry-ticker YULE.JK \
      --src-1m emiten/cache_1m --src-5m emiten/cache_5m \
      --src-15m emiten/cache_15m --src-daily emiten/cache_daily

Tuning threshold utama (opsional):
    python service/core-bpjs.py --zvol-min 1.5 --persist-m 6 --persist-n 3 \
      --micro-spread-max 0.8 --vwap-delta-min 0.2

Output CSV — kolom penting
--------------------------
- ticker, price_at_cutoff, daily_return, vol_pace
- zvol, vwap_delta_pct, persist_n, persist_m, micro_spread_pct
- afternoon_power, buy_1_pct  # <-- KOLOM YANG DIPERBAIKI
- score, last, flags

Catatan desain
--------------
- **Timezone**: Asia/Jakarta; tanggal default = hari ini.
- **ZVol minute‑of‑day**: menghilangkan bias kurva U (ramai di open/close).
- **Persistensi N/M**: menapis spike tunggal; N dari M menit terakhir.
- **ΔVWAP%**: harga relatif terhadap biaya rata‑rata intraday.
- **Proxy spread**: eksekusi realistis; spread lebar → turunkan kualitas.

Lisensi/Limitasi
----------------
Skrip ini ditujukan untuk riset/eksekusi internal. Akurasi bergantung pada
kualitas data cache; tangani saham illiquid dengan hati‑hati.
"""

# =============================================================================
# GATING & TUNING RINGKAS (Dokumentasi argumen CLI)
# -----------------------------------------------------------------------------
# Gate baru (ketat → kandidat lebih sedikit, precision naik):
# - ZVol(1m) < threshold            → drop     (--zvol-min)
# - ΔVWAP% < threshold              → drop     (--vwap-delta-min)
#   (Close ~ VWAP juga dianggap lemah)
# - Persistensi N/M kurang          → drop     (--persist-m, --persist-n)
#   (menghindari spike tunggal)
# - Micro-spread terlalu lebar      → drop     (--micro-spread-max)
#
# Filter lama (tetap aktif, bisa memangkas kandidat):
# - Return harian di luar rentang   → drop     (--ret-min, --ret-max)
# - Pace volume relatif < min       → drop     (--pace-min)
# - Harga minimum (opsional)        → filter   (--min-price)
#
# Cara cepat “balikin” jumlah kandidat (tanpa ubah kode):
# (Pilih salah satu/combination via CLI)
#   --zvol-min 1.2           # longgarkan syarat anomali volume
#   --persist-m 6 --persist-n 2
#                            # kurangi ketatnya persistensi
#   --vwap-delta-min 0.0     # jangan pakai ΔVWAP% sebagai gate keras
#   --micro-spread-max 1.2   # toleransi spread lebih lebar (small caps)
#   --ret-min 0.00 --ret-max 0.60
#                            # buka rentang return harian
#
# Contoh preset praktis:
# - Pagi (noise tinggi, ketat):
#     --zvol-min 1.8 --persist-n 3 --vwap-delta-min 0.2 --micro-spread-max 1.0
# - Tengah hari (likuiditas turun, longgar):
#     --zvol-min 1.2 --persist-n 2 --vwap-delta-min 0.0 --micro-spread-max 1.2
# - Menjelang tutup (momentum nyata, medium):
#     --zvol-min 1.5 --persist-n 3 --vwap-delta-min 0.1 --micro-spread-max 1.0
#
# Diagnosa cepat (tanpa tulis CSV):
#   python service/core-bpjs.py --cutoff 15:50 --dry-run \
#     --src-1m emiten/cache_1m --src-5m emiten/cache_5m \
#     --src-15m emiten/cache_15m --src-daily emiten/cache_daily
# Lihat [DIAG] "alasan drop teratas" untuk tahu gate mana yang paling sering
# membatasi; longgarkan satu per satu sesuai kebutuhan.
# =============================================================================

from pathlib import Path
from datetime import datetime, date, time
from collections import defaultdict, Counter
from typing import Optional, Dict, List, Tuple

import os, math, argparse, numpy as np, pandas as pd

# ============================================================================
# SECTION: Konstanta & Default
# ============================================================================
ROOT         = Path(__file__).resolve().parents[1]
OUT_DIR      = ROOT / "rekomendasi"
SESSION_TZ   = "Asia/Jakarta"

DEFAULT_CUTOFF_STR     = "09:30,11:30,14:15,15:50"
DEFAULT_TOP_N          = 10
DEFAULT_BASELINE_DAYS  = 60        # median historis untuk pace volume
DEFAULT_PACE_MIN       = 1.2
DEFAULT_RETURN_MIN     = 0.01
DEFAULT_RETURN_MAX     = 0.40

# Fitur markup (baru)
DEFAULT_PROF_DAYS      = 7         # hari historis untuk profil minute-of-day (ZVol)
DEFAULT_ZVOL_MIN       = 1.5
DEFAULT_PERSIST_M      = 6
DEFAULT_PERSIST_N      = 3
DEFAULT_SPREAD_MAX     = 0.8       # %
DEFAULT_VWAP_DELTA_MIN = 0.2       # %

# --- FITUR BARU (FIX) ---
SESSION_1_END = time(11, 30)
SESSION_2_START = time(13, 30)

def ensure_outdir() -> None:
    """Pastikan direktori output rekomendasi tersedia."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# SECTION: Lokasi Data (Resolver Direktori)
# ============================================================================

def _resolve_dir(cands: List[Path]) -> Path:
    """Kembalikan direktori pertama yang ada dari kandidat daftar path."""
    for p in cands:
        if p and p.exists():
            return p
    return cands[0]


def resolve_1m_dir(cli: Optional[str]) -> Path:
    """Tentukan folder sumber data 1‑menit (prioritas: CLI → ENV → default)."""
    c: List[Path] = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_1M_DIR"): c.append(Path(os.getenv("INTRADAY_1M_DIR")))
    c += [ROOT/"emiten"/"cache_1m", ROOT/"intraday"/"1m", ROOT/"data"/"idx-1m", ROOT/"data"/"intraday-1m"]
    return _resolve_dir(c)


def resolve_5m_dir(cli: Optional[str]) -> Path:
    """Tentukan folder sumber data 5‑menit (prioritas: CLI → ENV → default)."""
    c: List[Path] = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_5M_DIR"): c.append(Path(os.getenv("INTRADAY_5M_DIR")))
    c += [ROOT/"emiten"/"cache_5m", ROOT/"intraday"/"5m", ROOT/"data"/"idx-5m", ROOT/"data"/"intraday-5m"]
    return _resolve_dir(c)


def resolve_15m_dir(cli: Optional[str]) -> Path:
    """Tentukan folder sumber data 15‑menit (prioritas: CLI → ENV → default)."""
    c: List[Path] = []
    if cli: c.append(Path(cli))
    if os.getenv("INTRADAY_15M_DIR"): c.append(Path(os.getenv("INTRADAY_15M_DIR")))
    c += [ROOT/"emiten"/"cache_15m", ROOT/"intraday"/"15m", ROOT/"data"/"idx-15m", ROOT/"data"/"intraday-15m"]
    return _resolve_dir(c)


def resolve_daily_dir(cli: Optional[str]) -> Path:
    """Tentukan folder sumber data harian (prioritas: CLI → ENV → default)."""
    c: List[Path] = []
    if cli: c.append(Path(cli))
    if os.getenv("DAILY_DIR"): c.append(Path(os.getenv("DAILY_DIR")))
    c += [ROOT/"emiten"/"cache_daily", ROOT/"data"/"idx-daily", ROOT/"data"/"daily"]
    return _resolve_dir(c)

# ============================================================================
# SECTION: IO Helpers — Pembacaan Data
# ============================================================================

def read_intraday(folder: Path, ticker: str) -> Optional[pd.DataFrame]:
    """Baca CSV intraday untuk `ticker` dan siapkan kolom standar.

    Menghasilkan DataFrame berurutan waktu dengan kolom: Datetime, Date,
    Open/High/Low/Close (atau Adj Close→Close), Volume.
    """
    fp = folder / f"{ticker}.csv"
    if not fp.exists():
        return None
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

        for col in ["Open","High","Low","Close","Adj Close","Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = df["Adj Close"]

        df["Volume"] = df.get("Volume", 0).fillna(0.0)
        return df.sort_values("Datetime").reset_index(drop=True)
    except Exception:
        return None


def read_daily_from(folder: Path, ticker: str) -> Optional[pd.DataFrame]:
    """Baca data harian untuk `ticker` dan siapkan kolom standar (Date, OHLC, Volume)."""
    fp = folder / f"{ticker}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp, low_memory=False)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
        elif "date" in df.columns:
            df["Date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        else:
            return None

        for col in ["Open","High","Low","Close","Adj Close","Volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "Close" not in df.columns and "Adj Close" in df.columns:
            df["Close"] = df["Adj Close"]

        df["Volume"] = df.get("Volume", 0).fillna(0.0)
        return df.sort_values("Date").reset_index(drop=True)
    except Exception:
        return None

# ============================================================================
# SECTION: Metrik Dasar (Sebelumnya)
# ============================================================================

def baseline_volumes_up_to_cutoff(df: pd.DataFrame, work_date: date, cutoff_time: time, n_days: int) -> List[float]:
    """Kumpulkan total volume hingga `cutoff` untuk N hari historis sebelum `work_date`."""
    days = sorted([d for d in df["Datetime"].dt.date.unique() if d < work_date])[-n_days:]
    vols: List[float] = []
    for d in days:
        m = (df["Datetime"].dt.date == d) & (df["Datetime"].dt.time <= cutoff_time)
        v = float(df.loc[m, "Volume"].sum())
        if v > 0:
            vols.append(v)
    return vols


def price_at_cutoff(df: pd.DataFrame, work_date: date, cutoff_time: time) -> Optional[float]:
    """Ambil harga penutupan bar terakhir pada `work_date` hingga `cutoff`."""
    try:
        m = (df["Datetime"].dt.date == work_date) & (df["Datetime"].dt.time <= cutoff_time)
        sub = df.loc[m]
        if sub.empty:
            return None
        return float(sub["Close"].iloc[-1])
    except Exception:
        return None


def daily_return_until_cutoff(df: pd.DataFrame, work_date: date, cutoff_time: time) -> Optional[float]:
    """Hitung return harian dari close hari sebelumnya → close terkini (<= `cutoff`)."""
    try:
        days = sorted(df["Datetime"].dt.date.unique())
        if work_date not in days:
            return None
        idx = days.index(work_date)
        if idx == 0:
            return None
        prev_day   = days[idx-1]
        prev_close = float(df.loc[df["Datetime"].dt.date == prev_day, "Close"].tail(1).iloc[0])
        m = (df["Datetime"].dt.date == work_date) & (df["Datetime"].dt.time <= cutoff_time)
        sub = df.loc[m]
        if sub.empty or prev_close <= 0:
            return None
        cut_close = float(sub["Close"].iloc[-1])
        return (cut_close/prev_close) - 1.0
    except Exception:
        return None


def vol_pace_robust(
    ticker: str,
    work_date: date,
    cutoff_time: time,
    df_1m: Optional[pd.DataFrame],
    df_5m: Optional[pd.DataFrame],
    df_15m: Optional[pd.DataFrame],
    folder_daily: Path,
    baseline_days: int,
) -> float:
    """Ukuran **pace volume** relatif (today vs median historis) dengan fallback berlapis.

    Urutan prioritas: 1m → 5m → 15m → approx daily. Return 0.0 bila gagal.
    """
    # 1) 1m
    try:
        if df_1m is not None and not df_1m.empty:
            vols = baseline_volumes_up_to_cutoff(df_1m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(
                    df_1m.loc[
                        (df_1m["Datetime"].dt.date == work_date)
                        & (df_1m["Datetime"].dt.time <= cutoff_time),
                        "Volume",
                    ].sum()
                )
                return max(0.0, today / base)
    except Exception:
        pass
    # 2) 5m
    try:
        if df_5m is not None and not df_5m.empty:
            vols = baseline_volumes_up_to_cutoff(df_5m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(
                    df_5m.loc[
                        (df_5m["Datetime"].dt.date == work_date)
                        & (df_5m["Datetime"].dt.time <= cutoff_time),
                        "Volume",
                    ].sum()
                )
                return max(0.0, today / base)
    except Exception:
        pass
    # 3) 15m
    try:
        if df_15m is not None and not df_15m.empty:
            vols = baseline_volumes_up_to_cutoff(df_15m, work_date, cutoff_time, baseline_days)
            base = float(np.median(vols)) if vols else 0.0
            if base > 0:
                today = float(
                    df_15m.loc[
                        (df_15m["Datetime"].dt.date == work_date)
                        & (df_15m["Datetime"].dt.time <= cutoff_time),
                        "Volume",
                    ].sum()
                )
                return max(0.0, today / base)
    except Exception:
        pass
    # 4) daily approx (konservatif)
    try:
        df_d = read_daily_from(folder_daily, ticker)
        if df_d is not None and not df_d.empty and "Volume" in df_d.columns:
            vols = df_d["Volume"].tail(baseline_days).to_numpy()
            base = float(np.median(vols)) if vols.size > 0 else 0.0
            if base > 0:
                today_vol_approx = 0.75 * base
                return max(0.0, today_vol_approx / base)
    except Exception:
        pass
    return 0.0


def score_row(price: float, pace: float) -> float:
    """Skor dasar (monoton naik terhadap harga & pace volume)."""
    price_term = math.sqrt(max(price, 0.0))
    pace_term  = math.log1p(min(max(pace, 0.0), 50.0))
    return float(price_term * pace_term)

# ============================================================================
# SECTION: Fitur Markup (Baru)
# ============================================================================

def minute_key(t: pd.Timestamp) -> str:
    """Kunci minute-of-day dalam format 'HH:MM'."""
    return t.strftime("%H:%M")


def build_minute_profile(df1m: pd.DataFrame, work_date: date, hist_days: int) -> Dict[str, Tuple[float, float]]:
    """Bangun profil **minute-of-day** untuk volume: { 'HH:MM': (mean, std) }.

    Sumber: N hari sebelum `work_date` (hist_days). Berguna untuk ZVol per menit.
    """
    prof: Dict[str, Tuple[float, float]] = {}
    if df1m is None or df1m.empty:
        return prof
    mask_hist = df1m["Datetime"].dt.date < work_date
    if not mask_hist.any():
        return prof
    days = sorted(df1m.loc[mask_hist, "Datetime"].dt.date.unique())[-hist_days:]
    m = df1m["Datetime"].dt.date.isin(days)
    g = df1m.loc[m, ["Datetime", "Volume"]].copy()
    g["mod"] = g["Datetime"].dt.strftime("%H:%M")

    grp = g.groupby("mod")["Volume"]
    mean = grp.mean()
    std  = grp.std(ddof=0).fillna(0.0)
    for k in mean.index:
        prof[k] = (float(mean.loc[k]), float(std.loc[k]))
    return prof


def zvol_current(
    df1m: Optional[pd.DataFrame],
    work_date: date,
    cutoff_time: time,
    prof: Dict[str, Tuple[float, float]],
) -> Optional[float]:
    """Hitung **ZVol** (Z‑score volume) pada bar terakhir ≤ cutoff dibanding profil minute‑of‑day."""
    if df1m is None or df1m.empty or not prof:
        return None
    m = (df1m["Datetime"].dt.date == work_date) & (df1m["Datetime"].dt.time <= cutoff_time)
    sub = df1m.loc[m, ["Datetime", "Volume"]]
    if sub.empty:
        return None
    vol = float(sub["Volume"].iloc[-1])
    key = minute_key(sub["Datetime"].iloc[-1])
    mu, sd = prof.get(key, (0.0, 0.0))
    if sd <= 1e-9:
        return None
    return (vol - mu) / sd


def vwap_and_delta(df_any: pd.DataFrame, work_date: date, cutoff_time: time) -> Tuple[Optional[float], Optional[float]]:
    """VWAP kumulatif hingga cutoff dan ΔVWAP% (= (Close − VWAP)/VWAP × 100) pada bar terakhir."""
    m = (df_any["Datetime"].dt.date == work_date) & (df_any["Datetime"].dt.time <= cutoff_time)
    sub = df_any.loc[m, ["Datetime", "Close", "Volume"]].copy()
    if sub.empty:
        return None, None
    pv = (pd.to_numeric(sub["Close"], errors="coerce").fillna(0.0) *
          pd.to_numeric(sub["Volume"], errors="coerce").fillna(0.0))
    v  = pd.to_numeric(sub["Volume"], errors="coerce").fillna(0.0)
    vwap = float(pv.cumsum().iloc[-1] / max(v.cumsum().iloc[-1], 1.0))
    last = float(sub["Close"].iloc[-1])
    if vwap <= 0:
        return vwap, None
    delta_pct = (last / vwap - 1.0) * 100.0
    return vwap, delta_pct


def micro_spread_pct(df_any: pd.DataFrame, work_date: date, cutoff_time: time, window: int = 5) -> Optional[float]:
    """Proxy micro spread (%). Default merata‑rata 5 bar terakhir.

    Prefer `mean((High−Low)/Close)×100`; fallback ke range Close bila kolom High/Low tidak tersedia.
    """
    m = (df_any["Datetime"].dt.date == work_date) & (df_any["Datetime"].dt.time <= cutoff_time)
    sub = df_any.loc[m].tail(window)
    if sub.empty:
        return None
    if ("High" in sub.columns and "Low" in sub.columns and
        sub["High"].notna().all() and sub["Low"].notna().all()):
        rng = (sub["High"] - sub["Low"]).abs() / sub["Close"].replace(0, np.nan)
        return float((rng * 100.0).mean())
    rng = (sub["Close"].max() - sub["Close"].min()) / max(sub["Close"].iloc[-1], 1e-9)
    return float(rng * 100.0)


def persistency_nm(
    df1m: Optional[pd.DataFrame],
    work_date: date,
    cutoff_time: time,
    prof: Dict[str, Tuple[float, float]],
    M: int,
    zvol_min_a: float,
    zvol_min_b: float,
) -> Tuple[int, int]:
    """Hitung **persistensi**: dalam M menit terakhir, N menit memenuhi
        (ret_1m>0 & ZVol≥A) **atau** (Close>VWAP & ZVol≥B).
    """
    if df1m is None or df1m.empty or not prof:
        return (0, M)
    m = (df1m["Datetime"].dt.date == work_date) & (df1m["Datetime"].dt.time <= cutoff_time)
    sub = df1m.loc[m, ["Datetime", "Close", "Volume"]].tail(M + 1).copy()
    if len(sub) < 2:
        return (0, M)

    pv = (sub["Close"] * sub["Volume"]).fillna(0.0)
    v  = sub["Volume"].fillna(0.0)
    vwap_series = (pv.cumsum() / v.cumsum().replace(0, np.nan))

    sub["mod"]  = sub["Datetime"].dt.strftime("%H:%M")
    sub["mu"]   = sub["mod"].map(lambda k: prof.get(k, (0.0, 0.0))[0])
    sub["sd"]   = sub["mod"].map(lambda k: prof.get(k, (0.0, 0.0))[1])
    sub["zvol"] = (sub["Volume"] - sub["mu"]) / sub["sd"].replace(0, np.nan)

    sub["ret1m"] = sub["Close"].pct_change().fillna(0.0)
    cond_a = (sub["ret1m"] > 0) & (sub["zvol"] >= zvol_min_a)
    cond_b = (sub["Close"] > vwap_series) & (sub["zvol"] >= zvol_min_b)

    N = int((cond_a.tail(M) | cond_b.tail(M)).sum())
    return (N, M)

# --- FITUR BARU (FIX) ---
def calculate_afternoon_power(df_any: pd.DataFrame, work_date: date, cutoff_time: time) -> Optional[float]:
    """Hitung rasio volume sesi siang terhadap total volume hari itu."""
    if cutoff_time < SESSION_2_START:
        return 0.0
    m = (df_any["Datetime"].dt.date == work_date) & (df_any["Datetime"].dt.time <= cutoff_time)
    sub = df_any.loc[m]
    if sub.empty:
        return None
    
    total_vol = sub["Volume"].sum()
    if total_vol <= 0:
        return 0.0
    
    afternoon_vol = sub.loc[sub["Datetime"].dt.time >= SESSION_2_START, "Volume"].sum()
    return float(afternoon_vol / total_vol) if total_vol > 0 else 0.0

def calculate_buy_1_pct_proxy(df1m: pd.DataFrame, work_date: date, cutoff_time: time, window: int = 15) -> Optional[float]:
    """Proxy untuk buy pressure: % volume pada bar hijau (Close>Open) dalam N menit terakhir."""
    m = (df1m["Datetime"].dt.date == work_date) & (df1m["Datetime"].dt.time <= cutoff_time)
    sub = df1m.loc[m, ["Open", "Close", "Volume"]].tail(window)
    if sub.empty:
        return None
    
    total_vol = sub["Volume"].sum()
    if total_vol <= 0:
        return 0.0
        
    buy_vol = sub.loc[sub["Close"] > sub["Open"], "Volume"].sum()
    return float(buy_vol / total_vol) if total_vol > 0 else 0.0
# --- AKHIR FITUR BARU ---

def clamp(x: float, lo: float, hi: float) -> float:
    """Batasi nilai x pada selang [lo, hi]."""
    return max(lo, min(hi, x))

# ============================================================================
# SECTION: Engine — Seleksi Kandidat & Skoring
# ============================================================================

def bpjs_candidates(
    target_date: date,
    cutoff_time: time,
    baseline_days: int,
    pace_min: float,
    ret_min: float,
    ret_max: float,
    top_n: int,
    folder_1m: Path,
    folder_5m: Path,
    folder_15m: Path,
    folder_daily: Path,
    # fitur baru
    prof_days: int,
    zvol_min: float,
    persist_M: int,
    persist_N: int,
    spread_max: float,
    vwap_delta_min: float,
    diag: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """Bangun daftar kandidat untuk satu cutoff.

    Returns
    -------
    df : DataFrame terurut skor desc (bisa kosong). Kolom utama:
         ticker, price_at_cutoff, daily_return, vol_pace,
         zvol, vwap_delta_pct, persist_n, persist_m, micro_spread_pct,
         afternoon_power, buy_1_pct,
         score, last, flags
    drop: dict[ticker]→list alasan drop (jika `diag=True`).
    """
    SUMMARY: List[Dict] = []
    drop: Optional[Dict[str, List[str]]] = defaultdict(list) if diag else None

    universe = sorted(
        set(p.stem for p in folder_1m.glob("*.csv"))
        | set(p.stem for p in folder_5m.glob("*.csv"))
        | set(p.stem for p in folder_15m.glob("*.csv"))
    )

    for ticker in universe:
        try:
            df1  = read_intraday(folder_1m, ticker)
            df5  = read_intraday(folder_5m, ticker)
            df15 = read_intraday(folder_15m, ticker)
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

            pace = vol_pace_robust(
                ticker, target_date, cutoff_time, df1, df5, df15, folder_daily, baseline_days
            )
            if pace < pace_min:
                if diag: drop[ticker].append("pace_too_low")
                continue

            # ---------- Fitur markup (baru) ----------
            prof = build_minute_profile(df1, target_date, prof_days) if df1 is not None else {}
            zvol = zvol_current(df1, target_date, cutoff_time, prof) if df1 is not None else None
            vwap, dvwap = vwap_and_delta(df_px, target_date, cutoff_time)
            spr  = micro_spread_pct(df_px, target_date, cutoff_time, window=5)
            pn, pm = (
                persistency_nm(
                    df1, target_date, cutoff_time, prof, persist_M,
                    zvol_min_a=zvol_min, zvol_min_b=max(1.2, zvol_min - 0.3)
                )
                if df1 is not None
                else (0, persist_M)
            )
            
            # --- PERBAIKAN: Hitung metrik yang hilang ---
            apower = calculate_afternoon_power(df_px, target_date, cutoff_time)
            buy_pct = calculate_buy_1_pct_proxy(df1, target_date, cutoff_time) if df1 is not None else None
            # --- AKHIR PERBAIKAN ---

            # ---------- Gating ----------
            gated = False
            if zvol is not None and zvol < zvol_min:
                gated = True
                if diag: drop[ticker].append("zvol_low")
            if dvwap is not None and dvwap < vwap_delta_min:
                gated = True
                if diag: drop[ticker].append("vwap_delta_low")
            if pn < persist_N:
                gated = True
                if diag: drop[ticker].append("no_persist")
            if spr is not None and spr > spread_max:
                gated = True
                if diag: drop[ticker].append("wide_spread")
            if gated:
                continue

            # ---------- Skor ----------
            base = score_row(price_cut, pace)
            flags: List[str] = []

            if zvol is not None and zvol > 0:
                boost = clamp(zvol / 5.0, 0.0, 0.30)    # maks +30%
                base *= (1.0 + boost)
                flags.append(f"+zvol{boost:+.2f}")
            if dvwap is not None and dvwap > 0:
                boost = clamp(dvwap / 100.0, 0.0, 0.20) # 1% → 0.01; maks +20%
                base *= (1.0 + boost)
                flags.append(f"+vwap{boost:+.2f}")
            if pm > 0:
                boost = clamp((pn / pm) * 0.10, 0.0, 0.10)  # maks +10%
                base *= (1.0 + boost)
                flags.append(f"+persist{boost:+.2f}")
            if spr is not None and spr > 0:
                if spr > spread_max:
                    pen = clamp((spr - spread_max) / (2 * spread_max), 0.0, 0.30)
                    base *= (1.0 - pen)
                    flags.append(f"-spread{pen:+.2f}")

            SUMMARY.append(
                {
                    "ticker": ticker,
                    "price_at_cutoff": price_cut,
                    "daily_return": ret,
                    "vol_pace": pace,
                    "zvol": None if zvol is None else float(zvol),
                    "vwap_delta_pct": None if dvwap is None else float(dvwap),
                    "persist_n": int(pn),
                    "persist_m": int(pm),
                    "micro_spread_pct": None if spr is None else float(spr),
                    "afternoon_power": None if apower is None else float(apower),
                    "buy_1_pct": None if buy_pct is None else float(buy_pct),
                    "score": float(base),
                    "last": price_cut,
                    "flags": "|".join(flags) if flags else "",
                    "vwap": None if vwap is None else float(vwap),  # <-- TAMBAHAN
                }
            )
        except Exception:
            if diag:
                drop[ticker].append("exception")
            continue

    if not SUMMARY:
        return pd.DataFrame(), (drop or {})

    df = pd.DataFrame(SUMMARY).sort_values("score", ascending=False).reset_index(drop=True)
    return df, (drop or {})

# ============================================================================
# SECTION: Dry‑Run — Debug Minute‑by‑Minute
# ============================================================================

def dry_run_debug(
    df1m: pd.DataFrame,
    work_date: date,
    cutoff_time: time,
    prof_days: int,
    zvol_min: float,
    persist_M: int,
    persist_N: int,
    spread_max: float,
    vwap_delta_min: float,
) -> None:
    """Cetak tabel menit‑per‑menit menjelang cutoff + keputusan gating terakhir.

    Berguna untuk tuning threshold & validasi visual pada 10–12 bar terakhir.
    """
    prof = build_minute_profile(df1m, work_date, prof_days)
    m = (df1m["Datetime"].dt.date == work_date) & (df1m["Datetime"].dt.time <= cutoff_time)
    sub = df1m.loc[m, ["Datetime", "Open", "High", "Low", "Close", "Volume"]].copy()
    if sub.empty:
        print("[DRY] Tidak ada data untuk tanggal/cutoff ini.")
        return

    pv = (sub["Close"] * sub["Volume"]).fillna(0.0)
    v  = sub["Volume"].fillna(0.0)
    sub["vwap"] = (pv.cumsum() / v.cumsum().replace(0, np.nan))
    sub["mod"]  = sub["Datetime"].dt.strftime("%H:%M")
    sub["mu"]   = sub["mod"].map(lambda k: prof.get(k, (0.0, 0.0))[0])
    sub["sd"]   = sub["mod"].map(lambda k: prof.get(k, (0.0, 0.0))[1])
    sub["zvol"] = (sub["Volume"] - sub["mu"]) / sub["sd"].replace(0, np.nan)
    sub["ret1m_pct"] = sub["Close"].pct_change().fillna(0.0) * 100.0
    sub["dVWAP_pct"] = (sub["Close"] / sub["vwap"] - 1.0) * 100.0

    if "High" in sub.columns and "Low" in sub.columns:
        ms = ((sub["High"] - sub["Low"]).abs() / sub["Close"].replace(0, np.nan)) * 100.0
    else:
        ms = ((sub["Close"].rolling(5).max() - sub["Close"].rolling(5).min()) / sub["Close"]) * 100.0
    sub["micro_spread_pct"] = ms.rolling(5, min_periods=1).mean()

    sub["condA"] = (sub["ret1m_pct"] > 0) & (sub["zvol"] >= zvol_min)
    sub["condB"] = (sub["Close"] > sub["vwap"]) & (sub["zvol"] >= max(1.2, zvol_min - 0.3))

    show = sub.tail(max(10, persist_M + 1))[
        [
            "Datetime",
            "Close",
            "Volume",
            "vwap",
            "zvol",
            "dVWAP_pct",
            "ret1m_pct",
            "micro_spread_pct",
            "condA",
            "condB",
        ]
    ]
    with pd.option_context("display.max_rows", None, "display.width", 160):
        print("\n[DRY] Minute-by-minute near cutoff:")
        print(show.to_string(index=False))

    last = sub.tail(1).iloc[0]
    pn = int((sub["condA"].tail(persist_M) | sub["condB"].tail(persist_M)).sum())
    spr = float(last["micro_spread_pct"])
    reasons: List[str] = []
    if pd.notna(last["zvol"]) and float(last["zvol"]) < zvol_min:
        reasons.append("zvol_low")
    if pd.notna(last["dVWAP_pct"]) and float(last["dVWAP_pct"]) < vwap_delta_min:
        reasons.append("vwap_delta_low")
    if pn < persist_N:
        reasons.append("no_persist")
    if pd.notna(spr) and spr > spread_max:
        reasons.append("wide_spread")

    print(
        f"\n[DRY] Persistensi: N/M = {pn}/{persist_M}  |  ZVol={last['zvol']:.2f}  |  ΔVWAP={last['dVWAP_pct']:.2f}%  |  micro_spread={spr:.2f}%"
    )
    if reasons:
        print("[DRY] GATED (drop) karena:", ", ".join(reasons))
    else:
        print("[DRY] LULUS gating (siap dinilai/skor).")

# ============================================================================
# SECTION: CLI — Argumen & Orkestrasi Multi‑Cutoff
# ============================================================================

def main() -> None:
    """Parser argumen, orkestrasi multi‑cutoff, dry‑run, dan penulisan CSV."""
    p = argparse.ArgumentParser(
        "bpjs v3.1 core (1m→5m→15m→daily) + markup features + dry-run + fix"
    )
    p.add_argument("--date", help="YYYY-MM-DD (default: today)")
    p.add_argument(
        "--cutoff",
        action="append",
        help=(
            "HH:MM; repeatable or comma-separated (e.g. --cutoff 09:30 --cutoff 14:15 or \"09:30,14:15\")"
        ),
    )
    p.add_argument("--top", type=int, default=DEFAULT_TOP_N)
    p.add_argument("--baseline-days", type=int, default=DEFAULT_BASELINE_DAYS)
    p.add_argument("--pace-min", type=float, default=DEFAULT_PACE_MIN)
    p.add_argument("--ret-min", type=float, default=DEFAULT_RETURN_MIN)
    p.add_argument("--ret-max", type=float, default=DEFAULT_RETURN_MAX)
    p.add_argument("--min-price", "--minprice", dest="min_price", type=float, default=None)
    p.add_argument("--resolutions", "--fetchlist", dest="resolutions", default=None)
    p.add_argument("--src-1m", dest="src_1m", default=None)
    p.add_argument("--src-5m", dest="src_5m", default=None)
    p.add_argument("--src-15m", dest="src_15m", default=None)
    p.add_argument("--src-daily", dest="src_daily", default=None)

    # Threshold fitur markup
    p.add_argument("--minute-profile-days", type=int, default=DEFAULT_PROF_DAYS)
    p.add_argument("--zvol-min", type=float, default=DEFAULT_ZVOL_MIN)
    p.add_argument("--persist-m", type=int, default=DEFAULT_PERSIST_M)
    p.add_argument("--persist-n", type=int, default=DEFAULT_PERSIST_N)
    p.add_argument("--micro-spread-max", type=float, default=DEFAULT_SPREAD_MAX)
    p.add_argument("--vwap-delta-min", type=float, default=DEFAULT_VWAP_DELTA_MIN)

    # Dry‑run
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Jalankan tanpa menulis CSV. Jika --dry-ticker diisi, tampilkan debug menit.",
    )
    p.add_argument("--dry-ticker", default=None, help="Ticker spesifik untuk debug minute-by-minute (butuh --dry-run).")
    p.add_argument("--dry-lookback", type=int, default=10, help="(Reserved) jumlah menit terakhir untuk ditampilkan saat --dry-run.")

    args = p.parse_args()

    if args.resolutions:
        print(f"[INFO] Resolutions (audit only): {args.resolutions}")

    today = pd.Timestamp("today", tz=SESSION_TZ).normalize().date()
    target_date = date.fromisoformat(args.date) if args.date else today

    F1  = resolve_1m_dir(args.src_1m)
    F5  = resolve_5m_dir(args.src_5m)
    F15 = resolve_15m_dir(args.src_15m)
    FD  = resolve_daily_dir(args.src_daily)

    if not args.dry_run:
        ensure_outdir()

    print(f"Hari ini: {today} | target_date: {target_date}")
    print(f"[INFO] 1m dir: {F1}  | files: {len(list(F1.glob('*.csv')))}")
    print(f"[INFO] 5m dir: {F5}  | files: {len(list(F5.glob('*.csv')))}")
    print(f"[INFO] 15m dir: {F15} | files: {len(list(F15.glob('*.csv')))}")
    print(f"[INFO] daily dir: {FD}")

    # Kumpulan cutoff (bisa banyak)
    cutoffs: List[str] = []
    if not args.cutoff:
        cutoffs = [DEFAULT_CUTOFF_STR]
    else:
        for item in args.cutoff:
            cutoffs += [x.strip() for x in item.split(",") if x.strip()]
    print(f"[INFO] Cutoffs: {', '.join(cutoffs)}")

    # Mode dry‑run spesifik 1 ticker (inspect menit‑per‑menit)
    if args.dry_run and args.dry_ticker:
        df1 = read_intraday(F1, args.dry_ticker)
        if df1 is None:
            print(f"[DRY] Tidak ada data 1m untuk {args.dry_ticker}")
        else:
            for cstr in cutoffs:
                t = datetime.strptime(cstr, "%H:%M").time()
                print(f"\n[DRY] === {args.dry_ticker} @ {cstr} ===")
                dry_run_debug(
                    df1,
                    target_date,
                    t,
                    args.minute_profile_days,
                    args.zvol_min,
                    args.persist_m,
                    args.persist_n,
                    args.micro_spread_max,
                    args.vwap_delta_min,
                )
        return

    any_nonempty = False
    for cstr in cutoffs:
        t = datetime.strptime(cstr, "%H:%M").time()
        df, drop = bpjs_candidates(
            target_date=target_date,
            cutoff_time=t,
            baseline_days=args.baseline_days,
            pace_min=args.pace_min,
            ret_min=args.ret_min,
            ret_max=args.ret_max,
            top_n=args.top,
            folder_1m=F1,
            folder_5m=F5,
            folder_15m=F15,
            folder_daily=FD,
            prof_days=args.minute_profile_days,
            zvol_min=args.zvol_min,
            persist_M=args.persist_m,
            persist_N=args.persist_n,
            spread_max=args.micro_spread_max,
            vwap_delta_min=args.vwap_delta_min,
            diag=True,
        )

        # Filter harga minimal (opsional)
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

        # Urutan kolom yang konsisten
        # Urutan kolom yang konsisten (VWAP di paling kanan)
        if not df.empty:
            preferred = [
                "ticker",
                "price_at_cutoff",
                "daily_return",
                "vol_pace",
                "zvol",
                "persist_n",
                "persist_m",
                "micro_spread_pct",
                "afternoon_power",
                "buy_1_pct",
                "score",
                "last",
                "flags",
                "vwap",             # tetap di kanan
                "vwap_delta_pct",   # tetap di kanan
            ]
            df = df[[c for c in preferred if c in df.columns] +
                    [c for c in df.columns if c not in preferred]]



        if args.dry_run:
            # Hanya tampilkan ke terminal
            if df.empty:
                print(f"[{cstr}] ❌ Tidak ada kandidat.")
            else:
                print(f"\n[{cstr}] [✓] TOP CANDIDATES (dry-run)")
                with pd.option_context("display.max_rows", args.top, "display.max_columns", None, "display.width", 160):
                    print(df.head(args.top).to_string(index=False))
            # Statistik alasan drop
            flat = [r for reasons in drop.values() for r in reasons]
            if flat:
                print("\n[DIAG] Alasan drop teratas:")
                for k, v in Counter(flat).most_common(8):
                    print(f"  - {k:>18}: {v}")
            continue

        # Tulis CSV produksi
        hhmm = cstr.replace(":", "")
        outfile = OUT_DIR / f"bpjs_rekomendasi_{target_date}_{hhmm}.csv"
        if df.empty:
            print(f"[{cstr}] ❌ Tidak ada kandidat. (menyimpan file kosong)")
            # Safety: jangan biarkan ada header kosong (jaga kompatibilitas excel)
            df.columns = [(f"col_{i}" if (not isinstance(c, str) or not c.strip()) else c)
              for i, c in enumerate(df.columns)]

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

    if not any_nonempty and not args.dry_run:
        print("\n[INFO] Semua cutoff kosong. Cek data & filter.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        raise