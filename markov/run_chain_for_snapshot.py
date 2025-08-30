# markov/run_chain_for_snapshot.py
import os, glob, math, argparse, re
import pandas as pd
from markov.runtime_prior import PriorLookup

# --------------------- utils io ---------------------
def read_csv_or_parquet(path):
    if path.lower().endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)

def normalize_cols(df):
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

# --------------------- heuristics ---------------------
def heuristic_like(closing_strength: float, afternoon_power: float, persist_count_norm: float) -> float:
    # logistic ringan (0..1)
    z = 2.2*closing_strength + 1.4*afternoon_power + 0.6*persist_count_norm - 2.0
    return 1.0/(1.0 + math.exp(-z))

def blend(prior: float, like: float, alpha: float) -> float:
    out = alpha*prior + (1.0 - alpha)*like
    return max(0.0, min(1.0, out))

def liq_penalty(illiq_score: float, lam: float = 0.35) -> float:
    return max(0.0, 1.0 - lam*illiq_score)

# --------------------- daily stats lookup ---------------------
def latest_daily_stats(cache_dir: str, ticker: str):
    """
    Ambil turnover/atr14/gap terbaru dari cache_daily/<ticker>.(csv|parquet).
    gap = open/prev_close - 1 (kalau prev_close tidak ada, infer dari close-1).
    """
    # cari file: prefer exact match
    cands = []
    for ext in ("csv","parquet"):
        p = os.path.join(cache_dir, f"{ticker}.{ext}")
        if os.path.exists(p): cands.append(p)
    if not cands:
        # fallback cari by glob
        cands = glob.glob(os.path.join(cache_dir, f"{ticker}.*"))

    if not cands:
        # fallback → default median-ish
        return dict(turnover=1e9, atr14=25.0, gap=0.0)

    df = read_csv_or_parquet(cands[0])
    df = normalize_cols(df)

    # normalize essentials
    if "date" not in df.columns and "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"]).dt.date
    if "close" not in df.columns and "adj_close" in df.columns:
        df["close"] = df["adj_close"]

    # numeric
    for col in ["open","high","low","close","volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open","high","low","close"])
    df = df.sort_values("date")

    # turnover proxy
    if "volume" in df.columns:
        df["turnover"] = df["close"] * df["volume"]
    else:
        df["turnover"] = df["close"]

    # atr14 proxy
    df["atr14"] = (df["high"] - df["low"]).rolling(14, min_periods=1).mean()

    # gap
    if "prev_close" in df.columns:
        base_prev = df["prev_close"]
    else:
        base_prev = df["close"].shift(1)
    df["gap"] = (df["open"]/base_prev - 1.0).fillna(0.0)

    last = df.iloc[-1]
    return dict(turnover=float(last["turnover"]), atr14=float(last["atr14"]), gap=float(last["gap"]))

# --------------------- snapshots & persist ---------------------
def load_snapshot(path):
    df = read_csv_or_parquet(path)
    df = normalize_cols(df)

    # --- PATCH kompatibilitas schema baru ---
    if "closing_strength" not in df.columns:
        if "daily_return" in df.columns:
            df["closing_strength"] = df["daily_return"]
        else:
            df["closing_strength"] = 0.0
    # ----------------------------------------

    need = {"ticker","closing_strength","afternoon_power","vol_pace"}
    miss = need - set(df.columns)
    if miss:
        raise SystemExit(f"Snapshot missing columns: {miss} in {path}")
    return df

def build_persist_count(tickers, snap0930=None, snap1130=None):
    if snap0930 is None and snap1130 is None:
        return {t: 1 for t in tickers}  # default minimal
    s0930 = set()
    s1130 = set()
    if snap0930 is not None:
        s0930 = set(load_snapshot(snap0930)["ticker"].tolist())
    if snap1130 is not None:
        s1130 = set(load_snapshot(snap1130)["ticker"].tolist())
    out = {}
    for t in tickers:
        pc = 1 + int(t in s0930) + int(t in s1130)
        out[t] = pc
    return out

# --------------------- date/filename helpers ---------------------
def derive_date_parts_from_filename(path_1415: str):
    """
    Ambil tanggal dari nama file snapshot 14:15:
    Contoh: bpjs_rekomendasi_2025-08-25_1415.csv -> ('2025','08','25')
    """
    base = os.path.basename(path_1415)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", base)
    if not m:
        return ("YYYY","MM","DD")
    return (m.group(1), m.group(2), m.group(3))

# --------------------- recommendation rules ---------------------
def make_recommendation(row, q, guards, pchain_q80):
    """
    Kriteria singkat (adaptif):
    - Fast-lane: jika Peluang Total Berantai >= p80 → "beli keluar lusa"
    - Guard awal (skip): persist < min_persist, vol_pace < min_vol_pace, p_eod < min_p_eod
    - Jika p_eod kuat & p_am3 tinggi → beli keluar besok
      - Jika p_d2 juga tinggi → beli keluar lusa
    - Borderline → neutral
    - Lainnya → skip
    """
    # FAST-LANE (di atas kebanyakan guard, tapi tetap butuh sedikit likuid)
    if (row["p_chain"] >= pchain_q80) and (row["vol_pace"] >= 5.0) and (row["persist_count"] >= 1):
        return "beli keluar lusa"

    # Guards dasar
    if row["persist_count"] < guards["min_persist"]:
        return "skip"
    if row["vol_pace"] < guards["min_vol_pace"]:
        return "skip"
    if row["p_eod"] < guards["min_p_eod"]:
        return "skip"

    p2_cut = max(guards["p2_abs"], q["p2_q60"])
    p3_cut = max(guards["p3_abs"], q["p3_q60"])
    p1_strong = max(guards["min_p_eod_strong"], q["p1_q60"])

    if row["p_eod"] >= p1_strong and row["p_am3"] >= p2_cut:
        if row["p_d2"] >= p3_cut:
            return "beli keluar lusa"
        else:
            return "beli keluar besok"

    if row["p_eod"] >= q["p1_q50"] and row["p_am3"] >= q["p2_q50"]:
        return "neutral"

    return "skip"

# --------------------- main ---------------------
def main():
    ap = argparse.ArgumentParser(description="Run probability chain for a snapshot.")
    ap.add_argument("--snapshot1415", help="Path CSV/Parquet snapshot 14:15 (kolom rekom).")
    ap.add_argument("--snapshot0930", help="(Opsional) snapshot 09:30 untuk persist_count.")
    ap.add_argument("--snapshot1130", help="(Opsional) snapshot 11:30 untuk persist_count.")
    ap.add_argument("--cache_daily_dir", default="emiten/cache_daily", help="Folder cache_daily per-emiten.")
    ap.add_argument("--sector_default", default="UNK", help="Sector default bila tidak diketahui.")
    ap.add_argument("--slot", type=str, help="Cutoff slot (0930, 1130, 1415, 1550)")   # <-- PATCH
    args = ap.parse_args()


    # Muat snapshot 14:15
    snap = load_snapshot(args.snapshot1415)
    tickers = snap["ticker"].tolist()

    # Persist count dari snapshot pagi
    persist_map = build_persist_count(tickers, args.snapshot0930, args.snapshot1130)

    # Prior lookup
    PL = PriorLookup()

    rows = []
    for _, r in snap.iterrows():
        tkr = r["ticker"]
        closing_strength = float(r.get("closing_strength", 0.0))
        afternoon_power  = float(r.get("afternoon_power", 0.0))
        vol_pace         = float(r.get("vol_pace", 1.0))
        score            = float(r.get("score", 0.0))
        persist_count    = persist_map.get(tkr, 1)
        persist_count_norm = min(1.0, persist_count/3.0)

        # illiquidity proxy dari vol_pace
        illiq_score = 1.0/(1.0 + max(1.0, vol_pace))

        # ambil turnover/atr/gap terbaru dari cache_daily
        daily = latest_daily_stats(args.cache_daily_dir, tkr)
        liq_b, vol_b, gap_b = PL.derive_buckets(daily["turnover"], daily["atr14"], daily["gap"])

        pri = PL.get_prior(args.sector_default, liq_b, vol_b, gap_b)

        # likelihood heuristik (bisa diganti model nantinya)
        like = heuristic_like(closing_strength, afternoon_power, persist_count_norm)

        # alpha: makin illiquid → makin berat ke prior
        alpha = 0.6 + 0.3*min(1.0, illiq_score)

        p1 = blend(pri["pi_S0_S1"], like, alpha)
        p2 = blend(pri["pi_S1_S2"], like, alpha)
        p3 = blend(pri["pi_S2_S3"], like, alpha)
        p_chain = (p1*p2*p3) * liq_penalty(illiq_score)

        rows.append({
            "ticker": tkr,
            "score": round(score,3),
            "closing_strength": round(closing_strength,3),
            "afternoon_power": round(afternoon_power,3),
            "vol_pace": round(vol_pace,2),
            "persist_count": persist_count,
            "liq_bucket": liq_b,
            "vol_bucket": vol_b,
            "gap_bucket": gap_b,
            "pi_S0_S1": round(pri["pi_S0_S1"],3),
            "pi_S1_S2": round(pri["pi_S1_S2"],3),
            "pi_S2_S3": round(pri["pi_S2_S3"],3),
            "p_eod": round(p1,3),
            "p_am3": round(p2,3),
            "p_d2": round(p3,3),
            "p_chain": round(p_chain,3),
        })

    out_df = pd.DataFrame(rows).sort_values("p_chain", ascending=False)

    # ====== KUANTIL & GUARDS (ADAPTIF, LEBIH LONGGAR) ======
    q = {
        "p1_q50": float(out_df["p_eod"].quantile(0.50)),
        "p1_q60": float(out_df["p_eod"].quantile(0.60)),
        "p2_q50": float(out_df["p_am3"].quantile(0.50)),
        "p2_q60": float(out_df["p_am3"].quantile(0.60)),
        "p3_q60": float(out_df["p_d2"].quantile(0.60)),
    }
    guards = {
        "min_persist": 1,                           # dulunya 2
        "min_vol_pace": 5.0,                        # dulunya 10
        "min_p_eod": max(0.65, q["p1_q50"]),        # dulunya 0.75
        "min_p_eod_strong": max(0.80, q["p1_q60"]), # dulunya 0.85
        "p2_abs": 0.20,                              # dulunya 0.25
        "p3_abs": 0.50,                              # dulunya 0.55
    }
    pchain_q80 = float(out_df["p_chain"].quantile(0.80))
    out_df["rekomendasi"] = out_df.apply(lambda r: make_recommendation(r, q, guards, pchain_q80), axis=1)

    # ====== RENAME HEADER KE BAHASA AWAM ======
    rename_map = {
        "ticker": "Kode Saham",
        "score": "Skor Sistem",
        "closing_strength": "Kekuatan Menjelang Tutup",
        "afternoon_power": "Dorongan Sore",
        "vol_pace": "Kecepatan Volume",
        "persist_count": "Konsistensi Muncul",
        "liq_bucket": "Kelompok Likuiditas",
        "vol_bucket": "Kelompok Volatilitas",
        "gap_bucket": "Arah Buka",
        "pi_S0_S1": "Rata-rata: Bertahan sampai Tutup",
        "pi_S1_S2": "Rata-rata: Naik ≥3% Besok Pagi",
        "pi_S2_S3": "Rata-rata: Lanjut Naik Lusa",
        "p_eod": "Peluang Bertahan sampai Tutup",
        "p_am3": "Peluang Naik ≥3% Besok Pagi",
        "p_d2": "Peluang Lanjut Naik Lusa",
        "p_chain": "Peluang Total Berantai",
        "rekomendasi": "Rekomendasi Singkat",
    }
    out_df_readable = out_df.rename(columns=rename_map)

    # kolom urutan enak dibaca
    cols_order = [
        "Rekomendasi Singkat",
        "Kode Saham",
        "Skor Sistem",
        "Kekuatan Menjelang Tutup",
        "Dorongan Sore",
        "Konsistensi Muncul",
        "Kecepatan Volume",
        "Peluang Bertahan sampai Tutup",
        "Peluang Naik ≥3% Besok Pagi",
        "Peluang Lanjut Naik Lusa",
        "Peluang Total Berantai",
        "Kelompok Likuiditas",
        "Kelompok Volatilitas",
        "Arah Buka",
        "Rata-rata: Bertahan sampai Tutup",
        "Rata-rata: Naik ≥3% Besok Pagi",
        "Rata-rata: Lanjut Naik Lusa",
    ]
    cols_order = [c for c in cols_order if c in out_df_readable.columns]
    out_df_readable = out_df_readable[cols_order + [c for c in out_df_readable.columns if c not in cols_order]]

    # ====== NAMA FILE OUTPUT: result/bpjs_rekomendasi_YYYY-MM-DD.csv ======
    y, m, d = derive_date_parts_from_filename(args.snapshot1415)
    date_human = f"{y}-{m}-{d}"
    os.makedirs("rekomendasi", exist_ok=True)

    slot = args.slot if args.slot else "MARKOV"
    out_path = f"rekomendasi/bpjs_rekomendasi_{date_human}_MARKOV_{slot}.csv"

    out_df_readable.to_csv(out_path, index=False)
    print(f"[OK] wrote {out_path}  rows={len(out_df_readable)}")


if __name__ == "__main__":
    main()
