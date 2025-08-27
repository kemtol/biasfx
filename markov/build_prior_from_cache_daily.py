#!/usr/bin/env python3
import argparse, glob, os, sys, warnings
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")

SUPPORTED_EXT = (".csv", ".parquet")

# -----------------------------
# Helpers: scan & read
# -----------------------------
def list_input_files(input_dir_or_glob: str):
    # Directory → scan recursive
    if os.path.isdir(input_dir_or_glob):
        paths = []
        for root, _, files in os.walk(input_dir_or_glob):
            for fn in files:
                if fn.lower().endswith(SUPPORTED_EXT):
                    paths.append(os.path.join(root, fn))
        if not paths:
            raise SystemExit(f"No input files found under directory: {input_dir_or_glob}")
        return sorted(paths)
    # Glob pattern(s) (comma-separated supported)
    paths = []
    for pat in input_dir_or_glob.split(","):
        pat = pat.strip()
        if pat:
            paths.extend(glob.glob(pat))
    if not paths:
        raise SystemExit(f"No input files found for glob(s): {input_dir_or_glob}")
    return sorted(set(paths))

def read_any(path: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)

# -----------------------------
# Schema normalizer
# -----------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - lowercase all column names
    - replace spaces with underscores
    - map yahoo-style columns to our expected names
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # if we have datetime but not date → derive date (keep timezone if exists, but coerce to date)
    if "date" not in df.columns and "datetime" in df.columns:
        # parse then take .dt.date (lose time for daily granularity)
        df["date"] = pd.to_datetime(df["datetime"]).dt.date

    # Prefer 'close'; if absent but 'adj_close' exists → use it
    if "close" not in df.columns and "adj_close" in df.columns:
        df["close"] = df["adj_close"]

    # Minimal renames (some data may have symbol 'ticker' missing)
    # We won't infer 'ticker' here; we do it in the loader loop using filename if needed.

    # Ensure numeric types where relevant (silently coerce)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# -----------------------------
# Regime bucketing utilities
# -----------------------------
def _safe_qcut(s: pd.Series, q: int):
    s = s.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    try:
        return pd.qcut(s, q, labels=False, duplicates="drop")
    except Exception:
        return pd.Series(np.zeros(len(s), dtype=int), index=s.index)

def compute_regime(df: pd.DataFrame) -> pd.DataFrame:
    # Requires: turnover, atr14, sector, gap
    df = df.copy().sort_values(["ticker", "date"])
    # turnover fallback
    if "turnover" not in df:
        if "volume" in df:
            df["turnover"] = df["close"] * df["volume"]
        else:
            df["turnover"] = df["close"]  # fallback paling konservatif
    # atr14 fallback
    if "atr14" not in df:
        df["atr14"] = (df["high"] - df["low"]).rolling(14, min_periods=1).mean()
    # gap (gunakan prev_close bila ada; jika tidak, infer dari close-1)
    if "prev_close" in df:
        base_prev = df["prev_close"]
    else:
        base_prev = df.groupby("ticker")["close"].shift(1)
    df["gap"] = (df["open"] / base_prev - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # Buckets
    df["liq_bucket"] = _safe_qcut(df["turnover"], 10)
    df["vol_bucket"] = _safe_qcut(df["atr14"], 10)
    df["gap_bucket"] = pd.cut(df["gap"], bins=[-np.inf, -0.01, 0.01, np.inf],
                              labels=["down", "flat", "up"])
    if "sector" not in df:
        df["sector"] = "UNK"
    return df

# -----------------------------
# Label definitions (events)
# -----------------------------
def label_events(df: pd.DataFrame, epsilon_eod=0.0075, morning_thresh=0.03) -> pd.DataFrame:
    """
    Label:
    - EOD_hold: close >= price_at_1415 * (1 - epsilon) (fallback ke open bila price_at_1415 tak ada)
    - AM3_up: pada D+1, high/open >= +3% (proxy window pagi 09:00–10:00)
    - D2_continue: sinyal lanjutan (close_d1 >= open_d1) ATAU (close_d2 >= open_d2)
    """
    df = df.sort_values(["ticker", "date"]).copy()

    # pastikan date bertipe datetime64[ns] (bukan date object) supaya konsisten
    df["date"] = pd.to_datetime(df["date"])

    price_col = "price_at_1415" if "price_at_1415" in df.columns else "open"
    df["EOD_hold"] = (df["close"] >= df[price_col] * (1 - epsilon_eod)).astype(int)

    g = df.groupby("ticker")
    df["open_d1"]  = g["open"].shift(-1)
    df["high_d1"]  = g["high"].shift(-1)
    df["close_d1"] = g["close"].shift(-1)
    df["open_d2"]  = g["open"].shift(-2)
    df["close_d2"] = g["close"].shift(-2)

    df["AM3_up"] = ((df["high_d1"] / df["open_d1"] - 1.0) >= morning_thresh).astype(int)

    cont1 = (df["close_d1"] >= df["open_d1"]).fillna(False)
    cont2 = (df["close_d2"] >= df["open_d2"]).fillna(False)
    df["D2_continue"] = (cont1 | cont2).astype(int)
    return df

def aggregate_transition(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["sector", "liq_bucket", "vol_bucket", "gap_bucket"]
    grp = df.groupby(keys, dropna=False)
    prior = grp[["EOD_hold", "AM3_up", "D2_continue"]].mean().reset_index()
    prior = prior.rename(columns={
        "EOD_hold": "pi_S0_S1",
        "AM3_up": "pi_S1_S2",
        "D2_continue": "pi_S2_S3",
    })
    prior["support"] = grp.size().values
    return prior

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Build prior transitions from cache_daily (directory or glob)."
    )
    ap.add_argument(
        "--input",
        required=True,
        help="Path ke folder cache_daily (scan recursive) ATAU glob pattern (bisa koma-sep)."
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output .parquet untuk prior_transition (mis. artifacts/probability_chain/prior_transition.parquet)"
    )
    args = ap.parse_args()

    files = list_input_files(args.input)
    print(f"[INFO] Found {len(files)} files. Loading...")

    frames = []
    bad = 0
    for f in files:
        try:
            df = read_any(f)
            df = normalize_columns(df)
        except Exception as e:
            bad += 1
            print(f"[WARN] Skip {f}: read/normalize error: {e}", file=sys.stderr)
            continue

        # minimal kolom sesudah normalisasi
        need = {"date", "open", "high", "low", "close"}
        miss = need - set(df.columns)
        if miss:
            bad += 1
            print(f"[WARN] Skip {f}: missing {miss}", file=sys.stderr)
            continue

        # ticker fallback dari nama file jika tidak ada
        if "ticker" not in df.columns:
            base = os.path.basename(f)
            guess = os.path.splitext(base)[0]  # e.g., AADI.JK.csv → AADI.JK
            df["ticker"] = guess

        # type & sanitize
        try:
            df["date"] = pd.to_datetime(df["date"])
        except Exception:
            bad += 1
            print(f"[WARN] Skip {f}: invalid date parse", file=sys.stderr)
            continue

        frames.append(df)

    if not frames:
        raise SystemExit("No valid files after validation.")

    df_all = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Valid rows: {len(df_all):,} | Skipped files: {bad}")

    df_all = compute_regime(df_all)
    df_all = label_events(df_all)

    prior = aggregate_transition(df_all)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    prior.to_parquet(args.out, index=False)
    print(f"[OK] Saved prior to {args.out}")
    # print preview
    try:
        print(prior.head(10).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
