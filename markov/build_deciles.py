#!/usr/bin/env python3
import os, json, glob
import pandas as pd
import numpy as np

INP_DIR = "emiten/cache_daily"
OUT = "artifacts/probability_chain/deciles.json"

def list_files_recursively(inp_dir):
    paths = []
    for ext in ("*.csv", "*.parquet"):
        paths += glob.glob(os.path.join(inp_dir, "**", ext), recursive=True)
    return sorted(set(paths))

def main():
    files = list_files_recursively(INP_DIR)
    if not files:
        raise SystemExit(f"No files under {INP_DIR}")

    dfs = []
    for f in files:
        # read
        if f.lower().endswith(".parquet"):
            df = pd.read_parquet(f)
        else:
            df = pd.read_csv(f)

        # normalize columns
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # create date from datetime if needed
        if "date" not in df.columns and "datetime" in df.columns:
            df["date"] = pd.to_datetime(df["datetime"]).dt.date

        # prefer close; fallback to adj_close
        if "close" not in df.columns and "adj_close" in df.columns:
            df["close"] = df["adj_close"]

        # minimal columns
        need = {"date", "open", "high", "low", "close"}
        if not need.issubset(df.columns):
            continue

        # numeric coercion
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # proxies
        if "volume" in df.columns:
            df["turnover"] = df["close"] * df["volume"]
        else:
            df["turnover"] = df["close"]
        df["atr14"] = (df["high"] - df["low"]).rolling(14, min_periods=1).mean()

        dfs.append(df[["turnover", "atr14"]])

    if not dfs:
        raise SystemExit("No valid data found in cache_daily (missing required columns).")

    big = pd.concat(dfs, ignore_index=True)

    def deciles(series):
        s = series.replace([np.inf, -np.inf], np.nan).dropna()
        # kalau datanya kecil/konstan, percentile bisa sama—itu tidak apa
        return {str(i): float(np.percentile(s, i * 10)) for i in range(1, 10)}

    edges = {
        "turnover_deciles": deciles(big["turnover"]),
        "atr14_deciles": deciles(big["atr14"]),
    }

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(edges, f, indent=2)
    print("Saved", OUT)

if __name__ == "__main__":
    main()
