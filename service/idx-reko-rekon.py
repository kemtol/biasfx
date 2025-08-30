#!/usr/bin/env python3
"""
idx-reko-rekon.py — Decoupled REKON builder untuk roster BPJS.

Menghasilkan:
  - bpjs_rekomendasi_<DATE>_rekon_long.csv
  - bpjs_rekomendasi_<DATE>_rekon_latest_upto_0930.csv
  - bpjs_rekomendasi_<DATE>_rekon_latest_upto_1130.csv
  - bpjs_rekomendasi_<DATE>_rekon_latest_upto_1415.csv
  - bpjs_rekon_manifest_<DATE>.json
  - .rekon_ready_<DATE>_{0930,1130,1415} (sentinel per slot)

Semua output di --out-dir (disarankan: rekomendasi/).
Snapshot dibaca dari --input-dir (disarankan: rekomendasi/).

Contoh:
  python idx-reko-rekon.py --date 2025-08-27 --input-dir rekomendasi --out-dir rekomendasi --emit-first-slot build
  python idx-reko-rekon.py --date 2025-08-27 --input-dir rekomendasi --out-dir rekomendasi --emit-first-slot copy
  python idx-reko-rekon.py --date 2025-08-27 --input-dir rekomendasi --out-dir rekomendasi --emit-first-slot off
"""

import argparse
import json
import os
import re
import sys
import hashlib
import pandas as pd
from datetime import datetime
from typing import Dict

# ===== Konstanta & kolom =====
REQUIRED_COLS = ["ticker", "price_at_cutoff", "daily_return", "vol_pace", "score", "last"]
OPTIONAL_IMPUTE_ZERO = ["closing_strength", "afternoon_power"]
SLOT_ORDER = {"0930": 1, "1130": 2, "1415": 3}
SLOTS = ["0930", "1130", "1415"]


# ===== Util =====
def sha256sum(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def atomic_write_text(s: str, path: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(s)
    os.replace(tmp, path)


# ===== Loader snapshot =====
def find_daily_snapshots(input_dir: str, date_str: str) -> Dict[str, str]:
    """
    Cari file bpjs_rekomendasi_{date}_{slot}.csv di input_dir
    Mengembalikan dict: { "0930": "/path/.._0930.csv", ... } dengan urutan slot.
    """
    pat = re.compile(rf"^bpjs_rekomendasi_{re.escape(date_str)}_(\d+)\.csv$")
    found = {}
    try:
        names = os.listdir(input_dir)
    except FileNotFoundError:
        raise FileNotFoundError(f"Input dir not found: {input_dir}")
    for name in names:
        m = pat.match(name)
        if m:
            slot = m.group(1).zfill(4)
            if slot in SLOT_ORDER:
                found[slot] = os.path.join(input_dir, name)
    return dict(sorted(found.items(), key=lambda kv: SLOT_ORDER.get(kv[0], 99)))


def read_snapshot(path: str, slot: str, date_str: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(f"{os.path.basename(path)} missing required cols: {missing}")
    # Impute kolom opsional → 0.0 agar schema konsisten
    for c in OPTIONAL_IMPUTE_ZERO:
        if c not in df.columns:
            df[c] = 0.0
    # Meta
    df["date"] = date_str
    df["slot"] = slot
    df["slot_order"] = SLOT_ORDER[slot]
    df["source_file"] = os.path.basename(path)
    return df


# ===== Builder REKON =====
def build_rekon_long(snapshots: Dict[str, str], date_str: str) -> pd.DataFrame:
    frames = []
    for slot, path in snapshots.items():
        frames.append(read_snapshot(path, slot, date_str))
    if not frames:
        raise FileNotFoundError(f"No snapshots found for {date_str}")
    long_df = pd.concat(frames, ignore_index=True)

    # Agregasi per (date, ticker)
    grp = long_df.groupby(["date", "ticker"], as_index=False).agg(
        first_seen_slot=("slot_order", "min"),
        last_seen_slot=("slot_order", "max"),
        time_seen_count=("slot_order", "nunique"),
    )
    rev = {v: k for k, v in SLOT_ORDER.items()}
    grp["first_seen_slot"] = grp["first_seen_slot"].map(rev)
    grp["last_seen_slot"] = grp["last_seen_slot"].map(rev)
    grp["persist_count"] = grp["time_seen_count"]

    merged = long_df.merge(grp, on=["date", "ticker"], how="left")
    merged["is_latest_for_ticker"] = (merged["slot"] == merged["last_seen_slot"]).astype(int)

    # Urutan kolom yang rapi & stabil
    col_order = [
        "date", "slot", "slot_order",
        "ticker", "score", "vol_pace", "last", "price_at_cutoff", "daily_return",
        "closing_strength", "afternoon_power",
        "first_seen_slot", "last_seen_slot", "time_seen_count", "persist_count", "is_latest_for_ticker",
        "source_file",
    ]
    extras = [c for c in merged.columns if c not in col_order]
    merged = merged[col_order + extras]
    merged = merged.sort_values(["ticker", "slot_order"]).reset_index(drop=True)
    return merged


def latest_upto_slot(long_df: pd.DataFrame, upto_slot: str) -> pd.DataFrame:
    upto = SLOT_ORDER[upto_slot]
    df = long_df[long_df["slot_order"] <= upto].copy()
    if df.empty:
        return df
    idx = df.groupby(["date", "ticker"])["slot_order"].idxmax()
    latest = df.loc[idx].sort_values(["ticker"]).reset_index(drop=True)
    return latest


def write_sentinels_and_manifest(out_dir: str, date_str: str,
                                 snaps_present: Dict[str, bool],
                                 files_out: Dict[str, str],
                                 row_counts: Dict[str, int]) -> None:
    """
    Sentinel ready:
      - 0930 → butuh snapshot 0930
      - 1130 → butuh snapshot 0930 & 1130
      - 1415 → butuh snapshot 0930 & 1130 & 1415
    """
    ready_for = {
        "0930": snaps_present.get("0930", False),
        "1130": snaps_present.get("0930", False) and snaps_present.get("1130", False),
        "1415": snaps_present.get("0930", False) and snaps_present.get("1130", False) and snaps_present.get("1415", False),
    }

    # Tulis / hapus sentinel per slot
    for slot, is_ready in ready_for.items():
        sentinel = os.path.join(out_dir, f".rekon_ready_{date_str}_{slot}")
        if is_ready:
            atomic_write_text(f"ready:{date_str}:{slot}\n", sentinel)
        else:
            if os.path.exists(sentinel):
                os.remove(sentinel)

    # Manifest JSON
    checksums = {}
    for k, p in files_out.items():
        if p and os.path.exists(p):
            try:
                checksums[k] = {"sha256": sha256sum(p), "rows": row_counts.get(k, None)}
            except Exception:
                checksums[k] = {"sha256": None, "rows": row_counts.get(k, None)}
        else:
            checksums[k] = {"sha256": None, "rows": row_counts.get(k, None)}

    manifest = {
        "date": date_str,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "snapshots_present": snaps_present,
        "ready_for": ready_for,
        "outputs": files_out,
        "checksums": checksums,
    }
    manifest_path = os.path.join(out_dir, f"bpjs_rekon_manifest_{date_str}.json")
    atomic_write_text(json.dumps(manifest, indent=2), manifest_path)


# ===== Main =====
def main():
    ap = argparse.ArgumentParser(description="Build REKON files (decoupled) untuk BPJS.")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input-dir", required=True, help="Folder berisi bpjs_rekomendasi_{date}_{slot}.csv")
    ap.add_argument("--out-dir", required=True, help="Folder output REKON")
    ap.add_argument("--emit-first-slot", choices=["off", "copy", "build"], default="build",
                    help="Bagaimana cara emit *_rekon_latest_upto_0930.csv (off|copy|build)")
    args = ap.parse_args()

    date_str = args.date
    inp = os.path.abspath(args.input_dir)
    out = os.path.abspath(args.out_dir)
    os.makedirs(out, exist_ok=True)

    snaps = find_daily_snapshots(inp, date_str)
    snaps_present = {s: (s in snaps) for s in SLOTS}

    # Wajib ada minimal 1 snapshot untuk membangun long
    long_df = build_rekon_long(snaps, date_str)

    prefix = os.path.join(out, f"bpjs_rekomendasi_{date_str}")
    out_long = f"{prefix}_rekon_long.csv"
    atomic_write_csv(long_df, out_long)

    files_out = {"rekon_long": out_long}
    row_counts = {"rekon_long": len(long_df)}

    # 0930
    out_0930 = f"{prefix}_rekon_latest_upto_0930.csv"
    if args.emit_first_slot == "off":
        files_out["rekon_latest_upto_0930"] = None
    elif args.emit_first_slot == "copy":
        if snaps_present.get("0930", False):
            import shutil
            shutil.copy2(snaps["0930"], out_0930)
            files_out["rekon_latest_upto_0930"] = out_0930
            try:
                row_counts["rekon_latest_upto_0930"] = len(pd.read_csv(out_0930))
            except Exception:
                row_counts["rekon_latest_upto_0930"] = None
        else:
            files_out["rekon_latest_upto_0930"] = None
    else:  # build
        latest_0930 = latest_upto_slot(long_df, "0930")
        if not latest_0930.empty:
            atomic_write_csv(latest_0930, out_0930)
            files_out["rekon_latest_upto_0930"] = out_0930
            row_counts["rekon_latest_upto_0930"] = len(latest_0930)
        else:
            files_out["rekon_latest_upto_0930"] = None

    # 1130
    out_1130 = f"{prefix}_rekon_latest_upto_1130.csv"
    latest_1130 = latest_upto_slot(long_df, "1130")
    if not latest_1130.empty:
        atomic_write_csv(latest_1130, out_1130)
        files_out["rekon_latest_upto_1130"] = out_1130
        row_counts["rekon_latest_upto_1130"] = len(latest_1130)
    else:
        files_out["rekon_latest_upto_1130"] = None

    # 1415
    out_1415 = f"{prefix}_rekon_latest_upto_1415.csv"
    latest_1415 = latest_upto_slot(long_df, "1415")
    if not latest_1415.empty:
        atomic_write_csv(latest_1415, out_1415)
        files_out["rekon_latest_upto_1415"] = out_1415
        row_counts["rekon_latest_upto_1415"] = len(latest_1415)
    else:
        files_out["rekon_latest_upto_1415"] = None

    # Sentinel + manifest
    write_sentinels_and_manifest(out, date_str, snaps_present, files_out, row_counts)

    print(json.dumps({
        "status": "ok",
        "date": date_str,
        "outputs": files_out,
        "rows": row_counts,
        "snapshots_present": snaps_present
    }, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
