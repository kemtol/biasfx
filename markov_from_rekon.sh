#!/usr/bin/env bash
# markov_from_rekon.sh — build REKON + run Markov (builder ada di service/)
set -Eeuo pipefail
cd "$(dirname "$0")"  # ROOT repo

export PYTHONPATH=.

# --- Konfigurasi (bisa override via env) ---
DATE="${DATE:-$(date +%F)}"
EMIT_FIRST_SLOT="${EMIT_FIRST_SLOT:-build}"
IN_SNAPSHOT_DIR="${IN_SNAPSHOT_DIR:-rekomendasi}"
OUT_REKON_DIR="${OUT_REKON_DIR:-service/rekomendasi}"
OUT_MARKOV_DIR="${OUT_MARKOV_DIR:-rekomendasi}"

# --- venv (opsional) ---
if [[ -f "service/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source service/.venv/bin/activate
elif [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# --- pastikan package markov importable ---
[[ -f "markov/__init__.py" ]] || touch "markov/__init__.py"

# --- builder path (TIDAK di root) ---
REKON_BUILDER="${REKON_BUILDER:-service/idx-reko-rekon.py}"
if [[ ! -f "$REKON_BUILDER" ]]; then
  echo "ERR: builder tidak ditemukan di $REKON_BUILDER (set REKON_BUILDER=path lain bila perlu)"
  exit 1
fi

# --- 1) Build REKON ---
echo "[REKON] build for $DATE  (snapshots: $IN_SNAPSHOT_DIR -> outputs: $OUT_REKON_DIR)"
python3 "$REKON_BUILDER" \
  --date "$DATE" \
  --input-dir "$IN_SNAPSHOT_DIR" \
  --out-dir "$OUT_REKON_DIR" \
  --emit-first-slot "$EMIT_FIRST_SLOT"

# --- 2) Run Markov per slot bila sentinel ready ---
run_slot () {
  local s="$1"
  local SENT="$OUT_REKON_DIR/.rekon_ready_${DATE}_${s}"

  local SNAP0930="$OUT_REKON_DIR/bpjs_rekomendasi_${DATE}_rekon_latest_upto_0930.csv"
  local SNAP1130="$OUT_REKON_DIR/bpjs_rekomendasi_${DATE}_rekon_latest_upto_1130.csv"
  local SNAP1415="$OUT_REKON_DIR/bpjs_rekomendasi_${DATE}_rekon_latest_upto_${s}.csv"

  if [[ ! -f "$SENT" ]]; then
    echo "[SKIP] $s belum ready ($SENT tidak ada)"; return 0
  fi
  if [[ ! -f "$SNAP1415" ]]; then
    echo "[SKIP] $s: file REKON tidak ditemukan: $SNAP1415"; return 0
  fi

  echo "[RUN] Markov slot $s"
  python3 -m markov.run_chain_for_snapshot \
    --snapshot1415 "$SNAP1415" \
    --snapshot0930 "$SNAP0930" \
    --snapshot1130 "$SNAP1130"

  local SRC="$OUT_MARKOV_DIR/bpjs_rekomendasi_${DATE}.csv"
  local DST="$OUT_MARKOV_DIR/bpjs_rekomendasi_${DATE}_MARKOV_${s}.csv"
  if [[ -f "$SRC" ]]; then
    cp -v "$SRC" "$DST"
  else
    echo "[WARN] Output Markov tidak ditemukan: $SRC"
  fi
}

run_slot 0930
run_slot 1130
run_slot 1415

echo "[DONE] DATE=$DATE  outputs in $OUT_MARKOV_DIR  |  REKON in $OUT_REKON_DIR"
