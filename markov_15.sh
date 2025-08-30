#!/usr/bin/env bash
# markov_15.sh — BSJP probability chain runner (minute-friendly)
# ============================================================
# Cara pakai (contoh):
#   1) Jalan normal (MIN_PRICE opsional):
#        ./markov_15.sh
#        MIN_PRICE=65 ./markov_15.sh
#
#   2) Hapus LOCK saja (kalau job sebelumnya nyangkut):
#        ./markov_15.sh unlock
#
#   3) Reset HARI INI (hapus LOCK + sentinel .done hari ini):
#        ./markov_15.sh reset-today
#
#   4) Reset tanggal tertentu (hapus LOCK + sentinel .done untuk YYYY-MM-DD):
#        ./markov_15.sh reset-date 2025-08-27
#
# Cron contoh (berhenti otomatis setelah .done dibuat):
#   * 15 * * 1-5  [ -f "$HOME/Projects/SSSAHAM_SERVICE/logs/markov_15.$(date +\%F).done" ] || \
#                 $HOME/Projects/SSSAHAM_SERVICE/markov_15.sh >> $HOME/Projects/SSSAHAM_SERVICE/logs/markov_15_cron.log 2>&1
#   0-10/1 16 * * 1-5  [ -f "$HOME/Projects/SSSAHAM_SERVICE/logs/markov_15.$(date +\%F).done" ] || \
#                       $HOME/Projects/SSSAHAM_SERVICE/markov_15.sh >> $HOME/Projects/SSSAHAM_SERVICE/logs/markov_15_cron.log 2>&1
# ============================================================

set -Eeuo pipefail
IFS=$'\n\t'

# =============== CONFIG ===============
export TZ="Asia/Jakarta"
PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJ_ROOT"

VENV="$PROJ_ROOT/.venv"
REKODIR="$PROJ_ROOT/rekomendasi"
CACHEDIR="$PROJ_ROOT/emiten/cache_daily"
ARTDIR="$PROJ_ROOT/artifacts/probability_chain"
LOGDIR="$PROJ_ROOT/logs"
LOCKFILE="$PROJ_ROOT/.markov_15.lock"

TODAY="$(date +%F)"  # YYYY-MM-DD
S0930="$REKODIR/bpjs_rekomendasi_${TODAY}_0930.csv"
S1130="$REKODIR/bpjs_rekomendasi_${TODAY}_1130.csv"
S1415="$REKODIR/bpjs_rekomendasi_${TODAY}_1415.csv"
OUTCSV="$PROJ_ROOT/result/bpjs_rekomendasi_${TODAY}.csv"

# harga minimum (opsional). set via env: MIN_PRICE=65
MIN_PRICE="${MIN_PRICE:-0}"

mkdir -p "$ARTDIR" "$LOGDIR" "$PROJ_ROOT/result" "$PROJ_ROOT/config"

log(){ echo "[$(date '+%F %T %Z')] $*"; }

# =============== SUBCOMMANDS (sebelum lock) ===============
CMD="${1:-run}"
case "$CMD" in
  unlock)
    rm -f "$LOCKFILE"
    log "LOCK removed: $LOCKFILE"
    exit 0
    ;;
  reset-today)
    rm -f "$LOCKFILE" "$LOGDIR/markov_15.$(date +%F).done"
    log "RESET today → removed LOCK + sentinel: $LOCKFILE , $LOGDIR/markov_15.$(date +%F).done"
    exit 0
    ;;
  reset-date)
    D="${2:-}"
    if [[ -z "$D" ]]; then
      echo "usage: $0 reset-date YYYY-MM-DD" >&2
      exit 2
    fi
    rm -f "$LOCKFILE" "$LOGDIR/markov_15.$D.done"
    log "RESET date=$D → removed LOCK + sentinel: $LOCKFILE , $LOGDIR/markov_15.$D.done"
    exit 0
    ;;
  run|*)
    : # lanjut eksekusi normal
    ;;
esac

# =============== SINGLE INSTANCE LOCK ===============
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  log "another run is in progress; exiting"
  exit 0
fi

# =============== DAILY LOG ===============
exec > >(tee -a "$LOGDIR/markov_15_${TODAY}.log") 2>&1

# =============== GUARD: TRADING DAY ===============
is_trading_day() {
  local dow; dow="$(date +%u)"   # 1=Mon..7=Sun
  if [[ "$dow" -ge 6 ]]; then return 1; fi
  local HOL="$PROJ_ROOT/config/holidays.txt"
  if [[ -f "$HOL" ]]; then
    local today; today="$(date +%F)"
    if grep -qx "$today" "$HOL"; then return 1; fi
  fi
  return 0
}
if ! is_trading_day; then
  log "non-trading day; skip"
  exit 0
fi

# =============== PYTHON ENV ===============
if [[ -x "$VENV/bin/python" ]]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
  PY="$VENV/bin/python"
else
  PY="python"
fi
$PY -V || true

# =============== ARTIFACTS (sekali saja) ===============
if [[ ! -f "$ARTDIR/prior_transition.parquet" ]]; then
  log "building prior_transition.parquet..."
  $PY markov/build_prior_from_cache_daily.py \
    --input "$CACHEDIR" \
    --out   "$ARTDIR/prior_transition.parquet"
fi
if [[ ! -f "$ARTDIR/deciles.json" ]]; then
  log "building deciles.json..."
  $PY markov/build_deciles.py
fi

# =============== CORE (minute-friendly + filter harga) ===============
# 1) snapshot 14:15 belum ada -> keluar cepat
if [[ ! -f "$S1415" ]]; then
  log "snapshot 14:15 not found yet: $S1415 (try again next minute)"
  exit 0
fi

# 2) jika MIN_PRICE>0, buat snapshot terfilter di /tmp (aman & non-destruktif)
TMP_S1415="$S1415"
if [[ "${MIN_PRICE}" != "0" ]]; then
  TMP_S1415="/tmp/$(basename "$S1415" .csv)_minp${MIN_PRICE}.csv"
  log "filtering snapshot by price > ${MIN_PRICE} → $TMP_S1415"
  "$PY" - "$S1415" "$TMP_S1415" "$MIN_PRICE" <<'PY'
import sys, pandas as pd
src, dst, minp = sys.argv[1], sys.argv[2], float(sys.argv[3])
df = pd.read_csv(src)
col = 'last' if 'last' in df.columns else ('price_at_cutoff' if 'price_at_cutoff' in df.columns else None)
if col:
    df = df[df[col] > minp]
df.to_csv(dst, index=False)
print(f"[PY] {len(df)} rows -> {dst}")
PY
fi

# 3) output sudah ada & snapshot (setelah filter) TIDAK lebih baru -> skip
if [[ -f "$OUTCSV" && ! ( "$TMP_S1415" -nt "$OUTCSV" ) ]]; then
  log "output up-to-date ($OUTCSV); nothing to do"
  exit 0
fi

# 4) jalankan probability chain (pakai file filtered bila ada)
log "running probability chain using ${TMP_S1415} ..."
args=( -m markov.run_chain_for_snapshot --snapshot1415 "$TMP_S1415" --cache_daily_dir "$CACHEDIR" )
[[ -f "$S0930" ]] && args+=( --snapshot0930 "$S0930" )
[[ -f "$S1130" ]] && args+=( --snapshot1130 "$S1130" )
$PY "${args[@]}"

# 5) sentinel .done hanya saat sukses
if [[ -f "$OUTCSV" ]]; then
  log "done -> $OUTCSV"
  touch "$LOGDIR/markov_15.$(date +%F).done"
else
  log "completed but output not found; check logs"
fi

