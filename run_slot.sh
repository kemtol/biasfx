#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ========= Paths & env =========
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
MARK_DIR="$SCRIPT_DIR/.run_slot"
LOCK_FILE="$SCRIPT_DIR/.run_slot.lock"
mkdir -p "$LOG_DIR" "$MARK_DIR"

# Python (pakai venv kalau ada)
PY="$SCRIPT_DIR/.venv/bin/python3"
[[ -x "$PY" ]] || PY="$(command -v python3)"

# Services
FETCH_1M="$SCRIPT_DIR/service/idx-fetch-1m.py"
FETCH_5M="$SCRIPT_DIR/service/idx-fetch-5m.py"
FETCH_15M="$SCRIPT_DIR/service/idx-fetch-15m.py"
CORE_BPJS="$SCRIPT_DIR/service/core-bpjs.py"
# FETCH_DAILY="$SCRIPT_DIR/service/idx-fetch-daily.py"   # aktifkan kalau perlu

# ========= Slot mapping (HHMM) =========
# Intraday fetch trio
INTRA_SLOTS=(1000 1130 1430 1630)
# Daily slot (opsional)
DAILY_SLOT=1800

# Auto-generate rekomendasi pada slot berikut:
REKO_0930_AT=1000   # jalankan core_bpjs --cutoff 09:30 pada jam 10:00
REKO_1415_AT=1430   # jalankan core_bpjs --cutoff 14:15 pada jam 14:30
REKO_1550_AT=1630   # jalankan core_bpjs --cutoff 15:50 pada jam 16:30

# ========= Helpers =========
log(){ echo "[$(date '+%F %T %Z')]" "$@" | tee -a "$LOG_DIR/run_slot.log" ; }

run_with_log(){
  local cmd="$1"
  log "START  $cmd"
  # jalankan di subshell supaya prefix log per-baris
  ( timeout 3600 bash -lc "$cmd" ) 2>&1 | while IFS= read -r line; do
    echo "[$(date '+%T')] $cmd | $line"
  done | tee -a "$LOG_DIR/run_slot.log"
  local rc=${PIPESTATUS[0]}
  if (( rc==0 )); then log "DONE   $cmd ✅"; else log "FAIL   $cmd (exit=$rc) ❌"; fi
  return $rc
}

mark_path(){ echo "$MARK_DIR/$(date +%F)_$1.done"; }
is_marked(){ [[ -f "$(mark_path "$1")" ]]; }
mark(){ : > "$(mark_path "$1")"; }

run_slot_once(){
  local slot="$1"
  local now_hhmm; now_hhmm="$(date +%H%M)"

  # Hanya jalankan bila WAKTU SLOT SUDAH LEWAT & belum done (catch-up aware)
  if (( 10#$now_hhmm < 10#$slot )); then
    log "Skip $slot (belum waktunya)."
    return 0
  fi
  if is_marked "$slot"; then
    log "Skip $slot (sudah done)."
    return 0
  fi

  log "=== RUN slot $slot ==="

  case "$slot" in
    1000|1130|1430|1630)
      run_with_log "$PY $FETCH_1M"  || true
      run_with_log "$PY $FETCH_5M"  || true
      run_with_log "$PY $FETCH_15M" || true
      ;;
    1800)
      # (opsional) jalankan daily fetch
      # run_with_log "$PY $FETCH_DAILY" || true
      ;;
  esac

  # Generate rekomendasi sesuai slot pemicunya
  if [[ "$slot" == "$REKO_0930_AT" ]]; then
    run_with_log "$PY $CORE_BPJS --cutoff 09:30 --top 10" || true
  fi
  if [[ "$slot" == "$REKO_1415_AT" ]]; then
    run_with_log "$PY $CORE_BPJS --cutoff 14:15 --top 10" || true
  fi
  if [[ "$slot" == "$REKO_1550_AT" ]]; then
    run_with_log "$PY $CORE_BPJS --cutoff 15:50 --top 10" || true
  fi

  mark "$slot"
  log "=== DONE slot $slot ==="
}

run_auto(){
  local today now
  today="$(date +%F)"
  now="$(date +%H:%M)"
  log "=== run_slot (mode=auto today=$today now=$now) ==="

  local did=0
  for s in "${INTRA_SLOTS[@]}"; do
    run_slot_once "$s" && did=1 || true
  done

  # (opsional) daily
  # run_slot_once "$DAILY_SLOT" && did=1 || true

  if [[ "$did" == "0" ]]; then
    log "No-op: tidak ada slot due (yang lewat & belum done)."
  fi
}

# ========= Main =========
MODE="${1:-auto}"

# Global lock agar tidak ganda
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Lock aktif, job lain masih jalan. Keluar."
  exit 0
fi

case "$MODE" in
  auto) run_auto ;;
  [0-9][0-9][0-9][0-9]) run_slot_once "$MODE" ;;
  status)
    log "Status marker hari ini:"
    for s in "${INTRA_SLOTS[@]}" "$DAILY_SLOT"; do
      if is_marked "$s"; then echo " - $s: done"; else echo " - $s: pending"; fi
    done
    ;;
  clean-today)
    log "Bersihkan marker hari ini."
    rm -f "$MARK_DIR/$(date +%F)_*.done" || true
    ;;
  *)
    echo "Usage:"
    echo "  $0 auto            # jalankan semua slot yang due (catch-up)"
    echo "  $0 1430            # paksa jalankan slot tertentu"
    echo "  $0 status          # lihat status marker hari ini"
    echo "  $0 clean-today     # hapus marker hari ini"
    exit 2
    ;;
esac
