#!/usr/bin/env bash
# cron_wrap.sh — map SLOT → cutoff & fetchlist, lalu panggil run_slot.sh
set -Eeuo pipefail
IFS=$'\n\t'
export TZ="Asia/Jakarta"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

SLOT="${1:-}"; shift || true
# --- Passthrough mode (minim modif) ---
# Jika argumen pertama BUKAN slot yang dikenal, anggap sebagai JOB label
# dan sisanya adalah command mentah yang akan dijalankan apa adanya.
case "$SLOT" in
  1000|1200|1500|1600|0930|1130|1415|1550)
    # recognized slots → lanjut ke logika lama (mapping ke run_slot.sh)
    ;;
  *)
    JOB="${SLOT:-custom}"
    # izinkan separator opsional `--`
    if [[ "${1:-}" == "--" ]]; then shift; fi
    if [[ $# -lt 1 ]]; then
      echo "[ERR] no command provided for job '$JOB'" >&2
      exit 2
    fi
    CMD="$*"  # bangun command dari sisa argumen (boleh banyak token atau string panjang)
    LOG_FILE="$LOG_DIR/cron_wrap_$(date +%F).log"

    # catat start (pakai format yang sama/hampir sama dengan log lama)
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] START job=${JOB} cmd=${CMD}" | tee -a "$LOG_FILE"

    set +e
    bash -lc "$CMD" >> "$LOG_FILE" 2>&1
    RC=$?
    set -e

    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] EXIT rc=${RC} for ${JOB}" | tee -a "$LOG_FILE"

    # (opsional) update monitor.jsonl/monitor_latest.json seperti versi kamu sekarang.
    # Jika kamu sudah punya blok Python di bawah, biarkan — patch ini exit sebelum mencapai sana.

    exit $RC
    ;;
esac
# --- akhir Passthrough mode ---


JOB=""; FETCH_OVERRIDE=""; FORCE="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)        JOB="$2"; shift 2;;
    --fetchlist)  FETCH_OVERRIDE="$2"; shift 2;;
    --force)      FORCE="1"; shift 1;;
    *) echo "[ERR] unknown arg: $1" >&2; exit 2;;
  esac
done

ts(){ date +"%Y-%m-%d %H:%M:%S WIB"; }

# Mapping sesuai request kamu
CUTOFF="09:30"; FETCHLIST="1m"
case "$SLOT" in
  1000) CUTOFF="09:30"; FETCHLIST="1m" ;;
  1200) CUTOFF="11:30"; FETCHLIST="1m,5m" ;;
  1500) CUTOFF="14:15"; FETCHLIST="1m" ;;
  1600) CUTOFF="15:50"; FETCHLIST="1m,5m,15m,daily" ;;
  auto)
    H=$(date +%H)
    if   ((10<=H && H<12)); then SLOT="1000"; CUTOFF="09:30"; FETCHLIST="1m"
    elif ((12<=H && H<15)); then SLOT="1200"; CUTOFF="11:30"; FETCHLIST="1m,5m"
    elif ((15<=H && H<16)); then SLOT="1500"; CUTOFF="14:15"; FETCHLIST="1m"
    else                     SLOT="1600"; CUTOFF="15:50"; FETCHLIST="1m,5m,15m,daily"
    fi
    ;;
  *) echo "[ERR] slot harus 1000|1200|1500|1600|auto"; exit 2;;
esac

[[ -n "$FETCH_OVERRIDE" ]] && FETCHLIST="$FETCH_OVERRIDE"

JOB_TAG="${JOB:-slot-${SLOT}}"
LOG_FILE="$LOG_DIR/cron_wrap_$(date +%F).log"
echo "[$(ts)] JOB=${JOB_TAG} SLOT=${SLOT} → cutoff=${CUTOFF} min=65 fetch=${FETCHLIST}" | tee -a "$LOG_FILE"

CMD=( "./run_slot.sh" "once" "--cutoff" "$CUTOFF" "--minprice" "65" "--fetchlist" "$FETCHLIST" )
[[ "$FORCE" == "1" ]] && CMD+=("--force")

set +e
"${CMD[@]}" >> "$LOG_FILE" 2>&1
rc=$?
set -e
echo "[$(ts)] EXIT rc=${rc} for ${JOB_TAG}" | tee -a "$LOG_FILE"
exit $rc
