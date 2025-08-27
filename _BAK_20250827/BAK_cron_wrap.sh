#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ============ ENV & PATHS ============
export TZ="Asia/Jakarta"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

RUN_SLOT="$ROOT/run_slot.sh"

LOG_DIR="$ROOT/logs"
LOCK_FILE="$ROOT/.cron_wrap.lock"
MON_JSONL="$LOG_DIR/monitor.jsonl"
MON_LATEST="$LOG_DIR/monitor_latest.json"
mkdir -p "$LOG_DIR"

# ============ UTILS ============
ts_utc()   { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
ts_local() { date    +"%Y-%m-%d %H:%M:%S %Z"; }
json_escape(){ sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }
log(){ echo "[$(ts_local)] $*" | tee -a "$LOG_DIR/cron_wrap.log"; }

# Single-instance lock
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Another cron_wrap is running. Exit."
  exit 0
fi

usage(){
  cat <<'USG'
Usage:
  cron_wrap.sh {1000|1200|1500|1600} [--minprice N] [--fetchlist LIST] [--cutoff HH:MM] [--job NAME]
  cron_wrap.sh auto

Profiles:
  1000 -> cutoff=09:30, fetchlist=1m
  1200 -> cutoff=11:30, fetchlist=1m,5m
  1500 -> cutoff=14:15, fetchlist=1m
  1600 -> cutoff=15:50, fetchlist=1m,5m,15m,daily
USG
}

pick_profile(){
  local slot="$1"
  case "$slot" in
    1000) PROFILE_CUTOFF="09:30"; PROFILE_FETCHL="1m" ;;
    1200) PROFILE_CUTOFF="11:30"; PROFILE_FETCHL="1m,5m" ;;
    1500) PROFILE_CUTOFF="14:15"; PROFILE_FETCHL="1m" ;;
    1600) PROFILE_CUTOFF="15:50"; PROFILE_FETCHL="1m,5m,15m,daily" ;;
    *) echo "[ERR] Unknown slot: $slot" >&2; exit 2 ;;
  esac
}

pick_auto_slot(){
  local now; now="$(date +%H%M)"
  if   (( 10#$now >= 0955 && 10#$now < 1010 )); then SLOT="1000"
  elif (( 10#$now >= 1155 && 10#$now < 1210 )); then SLOT="1200"
  elif (( 10#$now >= 1455 && 10#$now < 1510 )); then SLOT="1500"
  elif (( 10#$now >= 1555 && 10#$now < 1610 )); then SLOT="1600"
  else SLOT=""; fi
}

# ============ ARG PARSER ============
MODE="${1:-auto}"
shift || true

MINP="${MIN_PRICE:-65}"
CUTOFF_OVERRIDE=""
FETCHL_OVERRIDE=""
JOB_NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --minprice)   MINP="${2:-}";            shift 2 ;;
    --fetchlist)  FETCHL_OVERRIDE="${2:-}"; shift 2 ;;
    --cutoff)     CUTOFF_OVERRIDE="${2:-}"; shift 2 ;;
    --job)        JOB_NAME="${2:-}";        shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

# ============ RESOLVE SLOT/PROFILE ============
if [[ "$MODE" == "auto" ]]; then
  pick_auto_slot
  if [[ -z "$SLOT" ]]; then
    log "auto: no slot window matched. Exit."
    exit 0
  fi
else
  if [[ ! "$MODE" =~ ^(1000|1200|1500|1600)$ ]]; then
    echo "[ERR] Mode must be 1000/1200/1500/1600 or 'auto'." >&2
    usage; exit 2
  fi
  SLOT="$MODE"
fi

pick_profile "$SLOT"

FINAL_CUTOFF="${CUTOFF_OVERRIDE:-$PROFILE_CUTOFF}"
FINAL_FETCHL="${FETCHL_OVERRIDE:-$PROFILE_FETCHL}"

# ============ SANITY ============
if [[ -z "$FINAL_CUTOFF" || -z "$FINAL_FETCHL" || -z "$MINP" ]]; then
  echo "[ERR] Missing required values (cutoff/fetchlist/minprice)" >&2
  usage; exit 2
fi
if [[ ! -x "$RUN_SLOT" ]]; then
  echo "[ERR] run_slot.sh not found or not executable at $RUN_SLOT" >&2
  exit 2
fi

# ============ MONITORED DISPATCH ============
JOB_NAME="${JOB_NAME:-slot-$SLOT}"
CMD="$RUN_SLOT once --cutoff \"$FINAL_CUTOFF\" --minprice \"$MINP\" --fetchlist \"$FINAL_FETCHL\""

START_TS="$(ts_utc)"; START_LOCAL="$(ts_local)"
echo "{\"ts\":\"$START_TS\",\"ts_local\":\"$START_LOCAL\",\"job\":\"$(printf '%s' "$JOB_NAME" | json_escape)\",\"command\":\"$(printf '%s' "$CMD" | json_escape)\",\"status\":\"start\"}" >> "$MON_JSONL"

TMP_OUT="$(mktemp)"
set +e
bash -lc "$CMD" > >(tee -a "$TMP_OUT") 2> >(tee -a "$TMP_OUT" >&2)
RC=$?
set -e

RESULT_RAW="$(tail -n 5 "$TMP_OUT" | tr -d '\r')"
rm -f "$TMP_OUT"
END_TS="$(ts_utc)"; END_LOCAL="$(ts_local)"
STATUS="success"; [[ $RC -ne 0 ]] && STATUS="error"

echo "{\"ts\":\"$END_TS\",\"ts_local\":\"$END_LOCAL\",\"job\":\"$(printf '%s' "$JOB_NAME" | json_escape)\",\"command\":\"$(printf '%s' "$CMD" | json_escape)\",\"status\":\"$STATUS\",\"exit_code\":$RC,\"result\":\"$(printf '%s' "$RESULT_RAW" | json_escape)\"}" >> "$MON_JSONL"

python3 - "$MON_JSONL" "$MON_LATEST" << 'PY'
import json, sys
jsonl, latest = sys.argv[1], sys.argv[2]
by_job = {}
with open(jsonl, 'r', encoding='utf-8') as f:
    for line in f:
        line=line.strip()
        if not line: continue
        try: obj=json.loads(line)
        except Exception: continue
        j=obj.get('job')
        if j: by_job[j]=obj
with open(latest, 'w', encoding='utf-8') as f:
    json.dump(by_job, f, ensure_ascii=False, indent=2)
PY

exit $RC
