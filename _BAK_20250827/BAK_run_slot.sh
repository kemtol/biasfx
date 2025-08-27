#!/usr/bin/env bash
# run_slot.sh — orchestrator fetch + core-bpjs
# v3.2 — supports --date, --force, src dirs, minute-friendly markers
# 09:30 (pagi) — fetch 1m, min price 65
#./run_slot.sh once --cutoff 09:30 --minprice 65 --fetchlist "1m"

# 11:30 (siang) — fetch 1m+5m
#./run_slot.sh once --cutoff 11:30 --minprice 65 --fetchlist "1m,5m"

# 14:15 — fetch 1m+5m+15m
#./run_slot.sh once --cutoff 14:15 --minprice 65 --fetchlist "1m,5m,15m"

# 15:50 — fetch lengkap 1m+5m+15m+daily
#./run_slot.sh once --cutoff 15:50 --minprice 65 --fetchlist "1m,5m,15m,daily"

# Backfill tanggal tertentu (mis. 2025-08-25) dengan paksa
#./run_slot.sh once --cutoff 11:30 --minprice 65 --fetchlist "1m,5m" --date 2025-08-25 --force

# Cek status marker untuk hari ini
#./run_slot.sh status

#./run_slot.sh once --cutoff 11:30 --minprice 65 --fetchlist "1m,5m" --date 2025-08-25 --force

# Cek status HARI INI (default)
#./run_slot.sh status

# Cek status tanggal tertentu (mis. 2025-08-26)
#./run_slot.sh status --date 2025-08-26

# Cek status kombinasi spesifik pada tanggal tertentu
#./run_slot.sh status --date 2025-08-26 --minprice 65 --fetchlist "1m,5m"


set -Eeuo pipefail
IFS=$'\n\t'
umask 002

# ====== Root guard (hindari sudo) ======
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  echo "[ERR] Jangan jalankan script ini sebagai root/sudo." >&2
  exit 1
fi

# ====== ENV & PATHS ======
export TZ="Asia/Jakarta"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/logs"
MARK_DIR="$ROOT/.run_slot"
LOCK_FILE="$ROOT/.run_slot.lock"
mkdir -p "$LOG_DIR" "$MARK_DIR" "$ROOT/config"

# Python (prefer venv)
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PY="$ROOT/.venv/bin/python"
elif [[ -x "$ROOT/.venv/bin/python3" ]]; then
  PY="$ROOT/.venv/bin/python3"
else
  PY="$(command -v python3)"
fi

# Services
CORE_BPJS="$ROOT/service/core-bpjs.py"
FETCH_1M="$ROOT/service/idx-fetch-1m.py"
FETCH_5M="$ROOT/service/idx-fetch-5m.py"
FETCH_15M="$ROOT/service/idx-fetch-15m.py"
FETCH_DAILY="$ROOT/service/idx-fetch-daily.py"   # optional

# Data sources (bisa override via env INTRADAY_*_DIR / DAILY_DIR)
INTRA_1M_DIR="${INTRADAY_1M_DIR:-$ROOT/emiten/cache_1m}"
INTRA_5M_DIR="${INTRADAY_5M_DIR:-$ROOT/emiten/cache_5m}"
INTRA_15M_DIR="${INTRADAY_15M_DIR:-$ROOT/emiten/cache_15m}"
DAILY_DIR="${DAILY_DIR:-$ROOT/emiten/cache_daily}"

# ====== Utils ======
ts() { date '+%F %T %Z'; }
log(){ echo "[$(ts)] $*" | tee -a "$LOG_DIR/run_slot.log"; }
sanitize(){ sed -E 's/[^A-Za-z0-9]+/-/g'; }

run_with_log(){
  local cmd="$1"
  log "START  $cmd"
  ( timeout 3600 bash -lc "$cmd" ) 2>&1 | while IFS= read -r line; do
    echo "[$(date +%T)] $cmd | $line"
  done | tee -a "$LOG_DIR/run_slot.log"
  local rc=${PIPESTATUS[0]}
  if (( rc==0 )); then log "DONE   $cmd ✅"; else log "FAIL   $cmd (exit=$rc) ❌"; fi
  return $rc
}

# Trading day guard (skip Sabtu/Minggu/libur). Diabaikan jika --date dipakai.
is_trading_day_today(){
  local dow; dow="$(date +%u)" # 1=Mon..7=Sun
  if [[ "$dow" -ge 6 ]]; then return 1; fi
  local HOL="$ROOT/config/holidays.txt"
  if [[ -f "$HOL" ]] && grep -qx "$(date +%F)" "$HOL"; then return 1; fi
  return 0
}

# ====== Markers ======
DATE_OPT=""   # diisi oleh parser jika --date diberikan
target_date(){ if [[ -n "$DATE_OPT" ]]; then echo "$DATE_OPT"; else date +%F; fi; }

mark_path(){
  local tdate cutoff minp fetch
  tdate="$(target_date)"
  cutoff="$(echo -n "$1" | sanitize)"
  minp="$(echo -n "$2" | sanitize)"
  fetch="$(echo -n "$3" | sanitize)"
  echo "$MARK_DIR/${tdate}_cut${cutoff}_min${minp}_f${fetch}.done"
}
is_marked(){ [[ -f "$(mark_path "$1" "$2" "$3")" ]]; }
mark(){ : >"$(mark_path "$1" "$2" "$3")"; }

# Decode helper untuk status scan
decode_marker(){
  # input: filename seperti 2025-08-26_cut0930_min65_f1m-5m.done
  local base fname="$1"
  base="$(basename "$fname" .done)"
  local d="${base%%_*}"
  local rest="${base#*_}"
  local cut="${rest#cut}"; cut="${cut%%_*}"      # 0930 atau 14-15
  local min="${rest#*_min}"; min="${min%%_*}"    # 65 atau 65-01
  local f="${rest##*_f}"                          # 1m-5m-15m
  # Normalisasi tampilan:
  [[ "$cut" =~ ^([0-2][0-9])-([0-5][0-9])$ ]] && cut="${BASH_REMATCH[1]}:${BASH_REMATCH[2]}"
  f="${f//-/,}"   # 1m-5m → 1m,5m
  echo "$d|$cut|$min|$f"
}

# ====== Fetch & Core ======
fetch_by_list(){
  local fetchlist="$1"
  IFS=',' read -r -a parts <<< "$(echo "$fetchlist" | tr -d ' ')"
  for p in "${parts[@]}"; do
    case "$p" in
      1m)    [[ -f "$FETCH_1M"    ]] && run_with_log "$PY \"$FETCH_1M\"" ;;
      5m)    [[ -f "$FETCH_5M"    ]] && run_with_log "$PY \"$FETCH_5M\"" ;;
      15m)   [[ -f "$FETCH_15M"   ]] && run_with_log "$PY \"$FETCH_15M\"" ;;
      daily) [[ -f "$FETCH_DAILY" ]] && run_with_log "$PY \"$FETCH_DAILY\"" || log "SKIP  daily fetch (script not found)" ;;
      *)     : ;;
    esac
  done
}

run_core(){
  local cutoff="$1"    # "HH:MM"
  local minp="$2"      # angka
  local fetchlist="$3" # "1m,5m,15m,daily"
  local date_arg=""
  [[ -n "$DATE_OPT" ]] && date_arg="--date \"$DATE_OPT\""

  local extra_src=""
  [[ -d "$INTRA_1M_DIR"  ]] && extra_src+=" --src-1m \"$INTRA_1M_DIR\""
  [[ -d "$INTRA_5M_DIR"  ]] && extra_src+=" --src-5m \"$INTRA_5M_DIR\""
  [[ -d "$INTRA_15M_DIR" ]] && extra_src+=" --src-15m \"$INTRA_15M_DIR\""
  [[ -d "$DAILY_DIR"     ]] && extra_src+=" --src-daily \"$DAILY_DIR\""

  run_with_log "$PY \"$CORE_BPJS\" \
    $date_arg \
    --cutoff \"$cutoff\" --top 10 \
    --min-price \"$minp\" --resolutions \"$fetchlist\" \
    $extra_src"
}

# ====== Usage & Arg parsing ======
usage(){
  cat <<USG
Usage:
  $(basename "$0") once --cutoff HH:MM --minprice N --fetchlist "1m[,5m][,15m][,daily]" [--date YYYY-MM-DD] [--force]
  $(basename "$0") auto
  $(basename "$0") status [--date YYYY-MM-DD] [--minprice N] [--fetchlist LIST]
  $(basename "$0") clean-today
  $(basename "$0") clean-date YYYY-MM-DD
USG
}

# Defaults
FORCE=0
CUTOFF=""
MINP=""
FETCHL=""

parse_once(){
  DATE_OPT=""; FORCE=0; CUTOFF=""; MINP=""; FETCHL=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --cutoff)    CUTOFF="${2:-}"; shift 2 ;;
      --minprice)  MINP="${2:-}";   shift 2 ;;
      --fetchlist) FETCHL="${2:-}"; shift 2 ;;
      --date)      DATE_OPT="${2:-}"; shift 2 ;;
      --force)     FORCE=1; shift ;;
      -h|--help)   usage; exit 0 ;;
      *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
    esac
  done
  # Validasi
  if [[ -z "$CUTOFF" || -z "$MINP" || -z "$FETCHL" ]]; then
    echo "[ERR] --cutoff, --minprice, --fetchlist wajib." >&2; usage; exit 2
  fi
  if [[ ! "$CUTOFF" =~ ^[0-2][0-9]:[0-5][0-9]$ ]]; then
    echo "[ERR] --cutoff harus format HH:MM" >&2; exit 2
  fi
  if [[ ! "$MINP" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    echo "[ERR] --minprice harus angka" >&2; exit 2
  fi
  if [[ -n "$DATE_OPT" && ! "$DATE_OPT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "[ERR] --date harus YYYY-MM-DD" >&2; exit 2
  fi
  FETCHL="$(echo "$FETCHL" | tr -d ' ')"  # normalize
}

# ====== Modes ======
MODE="${1:-once}"

# Global lock (single instance)
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Lock aktif; job lain masih jalan. Exit."
  exit 0
fi

case "$MODE" in
  once)
    shift
    parse_once "$@"

    # Trading day check (hanya jika tidak backfill)
    if [[ -n "$DATE_OPT" ]]; then
      : # backfill → abaikan trading-day guard
    else
      if ! is_trading_day_today; then
        log "Bukan hari trading (set --date YYYY-MM-DD bila backfill). Exit."
        exit 0
      fi
    fi

    if (( FORCE==0 )) && is_marked "$CUTOFF" "$MINP" "$FETCHL"; then
      log "Marker exist untuk date=$(target_date) cutoff=$CUTOFF min=$MINP fetch=$FETCHL → SKIP."
      exit 0
    fi
    (( FORCE==1 )) && log "FORCE mode: melewati pengecekan marker."

    log "RUN once: date=$(target_date) cutoff=$CUTOFF  minprice=$MINP  fetchlist=$FETCHL"
    fetch_by_list "$FETCHL"
    run_core "$CUTOFF" "$MINP" "$FETCHL"
    mark "$CUTOFF" "$MINP" "$FETCHL"
    log "DONE once."
    ;;

  auto)
    local_min="${MIN_PRICE:-65}"
    declare -a CUTS=("09:30" "11:30" "14:15" "15:50")
    declare -a FLST=("1m" "1m,5m" "1m,5m,15m" "1m,5m,15m,daily")

    if ! is_trading_day_today; then
      log "auto: bukan hari trading. Exit."
      exit 0
    fi

    for i in "${!CUTS[@]}"; do
      CUTOFF="${CUTS[$i]}"; FETCHL="${FLST[$i]}"; MINP="$local_min"
      if is_marked "$CUTOFF" "$MINP" "$FETCHL"; then
        log "Auto skip (marked): $(target_date) $CUTOFF | $FETCHL | min=$MINP"
        continue
      fi
      log "Auto run: date=$(target_date) cutoff=$CUTOFF  minprice=$MINP  fetchlist=$FETCHL"
      fetch_by_list "$FETCHL"
      run_core "$CUTOFF" "$MINP" "$FETCHL"
      mark "$CUTOFF" "$MINP" "$FETCHL"
    done
    log "DONE auto."
    ;;

  status)
    shift || true
    # Optional: --date, --minprice, --fetchlist
    local_min="${MIN_PRICE:-65}"; local_fetch=""; local_date=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --date)      local_date="${2:-}"; shift 2 ;;
        --minprice)  local_min="${2:-}";  shift 2 ;;
        --fetchlist) local_fetch="${2:-}"; shift 2 ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
      esac
    done
    if [[ -n "$local_date" ]]; then DATE_OPT="$local_date"; fi
    echo "Status marker untuk $(target_date):"

    if [[ -n "$local_fetch" ]]; then
      # Cek kombinasi spesifik (pakai minprice & fetchlist yang diberikan)
      # normalisasi fetchlist untuk nama file marker
      norm_fetch="$(echo "$local_fetch" | tr -d ' ' | sed -E 's/[^A-Za-z0-9]+/-/g')"
      for c in "09:30" "11:30" "14:15" "15:50"; do
        cut_norm="$(echo "$c" | sed 's/:/-/')"
        f="$MARK_DIR/$(target_date)_cut${cut_norm}_min${local_min}_f${norm_fetch}.done"
        if [[ -f "$f" ]]; then
          echo " - $c [$local_fetch, min=${local_min}]: done"
        else
          echo " - $c [$local_fetch, min=${local_min}]: pending"
        fi
      done
    else
      # Scan semua marker pada tanggal tsb
      shopt -s nullglob
      files=( "$MARK_DIR/$(target_date)_*.done" )
      if (( ${#files[@]} == 0 )); then
        echo " (tidak ada marker untuk tanggal ini)"
      else
        printf "%-8s %-7s %-8s %-18s\n" "DATE" "CUTOFF" "MIN" "FETCHLIST"
        for f in "${files[@]}"; do
          IFS='|' read -r d cut min fl <<< "$(decode_marker "$f")"
          printf "%-8s %-7s %-8s %-18s\n" "$d" "$cut" "$min" "$fl"
        done | sort
      fi
      shopt -u nullglob
    fi
    ;;

  clean-today)
    log "Bersihkan marker hari ini."
    rm -f "$MARK_DIR/$(date +%F)_*.done" || true
    ;;

  clean-date)
    shift || { echo "Usage: $0 clean-date YYYY-MM-DD"; exit 2; }
    target="$1"
    if [[ ! "$target" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      echo "[ERR] Tanggal harus YYYY-MM-DD" >&2; exit 2
    fi
    log "Bersihkan marker untuk $target."
    rm -f "$MARK_DIR/${target}_*.done" || true
    ;;

  -h|--help)
    usage; exit 0 ;;

  *)
    echo "Unknown mode: $MODE" >&2
    usage; exit 2 ;;
esac


