#!/usr/bin/env bash
# =====================================================================
# slot_prefetch.sh — Prefetch cerdas per SLOT (skip jika sudah segar)
# =====================================================================
# Usage  : ./slot_prefetch.sh <1000|1200|1500|1600> [--dry-run]
# Logic  : Untuk tiap resolusi (1m/5m/15m/daily) yang dibutuhkan SLOT,
#          cek dulu freshness relatif ke jam cutoff slot:
#             - 1m  : cukup jika mtime >= cutoff - 2m
#             - 5m  : cukup jika mtime >= cutoff - 10m
#             - 15m : cukup jika mtime >= cutoff - 20m
#             - daily: cukup jika mtime >= hari_ini 08:45
#          Jika cukup segar → SKIP fetch; kalau tidak → FETCH lalu re-cek.
# Exit   : 0 ok | 2 arg salah | 66 stale | 127 python tidak ada | 1+ fetch gagal
# =====================================================================

set -Eeuo pipefail
IFS=$'\n\t'
export TZ="Asia/Jakarta"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR" emiten/cache_{1m,5m,15m,daily} "$SCRIPT_DIR/.locks"

SLOT="${1:-}"; [[ -n "$SLOT" ]] || { echo "Usage: $0 <1000|1200|1500|1600> [--dry-run]"; exit 2; }
DRY=0; [[ "${2:-}" == "--dry-run" ]] && DRY=1

# Interpreter: .venv > python3 > python
PY="$SCRIPT_DIR/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3 || true)"; [[ -n "$PY" ]] || PY="$(command -v python || true)"
fi
[[ -n "$PY" ]] || { echo "[ERR] python/python3 tidak ditemukan"; exit 127; }

TICKERS="config/TICKERS.txt"
[[ -f "$TICKERS" ]] || echo "[WARN] $TICKERS tidak ditemukan — fetcher mungkin pakai default."

# --- Mapping SLOT → cutoff & fetchlist (samakan dengan cron_wrap kamu) ---
CUTOFF="09:30"; FETCHLIST="1m"
case "$SLOT" in
  1000) CUTOFF="09:30"; FETCHLIST="1m" ;;
  1200) CUTOFF="11:30"; FETCHLIST="1m,5m" ;;
  1500) CUTOFF="14:15"; FETCHLIST="1m,5m,15m,daily" ;;
  1600) CUTOFF="15:50"; FETCHLIST="1m,5m,15m,daily" ;;
  *) echo "[ERR] Unknown SLOT=$SLOT"; exit 2 ;;
esac

TODAY="$(date +%F)"
CUTOFF_TS="${TODAY} ${CUTOFF}"

# Ambang freshness default (bisa override via ENV jika mau)
DELTA_1M="${DELTA_1M:--2 minutes}"
DELTA_5M="${DELTA_5M:--10 minutes}"
DELTA_15M="${DELTA_15M:--20 minutes}"
DAILY_THRES="${DAILY_THRES:-${TODAY} 08:45}"

THRES_1M="$(date -d "${CUTOFF_TS} ${DELTA_1M}" +'%F %T')"
THRES_5M="$(date -d "${CUTOFF_TS} ${DELTA_5M}" +'%F %T')"
THRES_15M="$(date -d "${CUTOFF_TS} ${DELTA_15M}" +'%F %T')"
THRES_D="$(date -d "${DAILY_THRES}" +'%F %T')"

echo "[INFO] SLOT=${SLOT} cutoff=${CUTOFF_TS} fetch=${FETCHLIST} | PY=$PY"
echo "[INFO] Thresholds → 1m: ${THRES_1M} | 5m: ${THRES_5M} | 15m: ${THRES_15M} | daily: ${THRES_D}"

need_1m=0 need_5m=0 need_15m=0 need_d=0
IFS=',' read -ra parts <<< "$FETCHLIST"
for p in "${parts[@]}"; do
  case "$p" in
    1m) need_1m=1;;
    5m) need_5m=1;;
    15m) need_15m=1;;
    daily|d|D) need_d=1;;
  esac
done

run() {
  if [[ $DRY -eq 1 ]]; then
    echo "[DRY] $*"
  else
    eval "$@"
  fi
}

fresh_dir() {
  local dir="$1" thres="$2"
  # True bila ADA file di dir dengan mtime >= threshold
  find "$dir" -maxdepth 1 -type f -newermt "$thres" | grep -q .
}

fetch_1m()  { run "$PY service/idx-fetch-1m.py   --tickers '$TICKERS' --outdir emiten/cache_1m   | tee -a '$LOG_DIR/idx-fetch-1m.log'"; }
fetch_5m()  { run "$PY service/idx-fetch-5m.py   --tickers '$TICKERS' --outdir emiten/cache_5m   | tee -a '$LOG_DIR/idx-fetch-5m.log'"; }
fetch_15m() { run "$PY service/idx-fetch-15m.py  --tickers '$TICKERS' --outdir emiten/cache_15m  | tee -a '$LOG_DIR/idx-fetch-15m.log'"; }
fetch_d()   { run "$PY service/idx-fetch-daily.py --tickers '$TICKERS' --outdir emiten/cache_daily | tee -a '$LOG_DIR/idx-fetch-daily.log'"; }

rc_all=0

# ---------- 1m ----------
if [[ $need_1m -eq 1 ]]; then
  if fresh_dir "emiten/cache_1m" "$THRES_1M"; then
    echo "[SKIP] 1m sudah segar (>= $THRES_1M)"
  else
    echo "[FETCH] 1m..."
    fetch_1m || rc_all=$?
    [[ $rc_all -eq 0 && fresh_dir "emiten/cache_1m" "$THRES_1M" ]] || { echo "[ERR] 1m masih stale setelah fetch"; exit 66; }
  fi
fi

# ---------- 5m ----------
if [[ $need_5m -eq 1 ]]; then
  if fresh_dir "emiten/cache_5m" "$THRES_5M"; then
    echo "[SKIP] 5m sudah segar (>= $THRES_5M)"
  else
    echo "[FETCH] 5m..."
    fetch_5m || rc_all=$?
    [[ $rc_all -eq 0 && fresh_dir "emiten/cache_5m" "$THRES_5M" ]] || { echo "[ERR] 5m masih stale setelah fetch"; exit 66; }
  fi
fi

# ---------- 15m ----------
if [[ $need_15m -eq 1 ]]; then
  if fresh_dir "emiten/cache_15m" "$THRES_15M"; then
    echo "[SKIP] 15m sudah segar (>= $THRES_15M)"
  else
    echo "[FETCH] 15m..."
    fetch_15m || rc_all=$?
    [[ $rc_all -eq 0 && fresh_dir "emiten/cache_15m" "$THRES_15M" ]] || { echo "[ERR] 15m masih stale setelah fetch"; exit 66; }
  fi
fi

# ---------- daily ----------
if [[ $need_d -eq 1 ]]; then
  if fresh_dir "emiten/cache_daily" "$THRES_D"; then
    echo "[SKIP] daily sudah segar (>= $THRES_D)"
  else
    echo "[FETCH] daily..."
    fetch_d || rc_all=$?
    [[ $rc_all -eq 0 && fresh_dir "emiten/cache_daily" "$THRES_D" ]] || { echo "[ERR] daily masih stale setelah fetch"; exit 66; }
  fi
fi

[[ $rc_all -eq 0 ]] || { echo "[ERR] Salah satu fetcher gagal (rc=$rc_all)"; exit $rc_all; }
echo "[OK] Prefetch selesai (SLOT=$SLOT, cutoff=$CUTOFF_TS, fetch=$FETCHLIST)"
