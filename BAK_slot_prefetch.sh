#!/usr/bin/env bash
# =====================================================================
# slot_prefetch.sh — Prefetch data hulu per SLOT (fail-fast, modular)
# =====================================================================
# Lokasi  : Letakkan di root project, sejajar dengan run_slot.sh/cron_wrap.sh
# Argumen : slot_prefetch.sh <SLOT> [--dry-run]
# SLOT    : 1000 | 1200 | 1500 | 1600  (mengikuti crontab/cron_wrap)
#
# Mapping resolusi per SLOT:
#   1000  → 1m
#   1200  → 1m,5m
#   1500  → 1m,5m,15m,daily
#   1600  → 1m,5m,15m,daily
#
# Exit code:
#   0  = sukses
#   2  = argumen salah / SLOT tak dikenal
#   66 = freshness check gagal (cache belum “hari ini”)
#   127= python/python3 tidak ditemukan
#   1+ = salah satu fetcher gagal
# =====================================================================

set -Eeuo pipefail
IFS=$'\n\t'
export TZ="Asia/Jakarta"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR" emiten/cache_{1m,5m,15m,daily}

usage() { echo "Usage: $0 <1000|1200|1500|1600> [--dry-run]"; }

SLOT="${1:-}"; [[ -n "$SLOT" ]] || { usage; exit 2; }
DRY=0; [[ "${2:-}" == "--dry-run" ]] && DRY=1

# Pilih interpreter: .venv kalau ada, else python3, else python
PY="$SCRIPT_DIR/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3 || true)"
  [[ -n "$PY" ]] || PY="$(command -v python || true)"
fi
[[ -n "$PY" ]] || { echo "[ERR] python/python3 tidak ditemukan"; exit 127; }

TICKERS="config/TICKERS.txt"
[[ -f "$TICKERS" ]] || echo "[WARN] $TICKERS tidak ditemukan — fetcher kamu mungkin punya default sendiri."

# Mapping SLOT → fetchlist
FETCHLIST=""
case "$SLOT" in
  1000) FETCHLIST="1m" ;;
  1200) FETCHLIST="1m,5m" ;;
  1500) FETCHLIST="1m,5m,15m,daily" ;;
  1600) FETCHLIST="1m,5m,15m,daily" ;;
  *) echo "[ERR] Unknown SLOT=$SLOT"; usage; exit 2 ;;
esac

echo "[INFO] SLOT=$SLOT → FETCH=$FETCHLIST | PY=$PY"

# Parse fetchlist → flags
NEED_1M=0; NEED_5M=0; NEED_15M=0; NEED_D=0
IFS=',' read -ra parts <<< "$FETCHLIST"
for p in "${parts[@]}"; do
  case "$p" in
    1m)   NEED_1M=1 ;;
    5m)   NEED_5M=1 ;;
    15m)  NEED_15M=1 ;;
    daily|d|D) NEED_D=1 ;;
  esac
done

run() {
  if [[ $DRY -eq 1 ]]; then
    echo "[DRY] $*"
  else
    eval "$@"
  fi
}

rc_all=0

# --- Fetch berurutan (log terpisah per resolusi) ---
if [[ $NEED_1M -eq 1 ]]; then
  run "$PY service/idx-fetch-1m.py  --tickers '$TICKERS' --outdir emiten/cache_1m   | tee -a '$LOG_DIR/idx-fetch-1m.log'" || rc_all=$?
fi
if [[ $NEED_5M -eq 1 ]]; then
  run "$PY service/idx-fetch-5m.py  --tickers '$TICKERS' --outdir emiten/cache_5m   | tee -a '$LOG_DIR/idx-fetch-5m.log'" || rc_all=$?
fi
if [[ $NEED_15M -eq 1 ]]; then
  run "$PY service/idx-fetch-15m.py --tickers '$TICKERS' --outdir emiten/cache_15m  | tee -a '$LOG_DIR/idx-fetch-15m.log'" || rc_all=$?
fi
if [[ $NEED_D -eq 1 ]]; then
  run "$PY service/idx-fetch-daily.py --tickers '$TICKERS' --outdir emiten/cache_daily | tee -a '$LOG_DIR/idx-fetch-daily.log'" || rc_all=$?
fi

if [[ $rc_all -ne 0 ]]; then
  echo "[ERR] Salah satu fetcher gagal (rc=$rc_all)"; exit $rc_all
fi

# --- Freshness guard: pastikan ada file baru 'hari ini' ---
if [[ $DRY -eq 0 ]]; then
  TODAY="$(date +%F)"
  # Ambang aman: pasar sudah buka; ubah jika perlu per slot
  THRESHOLD="${TODAY} 08:45"

  to_check=()
  [[ $NEED_1M  -eq 1 ]] && to_check+=("emiten/cache_1m")
  [[ $NEED_5M  -eq 1 ]] && to_check+=("emiten/cache_5m")
  [[ $NEED_15M -eq 1 ]] && to_check+=("emiten/cache_15m")
  [[ $NEED_D   -eq 1 ]] && to_check+=("emiten/cache_daily")

  for d in "${to_check[@]}"; do
    if ! find "$d" -maxdepth 1 -type f -newermt "$THRESHOLD" | grep -q .; then
      echo "[ERR] STALE: $d belum ada file yang lebih baru dari $THRESHOLD"; exit 66
    else
      echo "[OK] Fresh: $d (>= $THRESHOLD)"
    fi
  done
fi

echo "[OK] Prefetch selesai untuk SLOT=$SLOT (FETCH=$FETCHLIST)"
