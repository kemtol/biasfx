#!/usr/bin/env bash
set -euo pipefail

# =========================
#  BPJS Rekomendasi → KV Sync
#  - Support file standar & _MARKOV
#  - Skip _rekon_* dan *_MARKOV_MARKOV.csv
#  - MARKOV: selalu upload (bypass exists) + &markov=1
# =========================

# ---------- Paths & env ----------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REKO_DIR="$ROOT/rekomendasi"
STATE_DIR="$REKO_DIR/.kv_synced"
LOG_DIR="$ROOT/logs"
LOG="$LOG_DIR/kv_sync.log"

mkdir -p "$STATE_DIR" "$LOG_DIR"

# Muat .env dan bersihkan CRLF jika ada
if [[ -f "$ROOT/.env" ]]; then
  TMPENV="$ROOT/.env.runtime"
  tr -d '\r' < "$ROOT/.env" > "$TMPENV"
  set -a
  # shellcheck disable=SC1090
  source "$TMPENV"
  set +a
  rm -f "$TMPENV"
fi

WORKER_BASE="${WORKER_BASE:-}"
CF_TOKEN="${CF_TOKEN:-}"

# ---------- Helpers ----------
ts(){ date '+%F %T'; }
log(){ echo "[$(ts)] $*" | tee -a "$LOG"; }

usage(){
  cat <<EOF
Usage: $(basename "$0") [--status] [--dry-run] [--force]

  --status   : Cek koneksi & tampilkan info, tanpa scan/upload.
  --dry-run  : Simulasi (tidak upload, tidak menulis .done).
  --force    : Abaikan mark lokal, tapi tetap cek KV terlebih dulu.
EOF
}

DRY=0
FORCE=0
MODE_STATUS=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  DRY=1; shift ;;
    --force)    FORCE=1; shift ;;
    --status)   MODE_STATUS=1; shift ;;
    -h|--help)  usage; exit 0 ;;
    *)          echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
done

# Validasi env
if [[ -z "$WORKER_BASE" || -z "$CF_TOKEN" ]]; then
  log "ERROR: WORKER_BASE / CF_TOKEN belum di-set. Isi di .env, mis:"
  log "  WORKER_BASE=https://bpjs-reko.mkemalw.workers.dev"
  log "  CF_TOKEN=<token rahasia>"
  exit 1
fi
WORKER_BASE="${WORKER_BASE%/}"

log "WORKER_BASE=$WORKER_BASE"
log "TKLEN=${#CF_TOKEN}"

# ---------- Status-only mode ----------
if [[ $MODE_STATUS -eq 1 ]]; then
  if [[ ! -d "$REKO_DIR" ]]; then
    log "ERROR: folder $REKO_DIR tidak ditemukan."
    exit 1
  fi
  FILES_COUNT=$(find "$REKO_DIR" -maxdepth 1 -type f -name 'bpjs_rekomendasi_*.csv' \
                  ! -name '*_rekon_*' ! -name '*_MARKOV_MARKOV.csv' | wc -l | tr -d ' ')
  MARKS_COUNT=$(find "$STATE_DIR" -maxdepth 1 -type f -name '*.done' 2>/dev/null | wc -l | tr -d ' ')
  log "Files: $FILES_COUNT | Marks: $MARKS_COUNT | StateDir: $STATE_DIR"
  CODE=$(curl -sS -o /dev/null -w "%{http_code}" "$WORKER_BASE/api/reko/dates" || echo "000")
  log "PING dates => $CODE"
  exit 0
fi

# ---------- Ping worker ----------
CODE=$(curl -sS -o /dev/null -w "%{http_code}" "$WORKER_BASE/api/reko/dates" || echo "000")
if [[ "$CODE" != "200" ]]; then
  log "ERROR: Worker tidak respons (HTTP $CODE) → abort."
  exit 1
fi
log "PING dates => 200 (base=$WORKER_BASE)"

# ---------- Scan files ----------
if [[ ! -d "$REKO_DIR" ]]; then
  log "ERROR: folder $REKO_DIR tidak ditemukan."
  exit 1
fi

# Exclude artefak & rekon
mapfile -t FILES < <(LC_ALL=C find "$REKO_DIR" -maxdepth 1 -type f \
  -name 'bpjs_rekomendasi_*.csv' \
  ! -name '*_rekon_*' \
  ! -name '*_MARKOV_MARKOV.csv' | sort)
log "Mulai scan ${#FILES[@]} file…"

# ---------- HTTP helpers ----------
kv_exists(){
  local date="$1" slot="$2" is_markov="${3:-0}"
  local url="$WORKER_BASE/api/reko/by-date?date=${date}&slot=${slot}"
  [[ "$is_markov" == "1" ]] && url="${url}&markov=1"
  local code
  code=$(curl -sS -o /dev/null -w "%{http_code}" "$url" || echo "000")
  [[ "$code" == "200" ]]
}

kv_upload(){
  local csv="$1" date="$2" slot="$3" top="$4" is_markov="${5:-0}"
  local url="$WORKER_BASE/api/reko/ingest?date=${date}&slot=${slot}&top=${top}"
  [[ "$is_markov" == "1" ]] && url="${url}&markov=1"
  local out rc=0 w code ip t
  out="$(mktemp)"
  w=$(curl -sS -X POST \
       -H "Authorization: Bearer $CF_TOKEN" \
       -H "Content-Type: text/csv" \
       --data-binary @"$csv" \
       --write-out "%{http_code} %{remote_ip} %{time_total}" \
       --output "$out" "$url") || rc=$?
  if [[ $rc -ne 0 ]]; then
    log "FAIL upload: transport rc=$rc file=$(basename "$csv")"
    [[ -s "$out" ]] && cat "$out"; rm -f "$out"; return 1
  fi
  read -r code ip t <<<"$w"
  if [[ "$code" == "200" ]]; then
    log "OK upload: $(basename "$csv") -> 200 ip=${ip:-?} t=${t:-?}"
    cat "$out"; rm -f "$out"; return 0
  else
    log "FAIL upload: code=$code ip=${ip:-} t=${t:-} file=$(basename "$csv")"
    [[ -s "$out" ]] && cat "$out"; rm -f "$out"; return 1
  fi
}

# ---------- Loop file ----------
for csv in "${FILES[@]}"; do
  base="$(basename "$csv")"
  mark="$STATE_DIR/$base.done"

  # Pola 1: intraday → bpjs_rekomendasi_YYYY-MM-DD(_MARKOV)?_HHMM.csv
  if [[ "$base" =~ ^bpjs_rekomendasi_([0-9]{4}-[0-9]{2}-[0-9]{2})(_MARKOV)?_([0-9]{4})\.csv$ ]]; then
    date_part="${BASH_REMATCH[1]}"
    is_markov=$([[ -n "${BASH_REMATCH[2]}" ]] && echo 1 || echo 0)
    slot_part="${BASH_REMATCH[3]}"
  # Pola 2: sum harian → bpjs_rekomendasi_YYYY-MM-DD(_MARKOV)?.csv
  elif [[ "$base" =~ ^bpjs_rekomendasi_([0-9]{4}-[0-9]{2}-[0-9]{2})(_MARKOV)?\.csv$ ]]; then
    date_part="${BASH_REMATCH[1]}"
    is_markov=$([[ -n "${BASH_REMATCH[2]}" ]] && echo 1 || echo 0)
    slot_part="sum"
  else
    log "Lewati (nama tidak cocok pola): $base"
    continue
  fi

  # Hitung top (baris - header)
  lines=$(wc -l < "$csv" | tr -d ' ')
  top_num=$(( lines > 0 ? lines - 1 : 0 ))

  # Skip jika sudah ada mark (kecuali --force)
  if [[ -f "$mark" && $FORCE -eq 0 ]]; then
    log "Sudah sinkron (mark lokal): $base"
    continue
  fi

  # === Kebijakan MARKOV: selalu upload (bypass exists) ===
  if [[ "$is_markov" -eq 1 ]]; then
    if [[ $DRY -eq 1 ]]; then
      log "DRY-RUN upload(MARKOV): $base -> date=$date_part slot=$slot_part top=$top_num"
    else
      if kv_upload "$csv" "$date_part" "$slot_part" "$top_num" 1; then
        : > "$mark"
      else
        log "Gagal upload(MARKOV): $base (akan dicoba lagi)"
      fi
    fi
    continue
  fi

  # === Non-MARKOV: cek dulu di KV ===
  if kv_exists "$date_part" "$slot_part" 0; then
    log "Sudah ada di KV (tambah mark): $base"
    [[ $DRY -eq 0 ]] && : > "$mark"
    continue
  fi

  # Upload non-MARKOV
  if [[ $DRY -eq 1 ]]; then
    log "DRY-RUN upload: $base -> date=$date_part slot=$slot_part top=$top_num"
  else
    if kv_upload "$csv" "$date_part" "$slot_part" "$top_num" 0; then
      : > "$mark"
    else
      log "Gagal upload: $base (akan dicoba lagi)"
    fi
  fi
done

log "Selesai sync."
