#!/usr/bin/env bash
set -euo pipefail

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

# Pastikan tidak ada trailing slash di WORKER_BASE
WORKER_BASE="${WORKER_BASE%/}"

log "WORKER_BASE=$WORKER_BASE"
log "TKLEN=${#CF_TOKEN}"

# ---------- Status-only mode ----------
if [[ $MODE_STATUS -eq 1 ]]; then
  if [[ ! -d "$REKO_DIR" ]]; then
    log "ERROR: folder $REKO_DIR tidak ditemukan."
    exit 1
  fi
  FILES_COUNT=$(find "$REKO_DIR" -maxdepth 1 -type f -name 'bpjs_rekomendasi_*.csv' | wc -l | tr -d ' ')
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

mapfile -t FILES < <(LC_ALL=C find "$REKO_DIR" -maxdepth 1 -type f -name 'bpjs_rekomendasi_*.csv' | sort)
log "Mulai scan ${#FILES[@]} file…"

# Fungsi: cek eksistensi di KV
kv_exists(){
  local date="$1" slot="$2"
  local url="$WORKER_BASE/api/reko/by-date?date=${date}&slot=${slot}"
  local code
  code=$(curl -sS -o /dev/null -w "%{http_code}" "$url" || echo "000")
  [[ "$code" == "200" ]]
}

# Fungsi: upload ke KV (FIX: tidak ada kurung tutup nyasar)
kv_upload(){
  local csv="$1" date="$2" slot="$3" top="$4"
  local url="$WORKER_BASE/api/reko/ingest?date=${date}&slot=${slot}&top=${top}"
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
    [[ -s "$out" ]] && cat "$out"
    rm -f "$out"
    return 1
  fi
  read -r code ip t <<<"$w"
  if [[ "$code" == "200" ]]; then
    log "OK upload: $(basename "$csv") -> 200 ip=${ip:-?} t=${t:-?}"
    cat "$out"
    rm -f "$out"
    return 0
  else
    log "FAIL upload: code=$code ip=${ip:-} t=${t:-} file=$(basename "$csv")"
    [[ -s "$out" ]] && cat "$out"
    rm -f "$out"
    return 1
  fi
}

# Loop file
for csv in "${FILES[@]}"; do
  base="$(basename "$csv")"
  mark="$STATE_DIR/$base.done"

  # Pola 1: intraday dgn slot HHMM → bpjs_rekomendasi_YYYY-MM-DD_HHMM.csv
  if [[ "$base" =~ ^bpjs_rekomendasi_([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{4})\.csv$ ]]; then
    date_part="${BASH_REMATCH[1]}"
    slot_part="${BASH_REMATCH[2]}"
    top_num=$(( $(wc -l < "$csv") - 1 ))
    (( top_num < 0 )) && top_num=0

  # Pola 2: sum harian TANPA slot → bpjs_rekomendasi_YYYY-MM-DD.csv  (slot=sum)
  elif [[ "$base" =~ ^bpjs_rekomendasi_([0-9]{4}-[0-9]{2}-[0-9]{2})\.csv$ ]]; then
    date_part="${BASH_REMATCH[1]}"
    slot_part="sum"
    top_num=$(( $(wc -l < "$csv") - 1 ))
    (( top_num < 0 )) && top_num=0

  else
    log "Lewati (nama tidak cocok pola): $base"
    continue
  fi

  # Skip jika sudah ada mark (kecuali --force)
  if [[ -f "$mark" && $FORCE -eq 0 ]]; then
    log "Sudah sinkron (mark lokal): $base"
    continue
  fi

  # Cek ke KV
  if kv_exists "$date_part" "$slot_part"; then
    log "Sudah ada di KV (tambah mark): $base"
    [[ $DRY -eq 0 ]] && : > "$mark"
    continue
  fi

  # Upload
  if [[ $DRY -eq 1 ]]; then
    log "DRY-RUN upload: $base -> date=$date_part slot=$slot_part top=$top_num"
    continue
  fi

  if kv_upload "$csv" "$date_part" "$slot_part" "$top_num"; then
    : > "$mark"
  else
    log "Gagal upload: $base (akan dicoba lagi di siklus berikutnya)"
  fi
done

log "Selesai sync."
