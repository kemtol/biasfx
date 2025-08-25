#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ====================== KONFIG ======================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REKO_DIR="${REKO_DIR:-$SCRIPT_DIR/rekomendasi}"

WORKER_BASE="${WORKER_BASE:-https://bpjs-reko.mkemalw.workers.dev}"

CF_TOKEN="${CF_TOKEN:-}"
if [[ -z "${CF_TOKEN}" && -f "$SCRIPT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
fi
CF_TOKEN="${CF_TOKEN:-}"

TOP_DEFAULT="${TOP_DEFAULT:-10}"
RETRY="${RETRY:-3}"
TIMEOUT_CONN="${TIMEOUT_CONN:-5}"
TIMEOUT_MAX="${TIMEOUT_MAX:-25}"

STATE_DIR="$REKO_DIR/.kv_synced"
LOCK_FILE="$REKO_DIR/.kv_sync.lock"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$STATE_DIR" "$LOG_DIR"
LOG_FILE="$LOG_DIR/kv_sync.log"
# ===================================================

log(){ echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }
die(){ log "ERROR: $*"; exit 1; }

command -v curl >/dev/null || die "curl tidak ditemukan"
command -v jq >/dev/null || true

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "Lock aktif, proses sync lain sedang berjalan. Keluar."
  exit 0
fi

[[ -d "$REKO_DIR" ]] || die "Folder rekomendasi tidak ada: $REKO_DIR"
[[ -n "$CF_TOKEN" ]] || die "CF_TOKEN kosong. Set ENV atau .env"

parse_name(){ # bpjs_rekomendasi_YYYY-MM-DD_HHMM.csv
  local fn="$1"
  if [[ "$fn" =~ bpjs_rekomendasi_([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{4})\.csv$ ]]; then
    echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]}"; return 0
  fi
  return 1
}

# cek KV: ada / tidak
kv_has(){
  local date="$1" slot="$2"
  curl -sfS -H "Origin: http://localhost" \
    "$WORKER_BASE/api/reko/by-date?date=${date}&slot=${slot}" >/dev/null
}

sync_one(){
  local file="$1" base; base="$(basename "$file")"
  local date slot
  if ! read -r date slot < <(parse_name "$base"); then
    log "Skip (nama tidak sesuai pola): $base"; return 0
  fi

  local mark="$STATE_DIR/${date}_${slot}.done"

  # 1) kalau marker ada & lebih baru/equal dari CSV → sudah sinkron
  if [[ -f "$mark" && "$mark" -nt "$file" ]]; then
    log "OK (up-to-date): $base"
    return 0
  fi

  # 2) kalau KV SUDAH ADA tetapi marker belum ada → buat marker & skip upload
  if kv_has "$date" "$slot"; then
    if [[ ! -f "$mark" ]]; then
      touch -r "$file" "$mark"
      log "Ada di KV (buat marker): $base"
      return 0
    fi
    # marker ada tapi CSV lebih baru → lanjut upload untuk perbarui KV
    log "Ada di KV, CSV lebih baru → upload: $base"
  else
    log "Belum ada di KV → upload: $base"
  fi

  # 3) upload / upsert
  local http_code
  http_code=$(curl -sS \
    --connect-timeout "$TIMEOUT_CONN" --max-time "$TIMEOUT_MAX" \
    --retry "$RETRY" --retry-delay 1 --retry-connrefused \
    -o "$STATE_DIR/.tmp_resp.json" -w "%{http_code}" \
    -X POST "$WORKER_BASE/api/reko/ingest?date=${date}&slot=${slot}&top=${TOP_DEFAULT}" \
    -H "Authorization: Bearer ${CF_TOKEN}" \
    -H "Content-Type: text/csv" \
    --data-binary @"$file" ) || http_code=000

  if [[ "$http_code" != "200" ]]; then
    log "GAGAL upload ($http_code): $base"
    if command -v jq >/dev/null; then jq . < "$STATE_DIR/.tmp_resp.json" || cat "$STATE_DIR/.tmp_resp.json"; else cat "$STATE_DIR/.tmp_resp.json"; fi
    rm -f "$STATE_DIR/.tmp_resp.json"
    return 1
  fi

  touch -r "$file" "$mark"
  log "SUKSES upload: $base"
  if command -v jq >/devnull; then jq . < "$STATE_DIR/.tmp_resp.json" || true; else cat "$STATE_DIR/.tmp_resp.json"; fi
  rm -f "$STATE_DIR/.tmp_resp.json"
}

main(){
  shopt -s nullglob
  local files=("$REKO_DIR"/bpjs_rekomendasi_*.csv)
  if [[ ${#files[@]} -eq 0 ]]; then log "Tidak ada file CSV."; return 0; fi
  log "Mulai scan ${#files[@]} file…"
  for f in "${files[@]}"; do sync_one "$f" || true; done
  log "Selesai sync."
}
main "$@"
