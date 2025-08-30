#!/usr/bin/env bash
set -Eeuo pipefail

# ====== KONFIG WAJIB ======
: "${CF_ACCOUNT_ID:?set CF_ACCOUNT_ID dulu}"
: "${CF_API_TOKEN:?set CF_API_TOKEN dulu}"
: "${KV_ID:?set KV_ID dulu}"

# ====== OPSI ======
export TZ="Asia/Jakarta"
DATE="${DATE:-$(date +%F)}"
PREFIX="${PREFIX:-bpjs_rekomendasi_${DATE}_}"

BASE="https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${KV_ID}"
AUTH=(-H "Authorization: Bearer ${CF_API_TOKEN}" -H "Content-Type: application/json")

# URL-encode helper (tanpa here-doc; aman di semua shell)
urlenc() { python3 -c 'import sys,urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }

echo "[KV] Account=${CF_ACCOUNT_ID}  KV_ID=${KV_ID}"
echo "[KV] Target prefix: ${PREFIX}"

# Pastikan jq ada
command -v jq >/dev/null || { echo "[ERR] jq belum terpasang."; exit 1; }

cursor=""
total_deleted=0
enc_prefix="$(urlenc "${PREFIX}")"

while : ; do
  URL="${BASE}/keys?prefix=${enc_prefix}&limit=1000${cursor:+&cursor=${cursor}}"
  RESP="$(curl -fsSL "$URL" "${AUTH[@]}")" || { echo "[ERR] Gagal list keys"; exit 1; }

  mapfile -t KEYS < <(printf '%s' "$RESP" | jq -r '.result[].name')
  cursor="$(printf '%s' "$RESP" | jq -r '.result_info.cursor // empty')"

  if [[ ${#KEYS[@]} -eq 0 ]]; then
    [[ -z "$cursor" ]] && break || continue
  fi

  for k in "${KEYS[@]}"; do
    echo "  - delete ${k}"
    DEL_URL="${BASE}/values/$(urlenc "${k}")"
    curl -fsS -X DELETE "$DEL_URL" "${AUTH[@]}" >/dev/null
    ((total_deleted++))
  done

  [[ -z "$cursor" ]] && break
done

echo "[KV] Deleted: ${total_deleted} key(s). Verifying…"
curl -fsSL "${BASE}/keys?prefix=${enc_prefix}&limit=10" "${AUTH[@]}" \
  | jq -r '.result[].name // empty'
echo "[KV] Done."
