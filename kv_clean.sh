#!/usr/bin/env bash
# Bersihkan Cloudflare KV untuk reko:<DATE>[:SLOT] (+varian :mk) dan pointer latest
# Contoh:
#   ./kv_clean.sh -d 2025-08-28                 # bersihkan semua slot tanggal itu (non + markov)
#   ./kv_clean.sh -d 2025-08-28 -s 1415         # hanya slot 1415
#   ./kv_clean.sh -d 2025-08-28 -m              # hanya Markov (:mk)
#   ./kv_clean.sh --all                         # NUK semua reko:* & latest:* (hati-hati)
# Opsi:
#   -d, --date YYYY-MM-DD
#   -s, --slot 0930|1130|1415|1550|sum
#   -m, --markov-only
#       --include-summary        # ikut hapus summary:<date>, latest:sum, latest:summary
#   -b, --binding REKO_KV        # default: REKO_KV
#       --config wrangler.toml   # kalau bukan di folder worker
#       --env production|preview
#       --nsid <namespace-id>    # override binding/config
#   --dry-run
#   -y, --yes                    # skip konfirmasi

set -Eeuo pipefail
IFS=$'\n\t'

DATE=""
SLOT=""
MARKOV_ONLY=0
INCLUDE_SUMMARY=0
DRY=0
YES=0
BINDING="REKO_KV"
CONFIG=""
ENVNAME=""
NSID=""

usage(){ sed -n '1,120p' "$0" | sed -n '/^# Bersihkan/,/^set -Eeuo/p'; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--date) DATE="${2:-}"; shift 2;;
    -s|--slot) SLOT="${2:-}"; shift 2;;
    -m|--markov-only) MARKOV_ONLY=1; shift;;
    --include-summary) INCLUDE_SUMMARY=1; shift;;
    -b|--binding) BINDING="${2:-}"; shift 2;;
    --config) CONFIG="${2:-}"; shift 2;;
    --env) ENVNAME="${2:-}"; shift 2;;
    --nsid) NSID="${2:-}"; shift 2;;
    --dry-run) DRY=1; shift;;
    -y|--yes) YES=1; shift;;
    -h|--help) usage;;
    --all) DATE="__ALL__"; shift;;
    *) echo "[ERR] unknown arg: $1"; usage;;
  esac
done

command -v wrangler >/dev/null 2>&1 || { echo "[ERR] wrangler v4 not found"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "[WARN] jq tidak ditemukan; masih bisa jalan tapi kurang nyaman"; }

if [[ -z "$DATE" ]]; then
  echo "[ERR] harus isi --date YYYY-MM-DD atau --all"; exit 2
fi
if [[ -n "$SLOT" && ! "$SLOT" =~ ^(0930|1130|1415|1550|sum)$ ]]; then
  echo "[ERR] slot invalid: $SLOT"; exit 2
fi

WF=()
[[ -n "$CONFIG" ]]  && WF+=(--config "$CONFIG")
[[ -n "$ENVNAME" ]] && WF+=(--env "$ENVNAME")

kv_list() {
  local prefix="$1"
  if [[ -n "$NSID" ]]; then
    wrangler kv key list --namespace-id "$NSID" --prefix "$prefix"
  else
    wrangler kv key list "${WF[@]}" --binding "$BINDING" --prefix "$prefix"
  fi
}
kv_del() {
  local key="$1"
  if [[ $DRY -eq 1 ]]; then
    echo "[DRY] delete $key"
    return 0
  fi
  if [[ -n "$NSID" ]]; then
    wrangler kv key delete --namespace-id "$NSID" --key "$key"
  else
    wrangler kv key delete "${WF[@]}" --binding "$BINDING" --key "$key"
  fi
}

collect_keys() {
  local -n OUT=$1
  OUT=()

  if [[ "$DATE" == "__ALL__" ]]; then
    # semua reko:* (non & markov) + latest:* (non & markov)
    local prefixes=("reko:" "latest:" )
    [[ $INCLUDE_SUMMARY -eq 1 ]] && prefixes+=("summary:")
    for p in "${prefixes[@]}"; do
      local json
      json="$(kv_list "$p" || true)"
      if command -v jq >/dev/null 2>&1; then
        mapfile -t arr < <(jq -r '.[].name' <<<"$json")
      else
        mapfile -t arr < <(awk -F\" '/"name":/ {print $4}' <<<"$json")
      fi
      OUT+=("${arr[@]}")
    done
  else
    # per tanggal
    local base="reko:${DATE}:"
    if [[ -n "$SLOT" ]]; then
      # satu slot (bisa sum)
      local json
      json="$(kv_list "${base}${SLOT}" || true)"
      if command -v jq >/dev/null 2>&1; then
        mapfile -t arr < <(jq -r '.[].name' <<<"$json")
      else
        mapfile -t arr < <(awk -F\" '/"name":/ {print $4}' <<<"$json")
      fi
      OUT+=("${arr[@]}")
      # markov pointer untuk slot tsb
      local mkjson
      mkjson="$(kv_list "${base}${SLOT}:mk" || true)"
      if command -v jq >/dev/null 2>&1; then
        mapfile -t mkarr < <(jq -r '.[].name' <<<"$mkjson")
      else
        mapfile -t mkarr < <(awk -F\" '/"name":/ {print $4}' <<<"$mkjson")
      fi
      OUT+=("${mkarr[@]}")
    else
      # semua slot tanggal itu
      local json
      json="$(kv_list "$base" || true)"
      if command -v jq >/dev/null 2>&1; then
        mapfile -t arr < <(jq -r '.[].name' <<<"$json")
      else
        mapfile -t arr < <(awk -F\" '/"name":/ {print $4}' <<<"$json")
      fi
      OUT+=("${arr[@]}")
    fi

    # pointer latest untuk tanggal tsb tidak ada prefix tanggal,
    # jadi kita hapus semua latest:* jika memang ingin “bersih total tanggal hari itu”.
    # Biasanya aman DIHAPUS semua latest (akan dibangun ulang saat ingest berikutnya).
    local latest=("latest:0930" "latest:1130" "latest:1415" "latest:1550" "latest:sum" "latest:summary" \
                  "latest:0930:mk" "latest:1130:mk" "latest:1415:mk" "latest:1550:mk")
    OUT+=("${latest[@]}")

    # summary per tanggal (opsional)
    if [[ $INCLUDE_SUMMARY -eq 1 ]]; then
      OUT+=("summary:${DATE}" "reko:${DATE}:sum")
    fi
  fi

  # filter markov-only jika diminta
  if [[ $MARKOV_ONLY -eq 1 ]]; then
    local tmp=()
    for k in "${OUT[@]}"; do
      [[ "$k" =~ :mk$ ]] && tmp+=( "$k" )
    done
    OUT=("${tmp[@]}")
  fi

  # unik + sortir
  if ((${#OUT[@]})); then
    mapfile -t OUT < <(printf "%s\n" "${OUT[@]}" | LC_ALL=C sort -u)
  fi
}

main() {
  local KEYS=()
  collect_keys KEYS

  echo "=== Target keys to delete ==="
  printf "%s\n" "${KEYS[@]}" | sed 's/^/ - /'
  echo "Total: ${#KEYS[@]}"
  [[ ${#KEYS[@]} -eq 0 ]] && { echo "(nothing to do)"; exit 0; }

  if [[ $YES -eq 0 ]]; then
    read -r -p "Proceed delete? (yes/NO): " ans
    [[ "$ans" == "yes" ]] || { echo "aborted"; exit 1; }
  fi

  for k in "${KEYS[@]}"; do
    kv_del "$k"
  done

  # index:dates opsional → akan terbangun lagi saat ingest
  if [[ "$DATE" == "__ALL__" ]]; then
    echo "[info] Mengosongkan index:dates (opsional)…"
    kv_del "index:dates" || true
  fi

  echo "Done."
}

main "$@"

