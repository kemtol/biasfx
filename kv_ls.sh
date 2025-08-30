#!/usr/bin/env bash
# List & inspect KV keys "reko:YYYY-MM-DD[:SLOT[:mk]]" (Wrangler v4)
# Usage:
#   ./kv_ls.sh                               # hari ini, semua slot non/markov (prefix reko:<today>:)
#   ./kv_ls.sh -d 2025-08-28                 # tanggal tertentu
#   ./kv_ls.sh -d 2025-08-28 -s 1130         # slot tertentu
#   ./kv_ls.sh -d 2025-08-28 -s 1130 -p      # + preview 3 entri dari rows[]
#   ./kv_ls.sh -m                            # hanya key Markov (:mk)
#   ./kv_ls.sh --config path/to/wrangler.toml [--env production]
#   ./kv_ls.sh --nsid <NAMESPACE_ID>
#
# Opsi:
#   -d, --date    YYYY-MM-DD   (default: today, TZ Asia/Jakarta)
#   -s, --slot    0930|1130|1415|1550
#   -b, --binding REKO_KV      (default: REKO_KV)
#       --config  wrangler.toml path
#       --env     wrangler env (mis. production / preview)
#       --nsid    namespace id (override binding/config)
#   -m, --markov-only          tampilkan hanya key dengan suffix ":mk"
#   -p, --peek                 preview 3 baris pertama dari rows[]
#   -h, --help

set -Eeuo pipefail
IFS=$'\n\t'
export TZ="${TZ:-Asia/Jakarta}"

BINDING="REKO_KV"
CONFIG=""
ENVNAME=""
NSID=""
DATE=""
SLOT=""
PEEK=0
MARKOV_ONLY=0

usage(){ sed -n '1,200p' "$0" | sed -n '/^# List & inspect/,/^set -Eeuo/p' ; exit 2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--date)      DATE="${2:-}"; shift 2;;
    -s|--slot)      SLOT="${2:-}"; shift 2;;
    -b|--binding)   BINDING="${2:-}"; shift 2;;
    --config)       CONFIG="${2:-}"; shift 2;;
    --env)          ENVNAME="${2:-}"; shift 2;;
    --nsid)         NSID="${2:-}"; shift 2;;
    -m|--markov-only) MARKOV_ONLY=1; shift;;
    -p|--peek)      PEEK=1; shift;;
    -h|--help)      usage;;
    *) echo "[ERR] unknown arg: $1"; usage;;
  esac
done

command -v wrangler >/dev/null 2>&1 || { echo "[ERR] wrangler v4 not found"; exit 1; }

[[ -z "$DATE" ]] && DATE="$(date +%F)"
if [[ -n "$SLOT" && ! "$SLOT" =~ ^(0930|1130|1415|1550)$ ]]; then
  echo "[ERR] invalid slot: $SLOT (use 0930|1130|1415|1550)"; exit 2
fi

# Prefix dasar:
#  - Tanpa slot: "reko:<DATE>:" → akan match semua slot (dan juga varian :mk)
#  - Dengan slot: "reko:<DATE>:<SLOT>" → akan match key normal & :mk untuk slot tsb
PREFIX="reko:${DATE}:"
[[ -n "$SLOT" ]] && PREFIX="reko:${DATE}:${SLOT}"

# ---- helpers wrangler ----
WF=() # wrangler flags
[[ -n "$CONFIG" ]]  && WF+=(--config "$CONFIG")
[[ -n "$ENVNAME" ]] && WF+=(--env "$ENVNAME")

list_keys() {
  if [[ -n "$NSID" ]]; then
    wrangler kv key list --namespace-id "$NSID" --prefix "$PREFIX"
  else
    wrangler kv key list "${WF[@]}" --binding "$BINDING" --prefix "$PREFIX"
  fi
}

get_value() {
  local key="$1"
  if [[ -n "$NSID" ]]; then
    wrangler kv key get --namespace-id "$NSID" --key "$key"
  else
    wrangler kv key get "${WF[@]}" --binding "$BINDING" --key "$key"
  fi
}

extract_names() {
  if command -v jq >/dev/null 2>&1; then jq -r '.[].name'
  else awk -F\" '/"name":/ {print $4}'; fi
}

rows_from_json() {
  # Kembalikan jumlah elemen di .rows (0 jika tidak ada / bukan JSON)
  if command -v jq >/dev/null 2>&1; then
    jq -r 'try (.rows|length) // 0' 2>/dev/null <<<"$1" || echo 0
  else
    # Fallback kasar tanpa jq: hitung kemunculan "ticker" di blok "rows"
    # (tidak seakurat jq, disarankan install jq)
    awk '
      BEGIN{inrows=0;c=0}
      /"rows"\s*:\s*\[/ {inrows=1; next}
      inrows && /\]/ {inrows=0}
      inrows && /\{/{c++}
      END{print c+0}
    ' <<<"$1"
  fi
}

peek_rows() {
  # Tampilkan 3 entri pertama dari .rows (ticker,score,rekom)
  if command -v jq >/dev/null 2>&1; then
    jq -r '
      try (.rows[:3] | .[] | [
        ( .t    // .ticker // .Ticker // "" ),
        ( .score // .Score // "" ),
        ( .rekom // .rekomendasi // .["rekomendasi singkat"] // "" )
      ] | @tsv) // empty
    ' 2>/dev/null <<<"$1" \
    | awk 'BEGIN{FS="\t"} {printf "    %s\t%s\t%s\n",$1,$2,$3}'
  else
    echo "    (install jq untuk preview yang rapi)"
  fi
}

is_markov_key() {
  [[ "$1" =~ :mk$ ]] && return 0 || return 1
}

# ---- main ----
echo "KV prefix: $PREFIX"
echo "Binding: ${BINDING}${NSID:+  (namespace-id=$NSID)}${CONFIG:+  (config=$CONFIG)}${ENVNAME:+  (env=$ENVNAME)}"
echo

RAW="$(list_keys || true)"
NAMES="$(printf "%s" "$RAW" | extract_names || true)"

if [[ -z "$NAMES" ]]; then
  echo "(no keys)"
  echo
  echo "Tips:"
  echo "  • Jalankan dari folder project Worker (yang ada wrangler.toml), atau"
  echo "  • Gunakan --config /path/ke/wrangler.toml, atau"
  echo "  • Gunakan --nsid <namespace-id> (lihat: wrangler kv namespace list)"
  exit 0
fi

printf "%-36s  %10s  %7s  %6s\n" "KEY" "BYTES" "ROWS" "MKV?"
printf "%-36s  %10s  %7s  %6s\n" "------------------------------------" "----------" "-------" "------"

for key in $NAMES; do
  # Filter markov-only jika diminta
  if [[ $MARKOV_ONLY -eq 1 ]]; then
    is_markov_key "$key" || continue
  fi

  VAL="$(get_value "$key" || true)"
  BYTES=$(printf "%s" "$VAL" | LC_ALL=C wc -c | tr -d ' ')
  ROWS=$(rows_from_json "$VAL")
  MKV=$([[ "$key" =~ :mk$ ]] && echo "yes" || echo "no")
  printf "%-36s  %10d  %7d  %6s\n" "$key" "$BYTES" "$ROWS" "$MKV"

  if [[ "$PEEK" -eq 1 && "$ROWS" -gt 0 ]]; then
    echo "----- preview(3): $key -----"
    peek_rows "$VAL"
    echo "----------------------------"
  fi
done
