#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/tests/assert.sh"

TMP="$("$ROOT/tests/make_sandbox.sh" | tail -n1)"
cd "$TMP"
export ALLOW_NONTRADING=1

# 1) 10:00: fetch 1m → core 09:30
./run_slot.sh once --cutoff 09:30 --minprice 65 --fetchlist "1m"
assert_file "rekomendasi/bpjs_rekomendasi_$(date +%F)_0930.csv"

# 2) 12:00: fetch 1m,5m → core 11:30
./run_slot.sh once --cutoff 11:30 --minprice 65 --fetchlist "1m,5m"
assert_file "rekomendasi/bpjs_rekomendasi_$(date +%F)_1130.csv"

# 3) 15:00: fetch 1m,5m,15m → core 14:15 (buat snapshot 14:15)
./run_slot.sh once --cutoff 14:15 --minprice 65 --fetchlist "1m,5m,15m"
assert_file "rekomendasi/bpjs_rekomendasi_$(date +%F)_1415.csv"

# 4) markov_15 (minute-friendly): result dibuat
./markov_15.sh
assert_file "result/bpjs_rekomendasi_$(date +%F).csv"
ok "integration pipeline end-to-end OK"
