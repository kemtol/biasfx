#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/tests/assert.sh"

TMP="$("$ROOT/tests/make_sandbox.sh" | tail -n1)"
cd "$TMP"

export ALLOW_NONTRADING=1

# 1) ONDEMAND run_slot once → cutoff 09:30 (1m only via fetchlist)
./run_slot.sh once --cutoff 09:30 --minprice 65 --fetchlist "1m"
SNAP="rekomendasi/bpjs_rekomendasi_$(date +%F)_0930.csv"
assert_file "$SNAP"
ok "ondemand snapshot created: $SNAP"

# 2) ONDEMAND markov_15: pertama kali snapshot 14:15 belum ada → exit cepat tanpa result
rm -f "result/bpjs_rekomendasi_$(date +%F).csv" || true
./markov_15.sh || true
[[ -f "result/bpjs_rekomendasi_$(date +%F).csv" ]] && fail "should not have result yet"
ok "minute-friendly: no result when 1415 missing"

# Buat snapshot 14:15 via core stub, lalu jalankan markov_15 lagi
./run_slot.sh once --cutoff 14:15 --minprice 65 --fetchlist "1m,5m,15m"
./markov_15.sh
assert_file "result/bpjs_rekomendasi_$(date +%F).csv"
ok "markov result created after snapshot present"
