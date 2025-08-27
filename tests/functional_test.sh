#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/tests/assert.sh"

TMP="$("$ROOT/tests/make_sandbox.sh" | tail -n1)"
cd "$TMP"
export ALLOW_NONTRADING=1

# 1) --minprice diteruskan ke core (stub menulis last >= minprice+1)
./run_slot.sh once --cutoff 11:30 --minprice 70 --fetchlist "1m,5m"
SNAP="rekomendasi/bpjs_rekomendasi_$(date +%F)_1130.csv"
assert_file "$SNAP"
# cek baris pertama last >= 71 (minprice+1)
FIRST_LAST=$(awk -F, 'NR==2{print $2}' "$SNAP")
(( ${FIRST_LAST%.*} >= 71 )) || fail "minprice not applied to core (got $FIRST_LAST)"
ok "--minprice propagated to core"

# 2) --fetchlist hemat bandwidth → hanya fetchers yang diminta yang jalan (cek log)
[[ -f logs/fetch_1m.log   ]] || fail "1m fetch not run"
[[ -f logs/fetch_5m.log   ]] || fail "5m fetch not run"
[[ -f logs/fetch_15m.log  ]] && fail "15m should NOT run"
[[ -f logs/fetch_daily.log ]] && fail "daily should NOT run"
ok "--fetchlist honored (1m,5m only)"

# 3) run_slot idempotent marker (kombinasi sama → skip)
before=$(wc -l < logs/run_slot.log)
./run_slot.sh once --cutoff 11:30 --minprice 70 --fetchlist "1m,5m"
after=$(wc -l < logs/run_slot.log)
assert_eq "$before" "$after" "should skip duplicate run"
ok "marker/idempotency works"
