#!/usr/bin/env bash
set -Eeuo pipefail

fail(){ echo "❌ $*" >&2; exit 1; }
ok(){   echo "✅ $*"; }

assert_eq(){ [[ "$1" == "$2" ]] || fail "assert_eq: '$1' != '$2' ($3)"; }
assert_ne(){ [[ "$1" != "$2" ]] || fail "assert_ne: '$1' == '$2' ($3)"; }
assert_file(){ [[ -f "$1" ]] || fail "file not found: $1"; }
assert_grep(){ grep -qE "$2" "$1" || fail "pattern '$2' not in $1"; }
