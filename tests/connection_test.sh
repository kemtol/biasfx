#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/tests/assert.sh"

# 1) venv/python tersedia (kalau tidak, pakai system python3)
if [[ -x "$ROOT/.venv/bin/python" ]]; then PY="$ROOT/.venv/bin/python"; else PY="$(command -v python3)"; fi
$PY -V >/dev/null || fail "python not found"
ok "python available: $($PY -V 2>&1)"

# 2) resolusi DNS & TCP basic (isi ENDPOINTS dengan host target—opsional)
ENDPOINTS="${ENDPOINTS:-google.com}"
for host in $(echo "$ENDPOINTS" | tr ',' ' '); do
  getent hosts "$host" >/dev/null || fail "DNS failed for $host"
  ok "DNS ok: $host"
done

# 3) curl HEAD (opsional; skip jika tidak ada curl)
if command -v curl >/dev/null; then
  for host in $(echo "$ENDPOINTS" | tr ',' ' '); do
    curl -sSfI "https://$host" >/dev/null || fail "HTTPS HEAD failed for $host"
    ok "HTTPS head ok: $host"
  done
else
  echo "curl not found—skipping HTTP check"
fi
