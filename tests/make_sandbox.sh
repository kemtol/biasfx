#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$ROOT/tests/_tmp"
rm -rf "$TMP"
mkdir -p "$TMP"/{service,rekomendasi,emiten/cache_daily,artifacts/probability_chain,logs,result,config}

# Copy orchestrators
cp "$ROOT/run_slot.sh"   "$TMP/run_slot.sh"
cp "$ROOT/markov_15.sh"  "$TMP/markov_15.sh"
chmod +x "$TMP/run_slot.sh" "$TMP/markov_15.sh"

# ---------- Stub: fetchers (hemat bandwidth, cuma log & sentuh file penanda) ----------
cat > "$TMP/service/idx-fetch-1m.py" <<'PY'
#!/usr/bin/env python3
import os, time, sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
(root/"logs").mkdir(exist_ok=True)
with open(root/"logs"/"fetch_1m.log","a") as f: f.write(f"[{time.strftime('%T')}] fetch 1m\n")
print("stub fetch 1m ok")
PY

cat > "$TMP/service/idx-fetch-5m.py" <<'PY'
#!/usr/bin/env python3
import os, time, sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
(root/"logs").mkdir(exist_ok=True)
with open(root/"logs"/"fetch_5m.log","a") as f: f.write(f"[{time.strftime('%T')}] fetch 5m\n")
print("stub fetch 5m ok")
PY

cat > "$TMP/service/idx-fetch-15m.py" <<'PY'
#!/usr/bin/env python3
import os, time, sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
(root/"logs").mkdir(exist_ok=True)
with open(root/"logs"/"fetch_15m.log","a") as f: f.write(f"[{time.strftime('%T')}] fetch 15m\n")
print("stub fetch 15m ok")
PY

cat > "$TMP/service/idx-fetch-daily.py" <<'PY'
#!/usr/bin/env python3
import os, time, sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
(root/"logs").mkdir(exist_ok=True)
with open(root/"logs"/"fetch_daily.log","a") as f: f.write(f"[{time.strftime('%T')}] fetch daily\n")
print("stub fetch daily ok")
PY

chmod +x "$TMP"/service/idx-fetch-*.py

# ---------- Stub: core-bpjs (buat snapshot sesuai cutoff & minprice) ----------
cat > "$TMP/service/core-bpjs.py" <<'PY'
#!/usr/bin/env python3
import argparse, pathlib, time, datetime as dt, csv, sys
p = argparse.ArgumentParser()
p.add_argument("--cutoff", required=True)
p.add_argument("--top", type=int, default=10)
p.add_argument("--min-price", type=float, default=0)
p.add_argument("--resolutions", default="")
args = p.parse_args()

root = pathlib.Path(__file__).resolve().parents[1]
today = dt.date.today().strftime("%Y-%m-%d")
hhmm = args.cutoff.replace(":","")
out = root / "rekomendasi" / f"bpjs_rekomendasi_{today}_{hhmm}.csv"

rows = []
price_base = max(args.min_price, 60)
for i in range(args.top):
    price = price_base + i + 1
    rows.append({"ticker": f"TST{i:02d}", "last": price, "closing_strength": 0.7, "afternoon_power": 0.6, "vol_pace": 12})

out.parent.mkdir(exist_ok=True, parents=True)
with out.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader(); w.writerows(rows)

print(f"[core-stub] wrote {out}  min_price={args.min_price} resolutions={args.resolutions}")
PY
chmod +x "$TMP/service/core-bpjs.py"

# ---------- Stub: "python -m markov.run_chain_for_snapshot" ----------
# menulis result/bpjs_rekomendasi_YYYY-MM-DD.csv supaya lulus markov_15.sh
mkdir -p "$TMP/markov"
cat > "$TMP/markov/__init__.py" <<'PY'
# stub package
PY

cat > "$TMP/markov/run_chain_for_snapshot.py" <<'PY'
#!/usr/bin/env python3
import argparse, pathlib, csv, datetime as dt
p = argparse.ArgumentParser()
p.add_argument("--snapshot1415", required=True)
p.add_argument("--snapshot0930")
p.add_argument("--snapshot1130")
p.add_argument("--cache_daily_dir")
args = p.parse_args()

root = pathlib.Path(__file__).resolve().parents[1]
today = dt.date.today().strftime("%Y-%m-%d")
out = root / "result" / f"bpjs_rekomendasi_{today}.csv"
out.parent.mkdir(exist_ok=True, parents=True)
with out.open("w", newline="") as f:
    w = csv.writer(f); w.writerow(["Rekomendasi Singkat","Kode Saham"]); w.writerow(["neutral","TST00"])
print(f"[markov-stub] wrote {out}")
PY
chmod +x "$TMP/markov/run_chain_for_snapshot.py"

echo "[sandbox] ready at $TMP"
echo "$TMP"
