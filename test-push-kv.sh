DATE=2025-08-22
SLOT=0930
FILE="rekomendasi/bpjs_rekomendasi_${DATE}_${SLOT}.csv"

curl -sS -X POST "https://bpjs-reko.mkemalw.workers.dev/api/reko/ingest?date=${DATE}&slot=${SLOT}&top=10" \
  -H "Authorization: Bearer b3f172649d534327b053952c57609ad12bca7a1d6ec8e13417af3efc6c027adc" \
  -H "Content-Type: text/csv" \
  --data-binary @"$FILE" | jq .
