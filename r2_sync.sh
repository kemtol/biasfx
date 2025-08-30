#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

# ====================== CONFIG (EDIT INI) ======================
# R2 bucket name (sudah kamu buat pakai: wrangler r2 bucket create <name>)
R2_BUCKET="emiten-archive"

# Folder proyek lokal kamu (root yang berisi folder emiten/)
PROJECT_ROOT="${PROJECT_ROOT:-$PWD}"

# Secara default: hanya sync cache_daily (disarankan untuk init awal)
INCLUDE_INTRADAY="${INCLUDE_INTRADAY:-false}"   # true untuk juga sync 1m/5m/15m
DRY_RUN="${DRY_RUN:-false}"                     # true = coba dulu tanpa upload
CONCURRENCY="${CONCURRENCY:-8}"                 # untuk mode wrangler (parallel put)

# ==== Jika pakai AWS CLI (disarankan, karena R2 kompatibel S3) ====
# Isi kalau mau pakai aws s3 sync:
USE_AWS="${USE_AWS:-false}"                     # set true untuk pakai AWS CLI
R2_ACCOUNT_ID="${R2_ACCOUNT_ID:-}"              # 32-char account id Cloudflare
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"      # R2 Access Key ID
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"  # R2 Secret Key
# Endpoint R2 S3:
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
# ================================================================

# ================== VALIDASI & INFO RINGKAS =====================
SRC_BASE="${PROJECT_ROOT}/emiten"
[[ -d "${SRC_BASE}" ]] || { echo "Folder tidak ditemukan: ${SRC_BASE}"; exit 1; }

echo "[INFO] PROJECT_ROOT   : ${PROJECT_ROOT}"
echo "[INFO] R2 BUCKET      : ${R2_BUCKET}"
echo "[INFO] INCLUDE_INTRADAY=${INCLUDE_INTRADAY} | DRY_RUN=${DRY_RUN} | USE_AWS=${USE_AWS}"

# daftar path sumber lokal
declare -a PATHS=("${SRC_BASE}/cache_daily")
if [[ "${INCLUDE_INTRADAY}" == "true" ]]; then
  PATHS+=("${SRC_BASE}/cache_1m" "${SRC_BASE}/cache_5m" "${SRC_BASE}/cache_15m")
fi

# ============== MODE 1: AWS CLI (smart sync, rekomen) ==============
aws_sync() {
  command -v aws >/dev/null 2>&1 || { echo "[ERR] aws cli tidak ditemukan"; exit 1; }
  [[ -n "${R2_ACCOUNT_ID}" && -n "${AWS_ACCESS_KEY_ID}" && -n "${AWS_SECRET_ACCESS_KEY}" ]] \
    || { echo "[ERR] R2_ACCOUNT_ID / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY belum di-set"; exit 1; }

  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  # R2 butuh --endpoint-url dan --no-verify-ssl (opsional)
  for SRC in "${PATHS[@]}"; do
    [[ -d "${SRC}" ]] || { echo "[WARN] Lewati (tidak ada): ${SRC}"; continue; }
    # contoh: emiten/cache_daily -> s3://emiten-archive/cache_daily/
    DEST="s3://${R2_BUCKET}/$(basename "${SRC}")/"
    echo "[SYNC] ${SRC}  →  ${DEST}"
    if [[ "${DRY_RUN}" == "true" ]]; then
      aws s3 sync "${SRC}/" "${DEST}" \
        --endpoint-url "${R2_ENDPOINT}" --size-only --dryrun
    else
      aws s3 sync "${SRC}/" "${DEST}" \
        --endpoint-url "${R2_ENDPOINT}" --size-only
    fi
  done
}

# ============== MODE 2: Wrangler (loop & parallel put) =============
wrangler_sync() {
  command -v wrangler >/dev/null 2>&1 || { echo "[ERR] wrangler tidak ditemukan"; exit 1; }

  for SRC in "${PATHS[@]}"; do
    [[ -d "${SRC}" ]] || { echo "[WARN] Lewati (tidak ada): ${SRC}"; continue; }
    PREFIX="$(basename "${SRC}")"
    echo "[SYNC] (wrangler) ${SRC}  →  r2://${R2_BUCKET}/${PREFIX}/"

    # kumpulkan file lalu upload paralel
    # NOTE: wrangler belum punya 'sync', jadi kita put satu per satu.
    find "${SRC}" -type f -print0 | xargs -0 -I{} -P "${CONCURRENCY}" bash -c '
      SRC_FILE="{}"
      REL="${SRC_FILE#'"${SRC}"'/}"
      KEY="'"${PREFIX}"'/${REL}"
      if [[ "'"${DRY_RUN}"'" == "true" ]]; then
        echo "[DRY] wrangler r2 object put '"${R2_BUCKET}"'/${KEY} --file ${SRC_FILE}"
      else
        wrangler r2 object put "'"${R2_BUCKET}"'/${KEY}" --file "${SRC_FILE}" >/dev/null \
          && echo "[OK] ${KEY}" || echo "[FAIL] ${KEY}" >&2
      fi
    '
  done
}

# ============================== RUN ===============================
if [[ "${USE_AWS}" == "true" ]]; then
  aws_sync
else
  wrangler_sync
fi

echo "[DONE] Sync selesai."
