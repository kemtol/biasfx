# =====================================================================
# cron_wrap.sh — Runner per SLOT (prefetch → compute) [Dokumentasi]
# =====================================================================
# RINGKASAN
#   Menjalankan pipeline untuk 1 SLOT waktu (prefetch data "hulu",
#   lalu menjalankan run_slot.sh untuk menghasilkan CSV rekomendasi).
#   Skrip ini *fail-fast*: jika prefetch gagal → STOP (tidak lanjut compute).
#
# PENGGUNAAN
#   ./cron_wrap.sh <SLOT> [--dry-run]
#
# ARGUMEN
#   SLOT        : salah satu dari 0930 | 1200 | 1415 | 1550
#   --dry-run   : hanya tampilkan perintah yang akan dijalankan, tanpa eksekusi
#
# PERILAKU
#   1) Mapping SLOT → (CUTOFF, FETCHLIST) contoh default:
#        0930  → CUTOFF="09:30" | FETCHLIST="1m"
#        1200  → CUTOFF="12:00" | FETCHLIST="1m,5m"
#        1415  → CUTOFF="14:15" | FETCHLIST="1m,5m,15m,daily"
#        1550  → CUTOFF="15:50" | FETCHLIST="1m,5m,15m,daily"
#   2) Panggil slot_prefetch.sh <SLOT>  (log: logs/prefetch_<SLOT>.log)
#      - WAJIB: set -o pipefail dan ambil exit code dari proses pertama:
#          bash -lc "...slot_prefetch..." |& tee -a logs/prefetch_${SLOT}.log
#          pref_rc=${PIPESTATUS[0]}; [ $pref_rc -ne 0 ] && exit $pref_rc
#   3) Panggil run_slot.sh once --cutoff "$CUTOFF" --fetchlist "$FETCHLIST" \
#         [opsi lain diteruskan dari ENV/CMD] (log: logs/slot_<SLOT>.log)
#
# VARIABEL LINGKUNGAN (opsional)
#   TZ              : default "Asia/Jakarta"
#   PROJ_ROOT       : root project (default: direktori skrip)
#   LOG_DIR         : default: "$PROJ_ROOT/logs"
#   SCRIPT_DIR      : default: "$PROJ_ROOT"
#   EXTRA_ARGS      : argumen tambahan yang diteruskan ke run_slot.sh
#
# EXIT CODE (disarankan)
#   0   : sukses
#   2   : SLOT tidak dikenal
#   10  : prefetch gagal
#   20  : run_slot gagal (lihat log)
#
# CONTOH
#   Manual:
#     ./cron_wrap.sh 1415
#     ./cron_wrap.sh 1550 --dry-run
#
#   Cron (sesuaikan menit sesuai durasi fetch/compute di mesin kamu):
#     # /etc/crontab (server dengan zona Asia/Jakarta)
#     PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
#     TZ=Asia/Jakarta
#     *  *   * * *   # contoh; ganti menit/ jam sesuai kebutuhan
#     28 9   * * 1-5 /path/cron_wrap.sh 0930 >>/path/logs/cron_0930.log 2>&1
#     58 11  * * 1-5 /path/cron_wrap.sh 1200 >>/path/logs/cron_1200.log 2>&1
#     12 14  * * 1-5 /path/cron_wrap.sh 1415 >>/path/logs/cron_1415.log 2>&1
#     47 15  * * 1-5 /path/cron_wrap.sh 1550 >>/path/logs/cron_1550.log 2>&1
#
# CEK CEPAT
#   tail -n 200 logs/prefetch_1550.log
#   tail -n 200 logs/slot_1550.log
# =====================================================================


# --- ensure venv on PATH (pakai .venv di root project) ---
VENV="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.venv"
if [ -x "$VENV/bin/python" ]; then export PATH="$VENV/bin:$PATH"; fi

set -Eeuo pipefail
umask 002
IFS=$'\n\t'
export TZ="Asia/Jakarta"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# dirs (aman kalau sudah ada)
mkdir -p "$SCRIPT_DIR"/{logs,.locks,service/rekomendasi,rekomendasi}

LOG_DIR="$SCRIPT_DIR/logs"

# ---------- helpers ----------
ts() { date +"%Y-%m-%d %H:%M:%S WIB"; }

die() { echo "[$(ts)] [ERR] $*" >&2; exit 2; }

# ---------- arg parsing / modes ----------
SLOT="${1:-}"; shift || true

# Passthrough mode:
# Jika arg pertama BUKAN slot yang dikenal → anggap label job dan jalankan command mentah setelahnya (opsional diawali `--`)
case "$SLOT" in
  1000|1200|1500|1600|0930|1130|1415|1550|auto)
    # recognized slots → lanjut
    ;;
  *)
    JOB="${SLOT:-custom}"
    if [[ "${1:-}" == "--" ]]; then shift; fi
    [[ $# -ge 1 ]] || die "no command provided for job '$JOB'"

    CMD="$*"
    LOG_FILE="$LOG_DIR/cron_wrap_$(date +%F).log"
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] START job=${JOB} cmd=${CMD}" | tee -a "$LOG_FILE"
    set +e
    bash -lc "$CMD" >> "$LOG_FILE" 2>&1
    RC=$?
    set -e
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] EXIT rc=${RC} for ${JOB}" | tee -a "$LOG_FILE"
    exit $RC
    ;;
esac
# --- akhir passthrough ---

# options untuk mode slot
JOB=""; FETCH_OVERRIDE=""; FORCE="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)        JOB="$2"; shift 2;;
    --fetchlist)  FETCH_OVERRIDE="$2"; shift 2;;
    --force)      FORCE="1"; shift 1;;
    *) die "unknown arg: $1";;
  esac
done

# Mapping SLOT → cutoff & fetchlist (default minprice 65)
CUTOFF="09:30"; FETCHLIST="1m"
case "$SLOT" in
  1000) CUTOFF="09:30"; FETCHLIST="1m" ;;
  1200) CUTOFF="11:30"; FETCHLIST="1m,5m" ;;
  1500) CUTOFF="14:15"; FETCHLIST="1m" ;;
  1600) CUTOFF="15:50"; FETCHLIST="1m,5m,15m,daily" ;;
  auto)
    H=$(date +%H)
    if   ((10<=H && H<12)); then SLOT="1000"; CUTOFF="09:30"; FETCHLIST="1m"
    elif ((12<=H && H<15)); then SLOT="1200"; CUTOFF="11:30"; FETCHLIST="1m,5m"
    elif ((15<=H && H<16)); then SLOT="1500"; CUTOFF="14:15"; FETCHLIST="1m"
    else                     SLOT="1600"; CUTOFF="15:50"; FETCHLIST="1m,5m,15m,daily"
    fi
    ;;
  *) die "slot harus 1000|1200|1500|1600|auto" ;;
esac
[[ -n "$FETCH_OVERRIDE" ]] && FETCHLIST="$FETCH_OVERRIDE"

JOB_TAG="${JOB:-slot-${SLOT}}"
LOG_FILE="$LOG_DIR/cron_wrap_$(date +%F).log"
echo "[$(ts)] JOB=${JOB_TAG} SLOT=${SLOT} → cutoff=${CUTOFF} min=65 fetch=${FETCHLIST}" | tee -a "$LOG_FILE"

# (opsional) PREFETCH per slot — hanya jalan jika skrip ada (FAIL-FAST)
if [[ -x "$SCRIPT_DIR/slot_prefetch.sh" ]]; then
  mkdir -p "$SCRIPT_DIR/.locks"
  set -o pipefail
  /usr/bin/flock -n "$SCRIPT_DIR/.locks/prefetch-${SLOT}.lock" \
    -c "$SCRIPT_DIR/slot_prefetch.sh ${SLOT}" \
    |& tee -a "$LOG_DIR/prefetch_${SLOT}.log"
  pref_rc=${PIPESTATUS[0]}
  if [[ $pref_rc -ne 0 ]]; then
    echo "[$(date +'%F %T')] PREFETCH gagal rc=${pref_rc} — ABORT compute" | tee -a "$LOG_DIR/cron_wrap_$(date +%F).log"
    exit $pref_rc
  fi
fi


# panggil runner utama
CMD=( "./run_slot.sh" "once" "--cutoff" "$CUTOFF" "--minprice" "65" "--fetchlist" "$FETCHLIST" )
[[ "$FORCE" == "1" ]] && CMD+=("--force")

set +e
"${CMD[@]}" >> "$LOG_FILE" 2>&1
rc=$?
set -e
echo "[$(ts)] EXIT rc=${rc} for ${JOB_TAG}" | tee -a "$LOG_FILE"
exit $rc
