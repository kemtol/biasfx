# =====================================================================
# run_slot.sh — Generator CSV rekomendasi per cutoff [Dokumentasi]
# =====================================================================
# RINGKASAN
#   Menjalankan pipeline komputasi rekomendasi untuk satu kali eksekusi
#   ("once") pada jam cutoff tertentu. Skrip ini mendukung "freshness guard"
#   agar tidak memakai data 5m/15m/daily yang basi.
#
# PENGGUNAAN
#   ./run_slot.sh once \
#       --cutoff "HH:MM" \
#       [--minprice <INT>] \
#       [--top <INT>] \
#       [--fetchlist "1m,5m,15m,daily"] \
#       [--force]
#
# ARGUMEN
#   once              : mode eksekusi satu kali (wajib)
#   --cutoff HH:MM    : waktu cutoff rekom (mis. "15:50") (wajib)
#   --minprice INT    : filter harga minimum (opsional)
#   --top INT         : batasi jumlah baris rekomendasi (opsional)
#   --fetchlist LIST  : kontrol guard sumber data (default disarankan
#                       per SLOT: "1m" / "1m,5m" / "1m,5m,15m,daily")
#                       Pilihan elemen: 1m | 5m | 15m | daily
#   --force           : abaikan freshness guard (pakai dengan hati-hati)
#
# PERILAKU
#   1) Menentukan tanggal hari ini: DATE=$(date +%F)
#   2) Freshness guard (disarankan AKTIF):
#        Untuk setiap direktori pada --fetchlist, pastikan ada file yang
#        *baru* hari ini. Contoh ambang default:
#          threshold = "${DATE} 08:45"
#        Logika contoh:
#          find emiten/cache_5m  -maxdepth 1 -type f -newermt "$threshold" | grep -q .
#        Jika tidak terpenuhi → exit 66 (STALE).
#   3) Jalankan komputasi (memanggil Python internal sesuai project kamu).
#   4) Tulis output CSV:
#        rekomendasi/bpjs_rekomendasi_${DATE}_${HHMM}.csv
#        (atau sesuai pola proyek kamu)
#   5) Marker sukses:
#        .run_slot/${DATE}_${HHMM}.done — dibuat HANYA setelah semua tahap OK.
#
# LOKASI DEFAULT (disarankan)
#   PROJ_ROOT          : direktori skrip
#   REKODIR            : "$PROJ_ROOT/rekomendasi"
#   CACHEDIR_*         : "$PROJ_ROOT/emiten/cache_{1m,5m,15m,daily}"
#   MARK_DIR           : "$PROJ_ROOT/.run_slot"
#   LOG_DIR            : "$PROJ_ROOT/logs"
#
# VARIABEL LINGKUNGAN (opsional)
#   TZ="Asia/Jakarta"
#   FRESHNESS_THRESHOLD="HH:MM"   # override ambang (default: 08:45)
#   PYTHON_BIN=".venv/bin/python" # kalau mau spesifik python
#
# EXIT CODE (disarankan)
#   0   : sukses
#   66  : STALE — data di salah satu fetchlist belum update hari ini
#   70  : gagal komputasi (script Python error)
#   74  : gagal sinkronisasi/penulisan output
#   2   : argumen tidak valid
#
# CONTOH
#   Eksekusi standar (slot 15:50):
#     ./run_slot.sh once --cutoff "15:50" --minprice 65 \
#       --fetchlist "1m,5m,15m,daily"
#
#   Paksa jalan walau guard gagal (tidak disarankan):
#     ./run_slot.sh once --cutoff "14:15" --force
#
#   Batasi 10 teratas:
#     ./run_slot.sh once --cutoff "15:50" --top 10
#
# CEK & DIAGNOSTIK
#   # lihat timestamp output vs cache
#   ls -l --time-style=+'%F %T' rekomendasi/bpjs_rekomendasi_$(date +%F)_1550.csv
#   find emiten/cache_5m  -maxdepth 1 -type f -newermt "$(date +%F) 08:45" | head
#   # validasi isi bar terakhir (15m), 5 sampel:
#   for f in emiten/cache_15m/*.csv; do tail -n 1 "$f" | cut -d, -f1 | sed "s|^|$f -> |"; done | tail -n 5
# =====================================================================

set -Eeuo pipefail
IFS=$'\n\t'

# ========= Paths & env =========
export TZ="Asia/Jakarta"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MARK_DIR="$SCRIPT_DIR/.run_slot"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$MARK_DIR" "$LOG_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"
PY="${VENV_DIR}/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3 || command -v python)"
fi

# ========= Helpers =========
ts() { date +"%Y-%m-%d %H:%M:%S WIB"; }

sanitize() {
  # turn "1m,5m,15m" -> "1m-5m-15m"
  echo -n "$1" | sed 's/[^A-Za-z0-9]\+/-/g' | sed 's/^-//; s/-$//'
}

mark_file_for() {
  local d="$1" cut="$2" min="$3" fetch="$4"
  local cut_san; cut_san="$(sanitize "$cut")"
  local f_san;   f_san="$(sanitize "$fetch")"
  echo "${MARK_DIR}/${d}_cut${cut_san}_min${min}_f${f_san}.done"
}

# ========= Commands =========
usage() {
  cat <<'EOF'
Usage:
  run_slot.sh once         --cutoff HH:MM [--minprice N] [--fetchlist "1m,5m,15m[,daily]"] [--force]
  run_slot.sh status       [--date YYYY-MM-DD]
  run_slot.sh clean-date   YYYY-MM-DD
  run_slot.sh clean-today
  run_slot.sh clean-all

Notes:
- Marker dir: ./.run_slot
- Log dir   : ./logs
EOF
}

cmd="${1:-}"; shift || true

case "${cmd:-}" in
  once)
    # defaults
    CUTOFF="09:30"
    MINPRICE="65"
    FETCHLIST="1m"
    FORCE="0"

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --cutoff)     CUTOFF="$2"; shift 2;;
        --minprice)   MINPRICE="$2"; shift 2;;
        --min-price)  MINPRICE="$2"; shift 2;;
        --fetchlist|--resolutions) FETCHLIST="$2"; shift 2;;
        --force)      FORCE="1"; shift 1;;
        *) echo "[ERR] unknown arg: $1" >&2; usage; exit 2;;
      esac
    done

    TODAY=$(date +%F)
    MARK_FILE="$(mark_file_for "$TODAY" "$CUTOFF" "$MINPRICE" "$FETCHLIST")"

    if [[ "$FORCE" != "1" && -e "$MARK_FILE" ]]; then
      echo "[$(ts)] Marker exist untuk date=${TODAY} cutoff=${CUTOFF} min=${MINPRICE} fetch=${FETCHLIST} → SKIP."
      exit 0
    fi

    # -------- actual work: recommendation core --------
    echo "[$(ts)] START once cutoff=${CUTOFF} min=${MINPRICE} fetch=${FETCHLIST}"
    set +e
    "$PY" service/core-bpjs.py \
       --cutoff "$CUTOFF" \
       --min-price "$MINPRICE" \
       --resolutions "$FETCHLIST" \
       >> "$LOG_DIR/run_slot_${TODAY}.log" 2>&1
    rc=$?
    set -e
    if [[ $rc -ne 0 ]]; then
      echo "[$(ts)] ERROR rc=$rc (lihat $LOG_DIR/run_slot_${TODAY}.log)"; exit $rc
    fi

    # touch marker after success
    : > "$MARK_FILE"
    echo "[$(ts)] DONE → marker: $MARK_FILE"
    ;;

  status)
    TARGET="${1:-}"
    if [[ "$TARGET" == "--date" && -n "${2:-}" ]]; then TARGET="$2"; fi
    if [[ -z "$TARGET" ]]; then TARGET="$(date +%F)"; fi
    echo "Status marker untuk $TARGET:"
    printf "%-10s %-6s %-8s %-18s\n" "DATE" "CUTOFF" "MIN" "FETCHLIST"
    shopt -s nullglob
    for f in "$MARK_DIR/${TARGET}_cut"*"_min"*"_f"*.done; do
      base="$(basename "$f")"   # e.g., 2025-08-27_cut09-30_min65_f1m-5m.done
      d="${base%%_*}"           # 2025-08-27
      rest="${base#*_cut}"; cut="${rest%%_min*}"
      rest="${rest#*_min}";  min="${rest%%_f*}"
      rest="${rest#*_f}";    fetch="${rest%.done}"
      fetch="${fetch//-/,}"
      printf "%-10s %-6s %-8s %-18s\n" "$d" "$cut" "$min" "$fetch"
    done
    shopt -u nullglob
    ;;

  clean-date)
    [[ $# -lt 1 ]] && { echo "[ERR] butuh YYYY-MM-DD"; exit 2; }
    D="$1"
    rm -f "${MARK_DIR}/${D}_"*.done
    echo "[$(ts)] Bersihkan marker untuk ${D}."
    ;;

  clean-today)
    D="$(date +%F)"
    rm -f "${MARK_DIR}/${D}_"*.done
    echo "[$(ts)] Bersihkan marker untuk ${D}."
    ;;

  clean-all)
    rm -f "${MARK_DIR}/"*".done" || true
    echo "[$(ts)] Bersihkan SEMUA marker."
    ;;

  *)
    usage; exit 2;;
esac
