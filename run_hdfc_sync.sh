#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"
LOCK_FILE="${LOCK_FILE:-$SCRIPT_DIR/.hdfc_sync.lock}"

CPU_THRESHOLD="${CPU_THRESHOLD:-50}"
RAM_THRESHOLD="${RAM_THRESHOLD:-85}"
UV_BIN="${UV_BIN:-uv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/hdfc_sync_$(date +%F).log"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
fi

cpu_usage_percent() {
  local total1 idle1 total2 idle2 dt di usage
  read -r _ user nice system idle iowait irq softirq steal _ < /proc/stat
  total1=$((user + nice + system + idle + iowait + irq + softirq + steal))
  idle1=$((idle + iowait))

  sleep 1

  read -r _ user nice system idle iowait irq softirq steal _ < /proc/stat
  total2=$((user + nice + system + idle + iowait + irq + softirq + steal))
  idle2=$((idle + iowait))

  dt=$((total2 - total1))
  di=$((idle2 - idle1))

  if ((dt <= 0)); then
    echo 100
    return
  fi

  usage=$(((100 * (dt - di)) / dt))
  echo "$usage"
}

ram_usage_percent() {
  local total available used usage
  total=$(awk '/MemTotal:/ {print $2}' /proc/meminfo)
  available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)

  if [[ -z "$total" || -z "$available" || "$total" -le 0 ]]; then
    echo 100
    return
  fi

  used=$((total - available))
  usage=$(((100 * used) / total))
  echo "$usage"
}

# Fire a desktop notification (Mako) for any failure, and log it.
# Resolves the DBUS session bus so it works from cron/systemd too.
notify_error() {
  local title="$1" msg="$2"
  local uid bus
  uid=$(id -u)
  bus=$(find /run/user/"$uid"/ -name "bus" 2>/dev/null | head -1)
  if [[ -n "$bus" ]]; then
    DBUS_SESSION_BUS_ADDRESS="unix:path=$bus" notify-send \
      --urgency=critical \
      --icon=dialog-error \
      --app-name="HDFC Sync" \
      "$title" \
      "$msg" 2>/dev/null || true
  fi
  printf '%s [ERROR] %s: %s\n' "$(date --iso-8601=seconds)" "$title" "$msg" >> "$LOG_FILE"
}

# Run a pipeline step; on any non-zero exit, fire a notification and abort.
# Recognises the OAuth invalid_grant case and gives a more actionable message.
run_step() {
  local label="$1"; shift
  local out rc
  out=$("$@" 2>&1); rc=$?
  printf '%s\n' "$out"
  if ((rc != 0)); then
    if echo "$out" | grep -q "invalid_grant"; then
      notify_error "OAuth Token Expired" \
        "HDFC Sync: Gmail OAuth token expired (invalid_grant). Re-authenticate with: gog auth add karneeshkar68@gmail.com"
    else
      local snippet
      snippet=$(printf '%s' "$out" | tail -n 5 | tr '\n' ' ')
      notify_error "$label failed (exit $rc)" "HDFC Sync: $snippet"
    fi
    exit "$rc"
  fi
}

exec 200>"$LOCK_FILE"
if ! flock -n 200; then
  printf '%s Another sync process is running. Skipping.\n' "$(date --iso-8601=seconds)" >> "$LOG_FILE"
  exit 0
fi

CPU_USAGE=$(cpu_usage_percent)
RAM_USAGE=$(ram_usage_percent)

if ((CPU_USAGE < CPU_THRESHOLD && RAM_USAGE < RAM_THRESHOLD)); then
  {
    printf '%s Starting sync (CPU=%s%% RAM=%s%%).\n' "$(date --iso-8601=seconds)" "$CPU_USAGE" "$RAM_USAGE"
    if command -v "$UV_BIN" >/dev/null 2>&1; then
      RUN="$UV_BIN run"
    else
      RUN="$PYTHON_BIN"
    fi

    run_step "Sync" $RUN "$SCRIPT_DIR/sync_hdfc_expenses.py"
    printf '%s Sync complete.\n' "$(date --iso-8601=seconds)"

    printf '%s Running retag...\n' "$(date --iso-8601=seconds)"
    run_step "Retag" $RUN "$SCRIPT_DIR/sync_hdfc_expenses.py" --retag

    printf '%s Generating report...\n' "$(date --iso-8601=seconds)"
    run_step "Report" $RUN "$SCRIPT_DIR/sync_hdfc_expenses.py" --report

    printf '%s Exporting dashboard data...\n' "$(date --iso-8601=seconds)"
    run_step "Dashboard export" $RUN "$SCRIPT_DIR/export_dashboard_data.py"
  } >> "$LOG_FILE" 2>&1
else
  printf '%s Skipped: CPU=%s%% RAM=%s%% (thresholds: CPU<%s%% RAM<%s%%).\n' \
    "$(date --iso-8601=seconds)" "$CPU_USAGE" "$RAM_USAGE" "$CPU_THRESHOLD" "$RAM_THRESHOLD" >> "$LOG_FILE"
fi
