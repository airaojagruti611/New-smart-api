#!/usr/bin/env bash
# Stop every pipeline worker started by run_all.sh.
#
# run_all.sh writes one pid file per worker to $LOG_DIR/<YYYY-MM-DD>/pids/<name>.pid.
# This script looks in EVERY date folder (so it still works after midnight),
# sends SIGTERM, waits up to STOP_TIMEOUT seconds, then SIGKILLs survivors.
# A pid file is removed only once its process is gone (or the pid is stale).
#
# Usage:
#   ./stop_all.sh            stop all workers
#   ./stop_all.sh --check    exit 1 if any worker is still running (no signals sent)
#
# Env:
#   LOG_DIR       base log dir (default: <script dir>/logs) — same as run_all.sh
#   STOP_TIMEOUT  seconds to wait after SIGTERM before SIGKILL (default: 20)
#   PIPELINE_DIR  working dir the workers run in (default: <script dir>). On Linux a
#                 pid is only signalled if /proc/<pid>/cwd is this dir, so a stale pid
#                 file (e.g. after a reboot) never kills an unrelated process.
set -uo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-$BASE/logs}"
STOP_TIMEOUT="${STOP_TIMEOUT:-20}"
PIPELINE_DIR="${PIPELINE_DIR:-$BASE}"
PIPELINE_DIR_REAL="$(cd "$PIPELINE_DIR" 2>/dev/null && pwd -P || echo "$PIPELINE_DIR")"

MODE="stop"
case "${1:-}" in
  --check) MODE="check" ;;
  ""|--stop) ;;
  -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
  *) echo "Unknown option: $1 (use --help)" >&2; exit 2 ;;
esac

alive() { kill -0 "$1" 2>/dev/null; }

# Is this pid one of OUR workers (and not a recycled pid)?
ours() {
  local pid="$1" cwd
  if [[ -d "/proc/$pid" ]]; then
    cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null)" || return 1
    [[ "$cwd" == "$PIPELINE_DIR_REAL" ]]
  else
    return 0   # no /proc (macOS): trust the pid file
  fi
}

if [[ ! -d "$LOG_DIR" ]]; then
  echo "No log dir $LOG_DIR — nothing to stop."
  exit 0
fi

declare -a PIDS=() NAMES=() FILES=()
stale=0
while IFS= read -r -d '' f; do
  pid="$(tr -cd '0-9' < "$f")"
  name="$(basename "$f" .pid)"
  if [[ -z "$pid" ]] || ! alive "$pid"; then
    [[ "$MODE" == "stop" ]] && rm -f "$f"
    stale=$((stale + 1))
    continue
  fi
  if ! ours "$pid"; then
    echo "  skip $name (pid $pid): not a pipeline process (pid reused?) — removing stale pid file"
    [[ "$MODE" == "stop" ]] && rm -f "$f"
    stale=$((stale + 1))
    continue
  fi
  PIDS+=("$pid"); NAMES+=("$name"); FILES+=("$f")
done < <(find "$LOG_DIR" -type f -path '*/pids/*.pid' -print0 2>/dev/null | sort -z)

if [[ "$MODE" == "check" ]]; then
  if (( ${#PIDS[@]} > 0 )); then
    echo "Pipeline workers still running (${#PIDS[@]}):"
    for i in "${!PIDS[@]}"; do echo "  ${NAMES[$i]} pid=${PIDS[$i]} (${FILES[$i]})"; done
    exit 1
  fi
  exit 0
fi

if (( ${#PIDS[@]} == 0 )); then
  echo "No running pipeline workers found under $LOG_DIR (removed $stale stale pid file(s))."
  exit 0
fi

echo "Sending SIGTERM to ${#PIDS[@]} worker(s)..."
for i in "${!PIDS[@]}"; do
  kill -TERM "${PIDS[$i]}" 2>/dev/null && echo "  TERM ${NAMES[$i]} (pid ${PIDS[$i]})"
done

deadline=$(( $(date +%s) + STOP_TIMEOUT ))
while :; do
  remaining=0
  for pid in "${PIDS[@]}"; do alive "$pid" && remaining=$((remaining + 1)); done
  (( remaining == 0 )) && break
  (( $(date +%s) >= deadline )) && break
  sleep 0.5
done

if (( remaining > 0 )); then
  echo "$remaining worker(s) still alive after ${STOP_TIMEOUT}s — sending SIGKILL"
  for i in "${!PIDS[@]}"; do
    if alive "${PIDS[$i]}"; then
      kill -KILL "${PIDS[$i]}" 2>/dev/null && echo "  KILL ${NAMES[$i]} (pid ${PIDS[$i]})"
    fi
  done
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    left=0
    for pid in "${PIDS[@]}"; do alive "$pid" && left=$((left + 1)); done
    (( left == 0 )) && break
    sleep 0.3
  done
fi

stopped=0 failed=0
for i in "${!PIDS[@]}"; do
  if alive "${PIDS[$i]}"; then
    echo "  FAILED to stop ${NAMES[$i]} (pid ${PIDS[$i]}); pid file kept: ${FILES[$i]}"
    failed=$((failed + 1))
  else
    rm -f "${FILES[$i]}"
    stopped=$((stopped + 1))
  fi
done

echo "Stopped $stopped worker(s); $failed failed; $stale stale pid file(s) removed."
(( failed == 0 ))
