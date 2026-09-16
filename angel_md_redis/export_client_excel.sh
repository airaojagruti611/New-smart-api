#!/usr/bin/env bash
# Production snapshot: Redis layer outputs → client Excel
# Pipeline must already be running (./run_all.sh). Redis on localhost:6379.
#
# Usage:
#   ./export_client_excel.sh
#   ./export_client_excel.sh RELIANCE,TCS,INFY
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE"

if [[ -f "$BASE/venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$BASE/venv/bin/activate"
fi

export PYTHONUNBUFFERED=1

ARGS=()
if [[ $# -gt 0 ]]; then
  ARGS+=(--symbols "$1")
fi

python3 "$BASE/export_client_excel.py" "${ARGS[@]}"
