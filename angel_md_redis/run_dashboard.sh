#!/usr/bin/env bash
# Production Streamlit UI — live Option Rider layers from Redis.
# Usage:  ./run_dashboard.sh
set -euo pipefail
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASE"
if [[ -f "$BASE/venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$BASE/venv/bin/activate"
fi
export PYTHONUNBUFFERED=1
PORT="${DASHBOARD_PORT:-8501}"
ADDR="${DASHBOARD_ADDR:-0.0.0.0}"
echo "Option Rider dashboard → http://127.0.0.1:${PORT}"
exec streamlit run "$BASE/streamlit_app.py" \
  --server.port "$PORT" \
  --server.address "$ADDR" \
  --server.headless true \
  --browser.gatherUsageStats false
