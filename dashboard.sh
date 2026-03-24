#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-8420}"

cd "$SCRIPT_DIR"

# Source env for gog account
[[ -f .env ]] && source .env

# Refresh data from Google Sheets
echo "Refreshing data..."
python3 "$SCRIPT_DIR/export_dashboard_data.py"

# Kill any existing server on this port
lsof -ti :"$PORT" 2>/dev/null | xargs -r kill 2>/dev/null || true

# Start server in background
python3 -m http.server "$PORT" &>/dev/null &
SERVER_PID=$!

URL="http://localhost:$PORT/dashboard.html"
echo "Dashboard: $URL"

# Open in browser
if command -v xdg-open &>/dev/null; then
  xdg-open "$URL" 2>/dev/null
elif command -v open &>/dev/null; then
  open "$URL"
fi

# Wait for Ctrl+C then cleanup
trap "kill $SERVER_PID 2>/dev/null; echo 'Stopped.'; exit 0" INT TERM
echo "Press Ctrl+C to stop"
wait $SERVER_PID
