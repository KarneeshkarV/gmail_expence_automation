#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-8420}"

cd "$SCRIPT_DIR"

# ─── colors ────────────────────────────────────────────────────────────────
if [[ -t 1 ]] && command -v tput &>/dev/null && [[ "$(tput colors 2>/dev/null || echo 0)" -ge 8 ]]; then
  BOLD=$(tput bold); DIM=$(tput dim); RESET=$(tput sgr0)
  FG_CYAN=$(tput setaf 6); FG_GREEN=$(tput setaf 2); FG_YELLOW=$(tput setaf 3)
  FG_RED=$(tput setaf 1); FG_MAGENTA=$(tput setaf 5); FG_GREY=$(tput setaf 8 2>/dev/null || tput setaf 7)
else
  BOLD=""; DIM=""; RESET=""
  FG_CYAN=""; FG_GREEN=""; FG_YELLOW=""; FG_RED=""; FG_MAGENTA=""; FG_GREY=""
fi

CHECK="${FG_GREEN}✓${RESET}"
ARROW="${FG_CYAN}❯${RESET}"
DOT="${FG_MAGENTA}•${RESET}"
CROSS="${FG_RED}✗${RESET}"

hr() { printf "${FG_GREY}%s${RESET}\n" "────────────────────────────────────────────────────────────"; }

banner() {
  printf "\n"
  printf "${BOLD}${FG_CYAN}  ╭──────────────────────────────────────────────╮${RESET}\n"
  printf "${BOLD}${FG_CYAN}  │${RESET}   ${BOLD}EXPENSE  DASHBOARD${RESET}   ${DIM}· local server${RESET}      ${BOLD}${FG_CYAN}│${RESET}\n"
  printf "${BOLD}${FG_CYAN}  ╰──────────────────────────────────────────────╯${RESET}\n\n"
}

step() { printf "  ${ARROW} ${BOLD}%s${RESET}\n" "$1"; }
ok()   { printf "  ${CHECK} ${DIM}%s${RESET}\n" "$1"; }
info() { printf "  ${DOT} ${DIM}%s${RESET}\n" "$1"; }
fail() { printf "  ${CROSS} ${FG_RED}%s${RESET}\n" "$1"; }

# ─── start ─────────────────────────────────────────────────────────────────
banner

# Source env for google account
if [[ -f .env ]]; then
  source .env
  ok ".env loaded"
else
  info ".env not found (skipping)"
fi

# Refresh data from Google Sheets
step "Refreshing data from Google Sheets"
if python3 "$SCRIPT_DIR/export_dashboard_data.py" >/dev/null 2>&1; then
  ok "dashboard_data.json updated"
else
  fail "data refresh failed"
  exit 1
fi

# Free the port
step "Preparing port ${BOLD}${FG_YELLOW}$PORT${RESET}"
if lsof -ti :"$PORT" >/dev/null 2>&1; then
  lsof -ti :"$PORT" 2>/dev/null | xargs -r kill 2>/dev/null || true
  ok "freed existing process on :$PORT"
else
  ok "port :$PORT is free"
fi

# Start server
step "Starting HTTP server"
python3 -m http.server "$PORT" &>/dev/null &
SERVER_PID=$!
sleep 0.3
ok "server up · pid ${BOLD}$SERVER_PID${RESET}"

URL="http://localhost:$PORT/dashboard.html"

printf "\n"
hr
printf "  ${DIM}dashboard${RESET}   ${BOLD}${FG_CYAN}%s${RESET}\n" "$URL"
printf "  ${DIM}port${RESET}        ${BOLD}%s${RESET}\n" "$PORT"
printf "  ${DIM}pid${RESET}         ${BOLD}%s${RESET}\n" "$SERVER_PID"
hr
printf "\n"

# Open in browser
if command -v xdg-open &>/dev/null; then
  xdg-open "$URL" 2>/dev/null && info "opened in default browser"
elif command -v open &>/dev/null; then
  open "$URL" && info "opened in default browser"
fi

# Cleanup on exit
cleanup() {
  printf "\n"
  step "Shutting down"
  kill "$SERVER_PID" 2>/dev/null || true
  ok "server stopped"
  printf "\n"
  exit 0
}
trap cleanup INT TERM

printf "  ${DIM}press${RESET} ${BOLD}Ctrl+C${RESET} ${DIM}to stop${RESET}\n\n"
wait $SERVER_PID
