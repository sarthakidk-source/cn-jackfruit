#!/bin/bash
# Start the Jackfruit Ryu controller from the project root, in the ryu venv.
# Refuses to start if something is already listening on the OpenFlow port, so you
# never end up with two controllers. Run as a NORMAL user (not sudo).
#   scripts/run_ryu.sh              # dynamic mode (default)
#   JACKFRUIT_MODE=static scripts/run_ryu.sh
set -e
cd "$(dirname "$0")/.."
PORT=6653
if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "Port $PORT is already in use: a controller is probably running. Not starting another."
  ss -ltnp 2>/dev/null | grep ":$PORT " || true
  exit 1
fi
# shellcheck disable=SC1090
source "${RYU_ENV:-$HOME/ryu-env}/bin/activate"
exec ryu-manager controller/sdn_controller.py
