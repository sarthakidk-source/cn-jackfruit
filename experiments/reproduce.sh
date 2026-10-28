#!/bin/bash
# Reproduce the Stage 9 experiments. Run as a NORMAL user from anywhere.
#   experiments/reproduce.sh gen       regenerate experiments/mn/*.mn (3 repetitions)
#   experiments/reproduce.sh analyze   build tables + graphs from results/exp/
# The measurements themselves need the real network, so they are run by hand:
#   Terminal 1 (normal user):  scripts/run_ryu.sh
#   Terminal 2 (sudo):         sudo /usr/bin/python3 topology/topo.py
#   in the mininet> prompt:    source experiments/mn/exp_sched.mn
#                              source experiments/mn/exp_wfail.mn
#                              source experiments/mn/exp_linkfail.mn
#                              source experiments/mn/exp_congestion.mn
# Do not run `mn -c` while Ryu is running (it kills the controller).
set -e
cd "$(dirname "$0")/.."
case "${1:-}" in
  gen)     python3 experiments/gen_experiments.py --reps "${REPS:-3}" ;;
  analyze) python3 experiments/analyze.py ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
