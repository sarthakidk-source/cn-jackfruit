#!/usr/bin/env python3
"""
Query or command the running Ryu controller over its control socket.
Standard library only, so it works in the Mininet CLI (as root) or as a normal user.

  python3 scripts/ctl.py ping
  python3 scripts/ctl.py state             # mode, down links, utilisation, app paths, events
  python3 scripts/ctl.py rules             # flow tables the controller believes it installed
  python3 scripts/ctl.py mode static|dynamic
  python3 scripts/ctl.py worker-up 10.0.0.11
  python3 scripts/ctl.py worker-down 10.0.0.11 [quarantine_seconds]
Environment: JACKFRUIT_CTL_SOCKET overrides /tmp/jackfruit-ctl.sock
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from app.controller_client import DEFAULT_SOCKET, request  # noqa: E402


def main(argv):
    if not argv:
        print(__doc__)
        return 1
    path = os.environ.get("JACKFRUIT_CTL_SOCKET", DEFAULT_SOCKET)
    cmd = argv[0]
    if cmd == "ping":
        msg = {"type": "PING"}
    elif cmd == "state":
        msg = {"type": "GET_STATE", "events": 30}
    elif cmd == "rules":
        msg = {"type": "GET_RULES"}
    elif cmd == "mode" and len(argv) == 2:
        msg = {"type": "SET_MODE", "mode": argv[1]}
    elif cmd == "worker-up" and len(argv) == 2:
        msg = {"type": "WORKER_UP", "ip": argv[1]}
    elif cmd == "worker-down" and len(argv) >= 2:
        msg = {"type": "WORKER_DOWN", "ip": argv[1],
               "quarantine_s": float(argv[2]) if len(argv) > 2 else 0}
    else:
        print(__doc__)
        return 1
    try:
        reply = request(path, msg)
    except OSError as e:
        print("cannot reach the controller at %s: %s\nIs Ryu running?" % (path, e))
        return 2
    if cmd == "state":
        for ev in reply.get("events", []):
            ev["t"] = time.strftime("%H:%M:%S", time.localtime(ev["t"])) + (".%03d" % (
                (ev["t"] % 1) * 1000))
        reply.pop("now", None)
    if cmd == "rules":
        for dpid, rules in reply["rules"].items():
            print("s%s" % dpid)
            for prio, src, dst, out in rules:
                print("   prio %-2d src=%-10s dst=%-10s -> %s" % (
                    prio, src or "*", dst or "*", "DROP" if out == -1 else "port %d" % out))
        return 0
    print(json.dumps(reply, indent=1))
    return 0 if reply.get("type") != "ERROR" else 3


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
