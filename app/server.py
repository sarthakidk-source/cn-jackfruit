"""
Client-facing server (Demo 2). Start several of them, each on its own port.

A server never runs tasks and never talks to workers. It only
  * accepts client connections (one thread per connection),
  * puts submitted tasks into the shared SQLite queue,
  * reads task status and results back from the same queue.
Because every server uses the same database file, a client can submit through one
server and read its results through another.

Protocol: the existing client messages (SUBMIT, STATUS_REQ, METRICS_REQ) with one
extra field, "client_id". Task ids are stored as "<client_id>-<task id>" so two
clients can never collide.

Run from the project root, e.g. on h1:
    python3 -u -m app.server --id server1 --port 9201 --db results/demo2.db
"""
import argparse
import logging
import re
import signal
import socket
import threading
import time
import uuid

from app import metrics
from app import shared_queue as sq
from app import taskmodel as tm
from app.protocol import (ERROR, METRICS, METRICS_REQ, STATUS, STATUS_REQ, SUBMIT,
                          SUBMIT_ACK, MessageChannel, ProtocolError)

log = logging.getLogger("server")

CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,16}$")
MAX_BATCH = 5000


def submit(db, server_id, client_id, msg):
    specs = msg.get("tasks")
    if not isinstance(specs, list) or not specs:
        return {"type": ERROR, "reason": "SUBMIT needs a non-empty 'tasks' list"}
    if len(specs) > MAX_BATCH:
        return {"type": ERROR, "reason": "too many tasks in one SUBMIT (max %d)" % MAX_BATCH}
    items, rejected = [], []
    for i, raw in enumerate(specs):
        try:
            spec = tm.normalise_spec(raw)
        except tm.InvalidSpec as e:
            rejected.append({"index": i, "reason": str(e)})
            continue
        local_id = spec.pop("task_id") or uuid.uuid4().hex[:8]
        task_id = "%s-%s" % (client_id, local_id)
        if len(task_id) > 64:
            rejected.append({"index": i, "reason": "task id too long"})
            continue
        items.append((task_id, spec))
    accepted = sq.add_tasks(db, client_id, server_id, items)
    duplicates = [tid for tid, _ in items if tid not in accepted]
    log.info("SUBMIT from %s: %d accepted, %d duplicate, %d rejected",
             client_id, len(accepted), len(duplicates), len(rejected))
    return {"type": SUBMIT_ACK, "accepted": accepted, "duplicates": duplicates,
            "rejected": rejected}


def status(db, client_id, msg):
    ids = msg.get("task_ids")
    if ids is not None and not isinstance(ids, list):
        return {"type": ERROR, "reason": "task_ids must be a list"}
    tasks = sq.get_tasks(db, client_id, ids)
    if ids is not None:
        known = {t["task_id"] for t in tasks}
        tasks += [{"task_id": i, "state": "UNKNOWN"} for i in ids if i not in known]
    count = lambda state: sum(1 for t in tasks if t["state"] == state)
    pending = count(sq.PENDING) + count(sq.CLAIMED)
    return {"type": STATUS, "tasks": tasks, "workers": [],
            "summary": {"tasks": len(tasks), "pending": pending, "assigned": count(sq.ASSIGNED),
                        "completed": count(sq.COMPLETED), "failed": count(sq.FAILED),
                        "all_done": bool(tasks) and pending == 0 and count(sq.ASSIGNED) == 0}}


def handle_client(conn, addr, server_id, db_path):
    ch = MessageChannel(conn)
    db = sq.connect(db_path)          # each thread has its own database connection
    client_id = None
    try:
        while True:
            msg = ch.recv()
            if msg is None:
                break
            sq.count_request(db, server_id)
            cid = msg.get("client_id")
            if cid is not None:
                if not isinstance(cid, str) or not CLIENT_ID_RE.match(cid):
                    ch.send({"type": ERROR, "reason": "client_id must match [A-Za-z0-9_.-]{1,16}"})
                    continue
                if cid != client_id:
                    client_id = cid
                    sq.client_seen(db, client_id, server_id, True)
                    log.info("client %s connected from %s", client_id, addr[0])
            mtype = msg["type"]
            if mtype == SUBMIT:
                if client_id is None:
                    ch.send({"type": ERROR, "reason": "SUBMIT needs a client_id"})
                else:
                    ch.send(submit(db, server_id, client_id, msg))
            elif mtype == STATUS_REQ:
                ch.send(status(db, client_id, msg))
            elif mtype == METRICS_REQ:
                ch.send(dict(metrics.get_stats(db), type=METRICS))
            else:
                ch.send({"type": ERROR, "reason": "unexpected %s from client" % mtype})
    except ProtocolError as e:
        try:
            ch.send({"type": ERROR, "reason": str(e)})
        except OSError:
            pass
    except OSError:
        pass
    finally:
        if client_id is not None:
            sq.client_seen(db, client_id, server_id, False)
            log.info("client %s disconnected", client_id)
        ch.close()
        db.close()


def heartbeat_loop(server_id, db_path, stop):
    db = sq.connect(db_path)
    while not stop.wait(1.0):
        sq.server_heartbeat(db, server_id)
    sq.server_heartbeat(db, server_id, alive=False)    # clean shutdown: show as down


def serve(server_id, host, port, db_path, stop):
    """Run until `stop` is set. Returns after printing the real port."""
    db = sq.connect(db_path)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(64)
    srv.settimeout(0.3)
    port = srv.getsockname()[1]
    sq.register_server(db, server_id, port)
    hb = threading.Thread(target=heartbeat_loop, args=(server_id, db_path, stop))
    hb.start()
    print("server %s listening on port %d" % (server_id, port), flush=True)
    while not stop.is_set():
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=handle_client, args=(conn, addr, server_id, db_path),
                         daemon=True).start()
    srv.close()
    hb.join()


def main():
    p = argparse.ArgumentParser(description="Client-facing server")
    p.add_argument("--id", required=True, help="server name, e.g. server1")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", required=True, help="shared SQLite file")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        datefmt="%H:%M:%S",
                        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    serve(args.id, args.host, args.port, args.db, stop)


if __name__ == "__main__":
    main()
