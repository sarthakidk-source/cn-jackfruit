"""
SDN forwarding policy for the CN Jackfruit project (pure Python, no Ryu import, so
it can be unit-tested without a controller or a network).

The Ryu app (sdn_controller.py) is a thin layer that feeds events into a
PolicyState and then pushes the resulting flow table to the switches.

Network map (must mirror topology/topo.py)
-------------------------------------------
        h1(10.0.0.1)  h2(10.0.0.2)
                 \\    /
                   s1
                 /    \\
               s2 ---- s3
              /  \\    /  \\
            w1   w2  w3   w4

Flow rules (OpenFlow 1.3, all match IPv4)
------------------------------------------
priority 10  BASE        match ipv4_dst=<host>            -> shortest path to host
priority 20  APP         match ipv4_src=h1,ipv4_dst=<w>   -> path chosen by policy
                         match ipv4_src=<w>,ipv4_dst=h1   -> same path, reversed
priority 30  QUARANTINE  match ipv4_src=<w> / ipv4_dst=<w> -> DROP (temporary)

Modes
-----
static   Baseline. Only BASE rules, computed as if every link were up, never
         changed afterwards. Ignores link failures, load and application events.
dynamic  The proposed system. Reacts to
           * link up/down  (PORT_STATUS)           -> recompute every path
           * link load     (PORT_STATS polling)    -> steer application flows away
                                                      from congested core links
           * application   (WORKER_UP / WORKER_DOWN from the task manager)
                                                   -> create / remove APP flows,
                                                      optional quarantine

"APP" flows exist only for workers the task manager has announced, so the
controller only steers traffic it has been told is application traffic. That is
the concrete way application information changes forwarding.
"""
import time

# ---- network map ------------------------------------------------------------------
# ip: (name, mac, switch dpid, switch port the host is plugged into)
HOSTS = {
    "10.0.0.1":  ("h1", "00:00:00:00:00:01", 1, 3),
    "10.0.0.2":  ("h2", "00:00:00:00:00:02", 1, 4),
    "10.0.0.11": ("w1", "00:00:00:00:00:11", 2, 3),
    "10.0.0.12": ("w2", "00:00:00:00:00:12", 2, 4),
    "10.0.0.13": ("w3", "00:00:00:00:00:13", 3, 3),
    "10.0.0.14": ("w4", "00:00:00:00:00:14", 3, 4),
}

# (switch, neighbour switch) -> port on 'switch' that faces 'neighbour'
LINK_PORTS = {
    (1, 2): 1, (2, 1): 1,
    (1, 3): 2, (3, 1): 1,
    (2, 3): 2, (3, 2): 2,
}

MANAGER_IP = "10.0.0.1"
WORKER_IPS = frozenset(ip for ip, h in HOSTS.items() if h[0].startswith("w"))
SWITCHES = tuple(sorted({a for a, _ in LINK_PORTS}))
# (switch, port) -> neighbouring switch, for core ports only
PORT_NEIGHBOR = {(a, p): b for (a, b), p in LINK_PORTS.items()}
CORE_CAPACITY_MBPS = 10.0

PRIO_TABLE_MISS = 0
PRIO_BASE = 10
PRIO_APP = 20
PRIO_QUARANTINE = 30
DROP = -1

MODE_STATIC = "static"
MODE_DYNAMIC = "dynamic"
MODES = (MODE_STATIC, MODE_DYNAMIC)

# ---- steering parameters (documented in README) --------------------------------------
UTIL_ALPHA = 0.5         # EWMA weight of the newest utilisation sample
UTIL_HIGH = 0.70         # a path is "congested" above this fraction of link capacity
UTIL_MARGIN = 0.30       # only move to a path at least this much less loaded
STEER_COOLDOWN_S = 5.0   # minimum time between path changes of one flow (anti-flap)
UTIL_BUCKET = 0.10       # initial path choice treats loads within 10% as equal


def link_key(a, b):
    return frozenset((a, b))


# ---- graph helpers -----------------------------------------------------------------------
def neighbors(sw, down=frozenset()):
    return sorted(b for (a, b) in LINK_PORTS if a == sw and link_key(a, b) not in down)


def shortest_path(src, dst, down=frozenset()):
    """BFS shortest path between two switches (ties broken by lowest switch id).
    Returns a list of dpids, or None if unreachable."""
    if src == dst:
        return [src]
    prev = {src: None}
    queue = [src]
    while queue:
        u = queue.pop(0)
        if u == dst:
            break
        for v in neighbors(u, down):
            if v not in prev:
                prev[v] = u
                queue.append(v)
    if dst not in prev:
        return None
    path = [dst]
    while path[-1] != src:
        path.append(prev[path[-1]])
    return path[::-1]


def simple_paths(src, dst, down=frozenset(), limit=6):
    """All loop-free switch paths src -> dst avoiding down links, shortest first."""
    out = []

    def dfs(node, path):
        if len(out) >= limit:
            return
        if node == dst:
            out.append(tuple(path))
            return
        for v in neighbors(node, down):
            if v not in path:
                path.append(v)
                dfs(v, path)
                path.pop()
    dfs(src, [src])
    return sorted(out, key=lambda p: (len(p), p))


def path_ports(path, last_port):
    """[(switch, output port)] for forwarding along `path`; the last switch
    outputs on `last_port` (the port the destination host hangs off)."""
    hops = []
    for i, sw in enumerate(path):
        out = LINK_PORTS[(sw, path[i + 1])] if i + 1 < len(path) else last_port
        hops.append((sw, out))
    return hops


# ---- rule diffing --------------------------------------------------------------------------
def diff_rules(installed, desired):
    """installed/desired: {key: out_port}. Returns (to_add, to_delete) where to_add
    maps keys that are new or changed, and to_delete lists keys no longer wanted."""
    to_add = {k: v for k, v in desired.items() if installed.get(k) != v}
    to_delete = [k for k in installed if k not in desired]
    return to_add, to_delete


# ---- the state machine ---------------------------------------------------------------------
class PolicyState:
    def __init__(self, mode=MODE_DYNAMIC, clock=time.time, capacity_mbps=CORE_CAPACITY_MBPS):
        if mode not in MODES:
            raise ValueError("mode must be one of %s" % (MODES,))
        self.mode = mode
        self.clock = clock
        self.capacity_mbps = capacity_mbps
        self.port_down = set()     # (dpid, port) reported down by the switches
        self.util = {}             # (a, b) -> EWMA fraction of capacity, direction a->b
        self.app_workers = {}      # worker ip -> worker name, announced by the manager
        self.app_paths = {}        # worker ip -> tuple of dpids (manager sw ... worker sw)
        self.last_steer = {}       # worker ip -> time of last path change
        self.quarantine = {}       # worker ip -> expiry time
        self.events = []

    # -- bookkeeping --------------------------------------------------------------
    def log(self, kind, **detail):
        ev = dict(detail, t=self.clock(), kind=kind)
        self.events.append(ev)
        if len(self.events) > 2000:
            del self.events[:500]
        return ev

    def set_mode(self, mode):
        if mode not in MODES:
            raise ValueError("mode must be one of %s" % (MODES,))
        if mode != self.mode:
            self.mode = mode
            self.log("mode_changed", mode=mode)
            if mode == MODE_DYNAMIC:
                self.refresh_paths(reselect=True)
            return True
        return False

    # -- link state ------------------------------------------------------------------
    def down_links(self):
        """Links with at least one end reported down."""
        down = set()
        for dpid, port in self.port_down:
            nb = PORT_NEIGHBOR.get((dpid, port))
            if nb is not None:
                down.add(link_key(dpid, nb))
        return down

    def effective_down(self):
        return frozenset(self.down_links()) if self.mode == MODE_DYNAMIC else frozenset()

    def set_port(self, dpid, port, is_down):
        """Record a port state report. Returns True if the set of down links changed."""
        if (dpid, port) not in PORT_NEIGHBOR:
            return False                      # host-facing port: irrelevant
        before = self.down_links()
        if is_down:
            self.port_down.add((dpid, port))
        else:
            self.port_down.discard((dpid, port))
        after = self.down_links()
        if after == before:
            return False
        for lk in sorted(after - before, key=sorted):
            a, b = sorted(lk)
            self.log("link_down", link="s%d-s%d" % (a, b))
        for lk in sorted(before - after, key=sorted):
            a, b = sorted(lk)
            self.log("link_up", link="s%d-s%d" % (a, b))
        if self.mode == MODE_DYNAMIC:
            self.refresh_paths()
        return True

    # -- utilisation -----------------------------------------------------------------------
    def update_util(self, a, b, mbps):
        frac = min(max(mbps, 0.0) / self.capacity_mbps, 2.0)
        old = self.util.get((a, b))
        self.util[(a, b)] = frac if old is None else \
            UTIL_ALPHA * frac + (1 - UTIL_ALPHA) * old

    def path_cost(self, path):
        """Load of the most loaded link on the path, either direction."""
        worst = 0.0
        for a, b in zip(path, path[1:]):
            worst = max(worst, self.util.get((a, b), 0.0), self.util.get((b, a), 0.0))
        return worst

    # -- application events ---------------------------------------------------------------------
    def _endpoints(self, ip):
        return HOSTS[MANAGER_IP][2], HOSTS[ip][2]

    def _best_path(self, ip, down, avoid_bucket=True):
        src, dst = self._endpoints(ip)
        cands = simple_paths(src, dst, down)
        if not cands:
            return None
        if avoid_bucket:
            return min(cands, key=lambda p: (round(self.path_cost(p) / UTIL_BUCKET), len(p), p))
        return min(cands, key=lambda p: (self.path_cost(p), len(p), p))

    def on_worker_up(self, ip, name=None):
        """The manager announced a worker. Returns False for an unknown address."""
        if ip not in WORKER_IPS:
            return False
        self.app_workers[ip] = name or HOSTS[ip][0]
        self.quarantine.pop(ip, None)
        path = self._best_path(ip, frozenset(self.down_links()))
        self.app_paths[ip] = path
        self.last_steer[ip] = self.clock()
        self.log("worker_up", worker=HOSTS[ip][0], ip=ip,
                 path=None if path is None else list(path))
        return True

    def on_worker_down(self, ip, quarantine_s=0.0):
        if ip not in WORKER_IPS:
            return False
        self.app_workers.pop(ip, None)
        self.app_paths.pop(ip, None)
        self.last_steer.pop(ip, None)
        if quarantine_s and quarantine_s > 0:
            self.quarantine[ip] = self.clock() + quarantine_s
        self.log("worker_down", worker=HOSTS[ip][0], ip=ip, quarantine_s=quarantine_s)
        return True

    def expire_quarantine(self):
        """Drop expired quarantines. Returns True if anything changed."""
        now = self.clock()
        gone = [ip for ip, exp in self.quarantine.items() if exp <= now]
        for ip in gone:
            del self.quarantine[ip]
            self.log("quarantine_expired", worker=HOSTS[ip][0], ip=ip)
        return bool(gone)

    # -- path maintenance ----------------------------------------------------------------------------
    def refresh_paths(self, reselect=False):
        """Make sure every application flow uses a valid path. Called after link
        changes. With reselect=True every flow is re-chosen from scratch."""
        down = frozenset(self.down_links())
        changed = []
        for ip in list(self.app_workers):
            cur = self.app_paths.get(ip)
            valid = cur is not None and all(
                link_key(a, b) not in down for a, b in zip(cur, cur[1:]))
            if valid and not reselect:
                continue
            new = self._best_path(ip, down)
            if new != cur:
                self.app_paths[ip] = new
                self.last_steer[ip] = self.clock()
                self.log("path_changed", ip=ip, worker=HOSTS[ip][0], reason="link state",
                         old=None if cur is None else list(cur),
                         new=None if new is None else list(new))
                changed.append(ip)
        return changed

    def steer(self):
        """Congestion-aware steering. Moves at most ONE application flow per call to a
        clearly less loaded path. Returns (ip, old_path, new_path) or None."""
        if self.mode != MODE_DYNAMIC:
            return None
        now = self.clock()
        down = frozenset(self.down_links())
        ranked = sorted(
            (ip for ip in self.app_workers if self.app_paths.get(ip)),
            key=lambda ip: -self.path_cost(self.app_paths[ip]))
        for ip in ranked:
            cur = self.app_paths[ip]
            cur_cost = self.path_cost(cur)
            if cur_cost < UTIL_HIGH:
                break                      # sorted: nobody below is congested either
            if now - self.last_steer.get(ip, 0) < STEER_COOLDOWN_S:
                continue
            best = self._best_path(ip, down, avoid_bucket=False)
            if best is None or best == cur:
                continue
            if self.path_cost(best) <= cur_cost - UTIL_MARGIN:
                self.app_paths[ip] = best
                self.last_steer[ip] = now
                self.log("steered", ip=ip, worker=HOSTS[ip][0], old=list(cur), new=list(best),
                         old_load=round(cur_cost, 2), new_load=round(self.path_cost(best), 2))
                return ip, cur, best
        return None

    # -- the flow table --------------------------------------------------------------------------------
    def compute_rules(self):
        """Desired flow table: {dpid: {(priority, ipv4_src|None, ipv4_dst): out_port|DROP}}"""
        down = self.effective_down()
        rules = {sw: {} for sw in SWITCHES}
        for ip, (_n, _m, host_sw, host_port) in HOSTS.items():
            for sw in SWITCHES:
                if sw == host_sw:
                    out = host_port
                else:
                    path = shortest_path(sw, host_sw, down)
                    out = LINK_PORTS[(sw, path[1])] if path else None
                if out is not None:
                    rules[sw][(PRIO_BASE, None, ip)] = out
        if self.mode != MODE_DYNAMIC:
            return rules
        now = self.clock()
        mgr_port = HOSTS[MANAGER_IP][3]
        for ip, path in self.app_paths.items():
            if path is None or ip not in self.app_workers:
                continue
            if self.quarantine.get(ip, 0) > now:
                continue
            for sw, out in path_ports(path, HOSTS[ip][3]):
                rules[sw][(PRIO_APP, MANAGER_IP, ip)] = out
            for sw, out in path_ports(tuple(reversed(path)), mgr_port):
                rules[sw][(PRIO_APP, ip, MANAGER_IP)] = out
        for ip, exp in self.quarantine.items():
            if exp > now:
                for sw in SWITCHES:
                    rules[sw][(PRIO_QUARANTINE, ip, None)] = DROP
                    rules[sw][(PRIO_QUARANTINE, None, ip)] = DROP
        return rules

    # -- introspection -----------------------------------------------------------------------------------
    def snapshot(self):
        now = self.clock()
        return {
            "mode": self.mode,
            "down_links": sorted("s%d-s%d" % tuple(sorted(lk)) for lk in self.down_links()),
            "util": {"s%d->s%d" % k: round(v, 3) for k, v in sorted(self.util.items())},
            "app_workers": {ip: n for ip, n in sorted(self.app_workers.items())},
            "app_paths": {ip: (None if p is None else ["s%d" % s for s in p])
                          for ip, p in sorted(self.app_paths.items())},
            "quarantine": {ip: round(exp - now, 2) for ip, exp in self.quarantine.items()
                           if exp > now},
        }


# ---- data-plane simulator (used by tests and by the wire test) ---------------------------------------
def simulate_path(tables, src_ip, dst_ip, physically_down=frozenset(), max_hops=8):
    """Follow a packet src_ip -> dst_ip through flow tables, like the real switches
    would. tables: {dpid: {key: out_port|DROP}}. Returns the list of switches
    visited, or None if the packet is dropped / blackholed / loops."""
    if src_ip not in HOSTS or dst_ip not in HOSTS:
        return None
    sw = HOSTS[src_ip][2]
    visited = []
    for _ in range(max_hops):
        visited.append(sw)
        best = None
        for (prio, src, dst), out in tables.get(sw, {}).items():
            if dst == dst_ip and (src is None or src == src_ip):
                if best is None or prio > best[0]:
                    best = (prio, out)
            elif dst is None and src == src_ip:      # quarantine rule on source only
                if best is None or prio > best[0]:
                    best = (prio, out)
        if best is None or best[1] == DROP:
            return None
        out = best[1]
        if out == HOSTS[dst_ip][3] and sw == HOSTS[dst_ip][2]:
            return visited
        nxt = PORT_NEIGHBOR.get((sw, out))
        if nxt is None or link_key(sw, nxt) in physically_down:
            return None                            # out a host port / dead link
        sw = nxt
    return None
