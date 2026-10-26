"""
CN Jackfruit SDN controller (Ryu 4.34, OpenFlow 1.3).

Stage 3 behaviour is kept: on switch connect it wipes the flow table, installs a
table-miss rule and proactive shortest-path IPv4 rules, and answers ARP requests
itself (ARP proxy) so nothing is ever flooded around the s1-s2-s3 loop.

Stage 6-8 adds the dynamic behaviour. All forwarding decisions live in
sdn_policy.py (pure Python, unit-tested); this file only
  * feeds events into the policy:  PORT_STATUS, PORT_DESC, PORT_STATS replies,
                                   and application messages on the control socket
  * "reconciles": computes the desired flow table and sends only the difference
    (FlowMod ADD for new/changed rules, DELETE_STRICT for removed ones)

Events and what they cause
  PORT_STATUS / PORT_DESC  link up/down      -> recompute paths, reinstall flows
  PORT_STATS (every 1 s)   link utilisation  -> steer application flows
  WORKER_UP / WORKER_DOWN  from task manager -> add / remove application flows,
                                                optional quarantine (DROP)
  SET_MODE                 from experiments  -> "static" (baseline) or "dynamic"

Control channel: Unix socket (default /tmp/jackfruit-ctl.sock), one JSON object
per line, one JSON reply per request. See README for the message list.

Run (from the project root, inside ~/ryu-env):
    ryu-manager controller/sdn_controller.py
Environment overrides (all optional):
    JACKFRUIT_MODE=dynamic|static     initial mode        (default dynamic)
    JACKFRUIT_CTL_SOCKET=<path>       control socket path
    JACKFRUIT_STATS_INTERVAL=<sec>    port-stats polling  (default 1.0)
    JACKFRUIT_CORE_MBPS=<mbps>        core link capacity  (default 10)
"""
import json
import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sdn_policy as pol  # noqa: E402

from ryu.base import app_manager  # noqa: E402
from ryu.controller import ofp_event  # noqa: E402
from ryu.controller.handler import (CONFIG_DISPATCHER, DEAD_DISPATCHER,  # noqa: E402
                                    MAIN_DISPATCHER, set_ev_cls)
from ryu.lib import hub  # noqa: E402
from ryu.lib.packet import arp, ether_types, ethernet, packet  # noqa: E402
from ryu.ofproto import ofproto_v1_3  # noqa: E402

CTL_SOCKET = os.environ.get("JACKFRUIT_CTL_SOCKET", "/tmp/jackfruit-ctl.sock")
STATS_INTERVAL = float(os.environ.get("JACKFRUIT_STATS_INTERVAL", "1.0"))
INITIAL_MODE = os.environ.get("JACKFRUIT_MODE", pol.MODE_DYNAMIC)
CORE_MBPS = float(os.environ.get("JACKFRUIT_CORE_MBPS", pol.CORE_CAPACITY_MBPS))
MAX_CTL_LINE = 1 << 20


class JackfruitController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}            # dpid -> datapath
        self.installed = {}            # dpid -> {rule key: out port} as pushed to the switch
        self.last_tx = {}              # (dpid, port) -> (time, tx_bytes)
        self.state = pol.PolicyState(INITIAL_MODE, capacity_mbps=CORE_MBPS)
        self.flow_mods_sent = 0
        self.logger.info("Jackfruit controller starting: mode=%s stats=%.1fs ctl=%s",
                         self.state.mode, STATS_INTERVAL, CTL_SOCKET)
        self._stats_thread = hub.spawn(self._stats_loop)
        self._ctl_thread = hub.spawn(self._ctl_server)

    # ================================================================ flow output
    def _match(self, parser, key):
        _prio, src, dst = key
        kw = {"eth_type": ether_types.ETH_TYPE_IP}
        if src:
            kw["ipv4_src"] = src
        if dst:
            kw["ipv4_dst"] = dst
        return parser.OFPMatch(**kw)

    def _add_flow(self, dp, key, out):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        if out == pol.DROP:
            inst = []                                   # no instructions = drop
        else:
            inst = [parser.OFPInstructionActions(
                ofp.OFPIT_APPLY_ACTIONS, [parser.OFPActionOutput(out)])]
        dp.send_msg(parser.OFPFlowMod(datapath=dp, priority=key[0],
                                      match=self._match(parser, key), instructions=inst))
        self.flow_mods_sent += 1

    def _del_flow(self, dp, key):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        dp.send_msg(parser.OFPFlowMod(datapath=dp, command=ofp.OFPFC_DELETE_STRICT,
                                      priority=key[0], match=self._match(parser, key),
                                      out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY))
        self.flow_mods_sent += 1

    def _clear_flows(self, dp):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        dp.send_msg(parser.OFPFlowMod(datapath=dp, table_id=ofp.OFPTT_ALL,
                                      command=ofp.OFPFC_DELETE,
                                      out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                                      match=parser.OFPMatch()))

    def reconcile(self, reason, t0=None):
        """Push the difference between the desired and the installed flow tables."""
        t0 = t0 if t0 is not None else time.time()
        desired = self.pol_rules()
        sent = 0
        for dpid, dp in list(self.datapaths.items()):
            inst = self.installed.setdefault(dpid, {})
            add, rm = pol.diff_rules(inst, desired.get(dpid, {}))
            for key, out in add.items():          # add/overwrite first (make before break)
                self._add_flow(dp, key, out)
                inst[key] = out
            for key in rm:
                self._del_flow(dp, key)
                inst.pop(key, None)
            sent += len(add) + len(rm)
        if sent:
            ms = round((time.time() - t0) * 1000, 2)
            self.state.log("reconcile", reason=reason, flow_mods=sent, ms=ms)
            self.logger.info("RECONCILE (%s): %d flow-mods in %.1f ms", reason, sent, ms)
        return sent

    def pol_rules(self):
        return self.state.compute_rules()

    # ================================================================ switch events
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def on_switch_connect(self, ev):
        dp = ev.msg.datapath
        ofp, parser = dp.ofproto, dp.ofproto_parser
        self.datapaths[dp.id] = dp
        self.installed[dp.id] = {}
        self._clear_flows(dp)
        # table-miss: unknown packets (ARP) go to the controller
        miss = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        dp.send_msg(parser.OFPFlowMod(
            datapath=dp, priority=pol.PRIO_TABLE_MISS, match=parser.OFPMatch(),
            instructions=[parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, miss)]))
        self.state.log("switch_connected", dpid=dp.id)
        self.reconcile("switch s%d connected" % dp.id)
        self.logger.info("Switch s%d connected: table-miss + %d rules installed",
                         dp.id, len(self.installed[dp.id]))
        # learn which ports are already down (a link may be dead before we connect)
        dp.send_msg(parser.OFPPortDescStatsRequest(dp, 0))

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def on_state_change(self, ev):
        if ev.state == DEAD_DISPATCHER and ev.datapath is not None:
            dpid = ev.datapath.id
            if self.datapaths.get(dpid) is ev.datapath:
                del self.datapaths[dpid]
                self.installed.pop(dpid, None)
                self.state.log("switch_disconnected", dpid=dpid)
                self.logger.warning("Switch s%d disconnected", dpid)

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, MAIN_DISPATCHER)
    def on_port_desc(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        t0 = time.time()
        changed = False
        for p in ev.msg.body:
            down = bool(p.state & ofp.OFPPS_LINK_DOWN) or bool(p.config & ofp.OFPPC_PORT_DOWN)
            changed |= self.state.set_port(dp.id, p.port_no, down)
        if changed:
            self.reconcile("port state at connect (s%d)" % dp.id, t0)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def on_port_status(self, ev):
        t0 = time.time()
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        p = msg.desc
        down = (msg.reason == ofp.OFPPR_DELETE or bool(p.state & ofp.OFPPS_LINK_DOWN)
                or bool(p.config & ofp.OFPPC_PORT_DOWN))
        self.logger.info("PORT_STATUS s%d port %d: %s", dp.id, p.port_no,
                         "DOWN" if down else "UP")
        if self.state.set_port(dp.id, p.port_no, down):
            self.reconcile("link %s (s%d port %d)" % ("down" if down else "up", dp.id,
                                                      p.port_no), t0)

    # ================================================================ statistics
    def _stats_loop(self):
        while True:
            hub.sleep(STATS_INTERVAL)
            try:
                if self.state.expire_quarantine():
                    self.reconcile("quarantine expired")
                for dp in list(self.datapaths.values()):
                    dp.send_msg(dp.ofproto_parser.OFPPortStatsRequest(
                        dp, 0, dp.ofproto.OFPP_ANY))
            except Exception:
                self.logger.exception("stats loop error")

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def on_port_stats(self, ev):
        dpid = ev.msg.datapath.id
        now = time.time()
        for st in ev.msg.body:
            nb = pol.PORT_NEIGHBOR.get((dpid, st.port_no))
            if nb is None:
                continue                                  # host-facing port
            prev = self.last_tx.get((dpid, st.port_no))
            self.last_tx[(dpid, st.port_no)] = (now, st.tx_bytes)
            if prev and now > prev[0] and st.tx_bytes >= prev[1]:
                mbps = (st.tx_bytes - prev[1]) * 8.0 / (now - prev[0]) / 1e6
                self.state.update_util(dpid, nb, mbps)
        if self.state.mode == pol.MODE_DYNAMIC:
            move = self.state.steer()
            if move:
                ip, old, new = move
                self.logger.info("STEER %s: s%s -> s%s", pol.HOSTS[ip][0],
                                 "-s".join(map(str, old)), "-s".join(map(str, new)))
                self.reconcile("steer %s off congested path" % pol.HOSTS[ip][0])

    # ================================================================ ARP proxy
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def on_packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        req = pkt.get_protocol(arp.arp)
        if req is None:
            return  # IPv6, LLDP, etc.: ignore. Never flood (the topology has a loop).
        if req.opcode == arp.ARP_REQUEST and req.dst_ip in pol.HOSTS:
            self.send_arp_reply(dp, in_port, req)

    def send_arp_reply(self, dp, out_port, req):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        target_mac = pol.HOSTS[req.dst_ip][1]
        eth = ethernet.ethernet(dst=req.src_mac, src=target_mac,
                                ethertype=ether_types.ETH_TYPE_ARP)
        rep = arp.arp(opcode=arp.ARP_REPLY, src_mac=target_mac, src_ip=req.dst_ip,
                      dst_mac=req.src_mac, dst_ip=req.src_ip)
        p = packet.Packet()
        p.add_protocol(eth)
        p.add_protocol(rep)
        p.serialize()
        dp.send_msg(parser.OFPPacketOut(
            datapath=dp, buffer_id=ofp.OFP_NO_BUFFER, in_port=ofp.OFPP_CONTROLLER,
            actions=[parser.OFPActionOutput(out_port)], data=p.data))
        self.logger.info("ARP proxy: %s asked for %s -> replied on s%d port %d",
                         req.src_ip, req.dst_ip, dp.id, out_port)

    # ================================================================ control channel
    def _ctl_server(self):
        try:
            try:
                os.unlink(CTL_SOCKET)
            except OSError:
                pass
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(CTL_SOCKET)
            os.chmod(CTL_SOCKET, 0o666)   # Mininet hosts run as root; the controller as you
            srv.listen(16)
        except OSError as e:
            self.logger.error("cannot open control socket %s: %s", CTL_SOCKET, e)
            return
        self.logger.info("control channel listening on %s", CTL_SOCKET)
        while True:
            conn, _ = srv.accept()
            hub.spawn(self._ctl_conn, conn)

    def _ctl_conn(self, conn):
        buf = b""
        try:
            while True:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
                if len(buf) > MAX_CTL_LINE and b"\n" not in buf:
                    return
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    reply = self._ctl_handle(line)
                    conn.sendall(json.dumps(reply, separators=(",", ":")).encode() + b"\n")
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _ctl_handle(self, line):
        try:
            msg = json.loads(line.decode("utf-8"))
            if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
                raise ValueError("need a JSON object with a string 'type'")
        except (ValueError, UnicodeDecodeError) as e:
            return {"type": "ERROR", "reason": "bad request: %s" % e}
        try:
            return self._ctl_dispatch(msg)
        except Exception as e:      # never let a bad request kill the controller
            self.logger.exception("control request failed")
            return {"type": "ERROR", "reason": "internal error: %s" % e}

    def _ctl_dispatch(self, msg):
        mtype = msg["type"]
        t0 = time.time()
        if mtype == "PING":
            return {"type": "PONG", "t": t0}
        if mtype == "WORKER_UP":
            ip = msg.get("ip")
            if not self.state.on_worker_up(ip, msg.get("worker")):
                return {"type": "ERROR", "reason": "unknown worker ip %r" % (ip,)}
            n = self.reconcile("WORKER_UP %s" % pol.HOSTS[ip][0], t0)
            path = self.state.app_paths.get(ip)
            self.logger.info("WORKER_UP %s (%s): app path %s, %d flow-mods", pol.HOSTS[ip][0],
                             ip, path, n)
            return {"type": "OK", "flow_mods": n,
                    "path": None if path is None else ["s%d" % s for s in path]}
        if mtype == "WORKER_DOWN":
            ip = msg.get("ip")
            q = msg.get("quarantine_s", 0) or 0
            if isinstance(q, bool) or not isinstance(q, (int, float)) or q < 0:
                return {"type": "ERROR", "reason": "quarantine_s must be a number >= 0"}
            if not self.state.on_worker_down(ip, q):
                return {"type": "ERROR", "reason": "unknown worker ip %r" % (ip,)}
            n = self.reconcile("WORKER_DOWN %s" % pol.HOSTS[ip][0], t0)
            self.logger.info("WORKER_DOWN %s (%s): quarantine %.1fs, %d flow-mods",
                             pol.HOSTS[ip][0], ip, q, n)
            return {"type": "OK", "flow_mods": n}
        if mtype == "SET_MODE":
            mode = msg.get("mode")
            if mode not in pol.MODES:
                return {"type": "ERROR", "reason": "mode must be one of %s" % (pol.MODES,)}
            self.state.set_mode(mode)
            n = self.reconcile("mode -> %s" % mode, t0)
            self.logger.info("MODE set to %s (%d flow-mods)", mode, n)
            return {"type": "OK", "mode": mode, "flow_mods": n}
        if mtype == "GET_STATE":
            snap = self.state.snapshot()
            snap.update({
                "type": "STATE", "now": time.time(),
                "switches": sorted(self.datapaths),
                "flows": {str(d): len(r) for d, r in sorted(self.installed.items())},
                "flow_mods_sent": self.flow_mods_sent,
                "events": self.state.events[-int(msg.get("events", 200)):],
            })
            return snap
        if mtype == "GET_RULES":
            return {"type": "RULES", "rules": {
                str(d): [[k[0], k[1], k[2], out] for k, out in sorted(
                    r.items(), key=lambda kv: (-kv[0][0], str(kv[0][1]), str(kv[0][2])))]
                for d, r in sorted(self.installed.items())}}
        return {"type": "ERROR", "reason": "unknown request type %r" % mtype}
