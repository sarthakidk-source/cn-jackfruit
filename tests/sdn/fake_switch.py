"""
A fake OpenFlow 1.3 switch for testing the Ryu controller WITHOUT Open vSwitch.

It opens a real TCP connection to the controller and speaks the real wire protocol
(hand-packed with the structures from ryu.ofproto, parsed with Ryu's own parsers),
keeps a flow table from the FlowMods it receives, answers port-description and
port-statistics requests, and can emit PORT_STATUS and PACKET_IN messages.

Requires the Ryu environment:  ~/ryu-env/bin/python
"""
import socket
import struct
import threading
import time

from ryu.lib.packet import arp, ethernet, packet
from ryu.ofproto import ofproto_v1_3 as ofp
from ryu.ofproto import ofproto_v1_3_parser as parser

DROP = -1
HDR = struct.Struct(ofp.OFP_HEADER_PACK_STR)

# core ports of each switch in the project topology (port numbers from topo.py)
CORE_PORTS = {1: (1, 2), 2: (1, 2), 3: (1, 2)}
HOST_PORTS = {1: (3, 4), 2: (3, 4), 3: (3, 4)}


def _port_bytes(port_no, down):
    name = ("eth%d" % port_no).encode().ljust(16, b"\0")
    state = ofp.OFPPS_LINK_DOWN if down else 0
    return struct.pack(ofp.OFP_PORT_PACK_STR, port_no, b"\x00\x00\x00\x00\x00\x01", name,
                       0, state, 0, 0, 0, 0, 10000, 10000)


class FakeSwitch(threading.Thread):
    def __init__(self, dpid, port, down_ports=()):
        super().__init__(daemon=True)
        self.dpid = dpid
        self.ctl_port = port
        self.down = set(down_ports)          # ports currently reported down
        self.flows = {}                      # (priority, src, dst) -> out port or DROP
        self.table_miss = False
        self.rates = {}                      # port -> bytes per second being "sent"
        self._acc = {}
        self._last_stats = time.time()
        self.packet_outs = []                # (out_port, ethernet+arp packet)
        self.flow_mod_count = 0
        self.connected = threading.Event()
        self.main = threading.Event()        # set once we answered features
        self.sock = None
        self._buf = b""
        self._lock = threading.Lock()
        self._stop = False
        self.log = []

    # ------------------------------------------------------------------ plumbing
    def run(self):
        for _ in range(100):
            try:
                self.sock = socket.create_connection(("127.0.0.1", self.ctl_port), timeout=2)
                break
            except OSError:
                time.sleep(0.1)
        else:
            return
        self.sock.settimeout(0.2)
        self.connected.set()
        self._send(ofp.OFPT_HELLO, b"")
        while not self._stop:
            try:
                msg = self._read_msg()
            except socket.timeout:
                continue
            except OSError:
                return
            if msg is None:
                return
            self._handle(*msg)

    def stop(self):
        self._stop = True
        try:
            if self.sock:
                self.sock.close()
        except OSError:
            pass

    def _send(self, mtype, body, xid=0):
        with self._lock:
            try:
                self.sock.sendall(HDR.pack(ofp.OFP_VERSION, mtype, 8 + len(body), xid) + body)
            except OSError:
                pass

    def _read_msg(self):
        while True:
            if len(self._buf) >= 8:
                _v, mtype, length, xid = HDR.unpack_from(self._buf)
                if len(self._buf) >= length:
                    body = self._buf[8:length]
                    full = self._buf[:length]
                    self._buf = self._buf[length:]
                    return mtype, xid, body, full
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self._buf += chunk

    # ------------------------------------------------------------------ handlers
    def _handle(self, mtype, xid, body, full):
        if mtype == ofp.OFPT_FEATURES_REQUEST:
            self._send(ofp.OFPT_FEATURES_REPLY,
                       struct.pack(ofp.OFP_SWITCH_FEATURES_PACK_STR, self.dpid, 0, 254, 0, 0, 0),
                       xid)
            self.main.set()
        elif mtype == ofp.OFPT_ECHO_REQUEST:
            self._send(ofp.OFPT_ECHO_REPLY, body, xid)
        elif mtype == ofp.OFPT_BARRIER_REQUEST:
            self._send(ofp.OFPT_BARRIER_REPLY, b"", xid)
        elif mtype == ofp.OFPT_MULTIPART_REQUEST:
            self._multipart(xid, body)
        elif mtype == ofp.OFPT_FLOW_MOD:
            self._flow_mod(full)
        elif mtype == ofp.OFPT_PACKET_OUT:
            self._packet_out(full)
        # SET_CONFIG, GET_CONFIG etc.: ignore

    def _multipart(self, xid, body):
        mp_type, _flags = struct.unpack_from("!HH", body)
        if mp_type == ofp.OFPMP_PORT_DESC:
            ports = b"".join(_port_bytes(p, p in self.down)
                             for p in CORE_PORTS[self.dpid] + HOST_PORTS[self.dpid])
            self._send(ofp.OFPT_MULTIPART_REPLY,
                       struct.pack(ofp.OFP_MULTIPART_REPLY_PACK_STR, ofp.OFPMP_PORT_DESC, 0)
                       + ports, xid)
        elif mp_type == ofp.OFPMP_PORT_STATS:
            now = time.time()
            dt = now - self._last_stats
            self._last_stats = now
            out = b""
            for p in CORE_PORTS[self.dpid] + HOST_PORTS[self.dpid]:
                self._acc[p] = self._acc.get(p, 0.0) + self.rates.get(p, 0.0) * dt
                # port, rx_pkts, tx_pkts, rx_bytes, tx_bytes, 8 more counters, dur_s, dur_ns
                out += struct.pack(ofp.OFP_PORT_STATS_PACK_STR, p, 0, 0, 0, int(self._acc[p]),
                                   0, 0, 0, 0, 0, 0, 0, 0, 1, 0)
            self._send(ofp.OFPT_MULTIPART_REPLY,
                       struct.pack(ofp.OFP_MULTIPART_REPLY_PACK_STR, ofp.OFPMP_PORT_STATS, 0)
                       + out, xid)

    def _flow_mod(self, full):
        (_cookie, _mask, table_id, command, _idle, _hard, priority, _buf, _outp, _outg,
         _flags) = struct.unpack_from(ofp.OFP_FLOW_MOD_PACK_STR0, full, 8)
        match = parser.OFPMatch.parser(bytes(full), 48)
        fields = dict(match.items())
        off = 48 + ((match.length + 7) // 8) * 8
        out = DROP
        while off < len(full):
            inst = parser.OFPInstruction.parser(bytes(full), off)
            if isinstance(inst, parser.OFPInstructionActions):
                for a in inst.actions:
                    if isinstance(a, parser.OFPActionOutput):
                        out = a.port
            off += inst.len
        key = (priority, fields.get("ipv4_src"), fields.get("ipv4_dst"))
        with self._lock:
            self.flow_mod_count += 1
            if command == ofp.OFPFC_ADD:
                if not fields:
                    self.table_miss = True        # priority-0 match-all (to controller)
                else:
                    self.flows[key] = out
            elif command == ofp.OFPFC_DELETE_STRICT:
                self.flows.pop(key, None)
            elif command == ofp.OFPFC_DELETE:
                if not fields:
                    self.flows.clear()
                    self.table_miss = False
            self.log.append((time.time(), command, key, out))

    def _packet_out(self, full):
        # header(8) buffer_id(4) in_port(4) actions_len(2) pad(6) actions data
        buffer_id, in_port, actions_len = struct.unpack_from(ofp.OFP_PACKET_OUT_PACK_STR, full, 8)
        off = 24
        port = None
        end = off + actions_len
        while off < end:
            atype, alen = struct.unpack_from("!HH", full, off)
            if atype == ofp.OFPAT_OUTPUT:
                port = struct.unpack_from("!I", full, off + 4)[0]
            off += alen
        pkt = packet.Packet(bytes(full[end:]))
        self.packet_outs.append((port, pkt))

    # ------------------------------------------------------------------ test API
    def snapshot(self):
        with self._lock:
            return dict(self.flows)

    def send_port_status(self, port_no, down):
        body = struct.pack("!B7x", ofp.OFPPR_MODIFY) + _port_bytes(port_no, down)
        self._send(ofp.OFPT_PORT_STATUS, body)
        (self.down.add if down else self.down.discard)(port_no)

    def send_arp_request(self, in_port, src_mac, src_ip, dst_ip):
        eth = ethernet.ethernet(dst="ff:ff:ff:ff:ff:ff", src=src_mac,
                                ethertype=0x0806)
        req = arp.arp(opcode=arp.ARP_REQUEST, src_mac=src_mac, src_ip=src_ip,
                      dst_mac="00:00:00:00:00:00", dst_ip=dst_ip)
        p = packet.Packet()
        p.add_protocol(eth)
        p.add_protocol(req)
        p.serialize()
        m = bytearray()
        parser.OFPMatch(in_port=in_port).serialize(m, 0)
        data = bytes(p.data)
        body = struct.pack(ofp.OFP_PACKET_IN_PACK_STR, 0xffffffff, len(data), 0, 0, 0)
        body += bytes(m) + b"\x00\x00" + data
        self._send(ofp.OFPT_PACKET_IN, body)

    def set_rate_mbps(self, port, mbps):
        self.rates[port] = mbps * 1e6 / 8.0
