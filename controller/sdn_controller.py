"""
CN Jackfruit - Stage 3 SDN controller (Ryu, OpenFlow 1.3)

What it does:
  1. On switch connect: clears old flows, installs a table-miss rule, and installs
     one proactive IPv4 forwarding rule per destination host (shortest path).
  2. Answers ARP requests itself (ARP proxy) so nothing is ever flooded,
     which keeps the s1-s2-s3 triangle loop-free in practice.

Run:  ryu-manager controller/sdn_controller.py
"""
from collections import deque

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib.packet import arp, ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_3

# ---- Network map (must mirror topology/topo.py) -------------------------
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

PRIO_TABLE_MISS = 0
PRIO_ROUTE = 10


class JackfruitController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}       # dpid -> datapath object
        self.down_links = set()   # frozenset({a, b}) of failed switch links (used in Stage 8)

    # ---------------- path computation ----------------
    def next_hop_port(self, src, dst):
        """Port on switch `src` leading toward switch `dst` along a shortest path.
        Returns None if src == dst or dst is unreachable."""
        if src == dst:
            return None
        prev = {src: None}
        queue = deque([src])
        while queue:
            u = queue.popleft()
            if u == dst:
                break
            for (a, b) in sorted(LINK_PORTS):
                if a == u and b not in prev and frozenset((a, b)) not in self.down_links:
                    prev[b] = u
                    queue.append(b)
        if dst not in prev:
            return None
        node = dst
        while prev[node] != src:
            node = prev[node]
        return LINK_PORTS[(src, node)]

    # ---------------- flow helpers ----------------
    def add_flow(self, dp, priority, match, actions):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        inst = [parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS, actions)]
        dp.send_msg(parser.OFPFlowMod(datapath=dp, priority=priority,
                                      match=match, instructions=inst))

    def clear_flows(self, dp):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        dp.send_msg(parser.OFPFlowMod(datapath=dp, table_id=ofp.OFPTT_ALL,
                                      command=ofp.OFPFC_DELETE,
                                      out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                                      match=parser.OFPMatch()))

    def install_host_routes(self, dp):
        parser = dp.ofproto_parser
        count = 0
        for ip, (_name, _mac, host_sw, host_port) in HOSTS.items():
            out = host_port if host_sw == dp.id else self.next_hop_port(dp.id, host_sw)
            if out is None:
                continue
            match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP, ipv4_dst=ip)
            self.add_flow(dp, PRIO_ROUTE, match, [parser.OFPActionOutput(out)])
            count += 1
        return count

    # ---------------- OpenFlow events ----------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def on_switch_connect(self, ev):
        dp = ev.msg.datapath
        ofp, parser = dp.ofproto, dp.ofproto_parser
        self.datapaths[dp.id] = dp

        self.clear_flows(dp)
        # table-miss: send unknown packets (ARP etc.) to the controller
        miss_actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, PRIO_TABLE_MISS, parser.OFPMatch(), miss_actions)
        n = self.install_host_routes(dp)
        self.logger.info("Switch s%d connected: table-miss + %d host routes installed",
                         dp.id, n)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def on_packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        req = pkt.get_protocol(arp.arp)
        if req is None:
            return  # IPv6, LLDP, etc.: ignore. Never flood (the topology has a loop).
        if req.opcode == arp.ARP_REQUEST and req.dst_ip in HOSTS:
            self.send_arp_reply(dp, in_port, req)

    def send_arp_reply(self, dp, out_port, req):
        ofp, parser = dp.ofproto, dp.ofproto_parser
        target_mac = HOSTS[req.dst_ip][1]
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
                         