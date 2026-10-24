#!/usr/bin/env python3
"""
Stage 2 topology for the CN Jackfruit project.

        h1   h2
         \  /
          s1
         /  \
   (10M)/    \(10M)
       s2 --- s3      <- s2-s3 is the backup link (optional)
      /  \   /  \
    w1   w2 w3  w4

Usage:
  sudo /usr/bin/python3 topology/topo.py --no-backup   # tree, safe with a learning switch
  sudo /usr/bin/python3 topology/topo.py               # full triangle (needs loop-aware controller)
  sudo /usr/bin/python3 topology/topo.py --no-backup --test   # run pingAll and exit
"""
import argparse
import os
import sys
import time

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController

CORE_LINK_BW_MBPS = 10  # bandwidth-limited core links make congestion demonstrable

# name: (ip, mac, switch, switch_port)
HOSTS = {
    "h1": ("10.0.0.1", "00:00:00:00:00:01", "s1", 3),   # task manager
    "h2": ("10.0.0.2", "00:00:00:00:00:02", "s1", 4),   # client
    "w1": ("10.0.0.11", "00:00:00:00:00:11", "s2", 3),  # workers
    "w2": ("10.0.0.12", "00:00:00:00:00:12", "s2", 4),
    "w3": ("10.0.0.13", "00:00:00:00:00:13", "s3", 3),
    "w4": ("10.0.0.14", "00:00:00:00:00:14", "s3", 4),
}


def build_net(controller_ip="127.0.0.1", controller_port=6653,
              backup=True, bw=CORE_LINK_BW_MBPS):
    """Create (but do not start) the network. Reused by later experiment scripts."""
    net = Mininet(controller=None, switch=OVSSwitch, link=TCLink,
                  autoSetMacs=False, autoStaticArp=False, build=False)
    net.addController("c0", controller=RemoteController,
                      ip=controller_ip, port=controller_port)

    sw = {}
    for i in (1, 2, 3):
        sw["s%d" % i] = net.addSwitch("s%d" % i, dpid="%016x" % i,
                                      protocols="OpenFlow13")

    for name, (ip, mac, sname, port) in HOSTS.items():
        h = net.addHost(name, ip=ip + "/24", mac=mac)
        net.addLink(h, sw[sname], port2=port)  # host side is eth0

    # Core links (explicit port numbers so the controller can rely on them)
    net.addLink(sw["s1"], sw["s2"], port1=1, port2=1, bw=bw)
    net.addLink(sw["s1"], sw["s3"], port1=2, port2=1, bw=bw)
    if backup:
        net.addLink(sw["s2"], sw["s3"], port1=2, port2=2, bw=bw)
    return net


def main():
    parser = argparse.ArgumentParser(description="CN Jackfruit Mininet topology")
    parser.add_argument("--controller-ip", default="127.0.0.1")
    parser.add_argument("--controller-port", type=int, default=6653)
    parser.add_argument("--no-backup", action="store_true",
                        help="omit the s2-s3 link (no loop)")
    parser.add_argument("--test", action="store_true",
                        help="run pingAll, print result and exit")
    args = parser.parse_args()

    if os.geteuid() != 0:
        sys.exit("Run with sudo: sudo /usr/bin/python3 topology/topo.py ...")

    setLogLevel("info")
    net = build_net(args.controller_ip, args.controller_port,
                    backup=not args.no_backup)
    net.start()
    try:
        if args.test:
            time.sleep(3)  # let switches connect to the controller
            loss = net.pingAll()
            sys.exit(0 if loss == 0 else 1)
        CLI(net)
    finally:
        net.stop()


if __name__ == "__main__":
    main()
    
