"""Partition placement for the account service: which nodes hold which account prefix (Week 9)."""
import time

from common.communication import send_json
from common.config import ACCOUNT_PREFIX_DIGITS, ACCOUNT_REPLICATION, RATING_NODES
from naming.resolvers import NamingAdmin


def prefixes(count, digits=None):
    digits = ACCOUNT_PREFIX_DIGITS if digits is None else digits
    return [f"acct.C{i:0{digits}d}" for i in range(count)]


def assign(prefix_list, replication=None, nodes=None):
    nodes = list(nodes or RATING_NODES)
    replication = ACCOUNT_REPLICATION if replication is None else replication
    return {p: [nodes[(i + k) % len(nodes)] for k in range(min(replication, len(nodes)))]
            for i, p in enumerate(prefix_list)}


def reset_nodes():
    """Remove every owned partition (and its operations) from every rating node."""
    for node, (host, port) in RATING_NODES.items():
        try:
            owned = send_json(host, port, {"type": "OWN_LIST"}, timeout=3)["owned"]
            for prefix in owned:
                send_json(host, port, {"type": "OWN_DROP", "prefix": prefix}, timeout=10)
        except Exception:
            pass


def setup_partitions(prefix_list, replication=None, static_path=None, ttl=5.0, admin=None):
    """Clean slate: wipe nodes and name servers, publish the placement, tell the owners."""
    admin = admin or NamingAdmin()
    reset_nodes()
    admin.clear()
    placement = assign(prefix_list, replication)
    records = [{"name": p, "kind": "partition", "locations": locs, "ttl": ttl, "is_prefix": True}
               for p, locs in placement.items()]
    records.append({"name": "svc.rating", "kind": "service", "locations": list(RATING_NODES), "ttl": ttl})
    admin.publish(records, static_path)
    for p, locs in placement.items():
        for node in locs:
            host, port = RATING_NODES[node]
            send_json(host, port, {"type": "OWN_ADD", "prefix": p, "epoch": 1}, timeout=5)
    return placement
