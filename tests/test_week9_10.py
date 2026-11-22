"""Unit tests for Week 9 (transparency) and Week 10 (naming, process management).

Run from the project root:   python -m unittest discover -s tests -v
No network or running system is needed: remote calls are replaced by in-process fakes.
"""
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from accounts.account_store import AccountStore
from common.config import NAME_SERVERS
from naming import resolvers as R
from naming.naming_store import NamingStore
from process_mgmt.node_runtime import ProcessRuntime, expected_batch
from process_mgmt.process_manager import ProcessManager
from transparency import gateway as G


# ----------------------------------------------------------------------------- naming
class NamingStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = NamingStore(str(Path(self.tmp.name) / "n.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_exact_match_wins_over_prefix_and_longest_prefix_wins(self):
        self.store.register("acct.C0", "partition", ["A"], is_prefix=True)
        self.store.register("acct.C00", "partition", ["B"], is_prefix=True)
        self.store.register("acct.C0042", "partition", ["C"])
        self.assertEqual(self.store.resolve("acct.C00099")["locations"], ["B"])
        self.assertEqual(self.store.resolve("acct.C09999")["locations"], ["A"])
        self.assertEqual(self.store.resolve("acct.C0042")["locations"], ["C"])
        self.assertIsNone(self.store.resolve("svc.rating"))

    def test_every_change_bumps_the_epoch(self):
        self.assertEqual(self.store.register("x.y", "service", ["A"])["epoch"], 1)
        self.assertEqual(self.store.register("x.y", "service", ["B"])["epoch"], 2)
        self.assertEqual(self.store.resolve("x.y")["epoch"], 2)

    def test_bulk_register_clear_and_survival_of_a_reopen(self):
        self.store.register_many([{"name": f"acct.K{i}", "kind": "partition", "locations": ["A"], "is_prefix": True}
                                  for i in range(50)])
        self.assertEqual(self.store.count(), 50)
        reopened = NamingStore(str(Path(self.tmp.name) / "n.db"))
        self.assertEqual(reopened.count(), 50)
        self.assertEqual(reopened.clear()["removed"], 50)
        self.assertEqual(reopened.count(), 0)

    def test_prefix_index_matches_longest_prefix(self):
        idx = R.PrefixIndex({"acct.C0": {"n": 1}, "acct.C000": {"n": 2}})
        self.assertEqual(idx.match("acct.C00012")["n"], 2)
        self.assertEqual(idx.match("acct.C01234")["n"], 1)
        self.assertIsNone(idx.match("svc.x"))


class FakeNameNetwork:
    """Answers NS_* and WHO_HAS messages for the resolver tests and counts the messages."""

    def __init__(self):
        self.by_port = {addr[1]: name for name, addr in NAME_SERVERS.items()}
        self.record = {"name": "acct.C000", "kind": "partition", "locations": ["NODE-3", "NODE-4"], "epoch": 1,
                       "ttl": 5.0, "is_prefix": True}
        self.sent = []

    def send_json(self, host, port, msg, timeout=0):
        self.sent.append((port, msg["type"]))
        server = self.by_port.get(port)
        if msg["type"] == "WHO_HAS":
            return {"has": port % 2 == 0, "epoch": 1}
        if server == "NS-ROOT":
            return {"referral": "NS-ACCT", "zone": "acct", "ttl": 60.0}
        if msg["type"] == "NS_PREFIX_TABLE":
            return {"records": [self.record]}
        if msg["type"] == "NS_RESOLVE" and msg["name"].startswith(self.record["name"]):
            return dict(self.record)
        return {"error": "NXDOMAIN"}


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.net = FakeNameNetwork()
        self.patch = mock.patch.object(R, "send_json", self.net.send_json)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_central_costs_one_message_per_lookup(self):
        r = R.CentralResolver()
        for _ in range(3):
            self.assertEqual(r.resolve("acct.C00042")["locations"], ["NODE-3", "NODE-4"])
        self.assertEqual(r.stats["messages"], 3)

    def test_dns_cold_costs_two_messages_then_the_cache_answers(self):
        r = R.DnsResolver(ttl=30)
        r.resolve("acct.C00042")
        self.assertEqual(r.stats["messages"], 2)          # root referral + zone server
        r.resolve("acct.C00099")                           # same partition record, covered by the cached prefix
        self.assertEqual(r.stats["messages"], 2)
        self.assertEqual(r.stats["cache_hits"], 1)

    def test_dns_cache_expires_after_the_ttl(self):
        r = R.DnsResolver(ttl=0.1)
        r.resolve("acct.C00042")
        time.sleep(0.2)
        r.resolve("acct.C00042")
        self.assertEqual(r.stats["messages"], 3)           # referral cached longer than the record: 2 + 1

    def test_prefix_table_is_fetched_once_and_refetched_after_invalidate(self):
        r = R.PrefixResolver()
        for _ in range(5):
            r.resolve("acct.C00042")
        self.assertEqual(r.stats["messages"], 1)
        r.invalidate()
        r.resolve("acct.C00042")
        self.assertEqual(r.stats["messages"], 2)

    def test_static_resolver_never_changes(self):
        r = R.StaticResolver(table={"acct.C000": {"name": "acct.C000", "locations": ["NODE-3"], "epoch": 0}})
        self.assertEqual(r.resolve("acct.C00042")["locations"], ["NODE-3"])
        r.invalidate()
        self.assertEqual(r.resolve("acct.C00042")["locations"], ["NODE-3"])
        self.assertEqual(r.stats["messages"], 0)

    def test_broadcast_sends_one_message_per_node_and_collects_the_owners(self):
        nodes = {f"N{p}": ("h", p) for p in (6000, 6001, 6002, 6003)}
        r = R.BroadcastResolver(nodes=nodes)
        rec = r.resolve("acct.C00042")
        self.assertEqual(r.stats["messages"], 4)
        self.assertEqual(rec["locations"], ["N6000", "N6002"])   # the fake says even ports own the name

    def test_partition_of_uses_the_prefix_length(self):
        self.assertEqual(R.partition_of("acct.C00042", digits=3), "acct.C000")
        self.assertEqual(R.partition_of("acct.C12345", digits=2), "acct.C12")


# ----------------------------------------------------------------------------- accounts
class AccountStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.a = AccountStore(str(Path(self.tmp.name) / "a.db"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_node_refuses_partitions_it_does_not_own(self):
        self.assertEqual(self.a.apply("acct.C000", "C1", "op1", 1.0, 1.0)["error"], "NOT_OWNER")
        self.assertEqual(self.a.get("acct.C000", "C1")["error"], "NOT_OWNER")

    def test_an_operation_is_applied_at_most_once(self):
        self.a.own_add("acct.C000")
        first = self.a.apply("acct.C000", "C1", "op1", 2.5, 1.0)
        again = self.a.apply("acct.C000", "C1", "op1", 2.5, 1.0)
        self.assertTrue(first["applied"])
        self.assertFalse(again["applied"])
        self.assertEqual(self.a.get("acct.C000", "C1")["total_charges"], 2.5)
        self.assertEqual(self.a.get("acct.C000", "C1")["op_count"], 1)

    def test_union_of_two_replicas_converges(self):
        b = AccountStore(str(Path(self.tmp.name) / "b.db"))
        for s in (self.a, b):
            s.own_add("acct.C000")
        self.a.apply("acct.C000", "C1", "x", 1.0, 1.0)
        b.apply("acct.C000", "C1", "y", 2.0, 1.0)
        self.a.ops_put(b.ops_fetch(b.op_ids("acct.C000")))
        b.ops_put(self.a.ops_fetch(self.a.op_ids("acct.C000")))
        self.assertEqual(set(self.a.op_ids("acct.C000")), set(b.op_ids("acct.C000")))
        self.assertEqual(self.a.get("acct.C000", "C1")["total_charges"], 3.0)

    def test_dropping_a_partition_removes_ownership_and_data(self):
        self.a.own_add("acct.C000")
        self.a.apply("acct.C000", "C1", "x", 1.0, 1.0)
        self.assertEqual(self.a.own_drop("acct.C000")["ops_removed"], 1)
        self.assertEqual(self.a.get("acct.C000", "C1")["error"], "NOT_OWNER")

    def test_who_has_answers_for_names_inside_the_partition(self):
        self.a.own_add("acct.C000")
        self.assertTrue(self.a.who_has("acct.C00042")["has"])
        self.assertFalse(self.a.who_has("acct.C01042")["has"])


# ----------------------------------------------------------------------------- gateway
class FakeCluster:
    def __init__(self, tmp):
        self.stores = {n: AccountStore(str(Path(tmp) / f"{n}.db")) for n in ("NODE-3", "NODE-4", "NODE-5", "NODE-6")}
        self.ports = {("h", 7000 + i): n for i, n in enumerate(self.stores)}
        self.nodes = {n: addr for addr, n in self.ports.items()}
        self.down = set()

    def send_json(self, host, port, msg, timeout=0):
        node = self.ports[(host, port)]
        if node in self.down:
            raise ConnectionRefusedError(node)
        s, t = self.stores[node], msg["type"]
        if t == "PROBE":
            return {"ok": True}
        if t == "OWN_ADD":
            return s.own_add(msg["prefix"], msg.get("epoch", 1))
        if t == "OWN_DROP":
            return s.own_drop(msg["prefix"])
        if t == "ACCT_APPLY":
            return s.apply(msg["prefix"], msg["customer"], msg["op_id"], msg["charge"], msg["usage"])
        if t == "ACCT_GET":
            return s.get(msg["prefix"], msg["customer"])
        if t == "ACCT_OP_IDS":
            return {"op_ids": s.op_ids(msg["prefix"])}
        if t == "ACCT_OPS_FETCH":
            return {"ops": s.ops_fetch(msg["op_ids"])}
        if t == "ACCT_OPS_PUT":
            return s.ops_put(msg["ops"])
        raise AssertionError(t)


class FakeResolver:
    kind = "fake"

    def __init__(self, records):
        self.records, self.stats, self.invalidated = records, {"messages": 0}, 0
        self.after_invalidate = None

    def resolve(self, name):
        for key in sorted(self.records, key=len, reverse=True):
            if name.startswith(key):
                return dict(self.records[key])
        return None

    def invalidate(self, name=None):
        self.invalidated += 1
        if self.after_invalidate:
            self.records.update(self.after_invalidate)


class FakeAdmin:
    def __init__(self, records):
        self.records = records

    def move(self, name, locations):
        self.records[name]["locations"] = list(locations)
        self.records[name]["epoch"] += 1


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cluster = FakeCluster(self.tmp.name)
        self.records = {"acct.C000": {"name": "acct.C000", "kind": "partition", "locations": ["NODE-3", "NODE-4"], "epoch": 1}}
        for n in ("NODE-3", "NODE-4"):
            self.cluster.stores[n].own_add("acct.C000")
        self.patch = mock.patch.object(G, "send_json", self.cluster.send_json)
        self.patch.start()
        self.resolver = FakeResolver(self.records)
        self.gw = G.TransparentGateway(self.resolver, nodes=self.cluster.nodes, admin=FakeAdmin(self.records))

    def tearDown(self):
        self.gw.running = False
        self.patch.stop()
        self.tmp.cleanup()

    def apply(self, customer, op_id, charge=1.0):
        return self.gw.invoke({"name": f"acct.{customer}", "op": "apply", "args": {"charge": charge, "usage": 1.0, "op_id": op_id}})

    def test_a_write_reaches_every_replica_and_reads_use_either(self):
        r = self.apply("C00001", "a")
        self.assertEqual(r["replicas_ok"], 2)
        for n in ("NODE-3", "NODE-4"):
            self.assertEqual(self.cluster.stores[n].op_ids("acct.C000"), ["a"])
        served = {self.gw.invoke({"name": "acct.C00001", "op": "get"})["served_by"] for _ in range(6)}
        self.assertEqual(served, {"NODE-3", "NODE-4"})

    def test_an_operation_retried_with_the_same_id_is_counted_once(self):
        self.apply("C00001", "same", 5.0)
        self.apply("C00001", "same", 5.0)
        self.assertEqual(self.gw.invoke({"name": "acct.C00001", "op": "get"})["result"]["total_charges"], 5.0)

    def test_a_failed_replica_is_masked_marked_stale_and_repaired(self):
        self.cluster.down.add("NODE-4")
        for i in range(5):
            self.assertTrue(self.apply("C00001", f"op{i}").get("ok"), "failure must be invisible to the caller")
        self.assertIn(("acct.C000", "NODE-4"), self.gw.stale)
        self.assertEqual(self.gw.invoke({"name": "acct.C00001", "op": "get"})["served_by"], "NODE-3")
        self.cluster.down.clear()
        deadline = time.time() + 5
        while self.gw.stale and time.time() < deadline:
            time.sleep(0.1)
        self.assertFalse(self.gw.stale, "the repair loop must clear the stale mark")
        self.assertEqual(set(self.cluster.stores["NODE-4"].op_ids("acct.C000")),
                         set(self.cluster.stores["NODE-3"].op_ids("acct.C000")))
        self.assertTrue(self.gw.verify("acct.C000")["converged"])

    def test_a_stale_location_is_corrected_by_a_fresh_lookup_and_a_retry(self):
        self.cluster.stores["NODE-5"].own_add("acct.C100")
        self.records["acct.C100"] = {"name": "acct.C100", "kind": "partition", "locations": ["NODE-6"], "epoch": 1}   # wrong
        self.resolver.after_invalidate = {"acct.C100": {"name": "acct.C100", "kind": "partition", "locations": ["NODE-5"], "epoch": 2}}
        r = self.apply("C10001", "x")
        self.assertTrue(r.get("ok"))
        self.assertEqual(r["attempts"], 2)
        self.assertEqual(self.resolver.invalidated, 1)
        self.assertEqual(self.gw.stats["redirects"], 1)

    def test_migration_keeps_every_operation_and_switches_the_name(self):
        for i in range(30):
            self.apply(f"C{i:05d}", f"m{i}")
        out = self.gw.migrate("acct.C000", "NODE-3", "NODE-5")
        self.assertTrue(out["ok"])
        self.assertEqual(self.records["acct.C000"]["locations"], ["NODE-5", "NODE-4"])
        self.assertEqual(len(self.cluster.stores["NODE-5"].op_ids("acct.C000")), 30)
        self.assertEqual(self.cluster.stores["NODE-3"].get("acct.C000", "C00001")["error"], "NOT_OWNER")
        self.assertEqual(self.gw.invoke({"name": "acct.C00001", "op": "get"})["result"]["op_count"], 1)
        self.assertTrue(self.gw.verify("acct.C000")["converged"])

    def test_replication_factor_can_grow_and_shrink(self):
        for i in range(10):
            self.apply(f"C{i:05d}", f"g{i}")
        self.assertTrue(self.gw.reconfigure("acct.C000", ["NODE-3", "NODE-4", "NODE-5"])["ok"])
        self.assertEqual(len(self.cluster.stores["NODE-5"].op_ids("acct.C000")), 10)
        self.assertTrue(self.gw.reconfigure("acct.C000", ["NODE-3"])["ok"])
        self.assertEqual(self.cluster.stores["NODE-4"].get("acct.C000", "C00001")["error"], "NOT_OWNER")
        self.assertEqual(self.gw.invoke({"name": "acct.C00001", "op": "get"})["result"]["op_count"], 1)

    def test_operations_wait_while_a_partition_is_frozen(self):
        gate = self.gw.gate("acct.C000")
        gate.freeze()
        done = []
        t = threading.Thread(target=lambda: done.append(self.apply("C00001", "wait")))
        t.start()
        time.sleep(0.25)
        self.assertEqual(done, [], "a write must not slip through a freeze")
        gate.unfreeze()
        t.join(timeout=3)
        self.assertTrue(done and done[0].get("ok"))


# ----------------------------------------------------------------------------- processes
class ProcessRuntimeTests(unittest.TestCase):
    def wait_done(self, rt, pid, timeout=10):
        end = time.time() + timeout
        while time.time() < end:
            st = rt.status(pid)
            if st["state"] != "RUNNING":
                return st
            time.sleep(0.01)
        self.fail("process did not finish")

    def test_a_batch_produces_the_deterministic_result(self):
        rt = ProcessRuntime("NODE-X")
        rt.start("p1", {"seed": 3, "start": 0, "count": 60, "work_ms": 0.2})
        st = self.wait_done(rt, "p1")
        self.assertEqual(st["state"], "DONE")
        self.assertAlmostEqual(st["charge"], expected_batch(3, 0, 60)["charge"], places=6)

    def test_stop_then_resume_from_the_checkpoint_gives_the_exact_result(self):
        rt = ProcessRuntime("NODE-X")
        rt.start("p1", {"seed": 4, "start": 0, "count": 300, "work_ms": 1.0})
        time.sleep(0.08)
        cp = rt.stop("p1")
        self.assertEqual(cp["state"], "TERMINATED")
        self.assertTrue(0 < cp["offset"] < 300)
        rt2 = ProcessRuntime("NODE-Y")          # another node resumes from the checkpoint
        rt2.start("p1", {"seed": 4, "start": 0, "count": 300, "work_ms": 0.2, "resume_offset": cp["offset"],
                         "resume_charge": cp["charge"], "resume_usage": cp["usage"]})
        st = self.wait_done(rt2, "p1")
        self.assertEqual(st["offset"], 300)
        self.assertAlmostEqual(st["charge"], expected_batch(4, 0, 300)["charge"], places=6)

    def test_reap_removes_a_finished_process_and_unknown_ones_report_unknown(self):
        rt = ProcessRuntime("NODE-X")
        rt.start("p1", {"seed": 1, "start": 0, "count": 5, "work_ms": 0.1})
        self.wait_done(rt, "p1")
        self.assertTrue(rt.reap("p1")["reaped"])
        self.assertEqual(rt.status("p1")["state"], "UNKNOWN")


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.pm = ProcessManager(detector=None, db_path=str(Path(self.tmp.name) / "p.db"), resolver_mode="central")
        self.pm.running = False
        self.pm.nodes = ["NODE-3", "NODE-4", "NODE-5", "NODE-6"]

    def tearDown(self):
        self.tmp.cleanup()

    def running(self, pid, node, count):
        self.pm.procs[pid] = {"proc_id": pid, "state": "RUNNING", "node": node, "offset": 0,
                              "spec": {"count": count, "work_ms": 2.0}, "charge": 0.0, "usage": 0.0, "restarts": 0,
                              "parent": None, "group": None}

    def test_least_loaded_picks_the_node_with_the_least_remaining_work(self):
        self.running("proc.job-1", "NODE-3", 500)
        self.running("proc.job-2", "NODE-4", 100)
        self.running("proc.job-3", "NODE-5", 300)
        self.assertEqual(self.pm.pick_node("least_loaded"), "NODE-6")
        self.assertEqual(self.pm.pick_node("least_loaded", exclude=["NODE-6"]), "NODE-4")

    def test_round_robin_cycles_over_the_nodes(self):
        picks = [self.pm.pick_node("round_robin") for _ in range(8)]
        self.assertEqual(picks, ["NODE-3", "NODE-4", "NODE-5", "NODE-6"] * 2)

    def test_reduce_adds_up_the_group(self):
        for i, (c, u) in enumerate([(10.0, 3.0), (5.5, 2.0)], start=1):
            self.running(f"proc.job-{i}", "NODE-3", 10)
            self.pm.procs[f"proc.job-{i}"].update(charge=c, usage=u, offset=10, state="DONE")
        out = self.pm.reduce(["proc.job-1", "proc.job-2"])
        self.assertEqual((out["charge"], out["usage"], out["records"]), (15.5, 5.0, 20))


if __name__ == "__main__":
    unittest.main()
