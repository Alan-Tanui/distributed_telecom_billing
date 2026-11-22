"""Transparency gateway (Week 9), hosted inside NODE-2.

Applications use LOGICAL resources (acct.C00042) and never see the physical ones (nodes, replicas,
ports, moves, failures). The gateway provides the five transparencies of the brief (section 4.9):

  access       one operation interface (LOGICAL get/apply) whatever protocol carried the request
  location     names are resolved through the naming service; the caller never holds an address
  replication  writes go to every available replica, reads to any up-to-date one; the replication
               factor can change at run time without the caller noticing
  migration    a partition can move to another node while requests continue: pre-copy, a short
               freeze to copy the last operations, an atomic switch of the name, then clean-up
  failure      a failed replica is skipped, marked stale and repaired later; a stale cached
               location (NOT_OWNER) triggers a fresh lookup and a retry. All operations carry an
               id, so a retry can never be applied twice.

Consistency model: write-all-available. A write succeeds when at least one replica applied it;
replicas that missed it are marked stale, are not read from, and are repaired by set union.
"""
import itertools
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from common.communication import send_json
from common.config import RATING_NODES
from naming.resolvers import NamingAdmin, partition_of

NODE_TIMEOUT = 1.5


class PartitionGate:
    """Lets operations through, or holds them for the few milliseconds of a migration switch."""

    def __init__(self):
        self.cond = threading.Condition()
        self.active = 0
        self.frozen = False

    @contextmanager
    def operation(self, timeout=15.0):
        with self.cond:
            if not self.cond.wait_for(lambda: not self.frozen, timeout):
                raise TimeoutError("partition frozen too long")
            self.active += 1
        try:
            yield
        finally:
            with self.cond:
                self.active -= 1
                self.cond.notify_all()

    def freeze(self, timeout=15.0):
        with self.cond:
            self.frozen = True
            self.cond.wait_for(lambda: self.active == 0, timeout)

    def unfreeze(self):
        with self.cond:
            self.frozen = False
            self.cond.notify_all()


class TransparentGateway:
    def __init__(self, resolver, nodes=None, admin=None):
        self.resolver = resolver
        self.nodes = dict(nodes or RATING_NODES)
        self.admin = admin or NamingAdmin()
        self._lock = threading.Lock()
        self.gates = {}
        self.stale = set()              # (prefix, node) pairs that missed a write
        self._rr = itertools.count()
        self.pool = ThreadPoolExecutor(max_workers=32)
        self.stats = {"ops": 0, "retries": 0, "redirects": 0, "failovers": 0, "stale_marks": 0, "repairs": 0,
                      "repaired_ops": 0, "migrations": 0, "reconfigurations": 0}
        self.running = True
        threading.Thread(target=self._repair_loop, daemon=True).start()

    # ------------------------------------------------------------------ helpers
    def _bump(self, key, n=1):
        with self._lock:
            self.stats[key] += n

    def gate(self, prefix):
        with self._lock:
            return self.gates.setdefault(prefix, PartitionGate())

    def _call(self, node, msg, timeout=NODE_TIMEOUT):
        host, port = self.nodes[node]
        return send_json(host, port, msg, timeout=timeout)

    def _mark_stale(self, prefix, node):
        with self._lock:
            if (prefix, node) not in self.stale:
                self.stale.add((prefix, node))
                self.stats["stale_marks"] += 1

    def _fanout(self, nodes, msg):
        def one(node):
            try:
                return node, self._call(node, msg), None
            except Exception as exc:
                return node, None, exc
        if len(nodes) == 1:
            return [one(nodes[0])]
        return list(self.pool.map(one, nodes))

    # ------------------------------------------------------------------ logical operations
    def invoke(self, request):
        name, op, args = request["name"], request["op"], request.get("args", {})
        prefix = partition_of(name)
        customer = name.split(".", 1)[1]
        self._bump("ops")
        timing = {"t_resolve_ms": 0.0, "t_exec_ms": 0.0}
        try:
            with self.gate(prefix).operation():
                if op == "apply":
                    return self._apply(name, prefix, customer, args, timing)
                if op == "get":
                    return self._get(name, prefix, customer, timing)
                return {"error": f"unknown operation {op!r}"}
        except TimeoutError as exc:
            return {"error": str(exc)}

    def _resolve(self, name, timing):
        t0 = time.perf_counter()
        rec = self.resolver.resolve(name)
        timing["t_resolve_ms"] += (time.perf_counter() - t0) * 1000
        return rec

    def _apply(self, name, prefix, customer, args, timing):
        op_id = args.get("op_id") or uuid.uuid4().hex
        msg = {"type": "ACCT_APPLY", "prefix": prefix, "customer": customer, "op_id": op_id,
               "charge": args.get("charge", 0.0), "usage": args.get("usage", 0.0)}
        last = "unresolvable name"
        for attempt in range(1, 5):
            rec = self._resolve(name, timing)
            if not rec:
                return {"error": f"NXDOMAIN {name}"}
            t0 = time.perf_counter()
            results = self._fanout(rec["locations"], msg)
            timing["t_exec_ms"] += (time.perf_counter() - t0) * 1000
            good = [(n, r) for n, r, e in results if r and "error" not in r]
            moved = any(r and r.get("error") == "NOT_OWNER" for _, r, _ in results)
            for n, r, e in results:
                if (r is None or ("error" in r and r["error"] != "NOT_OWNER")):
                    self._mark_stale(prefix, n)
                    self._bump("failovers")
            if moved:
                self.resolver.invalidate(name)
                self._bump("redirects")
            if good and not moved:
                r = good[0][1]
                return {"ok": True, "result": {"total_charges": r["total_charges"], "op_count": r["op_count"],
                        "applied": r["applied"], "op_id": op_id}, "replicas_ok": len(good),
                        "replicas_total": len(results), "attempts": attempt, **timing}
            self._bump("retries")
            last = str([(n, (r or {}).get("error") or str(e)) for n, r, e in results])
            time.sleep(0.02 * attempt)
        return {"error": f"apply failed after retries: {last}", "attempts": 4, **timing}

    def _get(self, name, prefix, customer, timing):
        msg = {"type": "ACCT_GET", "prefix": prefix, "customer": customer}
        last = "no replica answered"
        for attempt in range(1, 5):
            rec = self._resolve(name, timing)
            if not rec:
                return {"error": f"NXDOMAIN {name}"}
            locs = rec["locations"]
            fresh = [n for n in locs if (prefix, n) not in self.stale] or list(locs)
            shift = next(self._rr) % len(fresh)
            order = fresh[shift:] + fresh[:shift]
            moved = False
            for node in order:
                t0 = time.perf_counter()
                try:
                    r = self._call(node, msg)
                except Exception as exc:
                    timing["t_exec_ms"] += (time.perf_counter() - t0) * 1000
                    self._mark_stale(prefix, node)
                    self._bump("failovers")
                    last = f"{node}: {exc}"
                    continue
                timing["t_exec_ms"] += (time.perf_counter() - t0) * 1000
                if r.get("error") == "NOT_OWNER":
                    moved = True
                    continue
                if "error" in r:
                    self._mark_stale(prefix, node)
                    last = f"{node}: {r['error']}"
                    continue
                return {"ok": True, "result": {"total_charges": r["total_charges"], "op_count": r["op_count"],
                        "total_usage": r["total_usage"]}, "served_by": node, "attempts": attempt, **timing}
            if moved:
                self.resolver.invalidate(name)
                self._bump("redirects")
            self._bump("retries")
            time.sleep(0.02 * attempt)
        return {"error": f"get failed after retries: {last}", "attempts": 4, **timing}

    # ------------------------------------------------------------------ repair (failure transparency)
    def _op_ids(self, node, prefix):
        return set(self._call(node, {"type": "ACCT_OP_IDS", "prefix": prefix}, timeout=15)["op_ids"])

    def _copy_ops(self, src, dst, prefix, ids):
        ids = list(ids)
        for i in range(0, len(ids), 400):
            rows = self._call(src, {"type": "ACCT_OPS_FETCH", "op_ids": ids[i:i + 400]}, timeout=15)["ops"]
            self._call(dst, {"type": "ACCT_OPS_PUT", "ops": rows}, timeout=15)
        return len(ids)

    def repair(self, prefix, node, healthy):
        """Union of the operation sets of `node` and `healthy` (grow-only set: union is safe)."""
        a, b = self._op_ids(healthy, prefix), self._op_ids(node, prefix)
        copied = self._copy_ops(healthy, node, prefix, a - b) + self._copy_ops(node, healthy, prefix, b - a)
        return copied

    def _repair_loop(self):
        while self.running:
            time.sleep(0.5)
            for prefix, node in list(self.stale):
                try:
                    self._call(node, {"type": "PROBE"}, timeout=0.6)
                    rec = self.resolver.resolve(prefix)
                    if not rec or node not in rec["locations"]:
                        with self._lock:
                            self.stale.discard((prefix, node))
                        continue
                    healthy = next((n for n in rec["locations"] if n != node and (prefix, n) not in self.stale), None)
                    if healthy is None:
                        continue
                    with self.gate(prefix).operation():
                        copied = self.repair(prefix, node, healthy)
                    with self._lock:
                        self.stale.discard((prefix, node))
                        self.stats["repairs"] += 1
                        self.stats["repaired_ops"] += copied
                except Exception:
                    continue   # node still down: try again on the next pass

    # ------------------------------------------------------------------ migration and re-replication
    def reconfigure(self, prefix, new_locations):
        """Change the replica set of a partition while it keeps serving (migration is a special case)."""
        t_start = time.perf_counter()
        rec = self.resolver.resolve(prefix)
        if not rec:
            return {"error": f"unknown partition {prefix}"}
        old = list(rec["locations"])
        added = [n for n in new_locations if n not in old]
        removed = [n for n in old if n not in new_locations]
        source = next((n for n in old if n in new_locations and (prefix, n) not in self.stale), None) \
            or next((n for n in old if (prefix, n) not in self.stale), old[0])
        copied = 0
        for node in added:   # 1. join + pre-copy while traffic continues
            self._call(node, {"type": "OWN_ADD", "prefix": prefix, "epoch": rec["epoch"] + 1})
            copied += self._copy_ops(source, node, prefix, self._op_ids(source, prefix) - self._op_ids(node, prefix))
        t_pre = time.perf_counter()
        gate = self.gate(prefix)
        gate.freeze()        # 2. short freeze: copy what arrived meanwhile, switch the name atomically
        t_freeze = time.perf_counter()
        try:
            for node in added:
                copied += self._copy_ops(source, node, prefix, self._op_ids(source, prefix) - self._op_ids(node, prefix))
            self.admin.move(prefix, list(new_locations))
            self.resolver.invalidate(prefix)
        except Exception as exc:
            for node in added:   # roll back: nobody has switched to the new replicas
                try:
                    self._call(node, {"type": "OWN_DROP", "prefix": prefix})
                except Exception:
                    pass
            return {"error": f"reconfiguration aborted: {exc}"}
        finally:
            freeze_ms = (time.perf_counter() - t_freeze) * 1000
            gate.unfreeze()
        for node in removed:  # 3. clean-up on nodes that no longer hold the partition
            try:
                self._call(node, {"type": "OWN_DROP", "prefix": prefix})
            except Exception:
                pass
            with self._lock:
                self.stale.discard((prefix, node))
        self._bump("reconfigurations")
        return {"ok": True, "prefix": prefix, "old": old, "new": list(new_locations), "ops_copied": copied,
                "precopy_ms": round((t_pre - t_start) * 1000, 3), "freeze_ms": round(freeze_ms, 3),
                "total_ms": round((time.perf_counter() - t_start) * 1000, 3)}

    def migrate(self, prefix, from_node, to_node):
        rec = self.resolver.resolve(prefix)
        if not rec or from_node not in rec["locations"] or to_node in rec["locations"]:
            return {"error": f"cannot migrate {prefix} from {from_node} to {to_node}", "locations": (rec or {}).get("locations")}
        out = self.reconfigure(prefix, [to_node if n == from_node else n for n in rec["locations"]])
        if out.get("ok"):
            self._bump("migrations")
        return out

    # ------------------------------------------------------------------ verification and status
    def verify(self, prefix):
        """Compare the operation sets on every replica (used by the experiments to prove no loss)."""
        rec = self.resolver.resolve(prefix)
        sets, errors = {}, {}
        for node in rec["locations"]:
            try:
                sets[node] = self._op_ids(node, prefix)
            except Exception as exc:
                errors[node] = str(exc)
        union = set().union(*sets.values()) if sets else set()
        return {"prefix": prefix, "locations": rec["locations"], "epoch": rec["epoch"],
                "ops_per_replica": {n: len(s) for n, s in sets.items()}, "union": len(union),
                "converged": bool(sets) and all(s == union for s in sets.values()) and not errors,
                "unreachable": errors}

    def op_union(self, prefix):
        rec = self.resolver.resolve(prefix)
        out = set()
        for node in rec["locations"]:
            try:
                out |= self._op_ids(node, prefix)
            except Exception:
                pass
        return out

    def snapshot(self):
        with self._lock:
            return {"resolver": self.resolver.kind, "resolver_stats": dict(self.resolver.stats),
                    "stale": sorted(f"{p}@{n}" for p, n in self.stale), "stats": dict(self.stats)}
