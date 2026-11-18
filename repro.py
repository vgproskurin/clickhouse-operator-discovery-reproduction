#!/usr/bin/env python3
"""Keeper discovery timeout lab; Python stdlib and kubectl only.

Exit 0 from `run --mode discovery` means the BUG was reproduced, not a healthy
cluster. `run --mode nodes` is a negative control and requires healthy recovery.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
LABEL = "keeper-discovery-repro/role"
CH = ["chi-repro-cluster-0-0-0", "chi-repro-cluster-0-1-0"]
KEEPERS = [f"chk-repro-keeper-keeper-0-{i}-0" for i in range(3)]
XML_KEY = "chop-generated-zookeeper.xml"


class Lab:
    def __init__(self, args):
        self.a = args
        self.prefix = shlex.split(os.environ.get("KUBE_EXEC_PREFIX", ""))
        self.out = Path(args.results or HERE / ".work" / "results" /
                        (dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + args.mode))
        self.out.mkdir(parents=True, exist_ok=True)
        self.state_path = HERE / ".work" / (args.context.replace("/", "_") + "-" + args.namespace + ".json")
        self.state_path.parent.mkdir(exist_ok=True)
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}

    def log(self, msg):
        line = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds") + " " + msg
        print(line, flush=True)
        with (self.out / "runner.log").open("a") as f:
            f.write(line + "\n")

    def save(self):
        self.state_path.write_text(json.dumps(self.state, indent=2) + "\n")

    def write(self, name, value):
        (self.out / name).write_text(value if isinstance(value, str) else json.dumps(value, indent=2) + "\n")

    def k(self, *args, ns=None, timeout=90, check=True):
        cmd = self.prefix + ["kubectl", "--context", self.a.context]
        if self.a.kubeconfig:
            cmd += ["--kubeconfig", self.a.kubeconfig]
        cmd += ["-n", ns or self.a.namespace, *args]
        p = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
        with (self.out / "commands.jsonl").open("a") as f:
            f.write(json.dumps({"time": dt.datetime.now(dt.timezone.utc).isoformat(),
                                "command": cmd, "returncode": p.returncode,
                                "stderr": p.stderr[-3000:]}) + "\n")
        if check and p.returncode:
            raise RuntimeError(f"{shlex.join(cmd)}\n{p.stderr[-3000:]}\n{p.stdout[-1000:]}")
        return p.stdout

    def get(self, resource, name=None, ns=None):
        return json.loads(self.k("get", resource, *([name] if name else []), "-o", "json", ns=ns))

    def wait(self, label, predicate, seconds=600):
        deadline, announce, last = time.monotonic() + seconds, 0, ""
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    self.log("OK: " + label)
                    return value
            except (RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as e:
                last = str(e)[-1200:]
            if time.monotonic() >= announce:
                self.log("Waiting: " + label)
                announce = time.monotonic() + 30
            time.sleep(3)
        raise RuntimeError(f"Timed out ({seconds}s): {label}\n{last}")

    def ready(self, pod, ns=None):
        p = self.get("pod", pod, ns)
        return (p.get("status", {}).get("phase") == "Running" and
                any(c.get("type") == "Ready" and c.get("status") == "True"
                    for c in p.get("status", {}).get("conditions", [])))

    def sql(self, pod, query, timeout=45):
        return self.k("exec", pod, "-c", "clickhouse", "--", "clickhouse-client",
                      "--receive_timeout", "30", "--query", query, timeout=timeout)

    def replicas(self, pod):
        result = self.sql(pod, "SELECT database, table, is_readonly, is_session_expired, "
                          "queue_size, absolute_delay, zookeeper_exception FROM system.replicas "
                          "WHERE database='repro' AND table='smoke' FORMAT JSONEachRow")
        return [json.loads(row) for row in result.splitlines() if row.strip()]

    def healthy(self, pod):
        rows = self.replicas(pod)
        return len(rows) == 1 and not rows[0]["is_readonly"] and not rows[0]["is_session_expired"]

    def pod_identity(self, pod):
        info = self.get("pod", pod)
        container = next(c for c in info["status"]["containerStatuses"] if c["name"] == "clickhouse")
        return {"uid": info["metadata"]["uid"], "container_id": container["containerID"],
                "restart_count": container["restartCount"], "started_at": container["state"]["running"]["startedAt"]}

    def observe_reconcile_recovery(self, identities):
        started = time.monotonic()
        deadline = started + self.a.recovery_wait_seconds
        announce = 0
        while True:
            current = {p: self.pod_identity(p) for p in CH}
            if current != identities:
                self.write("recovery-identity-change.json", {"before": identities, "after": current})
                raise RuntimeError("Pod/container identity changed: cannot establish recovery without restart")
            rows = {p: self.replicas(p) for p in CH}
            metrics = {p: self.sql(p, "SELECT metric, value FROM system.metrics WHERE metric IN "
                                  "('ReadonlyReplica','ZooKeeperSession','ZooKeeperSessionExpired') FORMAT JSONEachRow") for p in CH}
            elapsed = time.monotonic() - started
            sample = {"time": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "elapsed_seconds": round(elapsed, 3), "replicas": rows,
                      "metrics": metrics, "identities": current}
            with (self.out / "reconcile-only-observations.jsonl").open("a") as f:
                f.write(json.dumps(sample) + "\n")
            if all(len(r) == 1 and not r[0]["is_readonly"] and not r[0]["is_session_expired"] for r in rows.values()):
                self.log(f"Both tables recovered without pod/container restart after {elapsed:.1f}s of observation")
                return elapsed
            if time.monotonic() >= deadline:
                self.write("recovery.json", {"result": "NOT_RECOVERED", "method": "spec.taskID only",
                           "observation_seconds": round(elapsed, 3), "restarted_pods": [],
                           "identities_unchanged": True, "replicas": rows})
                raise RuntimeError(f"Tables did not recover within {self.a.recovery_wait_seconds}s after mounted config repair; no pods restarted")
            if elapsed >= announce:
                states = {p: [(r["is_readonly"], r["is_session_expired"]) for r in rs] for p, rs in rows.items()}
                self.log(f"Reconcile-only observation {elapsed:.0f}/{self.a.recovery_wait_seconds}s, readonly/session_expired: {states}")
                announce = elapsed + 30
            time.sleep(min(5, max(0, deadline - time.monotonic())))

    @staticmethod
    def node_count(xml):
        return len(ET.fromstring(xml).findall("./zookeeper/node"))

    def configs(self):
        result = {}
        for pod in CH:
            for vol in self.get("pod", pod)["spec"]["volumes"]:
                name = vol.get("configMap", {}).get("name")
                if not name:
                    continue
                data = self.get("cm", name).get("data", {})
                if XML_KEY in data:
                    result[pod] = {"configmap": name, "xml": data[XML_KEY],
                                   "nodes": self.node_count(data[XML_KEY])}
        if set(result) != set(CH):
            raise RuntimeError("Missing host ZooKeeper XML; cannot call an absent file an empty config")
        return result

    def operator_pod(self):
        pods = self.get("pods", ns=self.a.operator_namespace)["items"]
        pods = [p for p in pods if not p["metadata"].get("deletionTimestamp") and
                any("clickhouse-operator" in c.get("image", "") for c in p["spec"]["containers"])]
        if len(pods) != 1:
            raise RuntimeError(f"Expected exactly one operator pod in {self.a.operator_namespace}, got {len(pods)}")
        return pods[0]

    def operator_log(self):
        info = self.operator_pod()
        pod = info["metadata"]["name"]
        container = next(c["name"] for c in info["spec"]["containers"] if "clickhouse-operator" in c.get("image", ""))
        return self.k("logs", pod, "-c", container, "--timestamps", "--tail=15000",
                      ns=self.a.operator_namespace)

    def fault_node(self):
        p = self.get("pod", KEEPERS[0])
        node = p["spec"].get("nodeName") or self.state.get("fault_node")
        if not node:
            raise RuntimeError("Keeper-0 has no nodeName and there is no saved fault state")
        n = self.get("node", node)
        if n["metadata"].get("labels", {}).get(LABEL) != "fault":
            raise RuntimeError("Refusing to cordon an unlabelled node; expected " + LABEL + "=fault")
        return n

    def require_fixtures(self):
        for kind, name in [("chi", "repro"), ("chk", "repro-keeper")]:
            resource = self.get(kind, name)
            if resource["metadata"].get("labels", {}).get("keeper-discovery-repro/fixture") != "true":
                raise RuntimeError("Refusing to operate on resources without the lab fixture label")

    def baseline(self):
        self.log("Checking baseline; fixture names are fixed and contain synthetic data only")
        self.require_fixtures()
        timeouts = set(re.findall(r"readyTimeout:\s*(\d+)", self.operator_log()))
        if timeouts != {str(self.a.ready_timeout)}:
            raise RuntimeError(f"Effective operator readyTimeout {timeouts} does not match --ready-timeout {self.a.ready_timeout}")
        for pod in KEEPERS + CH:
            self.wait(pod + " Ready", lambda p=pod: self.ready(p), 900)
        self.wait("CHI Completed", lambda: self.get("chi", "repro").get("status", {}).get("status") == "Completed")
        self.wait("CHK Completed", lambda: self.get("chk", "repro-keeper").get("status", {}).get("status") == "Completed")
        if any(v["nodes"] != 3 for v in self.configs().values()):
            raise RuntimeError("Baseline must have THREE Keeper nodes in BOTH host ConfigMaps")
        for pod in CH:
            self.sql(pod, "CREATE DATABASE IF NOT EXISTS repro")
            self.sql(pod, "CREATE TABLE IF NOT EXISTS repro.smoke (id UInt64) ENGINE=ReplicatedMergeTree("
                     f"'/keeper-discovery-repro/{self.a.namespace}/smoke', '{{replica}}') ORDER BY id")
            self.wait(pod + " replicated table writable", lambda p=pod: self.healthy(p), 180)
        token = time.time_ns()
        self.sql(CH[0], f"INSERT INTO repro.smoke VALUES ({token})")
        self.sql(CH[1], "SYSTEM SYNC REPLICA repro.smoke")
        if self.sql(CH[0], "SELECT count() FROM repro.smoke").strip() != self.sql(CH[1], "SELECT count() FROM repro.smoke").strip():
            raise RuntimeError("Baseline row counts differ")
        if any(self.sql(p, f"SELECT countIf(id={token}) FROM repro.smoke").strip() != "1" for p in CH):
            raise RuntimeError("New baseline row did not replicate")
        self.write("baseline-data.json", {"inserted_id": token})
        self.collect("baseline")

    def fault(self):
        self.require_fixtures()
        if self.state.get("fault_active"):
            raise RuntimeError("A saved fault is active; restore it before starting another")
        node = self.fault_node()
        name = node["metadata"]["name"]
        if node.get("spec", {}).get("unschedulable"):
            raise RuntimeError("Fault node is already cordoned; refusing to change pre-existing state")
        if self.get("pod", CH[1])["spec"]["nodeName"] != name:
            raise RuntimeError("Affected ClickHouse and Keeper-0 must be on the same fault node")
        op = self.operator_pod()
        if op["spec"]["nodeName"] == name:
            raise RuntimeError("Operator must be pinned to the primary node, outside the fault")
        self.state.pop("empty_config_observed_at", None)
        self.state.update(fault_node=name, fault_active=True,
                          fault_started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                          old_operator_uid=op["metadata"]["uid"],
                          old_operator_ip=op["status"]["podIP"], results=str(self.out))
        self.save()  # Record before cordon so `restore` is possible after interruption.
        self.k("cordon", name)
        self.k("delete", "pod", KEEPERS[0], CH[1], "--wait=true", "--timeout=120s", timeout=150)
        self.wait("Keeper-0 Pending on cordoned node", lambda: self.get("pod", KEEPERS[0])["status"]["phase"] == "Pending")
        self.wait("ClickHouse-1 Pending on cordoned node", lambda: self.get("pod", CH[1])["status"]["phase"] == "Pending")
        self.k("delete", "pod", op["metadata"]["name"], "--wait=true", "--timeout=120s",
               ns=self.a.operator_namespace, timeout=150)
        def new_operator():
            p = self.operator_pod()
            return (p["metadata"]["uid"] != self.state["old_operator_uid"] and
                    self.ready(p["metadata"]["name"], self.a.operator_namespace) and p)
        new = self.wait("replacement operator Ready", new_operator, 180)
        self.state["new_operator_ip"] = new["status"]["podIP"]
        self.save()
        if self.state["new_operator_ip"] == self.state["old_operator_ip"]:
            raise RuntimeError("Operator IP was reused; the intended Operator IP changed trigger was not established")
        self.wait("Operator IP changed reconcile branch", lambda: "Operator IP changed" in self.operator_log(), 180)
        if self.a.mode == "discovery":
            self.wait("timeout warning AND zero nodes in BOTH ConfigMaps", lambda:
                      "Failed to resolve keeper references: keeper not ready" in self.operator_log() and
                      all(v["nodes"] == 0 for v in self.configs().values()), self.a.ready_timeout * 3 + 600)
            self.state["empty_config_observed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            self.save()
        else:
            self.wait("explicit-nodes full reconcile reached both ConfigMaps", lambda:
                      all(v["nodes"] == 3 for v in self.configs().values()), 120)
            # Let a discovery timeout happen if a keeper ref was accidentally retained.
            time.sleep(self.a.ready_timeout + 5)
            if "Failed to resolve keeper references" in self.operator_log():
                raise RuntimeError("Negative control still hit Keeper discovery")
        self.collect("fault")

    def restore(self):
        if not self.state.get("fault_active"):
            self.log("No saved cordon to restore")
            return
        name = self.state["fault_node"]
        self.k("uncordon", name)
        self.state["fault_active"] = False
        self.state["uncordoned_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        self.save()
        for pod in KEEPERS + CH:
            self.wait(pod + " Ready after restoring scheduling", lambda p=pod: self.ready(p), 600)

    def keeper_health(self):
        result = {}
        for pod in KEEPERS:
            result[pod] = self.k("exec", pod, "-c", "clickhouse-keeper", "--", "sh", "-c",
                                 "printf 'mntr' | nc -w 2 127.0.0.1 2181", timeout=15)
        leaders = [x for x in result.values() if "leader" in x]
        if len(leaders) != 1 or not any("zk_synced_followers\t2" in x for x in leaders):
            raise RuntimeError("Keeper does not yet report one leader and two synced followers")
        return result

    def verify(self):
        self.wait("Keeper leader with two synced followers", self.keeper_health, 180)
        if self.a.mode == "discovery":
            self.wait("affected persisted table readonly with no Keeper hosts", lambda:
                      any(r["is_readonly"] for r in self.replicas(CH[1])) and
                      "No hosts passed to ZooKeeper constructor" in self.k(
                          "logs", CH[1], "-c", "clickhouse", "--tail=2000"), 240)
            mounted = self.k("exec", CH[1], "-c", "clickhouse", "--", "cat",
                             "/etc/clickhouse-server/conf.d/" + XML_KEY)
            if self.node_count(mounted) != 0:
                raise RuntimeError("Affected pod does not have the empty mounted ZooKeeper config")
            self.log(f"Observing missing automatic repair for {self.a.observe_seconds}s")
            end = time.monotonic() + self.a.observe_seconds
            while time.monotonic() < end:
                if not all(v["nodes"] == 0 for v in self.configs().values()):
                    raise RuntimeError("Config was automatically repaired; missing-recovery part NOT reproduced")
                time.sleep(5)
            if not any(r["is_readonly"] for r in self.replicas(CH[1])):
                raise RuntimeError("Readonly state disappeared during observation")
        else:
            self.wait("negative control: both tables writable", lambda: all(self.healthy(p) for p in CH), 240)
            if not all(v["nodes"] == 3 for v in self.configs().values()):
                raise RuntimeError("Negative control lost nodes")
        self.collect("restored")
        self.write("summary.json", {"result": "PASS", "mode": self.a.mode,
                   "meaning": "bug reproduced" if self.a.mode == "discovery" else "negative control healthy",
                   "ready_timeout_seconds": self.a.ready_timeout,
                   "observation_seconds": self.a.observe_seconds if self.a.mode == "discovery" else 0,
                   "requested_observe_seconds": self.a.observe_seconds,
                   "control_fault_hold_seconds": self.a.ready_timeout + 5 if self.a.mode == "nodes" else None,
                   "timing": {k: self.state.get(k) for k in ["fault_started_at", "empty_config_observed_at", "uncordoned_at"]},
                   "operator_ips": {k: self.state.get(k) for k in ["old_operator_ip", "new_operator_ip"]},
                   "host_config_nodes": {k: v["nodes"] for k, v in self.configs().items()},
                   "replicas": {p: self.replicas(p) for p in CH}})
        self.log("PASS: " + ("BUG REPRODUCED" if self.a.mode == "discovery" else "EXPLICIT-NODES CONTROL HEALTHY"))

    def recover(self):
        self.require_fixtures()
        identities = {p: self.pod_identity(p) for p in CH}
        self.write("recovery-identities-before.json", identities)
        task = "keeper-recovery-" + str(time.time_ns())
        self.k("patch", "chi", "repro", "--type=merge", "-p", json.dumps(
            {"spec": {"taskID": task}}))
        self.wait("reconcile restored three Keeper nodes to both ConfigMaps", lambda:
                  all(v["nodes"] == 3 for v in self.configs().values()), 600)
        def completed():
            status = self.get("chi", "repro").get("status", {})
            return status.get("status") == "Completed" and task in status.get("taskIDsCompleted", [])
        self.wait("recovery task Completed", completed, 600)
        for pod in CH:
            self.wait(pod + " mounted config has three nodes", lambda p=pod: self.node_count(
                self.k("exec", p, "-c", "clickhouse", "--", "cat",
                       "/etc/clickhouse-server/conf.d/" + XML_KEY)) == 3, 180)
        self.collect("config-repaired")
        observation_seconds = None
        if self.a.recovery_mode == "reconcile-only":
            observation_seconds = self.observe_reconcile_recovery(identities)
        restarted_replicas = []
        if self.a.recovery_mode == "restart-replica":
            for pod in CH:
                if not self.healthy(pod):
                    self.log("Restarting table replica without restarting the pod: " + pod + "/repro.smoke")
                    self.sql(pod, "SYSTEM RESTART REPLICA repro.smoke", timeout=120)
                    restarted_replicas.append({"pod": pod, "table": "repro.smoke"})
                    self.wait(pod + " table writable after SYSTEM RESTART REPLICA", lambda p=pod: self.healthy(p), 180)
        # A replica initially attached with NO Keeper hosts can keep the failed
        # initialization in memory. Do not equate repaired XML with writable tables.
        # Pod restarts are opt-in; default recovery uses SYSTEM RESTART REPLICA above.
        restarted = []
        for pod in CH:
            if self.a.recovery_mode == "restart-readonly" and not self.healthy(pod):
                old_uid = self.get("pod", pod)["metadata"]["uid"]
                restarted.append({"pod": pod, "old_uid": old_uid})
                self.log("Restarting readonly fixture pod after config repair: " + pod)
                self.k("delete", "pod", pod, "--wait=true", "--timeout=120s", timeout=150)
                self.wait(pod + " replaced and Ready", lambda p=pod, uid=old_uid:
                          self.get("pod", p)["metadata"]["uid"] != uid and self.ready(p), 300)
                self.wait(pod + " table writable after restart", lambda p=pod: self.healthy(p), 180)
        self.wait("both tables writable after full reconcile", lambda: all(self.healthy(p) for p in CH), 300)
        token = time.time_ns()
        self.sql(CH[0], f"INSERT INTO repro.smoke VALUES ({token})")
        self.sql(CH[1], "SYSTEM SYNC REPLICA repro.smoke")
        counts = {p: self.sql(p, "SELECT count() FROM repro.smoke").strip() for p in CH}
        if len(set(counts.values())) != 1:
            raise RuntimeError("Row counts differ after recovery")
        if any(self.sql(p, f"SELECT countIf(id={token}) FROM repro.smoke").strip() != "1" for p in CH):
            raise RuntimeError("New recovery row did not replicate")
        final_identities = {p: self.pod_identity(p) for p in CH}
        if self.a.recovery_mode != "restart-readonly" and final_identities != identities:
            raise RuntimeError("Pod/container identity changed during the recovery write check")
        self.collect("recovered")
        method = {"reconcile-only": "spec.taskID only",
                  "restart-replica": "spec.taskID; SYSTEM RESTART REPLICA; no pod restart",
                  "restart-readonly": "spec.taskID; restart readonly fixture pods after mounted config repair"}[self.a.recovery_mode]
        self.write("recovery.json", {"result": "PASS", "method": method,
                                     "observation_seconds": observation_seconds,
                                     "identities_before": identities, "identities_after": final_identities,
                                     "restarted_replicas": restarted_replicas,
                                     "restarted_pods": restarted,
                                     "keeper_ref_retained": bool(self.get("chi", "repro").get("spec", {}).get(
                                         "configuration", {}).get("zookeeper", {}).get("keeper")),
                                     "inserted_id": token,
                                     "counts": counts})

    def collect(self, stage="snapshot"):
        folder = self.out / stage
        folder.mkdir(exist_ok=True)
        def capture(name, fn):
            try:
                value = fn()
                (folder / name).write_text(value if isinstance(value, str) else json.dumps(value, indent=2) + "\n")
            except Exception as e:
                (folder / (name + ".error.txt")).write_text(str(e))
        capture("pods.json", lambda: self.get("pods"))
        capture("chi.json", lambda: self.get("chi", "repro"))
        capture("chk.json", lambda: self.get("chk", "repro-keeper"))
        capture("events.json", lambda: self.get("events"))
        capture("operator-pod.json", self.operator_pod)
        capture("operator.log", self.operator_log)
        capture("configs.json", self.configs)
        capture("keeper-mntr.json", self.keeper_health)
        for pod in CH:
            capture(pod + ".replicas.json", lambda p=pod: self.replicas(p))
            capture(pod + ".log", lambda p=pod: self.k("logs", p, "-c", "clickhouse", "--tail=2000"))
            capture(pod + ".mounted.xml", lambda p=pod: self.k("exec", p, "-c", "clickhouse", "--",
                    "cat", "/etc/clickhouse-server/conf.d/" + XML_KEY))
            def merged(p=pod):
                xml = self.k("exec", p, "-c", "clickhouse", "--", "cat",
                             "/var/lib/clickhouse/preprocessed_configs/config.xml")
                zk = ET.fromstring(xml).find("zookeeper")
                return ET.tostring(zk, encoding="unicode") if zk is not None else "<absent/>"
            capture(pod + ".preprocessed-zookeeper.xml", merged)
        self.log("Evidence: " + str(folder))

    def set_mode(self):
        self.require_fixtures()
        if self.a.mode == "nodes":
            nodes = [{"host": f"chk-repro-keeper-keeper-0-{i}-client.{self.a.namespace}.svc.cluster.local",
                      "port": 2181} for i in range(3)]
            zk = {"keeper": None, "nodes": nodes}
        else:
            zk = {"keeper": {"name": "repro-keeper", "serviceType": "replicas"}, "nodes": None}
        task = "keeper-baseline-" + str(time.time_ns())
        self.k("patch", "chi", "repro", "--type=merge", "-p", json.dumps(
            {"spec": {"taskID": task, "configuration": {"zookeeper": zk}}}))
        def applied():
            status = self.get("chi", "repro").get("status", {})
            return (status.get("status") == "Completed" and task in status.get("taskIDsCompleted", []) and
                    all(v["nodes"] == 3 for v in self.configs().values()))
        self.wait("mode applied and CHI Completed", lambda:
                  applied(), 600)

    def run(self):
        self.set_mode()
        self.baseline()
        try:
            self.fault()
        finally:
            # Always undo OUR cordon, including failed assertions/interrupts.
            self.restore()
        self.verify()
        if not self.a.skip_recovery:
            self.recover()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True, help="Explicit Kubernetes context; never uses current-context implicitly")
    p.add_argument("--kubeconfig")
    p.add_argument("--namespace", default="keeper-discovery-repro")
    p.add_argument("--operator-namespace", default="keeper-discovery-operator")
    p.add_argument("--ready-timeout", type=int, default=15, help="Must match the installed operator's effective timeout")
    p.add_argument("--observe-seconds", type=int, default=60)
    p.add_argument("--mode", choices=["discovery", "nodes"], default="discovery")
    p.add_argument("--results", help="Diagnostics directory (default: .work/results/<run>)")
    p.add_argument("--skip-recovery", action="store_true", help="Leave confirmed readonly/config failure for manual inspection")
    p.add_argument("--recovery-mode", choices=["restart-replica", "reconcile-only", "restart-readonly"], default="restart-replica",
                   help="Default: SYSTEM RESTART REPLICA without pod restart; alternatives: only reconcile or restart readonly pods")
    p.add_argument("--recovery-wait-seconds", type=int, default=600,
                   help="Reconcile-only observation window after mounted config repair (default: 600)")
    p.add_argument("command", choices=["run", "baseline", "fault", "restore", "verify", "recover", "collect", "set-mode"])
    args = p.parse_args()
    if args.ready_timeout < 1 or args.observe_seconds < 5 or args.recovery_wait_seconds < 5:
        p.error("ready-timeout must be positive; observe-seconds and recovery-wait-seconds must be >= 5")
    lab = Lab(args)
    try:
        getattr(lab, args.command.replace("-", "_"))()
    except (Exception, KeyboardInterrupt) as e:
        lab.log("FAIL: " + str(e))
        lab.write("failure.txt", str(e) + "\n")
        lab.collect("failure")
        return 1
    finally:
        print("Results:", lab.out, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
