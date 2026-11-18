# Keeper discovery timeout: reproduction in kind

In clickhouse-operator **0.27.4**, a discovery timeout can replace Keeper endpoints
with an empty `<zookeeper>` section in both ClickHouse replicas' configurations.
After Keeper recovers, the restarted ClickHouse replica remains readonly.

The lab runs a three-node kind **v1.32.2** cluster, three Keeper replicas, two
ClickHouse replicas, and a PVC-backed ReplicatedMergeTree table. ClickHouse and
Keeper use **26.8.36.3-alpine**. The failure is simulated by cordoning a node and
keeping replacement pods Pending, then recreating the operator with a new IP.
No actual node reboot is performed.

## Run

Requires Docker, kind, kubectl, Helm 3, Python 3.10+, and 8–12 GiB of available RAM.

If using rootless Docker, run this first:

```bash
export KUBE_EXEC_PREFIX="$PWD/rootless-net.sh"
```

```bash
./setup-kind.sh
python3 repro.py --context kind-keeper-discovery \
  --kubeconfig .work/kubeconfig --ready-timeout 15 run
```

If the lab already exists, use `./setup-kind.sh --reuse`.

**PASS means the bug was reproduced:** both XML configurations are empty, Keeper
quorum has recovered, but the table remains readonly and the configurations are
not repaired within 60 seconds. The runner then repairs the configuration and
recovers the table using `SYSTEM RESTART REPLICA`, without recreating ClickHouse
pods. Diagnostics are saved to `.work/results/`.

By default, the lab reduces `reconcile.coordination.keeper.readyTimeout` from
**120 to 15 seconds**. To use the original timeout:

```bash
READY_TIMEOUT=120 ./setup-kind.sh  # Add --reuse if the lab already exists.
python3 repro.py --context kind-keeper-discovery \
  --kubeconfig .work/kubeconfig --ready-timeout 120 run
```

For a control run with explicit Keeper endpoints, add `--mode nodes` before `run`.
To test recovery through reconciliation alone, add `--recovery-mode reconcile-only`
before `run`; it observes the table for up to 10 minutes without restarting it.

## Delete the lab

```bash
kind delete cluster --name keeper-discovery
```
