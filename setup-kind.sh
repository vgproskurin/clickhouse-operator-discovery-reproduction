#!/usr/bin/env bash
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
lab_name=${LAB_NAME:-keeper-discovery}
lab_ns=${REPRO_NAMESPACE:-keeper-discovery-repro}
operator_ns=${OPERATOR_NAMESPACE:-keeper-discovery-operator}
ready_timeout=${READY_TIMEOUT:-15}
lab_platform=${LAB_PLATFORM:-linux/amd64}
lab_work="$here/.work"
lab_kubeconfig="$lab_work/kubeconfig"
lab_context="kind-$lab_name"
mkdir -p "$lab_work/chart"
[[ "$lab_name" =~ ^[a-z0-9-]+$ && "$lab_ns" =~ ^[a-z0-9-]+$ && "$operator_ns" =~ ^[a-z0-9-]+$ ]]
[[ "$ready_timeout" =~ ^[1-9][0-9]*$ ]]
read -r -a exec_prefix <<< "${KUBE_EXEC_PREFIX:-}"
k() { "${exec_prefix[@]}" kubectl --kubeconfig "$lab_kubeconfig" --context "$lab_context" "$@"; }
h() { "${exec_prefix[@]}" helm --kubeconfig "$lab_kubeconfig" --kube-context "$lab_context" "$@"; }

if kind get clusters | grep -Fxq "$lab_name"; then
  [[ "${1:-}" == --reuse ]] || { echo "Cluster exists. Use --reuse for this lab, or choose LAB_NAME." >&2; exit 1; }
  kind export kubeconfig --name "$lab_name" --kubeconfig "$lab_kubeconfig"
else
  kind create cluster --name "$lab_name" --image kindest/node:v1.32.2 \
    --config "$here/kind.yaml" --kubeconfig "$lab_kubeconfig" --wait 180s
fi
k taint node "$lab_name-control-plane" node-role.kubernetes.io/control-plane:NoSchedule- || true

for lab_image in altinity/clickhouse-operator:0.27.4 \
  clickhouse/clickhouse-server:26.8.36.3-alpine \
  clickhouse/clickhouse-keeper:26.8.36.3-alpine; do
  lab_cached=true
  while read -r lab_node; do
    if ! docker exec "$lab_node" crictl inspecti "$lab_image" >/dev/null 2>&1; then
      lab_cached=false
    fi
  done < <(kind get nodes --name "$lab_name")
  if [[ "$lab_cached" == true ]]; then
    echo "Image already present on every kind node: $lab_image"
    continue
  fi
  docker image inspect "$lab_image" >/dev/null 2>&1 || docker pull --platform "$lab_platform" "$lab_image"
  # New Docker image stores can retain a multi-platform index with absent blobs.
  # Export just the selected platform if supported by the installed CLI.
  if docker image save --help | grep -q -- --platform; then
    docker image save --platform "$lab_platform" "$lab_image" -o "$lab_work/load-image.tar"
  else
    docker image save "$lab_image" -o "$lab_work/load-image.tar"
  fi
  kind load image-archive --name "$lab_name" "$lab_work/load-image.tar"
done
rm -f "$lab_work/load-image.tar"

lab_chart_archive="$lab_work/chart/altinity-clickhouse-operator-0.27.4.tgz"
if [[ ! -f "$lab_chart_archive" ]]; then
  helm pull altinity-clickhouse-operator --repo https://helm.altinity.com --version 0.27.4 --destination "$lab_work/chart"
fi
k create namespace "$lab_ns" --dry-run=client -o yaml | k apply -f -
# Helm installs the bundled CRDs on first install.
h upgrade --install keeper-discovery-operator "$lab_chart_archive" \
  -n "$operator_ns" --create-namespace -f "$here/operator.values.yaml" \
  --set "watchNamespaces[0]=$lab_ns" \
  --set "configs.files.config\\.yaml.reconcile.coordination.keeper.readyTimeout=$ready_timeout" \
  --wait --timeout 5m
sed "s/namespace: keeper-discovery-repro/namespace: $lab_ns/" "$here/manifests/keeper.yaml" | k apply -f -
for lab_i in 0 1 2; do
  lab_pod="chk-repro-keeper-keeper-0-$lab_i-0"
  for ((lab_wait=0; lab_wait<180; lab_wait++)); do
    k -n "$lab_ns" get pod "$lab_pod" >/dev/null 2>&1 && break
    sleep 2
  done
  k -n "$lab_ns" wait --for=condition=Ready "pod/$lab_pod" --timeout=10m
done
k -n "$lab_ns" wait --for=jsonpath='{.status.status}'=Completed chk/repro-keeper --timeout=10m
sed "s/namespace: keeper-discovery-repro/namespace: $lab_ns/" "$here/manifests/clickhouse.yaml" | k apply -f -
# Clear an explicit-nodes control from an earlier run, and wait for THIS task,
# rather than accidentally accepting the previous Completed status.
python3 "$here/repro.py" --context "$lab_context" --kubeconfig "$lab_kubeconfig" \
  --namespace "$lab_ns" --operator-namespace "$operator_ns" --ready-timeout "$ready_timeout" set-mode
python3 "$here/repro.py" --context "$lab_context" --kubeconfig "$lab_kubeconfig" \
  --namespace "$lab_ns" --operator-namespace "$operator_ns" --ready-timeout "$ready_timeout" baseline
{
  printf 'LAB_CONTEXT=%q\n' "$lab_context"
  printf 'LAB_KUBECONFIG=%q\n' "$lab_kubeconfig"
  printf 'REPRO_NAMESPACE=%q\n' "$lab_ns"
  printf 'OPERATOR_NAMESPACE=%q\n' "$operator_ns"
  printf 'READY_TIMEOUT=%q\n' "$ready_timeout"
} > "$lab_work/lab.env"
echo "Ready. Run:"
echo "python3 $here/repro.py --context $lab_context --kubeconfig $lab_kubeconfig --ready-timeout $ready_timeout run"
