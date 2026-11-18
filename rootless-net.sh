#!/usr/bin/env bash
# Optional workaround for this workspace's rootless Docker network namespace.
# Normal Docker installations do not need KUBE_EXEC_PREFIX or this script.
set -euo pipefail
lab_dockerd_pid=$(pgrep -u "$(id -u)" -x dockerd)
[[ "$lab_dockerd_pid" =~ ^[0-9]+$ ]] || { echo "Expected one rootless dockerd" >&2; exit 1; }
exec nsenter --target "$lab_dockerd_pid" --user --net --preserve-credentials "$@"
