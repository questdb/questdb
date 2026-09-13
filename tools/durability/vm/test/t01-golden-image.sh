#!/usr/bin/env bash
# t01 — the golden image boots and carries what the guest arms need.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$HERE/lib/qemu.sh"
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"; BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t01"; rm -rf "$RUN"; mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "" "$P"
vm_wait_ssh "$P" "$KEY" 240
vm_ssh "$P" "$KEY" "java -version" 2>&1 | grep -q version || { echo "FAIL t01: no java"; exit 1; }
vm_ssh "$P" "$KEY" "which dmsetup mkfs.ext4" >/dev/null || { echo "FAIL t01: missing dm/fs tooling"; exit 1; }
vm_ssh "$P" "$KEY" "lsmod | grep -q dm_flakey" || { echo "FAIL t01: dm-flakey not loaded"; exit 1; }
vm_ssh "$P" "$KEY" "test -d /opt/vmcrash" || { echo "FAIL t01: /opt/vmcrash absent"; exit 1; }
vm_kill "$RUN"; rm -rf "$RUN"; echo "PASS t01"
