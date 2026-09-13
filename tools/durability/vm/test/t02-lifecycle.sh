#!/usr/bin/env bash
# t02 — boot / kill / reboot on the same disks, and an fsync'd file survives.
# The complementary "un-flushed must vanish" half needs dm-flakey and is t03.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$HERE/lib/qemu.sh"
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"; BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t02"; rm -rf "$RUN"; mkdir -p "$RUN"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 8G "$RUN/data.raw"
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P"
vm_wait_ssh "$P" "$KEY" 240
vm_ssh "$P" "$KEY" "sudo mkfs.ext4 -F -q /dev/vdb && sudo mkdir -p /mnt/d && sudo mount /dev/vdb /mnt/d && sudo chown ubuntu /mnt/d"
vm_ssh "$P" "$KEY" "printf survivor > /mnt/d/kept && sync /mnt/d/kept && sudo sync"
vm_kill "$RUN"
P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2"
vm_wait_ssh "$P2" "$KEY" 240
vm_ssh "$P2" "$KEY" "sudo mkdir -p /mnt/d && sudo mount /dev/vdb /mnt/d"
got=$(vm_ssh "$P2" "$KEY" "cat /mnt/d/kept 2>/dev/null || echo MISSING")
vm_kill "$RUN"
[ "$got" = "survivor" ] || { echo "FAIL t02: fsync'd file did not survive (got '$got')"; exit 1; }
rm -rf "$RUN"; echo "PASS t02"
