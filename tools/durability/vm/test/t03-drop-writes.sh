#!/usr/bin/env bash
# t03 — the device layer actually drops un-flushed writes.
#
# A file fsync'd BEFORE the cut must survive; a file written AFTER arming must
# not. The second assertion is the one that matters: it is what distinguishes a
# real power cut from merely killing a process.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t03"; rm -rf "$RUN"; mkdir -p "$RUN"

qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 8G "$RUN/data.raw"

P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P"
vm_wait_ssh "$P" "$KEY" 240
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh" >/dev/null
vm_ssh "$P" "$KEY" "printf before > /mnt/qdb/before && sync /mnt/qdb/before"

# Pre-allocate and journal the device probe BEFORE the cut, then overwrite it in
# place with O_DIRECT afterwards. See guest/preflight.sh for why the page-cache
# probe below cannot carry this claim on its own: it vanishes from the VMM kill
# alone, under a real cut and a defanged one alike.
vm_ssh "$P" "$KEY" "head -c 4096 /dev/zero | tr '\\0' 'A' > /mnt/qdb/probe && sync /mnt/qdb/probe && sudo sync"

vm_ssh "$P" "$KEY" "bash /opt/vmcrash/guest/arm-cut.sh; \
    head -c 4096 /dev/zero | tr '\\0' 'B' | dd of=/mnt/qdb/probe bs=4096 count=1 conv=notrunc oflag=direct 2>/dev/null; \
    printf after > /mnt/qdb/after" >/dev/null 2>&1 &
armpid=$!
vm_wait_console "$RUN" "CUT-ARMED" 60
vm_kill "$RUN"
wait "$armpid" 2>/dev/null || true

P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2"
vm_wait_ssh "$P2" "$KEY" 240
vm_ssh "$P2" "$KEY" "bash /opt/vmcrash/guest/prepare-device.sh --reattach" >/dev/null
b=$(vm_ssh "$P2" "$KEY" "cat /mnt/qdb/before 2>/dev/null || echo MISSING")
a=$(vm_ssh "$P2" "$KEY" "cat /mnt/qdb/after  2>/dev/null || echo MISSING")
p=$(vm_ssh "$P2" "$KEY" "head -c 1 /mnt/qdb/probe 2>/dev/null || echo MISSING")
vm_kill "$RUN"

[ "$b" = "before" ] || { echo "FAIL t03: fsync'd file was LOST (got '$b') — the cut is too aggressive"; exit 1; }
[ "$a" = "MISSING" ] || { echo "FAIL t03: page-cache write SURVIVED (got '$a') — the VMM did not really die"; exit 1; }
[ "$p" = "A" ] || {
    echo "FAIL t03: device probe is '$p', expected 'A'."
    echo "  'B' means an un-flushed O_DIRECT write reached the disk and survived —"
    echo "  drop_writes is not dropping, so durability results would be false green."
    echo "  Anything else means the fsync'd baseline was lost."
    exit 1
}
rm -rf "$RUN"
echo "PASS t03 (fsync'd survived; page-cache write lost; device write DROPPED)"
