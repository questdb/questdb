#!/usr/bin/env bash
# t06 — dm-log-writes replay: validate the parser AND the boundary semantics.
#
# WHAT THIS PROVES
#   Replaying to flush N reconstructs the device as it stood at flush N, and
#   EXCLUDES everything written after it.
#
# That is exactly the property run-flush-sweep.sh depends on. If an earlier
# boundary leaked later writes, every enumerated crash point would be a blend of
# states that never existed on any real machine, and its verdicts would be
# fiction.
#
# HOW -- and why not the obvious way
#   Earlier versions tried to manufacture an "at the device but unflushed" write
#   and assert it was discarded. That kept testing the harness's ability to
#   create that state rather than the replay's correctness, and all three routes
#   failed for different reasons (each measured, not assumed):
#
#     * O_DIRECT -- ext4 forces a journal commit for the O_DIRECT path, and the
#       commit carries a FLUSH covering the write. Seen directly in the log with
#       marks bracketing it. commit=3600 does not help: not time-driven.
#     * crash immediately -- dm-log-writes queues entries to a kthread, so the
#       write is never recorded at all and its absence proves nothing.
#     * pause first -- a periodic journal commit flushes it.
#
#   So instead: write TWO files, each fsync'd, and use the replay itself as the
#   instrument. Find the first boundary at which each appears. If boundaries work,
#   `first` must appear strictly before `second`, and `second` must never appear
#   at a boundary where `first` is absent. No exotic state required, and it tests
#   the property the sweep relies on instead of a proxy for it.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"

STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t06"; rm -rf "$RUN"; mkdir -p "$RUN"

qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
truncate -s 4G "$RUN/data.raw"
truncate -s 4G "$RUN/log.raw"

P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/
vm_ssh "$P" "$KEY" "sudo sync"

echo "--- build the log-writes stack: data=/dev/vdb log=/dev/vdc"
vm_ssh "$P" "$KEY" "sudo modprobe dm-log-writes && \
    S=\$(sudo blockdev --getsz /dev/vdb) && \
    sudo dmsetup create qdblog --table \"0 \$S log-writes /dev/vdb /dev/vdc\" && \
    sudo mkfs.ext4 -F -q /dev/mapper/qdblog && \
    sudo mkdir -p /mnt/qdb && sudo mount /dev/mapper/qdblog /mnt/qdb && \
    sudo chown ubuntu /mnt/qdb && echo stack-ready"

echo "--- write FIRST (fsync), then SECOND (fsync); each forces a flush"
vm_ssh "$P" "$KEY" "printf one > /mnt/qdb/first && sync /mnt/qdb/first && sudo sync"
vm_ssh "$P" "$KEY" "sleep 1"
vm_ssh "$P" "$KEY" "printf two > /mnt/qdb/second && sync /mnt/qdb/second && sudo sync"
vm_ssh "$P" "$KEY" "sleep 2"   # let the log kthread drain its queue

echo "--- CRASH (no umount: that would flush and blur the boundaries)"
vm_kill "$RUN"

echo "--- reboot on a fresh OS disk; data + log carry over untouched"
rm -f "$RUN/overlay.qcow2"
qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
P2=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P2" "" "$RUN/log.raw"
vm_wait_ssh "$P2" "$KEY" 240
vm_scp_dir "$P2" "$KEY" "$HERE/guest" /opt/vmcrash/

nflush=$(vm_ssh "$P2" "$KEY" "sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --list | head -1" \
    | grep -oE '[0-9]+ flushes' | grep -oE '^[0-9]+')
echo "--- log holds $nflush flush boundaries; replaying each"
[ "${nflush:-0}" -ge 3 ] || { echo "FAIL t06: only ${nflush:-0} flushes recorded; cannot test a boundary"; exit 1; }

first_at=0; second_at=0; leaked=0
for n in $(seq 1 "$nflush"); do
    ls_out=$(vm_ssh "$P2" "$KEY" "sudo umount /mnt/raw 2>/dev/null; \
        sudo dd if=/dev/zero of=/dev/vdb bs=1M count=64 2>/dev/null; \
        sudo python3 /opt/vmcrash/guest/replay-log.py --log /dev/vdc --replay /dev/vdb --to-flush $n >/dev/null 2>&1; \
        sudo mkdir -p /mnt/raw; \
        if sudo mount /dev/vdb /mnt/raw 2>/dev/null; then ls /mnt/raw | tr '\n' ' '; else echo UNMOUNTABLE; fi")
    has_first=0; has_second=0
    case "$ls_out" in *first*) has_first=1 ;; esac
    case "$ls_out" in *second*) has_second=1 ;; esac
    printf '  flush %2d/%-2d -> %s\n' "$n" "$nflush" "$ls_out"

    # THE BOUNDARY VIOLATION: `second` cannot exist at a boundary where `first`
    # does not. That ordering is fixed by the order they were written and fsync'd.
    if [ "$has_second" -eq 1 ] && [ "$has_first" -eq 0 ]; then leaked=1; fi
    if [ "$first_at" -eq 0 ] && [ "$has_first" -eq 1 ]; then first_at=$n; fi
    if [ "$second_at" -eq 0 ] && [ "$has_second" -eq 1 ]; then second_at=$n; fi
done
vm_kill "$RUN"

echo "first appears at flush $first_at; second appears at flush $second_at"
[ "$leaked" -eq 0 ] || { echo "FAIL t06: 'second' present where 'first' was absent — replay is blending states"; exit 1; }
[ "$first_at" -gt 0 ] || { echo "FAIL t06: 'first' never appears — replay is dropping writes"; exit 1; }
[ "$second_at" -gt 0 ] || { echo "FAIL t06: 'second' never appears — the log tail is being truncated"; exit 1; }
[ "$first_at" -lt "$second_at" ] || {
    echo "FAIL t06: both files appear at the same boundary (flush $first_at)."
    echo "  The replay is not discriminating between boundaries, so every"
    echo "  enumerated crash point would report the same state."
    exit 1
}
rm -rf "$RUN"
echo "PASS t06 (boundaries discriminate: first@$first_at < second@$second_at, nothing leaked backwards)"
