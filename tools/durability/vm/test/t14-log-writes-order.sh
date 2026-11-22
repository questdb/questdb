#!/usr/bin/env bash
# t14 — dm-log-writes keeps overlapping flushes in order.
#
# Ubuntu's dm-log-writes logs a flush's batch of writes when that flush completes. Two flushes
# in flight at once can complete in the opposite order, and the log then holds an older write
# after a newer one: a replay rebuilds a device state that never existed. The golden image
# carries the module rebuilt with kernel/dm-log-writes-flush-order.patch (guest/
# build-dm-log-writes.sh); this control proves, every night, that the fix is what records.
#
# guest/dmlw-flush-order.py forces the overlap: an nbd server it runs in-process holds the first
# flush until a second one, issued from a CPU on the other hardware queue, has completed. Both
# arms run it on the same boot:
#
#   stock    the module Ubuntu ships, loaded from its own path   every run REORDERED
#   patched  what `modprobe dm-log-writes` loads                 no run REORDERED
#
# The stock arm is the negative control: it proves the check can see the fault, so a green
# patched arm means "fixed" and not "the check never fires". If the stock arm stops reordering,
# the kernel has changed under the image; read the message below before touching anything.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../lib/qemu.sh
source "$HERE/lib/qemu.sh"
STATE_DIR="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"; BASE="$STATE_DIR/base"; KEY="$BASE/id_ed25519"
RUN="$STATE_DIR/t14"; rm -rf "$RUN"; mkdir -p "$RUN"
RUNS="${QDB_T14_RUNS:-5}"
vm_kill_on_exit "$RUN"

qemu-img create -f qcow2 -b "$BASE/golden.qcow2" -F qcow2 "$RUN/overlay.qcow2" >/dev/null
# A data disk is attached only so the log disk is /dev/vdc, as in every other boot. The script
# writes through its own nbd device, never through /dev/vdb.
truncate -s 64M "$RUN/data.raw"
truncate -s 256M "$RUN/log.raw"
P=$(vm_free_port)
vm_boot "$RUN" "$RUN/overlay.qcow2" "$RUN/data.raw" "$P" "" "$RUN/log.raw"
vm_wait_ssh "$P" "$KEY" 240
vm_scp_dir "$P" "$KEY" "$HERE/guest" /opt/vmcrash/

facts=$(vm_ssh "$P" "$KEY" "cat /etc/qdb-vmcrash/dm-log-writes.env" 2>/dev/null || true)
fact() { printf '%s\n' "$facts" | sed -n "s/^$1=//p"; }
STOCK_KO=$(fact STOCK_KO); STOCK_SV=$(fact STOCK_SRCVERSION); FIXED_SV=$(fact FIXED_SRCVERSION)
if [ -z "$STOCK_KO" ] || [ -z "$STOCK_SV" ] || [ -z "$FIXED_SV" ]; then
    echo "FAIL t14: the golden image has no patched dm-log-writes (/etc/qdb-vmcrash/dm-log-writes.env);"
    echo "  it predates kernel/dm-log-writes-flush-order.patch. Rebuild it with build-image.sh."
    exit 1
fi
kver=$(vm_ssh "$P" "$KEY" "uname -r")
[ "$kver" = "$(fact KVER)" ] || { echo "FAIL t14: guest runs kernel $kver, the module was built for $(fact KVER)"; exit 1; }

# arm LABEL LOAD_CMD WANT_SRCVERSION: loads a module, runs the reproducer, prints its summary line.
arm() {
    vm_ssh "$P" "$KEY" "sudo rmmod dm_log_writes 2>/dev/null; $2" \
        || { echo "FAIL t14: could not load the $1 module" >&2; return 1; }
    local sv
    sv=$(vm_ssh "$P" "$KEY" "cat /sys/module/dm_log_writes/srcversion")
    [ "$sv" = "$3" ] || { echo "FAIL t14: $1 arm loaded srcversion $sv, expected $3" >&2; return 1; }
    vm_ssh "$P" "$KEY" "sudo python3 /opt/vmcrash/guest/dmlw-flush-order.py /dev/vdc $RUNS 2>&1" \
        > "$RUN/$1.out" || true
    tail -1 "$RUN/$1.out"
}

stock=$(arm stock "sudo insmod $STOCK_KO" "$STOCK_SV")
patched=$(arm patched "sudo modprobe dm-log-writes" "$FIXED_SV")
echo "  stock   ($STOCK_SV): $stock"
echo "  patched ($FIXED_SV): $patched"
warn=$(vm_ssh "$P" "$KEY" "sudo dmesg | grep -cE 'WARNING|BUG:|Call Trace'" || true)

rc=0
case "$stock" in
    "$RUNS of $RUNS runs:"*REORDERED*) ;;
    "0 of $RUNS runs:"*)
        echo "FAIL t14: the STOCK module kept the flushes in order in all $RUNS runs. Either the guest"
        echo "  kernel now carries a fix (check drivers/md/dm-log-writes.c upstream; if so, drop the"
        echo "  patch) or the reproducer no longer forces the overlap. Either way this control can no"
        echo "  longer tell the two modules apart."
        rc=1 ;;
    *)  echo "FAIL t14: the stock arm did not complete; see $RUN/stock.out"; rc=1 ;;
esac
case "$patched" in
    "0 of $RUNS runs:"*) ;;
    *REORDERED*) echo "FAIL t14: the PATCHED module recorded an older write after a newer one"; rc=1 ;;
    *) echo "FAIL t14: the patched arm did not complete; see $RUN/patched.out"; rc=1 ;;
esac
[ "${warn:-0}" -eq 0 ] || { echo "FAIL t14: $warn kernel warning line(s) in the guest's dmesg"; rc=1; }
[ "$rc" -eq 0 ] || exit "$rc"

vm_kill "$RUN"; rm -rf "$RUN"; echo "PASS t14 (stock module reorders, patched module does not)"
