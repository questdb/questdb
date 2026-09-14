# lib/preflight.sh — one preflight cut cycle. Source; do not execute.
#
# Shared by test/t04 (which runs it BOTH ways to prove the guard can fail) and
# run-matrix.sh (which runs it once per matrix as the standing gate).
#
# This is the guard that actually discriminates. The NOSYNC arm does not: under
# a sustained ingest the guest kernel writes back continuously, so by the time
# drop_writes is armed almost everything is already legitimately on disk and a
# no-sync workload loses nothing. See run-matrix.sh for the measurements.
#
# Requires lib/qemu.sh and lib/verdict.sh to be sourced first.

# run_preflight_cycle VARIANT VM_DIR
#   VARIANT = "real"     -> cut armed with drop_writes; expect PREFLIGHT_OK
#             "defanged" -> cut armed WITHOUT drop_writes; expect PREFLIGHT_FAILED
#   VM_DIR  = directory holding guest/ to ship
# Echoes one verdict token. Progress goes to stderr.
run_preflight_cycle() {
    local variant="$1"
    local vmdir="$2"
    local state_dir="${QDB_VMCRASH_STATE:-/data/qdb-vmcrash}"
    local base="$state_dir/base"
    local key="$base/id_ed25519"
    local run="$state_dir/preflight-$variant-$$"
    local dropw=1
    [ "$variant" = "defanged" ] && dropw=0

    rm -rf "$run"; mkdir -p "$run"
    qemu-img create -f qcow2 -b "$base/golden.qcow2" -F qcow2 "$run/overlay.qcow2" >/dev/null
    truncate -s 8G "$run/data.raw"

    local p
    p=$(vm_free_port)
    vm_boot "$run" "$run/overlay.qcow2" "$run/data.raw" "$p"
    vm_wait_ssh "$p" "$key" 240 >&2 || { echo UNPARSEABLE; return; }
    vm_scp_dir "$p" "$key" "$vmdir/guest" /opt/vmcrash/
    vm_ssh "$p" "$key" "sudo sync"
    vm_ssh "$p" "$key" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh" >/dev/null

    vm_ssh "$p" "$key" "QDB_CUT_DROP_WRITES=$dropw bash /opt/vmcrash/guest/preflight.sh --phase=write" >/dev/null 2>&1 &
    local armpid=$!
    vm_wait_console "$run" "CUT-ARMED" 60 >&2 || { kill "$armpid" 2>/dev/null; vm_kill "$run"; echo UNPARSEABLE; return; }
    vm_kill "$run"
    wait "$armpid" 2>/dev/null || true

    local p2
    p2=$(vm_free_port)
    vm_boot "$run" "$run/overlay.qcow2" "$run/data.raw" "$p2"
    vm_wait_ssh "$p2" "$key" 240 >&2 || { echo UNPARSEABLE; return; }
    vm_ssh "$p2" "$key" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh --reattach" >/dev/null
    local line
    line=$(vm_ssh "$p2" "$key" "bash /opt/vmcrash/guest/preflight.sh --phase=check")
    vm_kill "$run"
    rm -rf "$run"
    verdict_classify "$line"
}
