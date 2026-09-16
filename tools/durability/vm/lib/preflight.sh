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

# preflight_cleanup RUNDIR KEEP
#   KEEP = "keep" -> keep the cheap evidence, delete the disks
#          anything else -> delete the whole run directory
#
# ONE cleanup path, because four copies of a cleanup is how this file came to have three bail
# paths that each cleaned up differently -- and two that did not clean up at all.
#
# THE VM DIES FIRST, ON EVERY PATH. vm_boot daemonizes QEMU, so a bail that only returned left
# a live VM behind. That is not a tidiness problem, it is three separate faults:
#   * the harness assumes ONE VM AT A TIME; a stray QEMU corrupts whatever run is measured next.
#   * reap-state.sh DELIBERATELY refuses any directory whose qemu.pid is still alive, so the
#     leaked 8 GiB is invisible to the reaper -- permanently.
#   * it surfaces days later at check-host.sh's 200 GB free-space gate, reading as an
#     infrastructure outage rather than as the preflight bug it is.
# vm_kill is idempotent (no pidfile -> return 0), so calling it on an already-cut VM is safe.
#
# THE DISKS ALWAYS GO; THE CONSOLE LOG SURVIVES A BAIL. The rest of the harness keeps disks on
# failure on purpose (issues/15), and that is right where the disk IS the evidence -- a sweep's
# data+log devices can be replayed again. Preflight has no log device, so its 8 GiB overlay and
# data.raw cannot be replayed and answer no question after the fact. What does answer the
# question is console.log, which is where a guest that never reached SSH says why. Discarding it
# is the exact fault power-cut-vm.sh had on its liveness bail, where recovering it cut diagnosis
# from three VM cycles to one. So: delete the gigabytes, keep the kilobytes.
#
# The kept directory is preflight-<variant>-<pid>, which reap-state.sh covers (reap-state.sh:112)
# and ages out via --keep-days, so keeping it cannot accumulate unboundedly on a CI agent.
preflight_cleanup() {
    local run="$1" keep="${2:-}"
    vm_kill "$run" 2>/dev/null
    rm -f "$run/overlay.qcow2" "$run/data.raw"
    if [ "$keep" != keep ]; then
        rm -rf "$run"
    fi
    return 0
}

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

    # THE WHOLE CYCLE RUNS IN A SUBSHELL so that the traps below are confined to it.
    #
    # A bare `trap ... EXIT` inside a function is SHELL-global: it would still be armed after the
    # function returned, and would then fire in the CALLER's context -- deleting a directory the
    # caller never created, at a moment the caller did not choose. run-matrix.sh and run-fuzz.sh
    # both call this mid-run, so that would be a live foot-gun.
    #
    # The subshell also makes the cleanup cover a path that no explicit call can: all four
    # callers capture this function with $( ), and t04/t05 run under `set -e`, so a failing
    # vm_scp_dir or vm_ssh aborts the cycle at an arbitrary statement. Before, that abort leaked
    # exactly like the bails did. Now EXIT catches it too.
    #
    # THE CONTRACT THIS MUST NOT BREAK: stdout is one verdict token, and a bail exits 0 (t04
    # compares the token, and under `set -e` a non-zero status would abort the test instead of
    # letting it report the mismatch). So every bail is `exit 0`, not `exit 1`, and the success
    # path's status is still verdict_classify's.
    #
    # INT/TERM re-enter EXIT by exiting, so the cleanup itself is written once. This catches a
    # Ctrl-C or a CI job cancellation, which signal the whole process group -- the case that
    # leaves a stray VM on a shared agent. A kill aimed at the parent PID alone would not reach
    # this subshell; that is a known limit, not an oversight.
    (
        keep=keep
        trap 'preflight_cleanup "$run" "$keep"' EXIT
        trap 'exit 130' INT
        trap 'exit 143' TERM

        qemu-img create -f qcow2 -b "$base/golden.qcow2" -F qcow2 "$run/overlay.qcow2" >/dev/null
        truncate -s 8G "$run/data.raw"

        p=$(vm_free_port)
        vm_boot "$run" "$run/overlay.qcow2" "$run/data.raw" "$p"
        vm_wait_ssh "$p" "$key" 240 >&2 || { echo UNPARSEABLE; exit 0; }
        vm_scp_dir "$p" "$key" "$vmdir/guest" /opt/vmcrash/
        vm_ssh "$p" "$key" "sudo sync"
        vm_ssh "$p" "$key" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh" >/dev/null

        # THE WRITE PHASE IS LAUNCHED DETACHED, in its own subshell, so that THIS shell keeps no
        # job-table entry for it.
        #
        # Not a style choice -- measured. The EXIT trap above can run in a forked copy of this
        # shell, and a forked copy inherits the job table without being the job's parent. Bash
        # then prints
        #   lib/preflight.sh: line N: wait_for: No record of process NNNN
        # to stderr, which reads like a fault in the cut on the one path where an operator is
        # already hunting for a cause. Measured over 30 runs of the console bail: plain `&`
        # under the trap produced it 27/30 times, adding `wait`+`disown` still 22/30, and this
        # detached form 0/30 -- matching the pre-trap code, which was also 0/30.
        #
        # The pid still reaches us through the file, so the cut can still kill the writer; it
        # simply is not a job of the shell that carries the trap.
        ( vm_ssh "$p" "$key" "QDB_CUT_DROP_WRITES=$dropw bash /opt/vmcrash/guest/preflight.sh --phase=write" >/dev/null 2>&1 &
          echo $! > "$run/.armpid" )
        armpid=$(cat "$run/.armpid"); rm -f "$run/.armpid"
        vm_wait_console "$run" "CUT-ARMED" 60 >&2 || { kill "$armpid" 2>/dev/null; echo UNPARSEABLE; exit 0; }
        # THE CUT ITSELF, not cleanup: this kill is the power failure under test. The cycle
        # deliberately continues afterwards and reboots the same disks.
        vm_kill "$run"
        # The writer is no longer a child of this shell, so `wait` cannot be used. Poll instead,
        # BOUNDED: the ssh dies with the VM it was talking to, and an unbounded loop here would
        # turn a dead writer into a hung CI job.
        for _ in $(seq 1 200); do kill -0 "$armpid" 2>/dev/null || break; sleep 0.05; done

        p2=$(vm_free_port)
        vm_boot "$run" "$run/overlay.qcow2" "$run/data.raw" "$p2"
        vm_wait_ssh "$p2" "$key" 240 >&2 || { echo UNPARSEABLE; exit 0; }
        vm_ssh "$p2" "$key" "QDB_FS_MOUNT_OPTS='${QDB_FS_MOUNT_OPTS:-}' bash /opt/vmcrash/guest/prepare-device.sh --reattach" >/dev/null
        line=$(vm_ssh "$p2" "$key" "bash /opt/vmcrash/guest/preflight.sh --phase=check")
        # A completed cycle has a verdict, so the console log has nothing left to explain.
        keep=
        verdict_classify "$line"
    )
}
