#!/usr/bin/env bash
# t11 — the preflight cycle leaves no VM and no disks behind, on EVERY exit path.
#
# NO VM, NO QEMU, NO ROOT: every vm_* primitive and qemu-img is stubbed, so each bail path can
# be driven on demand. Seconds, not minutes. That is the only reason these paths are testable
# at all -- a real first-boot SSH timeout costs 240 s of waiting for a VM that will never answer.
#
# WHY THIS TEST EXISTS. run_preflight_cycle had three bail paths. Two returned without killing
# the VM that vm_boot had DAEMONIZED, and none of the three removed the 8 GiB run directory.
# The leak is not self-correcting:
#   * a stray QEMU breaks the harness's one-VM-at-a-time rule and corrupts the next measurement;
#   * reap-state.sh refuses any directory whose qemu.pid is alive, so the 8 GiB is invisible to
#     the reaper forever;
#   * it is finally noticed at check-host.sh's 200 GB gate, looking like an infrastructure fault.
# The success path always cleaned up, so every normal run hid the defect. Only a failing run
# leaked -- which on a CI agent is exactly the run that repeats.
#
# The contract this must also pin down: stdout stays ONE verdict token and a bail still exits 0.
# t04 captures this function with $( ) under `set -e`; a non-zero bail would abort t04 instead
# of letting it report the mismatch it exists to report.
#
# EACH SCENARIO RUNS IN A PRISTINE CHILD SHELL (this file re-executes itself with
# --child-cycle). Not ceremony: an EXIT trap in the calling shell changes how bash forks around
# the cycle's own EXIT trap, and the stderr-noise assertion below is silently unable to fail
# when the caller already holds one -- which this test does, for its own temp directory. A
# control that cannot fail is not a control, so the cycle is driven the way t04 drives it:
# from a shell that has no EXIT trap of its own.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/verdict.sh
source "$HERE/../lib/verdict.sh"
# shellcheck source=../lib/preflight.sh
source "$HERE/../lib/preflight.sh"

# ---- the stubs -------------------------------------------------------------------------
# They live HERE and never in lib/: a harness that ships its own test doubles can be green
# against the doubles and broken against QEMU.
#
# Calls are appended to $CALLS rather than counted in a variable, because the cycle body runs
# in a subshell and $( ) nests further subshells -- a counter variable would silently lose
# increments and the assertions would be measuring nothing. The file also crosses the
# parent/child boundary, which a variable could not.
# The overlay is the LAST argument of `qemu-img create -f qcow2 -b BASE -F qcow2 OUT`. Taking
# $2 instead creates a file literally named "-f" in the caller's working directory, which is
# how this stub first announced itself -- as an untracked file in the repo.
qemu-img() { echo "qemu-img $*" >> "$CALLS"; : > "${*: -1}"; }   # no-op image create
vm_free_port() { echo 2222; }
vm_scp_dir() { echo "vm_scp_dir" >> "$CALLS"; }

# vm_boot mirrors the two artifacts the real one leaves: a pidfile (which is what makes a leak
# a LEAK -- the reaper keys on it) and a console log (the only evidence a failed boot produces).
vm_boot() {
    echo "vm_boot $1" >> "$CALLS"
    echo "424242" > "$1/qemu.pid"
    echo "guest console output" > "$1/console.log"
}

# vm_kill mirrors the real one's observable effect: the pidfile goes away.
vm_kill() {
    echo "vm_kill $1" >> "$CALLS"
    rm -f "$1/qemu.pid"
}

vm_wait_ssh() {
    echo "vm_wait_ssh" >> "$CALLS"
    local n
    n=$(grep -c '^vm_wait_ssh$' "$CALLS")
    case "${FAIL_AT:-}:$n" in
        boot1:1) return 1 ;;
        boot2:2) return 1 ;;
    esac
    return 0
}

vm_wait_console() {
    echo "vm_wait_console" >> "$CALLS"
    [ "${FAIL_AT:-}" = console ] && return 1
    return 0
}

vm_ssh() {
    echo "vm_ssh" >> "$CALLS"
    case "$*" in
        # The write phase is BACKGROUNDED by the cycle and is expected to still be running when
        # the cut kills it. A stub that returned instantly would be reaped before the cut, which
        # is not the shape the bail has to survive.
        *--phase=write*) sleep 0.3 ;;
        *--phase=check*) echo "PREFLIGHT_OK kept=1 lost=1 ranged=1" ;;
    esac
}

# ---- child mode: one cycle, in a shell with no EXIT trap of its own ---------------------
if [ "${1:-}" = "--child-cycle" ]; then
    run_preflight_cycle real "$HERE/.."
    exit $?
fi

fails=0
ok()    { printf '  ok   %s\n' "$1"; }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

# run_cycle FAIL_AT -> sets $out, $rc, $RUN, $CALLS, $ERR for the assertions that follow.
# RUN is discovered by glob rather than computed, because the child's $$ names it -- and a
# success path that removed the directory correctly leaves the glob empty, which is exactly
# what the success assertion wants to see.
run_cycle() {
    local state="$TMP/state-${1:-ok}"
    mkdir -p "$state/base"
    CALLS="$state/calls.log"; : > "$CALLS"
    ERR="$state/stderr"
    out=$(FAIL_AT="$1" CALLS="$CALLS" QDB_VMCRASH_STATE="$state" \
          bash "$HERE/$(basename "${BASH_SOURCE[0]}")" --child-cycle 2>"$ERR"); rc=$?
    RUN=$(find "$state" -maxdepth 1 -name 'preflight-real-*' | head -1)
}

echo "t11 — preflight cleanup on every exit path"

# ---- 1. first-boot SSH timeout ----------------------------------------------------------
# The worst of the three: vm_boot has already daemonized QEMU, and the original code returned
# without touching it.
run_cycle boot1
check "first-boot timeout: verdict is still UNPARSEABLE" "$out" "UNPARSEABLE"
check "first-boot timeout: still exits 0 (t04 must see the token, not an abort)" "$rc" "0"
grep -q '^vm_kill ' "$CALLS" && ok "first-boot timeout: the VM was killed" \
    || bad "first-boot timeout: LEAKED A DAEMONIZED QEMU (no vm_kill)"
[ -n "$RUN" ] && [ -f "$RUN/qemu.pid" ] \
    && bad "first-boot timeout: qemu.pid survives — the reaper will refuse this dir forever" \
    || ok "first-boot timeout: no live pidfile left behind"
[ -n "$RUN" ] && { [ -f "$RUN/data.raw" ] || [ -f "$RUN/overlay.qcow2" ]; } \
    && bad "first-boot timeout: the 8 GiB disks were kept" \
    || ok "first-boot timeout: the disks are gone"

# ---- 2. the bail keeps the cheap evidence ----------------------------------------------
# Deliberately NOT symmetric with the disks. A guest that never reached SSH says why on its
# serial console, and discarding that is the fault power-cut-vm.sh had on its liveness bail --
# it cost three VM cycles per diagnosis instead of one.
[ -n "$RUN" ] && [ -f "$RUN/console.log" ] && ok "first-boot timeout: console.log survives as evidence" \
    || bad "first-boot timeout: console.log discarded — the only record of why the boot failed"

# ---- 3. console-wait timeout ------------------------------------------------------------
run_cycle console
check "console timeout: verdict is still UNPARSEABLE" "$out" "UNPARSEABLE"
check "console timeout: still exits 0" "$rc" "0"
grep -q '^vm_kill ' "$CALLS" && ok "console timeout: the VM was killed" \
    || bad "console timeout: LEAKED A DAEMONIZED QEMU"
[ -n "$RUN" ] && { [ -f "$RUN/data.raw" ] || [ -f "$RUN/overlay.qcow2" ]; } \
    && bad "console timeout: the 8 GiB disks were kept" \
    || ok "console timeout: the disks are gone"
[ -n "$RUN" ] && [ -f "$RUN/console.log" ] && ok "console timeout: console.log survives as evidence" \
    || bad "console timeout: console.log discarded"

# The cut backgrounds the write phase. If that job stays in the job table of the shell carrying
# the EXIT trap, a forked copy of that shell inherits the entry without being the job's parent,
# and bash prints "wait_for: No record of process N" to stderr. On a CI agent that line reads
# like the cut itself misfired, on the one path where an operator is already hunting for a cause.
#
# SAMPLED, because the underlying race is probabilistic: a single cycle catches a regression to
# a plain `&` only ~8 times in 10, while the detached launch measured 0/30. Four cycles put
# that miss rate below 1 in 1000, and each one costs well under a second.
noisy=0
for _ in 1 2 3 4; do
    run_cycle console
    grep -q 'wait_for\|No record of process' "$ERR" && noisy=$((noisy + 1))
done
check "console timeout: the bail is silent over 4 cycles (no job-control noise)" "$noisy" "0"

# ---- 4. second-boot SSH timeout ---------------------------------------------------------
# The reboot after the cut. Same leak as case 1, and the one most likely in practice: the disks
# have just been cut mid-write, so this is where a guest legitimately fails to come back.
run_cycle boot2
check "second-boot timeout: verdict is still UNPARSEABLE" "$out" "UNPARSEABLE"
check "second-boot timeout: still exits 0" "$rc" "0"
[ -n "$RUN" ] && [ -f "$RUN/qemu.pid" ] \
    && bad "second-boot timeout: qemu.pid survives — invisible to the reaper" \
    || ok "second-boot timeout: no live pidfile left behind"
[ -n "$RUN" ] && { [ -f "$RUN/data.raw" ] || [ -f "$RUN/overlay.qcow2" ]; } \
    && bad "second-boot timeout: the 8 GiB disks were kept" \
    || ok "second-boot timeout: the disks are gone"

# ---- 5. the success path is UNCHANGED ---------------------------------------------------
# The regression that would matter most: the fix must not alter the verdict contract, and a
# completed cycle has a verdict, so it leaves nothing behind at all.
run_cycle ""
check "success: the verdict token is echoed" "$out" "PREFLIGHT_OK"
check "success: exit status is the verdict's" "$rc" "0"
[ -n "$RUN" ] && bad "success: the run directory survived a clean cycle" \
    || ok "success: the run directory is removed entirely"

# ---- 6. the cut still happens ------------------------------------------------------------
# vm_kill appears on the success path for TWO different reasons -- the power cut under test,
# and the cleanup. If a refactor ever collapsed them, the cycle would stop cutting and every
# downstream verdict would be vacuous. Two boots and at least two kills is the shape.
n_boot=$(grep -c '^vm_boot ' "$CALLS")
n_kill=$(grep -c '^vm_kill ' "$CALLS")
check "success: the cycle booted twice (cut, then reboot)" "$n_boot" "2"
[ "$n_kill" -ge 2 ] && ok "success: the cut kill and the cleanup kill both happened" \
    || bad "success: only $n_kill vm_kill call(s) — the power cut may have been lost"

# ---- 7. the trap does not escape into the caller ----------------------------------------
# A bare `trap ... EXIT` inside a function is shell-GLOBAL. If it leaked, it would fire later in
# run-matrix.sh's own context and delete a directory that run was still using.
# Probed directly: clear any inherited EXIT trap, run a cycle, then ask the shell what EXIT
# trap it is now holding. Anything at all means the cycle armed one in its caller.
CALLS="$TMP/canary-calls"; : > "$CALLS"
leaked=$(
    trap - EXIT
    export CALLS
    QDB_VMCRASH_STATE="$TMP/canary-state"; mkdir -p "$QDB_VMCRASH_STATE/base"
    run_preflight_cycle real "$HERE/.." >/dev/null 2>&1
    trap -p EXIT
)
[ -z "$leaked" ] && ok "no EXIT trap armed in the caller after the cycle" \
    || bad "the cycle armed an EXIT trap in its CALLER: $leaked"

echo
if [ "$fails" -eq 0 ]; then
    echo "t11 PASSED"
    exit 0
fi
echo "t11 FAILED: $fails assertion(s)"
exit 1
