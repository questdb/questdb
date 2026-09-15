# lib/arms.sh — the arm vocabulary, in one place.
# Source this file; do not execute it.
#
# An "arm" here is a branch of the experiment, as in a clinical trial: the same invariant
# reached by a different route into the engine. (Not to be confused with "arm the cut",
# which is the verb -- see guest/arm-cut.sh. The collision is known and deliberate: renaming
# would touch every log line, README table and sweep directory name for no functional gain.)
#
#   reference  the engine embedded in the writer process. Sees C and Wm in-process.
#   qwp        a REAL server and a REAL WebSocket client. The SERVER tracks and recovers.
#   qwp-sf     as qwp, PLUS the client requests the LOCAL durable-ack tier, holds un-acked
#              rows in a store-and-forward buffer on the crashed device, and replays them.
#   product    the SHIPPED artifact and its real entry point. Not implemented yet.
#
# WHY THIS FILE EXISTS. The liveness pattern below was hardcoded in power-cut-vm.sh as
# [C]rashIngestWriter, and separately fixed -- for the qwp arm only -- inside
# run-flush-sweep.sh. The two copies drifted immediately: any qwp run driven through
# power-cut-vm.sh aborted as "workload was not running", because the pattern could never
# match the process that arm actually starts. A guard that fails closed on a healthy run is
# as useless as one that never fires.
#
# The same duplication bit the densify pass, which carried a hand-copied verify.sh
# invocation that never gained the new flags. One definition, sourced by both callers.

# arm_live_pattern ARM -> the pgrep -f pattern that matches THAT arm's workload process.
#
# THE BRACKET IS LOAD-BEARING and must survive any edit. `pgrep -f` matches whole command
# lines, and the ssh command running the check carries the class name in its OWN cmdline --
# so a plain pattern MATCHES ITSELF and reports the workload alive after it has exited.
# `[C]rash...` matches the real process but not the literal text searching for it. This was
# caught by a negative control: a deliberately tiny workload that had long since finished
# still returned DURABLE instead of failing as vacuous.
arm_live_pattern() {
    case "$1" in
        qwp|qwp-sf) echo "[Q]wpCrashIngestClient" ;;
        *)          echo "[C]rashIngestWriter" ;;
    esac
}

# arm_is_known ARM -> 0 if this is an arm the harness can run.
# `product` is deliberately absent: it parses but is not implemented, and reporting it as
# known here would let a caller start a cell that cannot produce a verdict.
arm_is_known() {
    case "$1" in
        reference|qwp|qwp-sf) return 0 ;;
        *) return 1 ;;
    esac
}

# arm_progress_file ARM -> the watermark file THAT arm's workload writes, relative to the DB root.
#
# The reference arm's writer owns the engine and writes _progress. The qwp arms run the client
# as a SEPARATE PROCESS from the server, so it writes its own _qwp_progress carrying the same
# C / Wm pair read back from the server's wal_tables().
#
# Both callers anchored on _progress unconditionally, and the two failed differently, which is
# why only one was noticed:
#   * power-cut-vm.sh BAILED -- "workload never reached its first commit" -- so no qwp cell
#     could run through it at all.
#   * run-flush-sweep.sh spun its 120-iteration anchor loop to exhaustion (~24s wasted on every
#     qwp run) and then carried on regardless, so the fault was invisible.
# A guard that silently gives up is worse than one that fails, because the run still looks fine.
arm_progress_file() {
    case "$1" in
        qwp|qwp-sf) echo "_qwp_progress" ;;
        *)          echo "_progress" ;;
    esac
}

# arm_verify_flags ARM -> the flags guest/verify.sh needs for that arm.
#
# verify.sh's --arm names which ORACLE to run, not which workload produced the data, and the
# only implemented oracle is `reference`. The qwp arms are expressed as that oracle plus
# modifiers (--qwp / --qwp-sf), which is the convention run-flush-sweep.sh already used.
#
# power-cut-vm.sh instead forwarded its WORKLOAD arm straight through, so `--arm=qwp` reached
# verify.sh's case statement and fell to `LOUD_FAILURE: unknown arm qwp` -- after a full
# record, cut and reboot cycle. Third arm-specific divergence between these two callers, and
# the third to be found only by actually running the path rather than reading it.
arm_verify_flags() {
    case "$1" in
        qwp)    echo "--arm=reference --qwp=true --qwp-sf=false" ;;
        qwp-sf) echo "--arm=reference --qwp=true --qwp-sf=true --sf-replay=compare" ;;
        *)      echo "--arm=reference --qwp=false --qwp-sf=false" ;;
    esac
}
