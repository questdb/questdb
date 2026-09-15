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
#   product    qwp-sf with the SERVER ARTIFACT SWAPPED: the release tarball
#              (questdb-<ver>-no-jre-bin.tar.gz), unpacked in the guest and started by the real
#              questdb.sh launcher, which runs QuestDB as a NAMED JPMS MODULE
#              (-p questdb.jar -m io.questdb/io.questdb.ServerMain). Every other arm runs the
#              engine on the CLASSPATH in the unnamed module, so nothing else here can see a
#              packaging, entry-point or module-configuration regression.
#
# WHY product IS qwp-sf AND NOT ITS OWN CONTRACT. One arm should vary ONE thing. The client,
# the payload, the oracle and the bar are all identical to qwp-sf; only the server binary and
# the way it is launched differ. That is what makes a product-vs-qwp-sf divergence attributable
# to packaging rather than to a different test.
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
        qwp|qwp-sf|product) echo "[Q]wpCrashIngestClient" ;;
        *)                  echo "[C]rashIngestWriter" ;;
    esac
}

# arm_sf_capable MODE -> 0 if the LOCAL durable-ack tier can exist at all in this commit mode.
#
# ONLY ADAPTIVE. This is a property of the PRODUCT, checked in the code rather than assumed:
# WalWriter's commit path guards the whole durable-ack bookkeeping with
#
#     if (commitMode == CommitMode.ADAPTIVE) { ... seqTxnTracker.setLocalDurableSeqTxn(seqTxn); }
#
# so under SYNC the commit IS fdatasync'd but localDurableSeqTxn is never advanced.
# LocalDurableAckRegistry then returns -1 ("the local-fsync tier ... for ADAPTIVE tables", its
# own javadoc), and the server emits no STATUS_LOCAL_DURABLE_ACK frames at all.
#
# WHY THIS EXISTS AS A FUNCTION. Without it the failure arrives 20,000 rows into a run, after a
# full boot and device setup, as the client's "the channel is dead" refusal -- which reaches the
# matrix as `LOUD_FAILURE: workload was not running at cut time`. That names the symptom, blames
# the wrong layer, and costs a VM cycle per occurrence. Found exactly that way by the first full
# run-matrix.sh run; qwp-sf reproduced it identically, which is what showed it was not the
# product arm's doing.
arm_sf_capable() {
    case "$(echo "$1" | tr 'A-Z' 'a-z')" in
        adaptive) return 0 ;;
        *) return 1 ;;
    esac
}

# arm_server_kind ARM -> which SERVER BINARY the arm runs, for callers that must start one.
#
#   classpath  java -cp benchmarks.jar io.questdb.ServerMain   (qwp, qwp-sf)
#   product    dist/questdb.sh start -d ROOT                   (product)
#
# verify.sh needs this as well as run-workload.sh: the store-and-forward replay starts a server
# for the client to reconnect to, and for the product arm that server must be the shipped one
# too. A product run whose RECOVERY happened on a classpath server would report product coverage
# for half the cycle -- the write half -- and quietly test the other artifact for the rest.
arm_server_kind() {
    case "$1" in
        product) echo "product" ;;
        *)       echo "classpath" ;;
    esac
}

# arm_is_known ARM -> 0 if this is an arm the harness can run.
# `product` joined this list when it gained a real implementation. It was deliberately absent
# while it was a stub, because reporting it as known would let a caller start a cell that
# cannot produce a verdict -- which is exactly what run-matrix.sh did for three of its four
# cells (issues/03).
arm_is_known() {
    case "$1" in
        reference|qwp|qwp-sf|product) return 0 ;;
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
        qwp|qwp-sf|product) echo "_qwp_progress" ;;
        *)                  echo "_progress" ;;
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
#
# MODE is the second argument and is NOT optional in spirit: the product arm's oracle depends on
# it. Defaulted to adaptive only so an old call site fails loudly on the flags rather than on an
# unbound variable.
arm_verify_flags() {
    local mode="${2:-adaptive}"
    case "$1" in
        qwp)    echo "--arm=reference --qwp=true --qwp-sf=false" ;;
        qwp-sf) echo "--arm=reference --qwp=true --qwp-sf=true --sf-replay=compare" ;;
        # product differs from qwp-sf by ONE flag, and that flag is the arm: --server=product
        # makes the recovery pass and the replay server the SHIPPED artifact. Drop it and the
        # run silently degrades into a qwp-sf run wearing a product label -- the same false-green
        # shape as a sweep reporting mode=sync while serving nosync.
        #
        # OUTSIDE ADAPTIVE THE SF HALF CANNOT EXIST (see arm_sf_capable), so the arm drops to the
        # plain-qwp contract and SAYS SO through its flags. The artifact claim is untouched --
        # the shipped server still writes, crashes and recovers -- but nothing downstream may
        # report ack-channel coverage that the product cannot provide in this mode. Asking for
        # --qwp-sf=true here would fail the run on the absence of a guarantee never on offer.
        product)
            if arm_sf_capable "$mode"; then
                echo "--arm=reference --qwp=true --qwp-sf=true --sf-replay=compare --server=product"
            else
                echo "--arm=reference --qwp=true --qwp-sf=false --server=product"
            fi
            ;;
        *)      echo "--arm=reference --qwp=false --qwp-sf=false" ;;
    esac
}
