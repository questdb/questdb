#!/usr/bin/env bash
# guest/verify.sh --arm=reference|product --mode=MODE
#
# Runs the arm's oracle and prints EXACTLY ONE verdict line on stdout. Anything
# else is classified UNPARSEABLE upstream and keeps the run's disks.
#
# The reference arm delegates to CrashVerifier, which runs the PRODUCTION
# recovery triple — RecoveryCoordinator.recover() -> notifyWalTxnRepublisher ->
# drainWalQueue — so the thing under test is the real recovery path, not a
# test-only reimplementation of it.
set -euo pipefail

ARM=reference
MODE=adaptive
WINDOW=0
EPOCH=1000
# The verifier runs in its OWN JVM, so every -D the oracle depends on must be
# passed again here. CrashIngestWriter.SIBLING_TABLE is a static read of
# `sibling.table`; without it the verifier reads FALSE and SKIPS the sibling
# check entirely -- the table is written, crashed, and never verified, and the
# run goes green having checked only the primary. Caught exactly that way: the
# sweep passed 6/6 while never once looking at t2.
SIBLING="${QDB_SIBLING_TABLE:-false}"
# Restart the engine under a DIFFERENT global commit mode than the data was
# written under. Empty = same mode. See CrashVerifier's recover.as.
RECOVER_AS="${QDB_RECOVER_AS:-}"
MATVIEW="${QDB_MAT_VIEW:-false}"
REBASE="${QDB_REBASE:-false}"

for a in "$@"; do
    case "$a" in
        --arm=*)  ARM="${a#*=}" ;;
        --mode=*) MODE="${a#*=}" ;;
        --window-us=*) WINDOW="${a#*=}" ;;
        --epoch-ms=*)  EPOCH="${a#*=}" ;;
        --sibling=*)   SIBLING="${a#*=}" ;;
        --recover-as=*) RECOVER_AS="${a#*=}" ;;
        --mat-view=*)  MATVIEW="${a#*=}" ;;
        --rebase=*)    REBASE="${a#*=}" ;;
        *) echo "LOUD_FAILURE: verify.sh unknown argument $a"; exit 0 ;;
    esac
done

JAR=/opt/vmcrash/benchmarks.jar
DB=/mnt/qdb/db

QDB_JVM="--enable-native-access=ALL-UNNAMED \
--sun-misc-unsafe-memory-access=allow \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.time.zone=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"

# Report a missing/truncated jar as what it is. Otherwise it surfaces as a bare
# ClassNotFoundException, which reads like a packaging fault rather than an
# artifact lost to the cut because it was never flushed.
if [ ! -s "$JAR" ]; then
    echo "LOUD_FAILURE: $JAR missing or empty after the cut — shipped artifacts were not durable"
    exit 0
fi

case "$ARM" in
    reference)
        # `|| true` deliberately: CrashVerifier signals its verdict through both
        # stdout AND a non-zero exit code, and we want the LINE, not the code.
        # -Dgroup.window.us AND -Depoch.interval.ms are REQUIRED, not optional.
        # CrashVerifier picks its bar from them: W=0 demands zero loss (F >= C),
        # W>0 applies the RPO bar (F >= Wm, loss confined to the window).
        # Passing only -DcommitMode left them at their defaults, so every W=50ms
        # run was silently graded against the W=0 zero-loss bar -- the RPO
        # contract, which is what actually measures the data-loss gap, was never
        # exercised. It failed SAFE (a stricter bar cannot produce a false pass)
        # but a legitimate RPO-bounded loss would have been reported as a
        # DURABILITY_FAILURE.
        # Capture stdout, stderr and the EXIT CODE separately. A verdict is
        # mandatory: the oracle must never be able to return "nothing", because
        # a silent exit is indistinguishable from a clean run to anything
        # downstream. Observed for real -- at one flush boundary the JVM
        # produced ZERO output and the caller reported UNPARSEABLE with an
        # unrelated stray line picked up by `tail -1`.
        vout=$(mktemp); verr=$(mktemp)
        # `|| rc=$?` is REQUIRED. Under `set -e` a non-zero exit -- which is the
        # NORMAL way this oracle signals a bad verdict, and the way a JVM crash
        # signals 134 -- kills this script before it can emit anything. The
        # first version of this hardening had exactly that bug: at a boundary
        # where the JVM took SIGSEGV, verify.sh died silently and the caller saw
        # no verdict at all, which is the failure mode the hardening existed to
        # remove.
        rc=0
        java $QDB_JVM -cp "$JAR" \
                -DcommitMode="$MODE" \
                -Dgroup.window.us="$WINDOW" \
                -Depoch.interval.ms="$EPOCH" \
                -Dsibling.table="$SIBLING" \
                -Drecover.as="$RECOVER_AS" \
                -Dmat.view="$MATVIEW" -Drebase="$REBASE" \
                org.questdb.CrashVerifier "$DB" >"$vout" 2>"$verr" || rc=$?
        out=$(cat "$vout" "$verr")
        line=$(grep -m1 -hE '^(DURABLE|RPO_OK|DURABILITY_FAILURE|SILENT_CORRUPTION|LOUD_FAILURE|CONSISTENT)' \
            "$vout" "$verr" || true)

        if [ -z "$line" ]; then
            # No verdict. Diagnose WHY rather than emitting a stray line, and
            # always classify it as a real failure: a boundary the oracle cannot
            # evaluate is a finding, not something to skip past.
            why="exit=$rc"
            if [ "$rc" -gt 128 ]; then
                why="$why killed-by-signal-$((rc - 128))"
            fi
            [ -s "$verr" ] && why="$why stderr='$(tail -c 900 "$verr" | tr '\n' ' ')'"
            [ ! -s "$vout" ] && [ ! -s "$verr" ] && why="$why produced-no-output-at-all"
            ls /tmp/hs_err_pid*.log >/dev/null 2>&1 && why="$why jvm-crash-log-present"
            [ -d "$DB" ] || why="$why db-root-absent"
            line="LOUD_FAILURE: verifier produced no verdict ($why)"
        fi
        # Emit the verifier's FULL output, prefixed, BEFORE the verdict. Callers take the
        # verdict with `tail -1` / `grep -m1`, so prefixed detail lines cannot be mistaken
        # for it -- and the evidence stops being discarded here. Two layers of truncation
        # (this function keeping only the verdict, and the sweep keeping only tail -1) are
        # why per-view mat-view results kept vanishing and produced two WRONG conclusions
        # about which dimensions were covered.
        if [ "${QDB_VERIFY_FULL_OUTPUT:-true}" = "true" ]; then
            sed 's/^/DETAIL /' "$vout" 2>/dev/null || true
            [ -s "$verr" ] && sed 's/^/DETAIL-ERR /' "$verr" 2>/dev/null || true
        fi
        rm -f "$vout" "$verr"

        # A cut early enough to land before the table was durably created makes
        # CrashVerifier report "table does not exist", which it classifies as a
        # hard LOUD_FAILURE. With randomised cut times that is a REACHABLE and
        # legitimate outcome: if nothing was ever committed, nothing should exist.
        #
        # The distinction that must NOT be lost: committed rows AND no table is
        # real data loss. So this only reclassifies when the durable watermark
        # says nothing was committed -- otherwise the failure stands.
        case "$line" in
            LOUD_FAILURE*"table does not exist"*)
                # `|| true` is REQUIRED. Under `set -euo pipefail`, a missing
                # _progress makes head fail, pipefail propagates it, the
                # assignment fails and set -e kills this script before it can
                # print anything -- so the verdict comes back EMPTY and is
                # classified UNPARSEABLE. That is precisely the case this branch
                # exists to handle: an early cut, before any commit, has no
                # _progress by definition. Observed at a 1209ms cut.
                committed=$(head -1 "$DB/_progress" 2>/dev/null | tr -dc '0-9' || true)
                if [ -z "$committed" ] || [ "$committed" -eq 0 ] 2>/dev/null; then
                    line="NO_COMMIT cut landed before any commit (watermark=${committed:-absent})"
                else
                    line="DURABILITY_FAILURE table absent but watermark=$committed rows were committed"
                fi
                ;;
        esac
        echo "$line"
        ;;

    product)
        echo "LOUD_FAILURE: product arm oracle is implemented by Task 6b; not yet available"
        ;;

    *)
        echo "LOUD_FAILURE: unknown arm $ARM"
        ;;
esac
