#!/usr/bin/env bash
# guest/verify.sh --arm=reference --mode=MODE [--server=classpath|product]
#
# Runs the arm's oracle and prints EXACTLY ONE verdict line on stdout. Anything
# else is classified UNPARSEABLE upstream and keeps the run's disks.
#
# --arm names the ORACLE, not the workload that produced the data, and `reference`
# is the only oracle there is. Every other arm is expressed as that oracle plus
# modifiers -- --qwp / --qwp-sf / --server -- which is the convention lib/arms.sh
# encodes in arm_verify_flags(). Forwarding a WORKLOAD arm straight through to
# --arm was one of the three bugs in issues/04, and it surfaced only after a full
# record, cut and reboot cycle.
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
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"
QWP="${QDB_QWP:-false}"
# The qwp-sf arm's oracle: identity/contiguity over DISTINCT ids, duplicates counted rather than
# fatal (store-and-forward replay is at-least-once), plus a hard bar that the durable-ack channel
# was actually live. Separate from --qwp on purpose: qwp-sf sets BOTH, because everything the
# server-side oracle checks still applies.
QWPSF="${QDB_QWP_SF:-false}"
REBASE="${QDB_REBASE:-false}"
SFREPLAY="${QDB_SF_REPLAY:-false}"
# WHICH SERVER BINARY runs during verification: `classpath` (benchmarks.jar, every other arm) or
# `product` (the shipped tarball through questdb.sh). The oracle itself is unchanged either way --
# CrashVerifier is the INSTRUMENT and must stay identical across arms, or verdicts stop being
# comparable. What this switches is the SERVER that performs crash recovery and serves the
# store-and-forward replay, which for the product arm must be the shipped artifact.
SERVER="${QDB_VERIFY_SERVER:-classpath}"

for a in "$@"; do
    case "$a" in
        --arm=*)  ARM="${a#*=}" ;;
        --mode=*) MODE="${a#*=}" ;;
        --window-us=*) WINDOW="${a#*=}" ;;
        --epoch-ms=*)  EPOCH="${a#*=}" ;;
        --sibling=*)   SIBLING="${a#*=}" ;;
        --recover-as=*) RECOVER_AS="${a#*=}" ;;
        --mat-view=*)  MATVIEW="${a#*=}" ;;
        --profile=*)   PROFILE="${a#*=}" ;;
        --qwp=*)       QWP="${a#*=}" ;;
        --qwp-sf=*)    QWPSF="${a#*=}" ;;
        --rebase=*)    REBASE="${a#*=}" ;;
        --sf-replay=*) SFREPLAY="${a#*=}" ;;
        --server=*)    SERVER="${a#*=}" ;;
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

case "$SERVER" in
    classpath) ;;
    product)
        # shellcheck source=product-dist.sh
        source /opt/vmcrash/guest/product-dist.sh
        product_dist_unpack || { echo "LOUD_FAILURE: product distribution unusable in the guest"; exit 0; }
        ;;
    *) echo "LOUD_FAILURE: verify.sh unknown --server=$SERVER"; exit 0 ;;
esac

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
        # STORE-AND-FORWARD REPLAY. The server's RPO window may legitimately have discarded txns
        # above Wm; the client held them in a DURABLE sf buffer that took the same power cut. Start
        # the server and let a restarted client replay before judging, or the oracle measures the
        # server's exposure alone and never tests the pairing that actually closes the gap.
        # PASS A -- WHAT THE SERVER ALONE KEPT, before the client is allowed to replay.
        #
        # Without this the arm cannot support its own claim. Measuring only AFTER the replay
        # gives `lost=0`, and `lost=0` has TWO causes that look identical:
        #   * the server discarded at-risk txns and the client put them back  (the mechanism)
        #   * the server never lost anything at this boundary                 (nothing happened)
        # run-sf-replay.sh documents exactly this trap and solves it the same way: verify the
        # SAME boundary twice and report the delta.
        #
        # The ORDER IS THE REAL SEQUENCE, not a trick: the server recovers first, then the client
        # reconnects. Pass A runs the production recovery and measures the server's own result;
        # the replay then happens on top of that, exactly as it would in a deployment.
        # Results come from a FILE, never from the shared stdout. See CrashVerifier's
        # RESULT_FILE javadoc: the engine logs to the same stream and a spliced line yielded
        # `distinctIds=2026` (a YEAR) and a false DURABILITY_FAILURE. Reading key=value from a
        # file removes the entire class of fault rather than hardening one regex against it.
        read_result() {  # FILE KEY -> value, or -1
            local v
            v=$(grep -oE "^$2=-?[0-9]+$" "$1" 2>/dev/null | head -1 | cut -d= -f2)
            echo "${v:--1}"
        }

        # ---- PRODUCT RECOVERY PASS ---------------------------------------------------------
        # The product arm claims "the SHIPPED artifact recovers". Without this pass it would not:
        # CrashVerifier opens the crashed database first and runs the production recovery triple
        # itself, so by the time the shipped server started for the replay there would be nothing
        # left to recover, and the arm would prove only that the shipped server WROTE the data.
        #
        # So let the shipped artifact open the crashed root FIRST, exactly as an operator
        # restarting a machine would, and let ITS recovery be the one under test. Everything after
        # this point measures the state the shipped server left behind.
        #
        # Cost is one extra server start/stop per boundary. QDB_PRODUCT_RECOVERY_PASS=false turns
        # it off, at the price of the arm's central claim -- so it is reported in the output either
        # way, never silently skipped.
        if [ "$SERVER" = product ]; then
            if [ "${QDB_PRODUCT_RECOVERY_PASS:-true}" != "true" ]; then
                echo "DETAIL PRODUCT_RECOVERY skipped (QDB_PRODUCT_RECOVERY_PASS=false) -- recovery was NOT performed by the shipped artifact"
            else
                rm -f /mnt/qdb/product-recovery.log
                if ! product_server_start "$(dirname "$DB")" "$MODE" "$WINDOW" "$EPOCH" /mnt/qdb/product-recovery.log; then
                    echo "DETAIL PRODUCT_RECOVERY server log tail:"; tail -20 /mnt/qdb/product-recovery.log 2>/dev/null | sed 's/^/DETAIL /'
                    echo "LOUD_FAILURE: the shipped server did not start on the crashed database"
                    exit 0
                fi
                if ! product_server_assert /mnt/qdb/product-recovery.log; then
                    product_server_stop || true
                    echo "LOUD_FAILURE: product premise assertion failed at verification time -- the server that recovered was not the shipped artifact"
                    exit 0
                fi
                # The PASSING case leaves evidence too, in the per-boundary output the sweep
                # archives. Otherwise the only trace of the arm's central premise lives in a
                # guest-side log that dies with the VM, and a reader of the results has to take
                # "it was the shipped server" on trust.
                echo "DETAIL PRODUCT_PREMISE $PRODUCT_PREMISE"
                # WAIT FOR THE WAL TO DRAIN. Answering SQL is not the same as having finished
                # recovery: the apply job runs behind the HTTP endpoint, so reading the row count
                # immediately would measure a recovery in progress and under-report it. Poll until
                # the count stops moving. `|| true` on every pipeline: under `set -euo pipefail` a
                # query against a table that does not exist yet -- a legitimate early boundary --
                # would otherwise kill this script before it could emit a verdict.
                _prev=-1; _stable=0; _rows=-1
                for _ in $(seq 1 120); do
                    _rows=$(curl -s -G http://localhost:9000/exec --data-urlencode "query=select count() from t" 2>/dev/null \
                            | grep -oE '\[\[[0-9]+\]\]' | grep -oE '[0-9]+' | head -1 || true)
                    : "${_rows:=-1}"
                    if [ "$_rows" = "$_prev" ]; then
                        _stable=$((_stable + 1))
                        [ "$_stable" -ge 3 ] && break
                    else
                        _stable=0
                    fi
                    _prev="$_rows"
                    sleep 0.5
                done
                echo "DETAIL PRODUCT_RECOVERY dist=$(product_dist_version) shippedServerRecoveredRows=$_rows"
                # The database must be RELEASED before the oracle opens it; a held lock is what
                # left 2 of 7 boundaries with no verdict at all in the first compare run.
                if ! product_server_stop; then
                    echo "LOUD_FAILURE: the shipped server would not stop after the recovery pass"
                    exit 0
                fi
            fi
        fi

        sfa_distinct=-1; sfa_f=-1; sfa_c=-1
        if [ "$SFREPLAY" = "compare" ]; then
            pares="/mnt/qdb/verify-armA.properties"; rm -f "$pares"
            java $QDB_JVM -cp "$JAR" \
                -DcommitMode="$MODE" -Dgroup.window.us="$WINDOW" -Depoch.interval.ms="$EPOCH" \
                -Dsibling.table="$SIBLING" -Drecover.as="$RECOVER_AS" \
                -Dmat.view="$MATVIEW" -Drebase="$REBASE" \
                -Dschema.profile="$PROFILE" -Dqwp="$QWP" -Dqwp.sf="$QWPSF" \
                -Dresult.file="$pares" \
                org.questdb.CrashVerifier "$DB" >/mnt/qdb/verify-armA.log 2>&1 || true
            sfa_distinct=$(read_result "$pares" distinctIds)
            sfa_f=$(read_result "$pares" F)
            sfa_c=$(read_result "$pares" C)
            echo "DETAIL SF_ARM_A serverAlone distinctIds=$sfa_distinct F=$sfa_f C=$sfa_c"
            # PASS A OPENED THE DATABASE. The replay server below opens the SAME root, and a
            # still-held lock makes it fail to start -- which is the likeliest cause of the
            # boundaries that produced no verdict at all (2 of 7 in the first compare run).
            # Wait for the JVM to be gone rather than assuming the shell's return implies it.
            for _ in $(seq 1 40); do
                pgrep -f '[C]rashVerifier' >/dev/null 2>&1 || break
                sleep 0.25
            done
        fi

        if [ "$SFREPLAY" = "true" ] || [ "$SFREPLAY" = "compare" ]; then
            # THE REPLAY SERVER IS THE ARM'S SERVER, not a convenient one. For the product arm the
            # client must reconnect to the shipped artifact, or the half of the cycle that accepts
            # the replayed rows would be tested on the other binary -- product coverage for the
            # write path and classpath coverage for the recovery path, reported as one verdict.
            if [ "$SERVER" = product ]; then
                if ! product_server_start "$(dirname "$DB")" "$MODE" "$WINDOW" "$EPOCH" /mnt/qdb/replay-server.log; then
                    echo "LOUD_FAILURE: the shipped server did not start for the store-and-forward replay"
                    exit 0
                fi
            else
                setsid env QDB_CAIRO_COMMIT_MODE="$MODE" \
                    QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW="${WINDOW}us" \
                    java $QDB_JVM -cp "$JAR" io.questdb.ServerMain -d "$(dirname "$DB")" \
                    </dev/null >/mnt/qdb/replay-server.log 2>&1 &
                for _ in $(seq 1 120); do
                    curl -s "http://localhost:9000/exec?query=select+1" >/dev/null 2>&1 && break
                    sleep 0.5
                done
            fi
            java $QDB_JVM -cp "$JAR" \
                -Dqwp.addr=localhost:9000 \
                -Dqwp.replay.only=true \
                -Dqwp.durable.ack="${QDB_QWP_DURABLE_ACK:-local}" \
                -Dqwp.sf.dir="${QDB_QWP_SF_DIR:-/mnt/qdb/sf}" \
                -Dqwp.sf.durability="${QDB_QWP_SF_DURABILITY:-periodic}" \
                org.questdb.QwpCrashIngestClient "$DB" >/mnt/qdb/replay-client.log 2>&1 || true
            # Stop the server so the verifier opens the database itself, exactly as it does for
            # every other arm -- a live writer would otherwise hold locks the verifier needs.
            # The product server is stopped THE SHIPPED WAY (questdb.sh stop: SIGTERM, poll,
            # escalate), which also waits for the process to be gone rather than sleeping and
            # hoping. The classpath branch keeps the pattern that matches its own cmdline.
            if [ "$SERVER" = product ]; then
                product_server_stop || {
                    echo "LOUD_FAILURE: the shipped server would not stop after the store-and-forward replay"
                    exit 0
                }
            else
                pkill -f "[S]erverMain -d" 2>/dev/null || true
                sleep 3
            fi
        fi

        vout=$(mktemp); verr=$(mktemp)
        # `|| rc=$?` is REQUIRED. Under `set -e` a non-zero exit -- which is the
        # NORMAL way this oracle signals a bad verdict, and the way a JVM crash
        # signals 134 -- kills this script before it can emit anything. The
        # first version of this hardening had exactly that bug: at a boundary
        # where the JVM took SIGSEGV, verify.sh died silently and the caller saw
        # no verdict at all, which is the failure mode the hardening existed to
        # remove.
        rc=0
        vbres="/mnt/qdb/verify-armB.properties"; rm -f "$vbres"
        java $QDB_JVM -cp "$JAR" \
                -Dresult.file="$vbres" \
                -DcommitMode="$MODE" \
                -Dgroup.window.us="$WINDOW" \
                -Depoch.interval.ms="$EPOCH" \
                -Dsibling.table="$SIBLING" \
                -Drecover.as="$RECOVER_AS" \
                -Dmat.view="$MATVIEW" -Drebase="$REBASE" \
                -Dschema.profile="$PROFILE" -Dqwp="$QWP" -Dqwp.sf="$QWPSF" \
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
        # PASS B vs PASS A. The delta is what the CLIENT put back, and it is the only number
        # that distinguishes the mechanism working from nothing having happened.
        if [ "$SFREPLAY" = "compare" ] && [ "${sfa_distinct:--1}" -ge 0 ] 2>/dev/null; then
            sfb_distinct=$(read_result "$vbres" distinctIds)
            if [ "$sfb_distinct" -ge 0 ] 2>/dev/null; then
                delta=$(( sfb_distinct - sfa_distinct ))
                # SANITY GATE, and it fails LOUD rather than safe. Pass B verifies the same data
                # as pass A plus whatever the client replayed, so distinct ids can only go UP.
                # A negative delta is therefore physically impossible and means the numbers were
                # mis-parsed. Reporting a durability verdict from unparsed numbers is how this
                # produced a false DURABILITY_FAILURE with delta=-1230974; a boundary the oracle
                # cannot evaluate is a finding about the harness, not a pass and not a defect.
                if [ "$delta" -lt 0 ]; then
                    echo "DETAIL SF_INDETERMINATE armA=$sfa_distinct armB=$sfb_distinct delta=$delta"
                    line="LOUD_FAILURE qwp-sf: impossible negative replay delta ($delta) — the oracle's own numbers did not parse, so this boundary was not evaluated"
                    sfb_distinct=-1
                fi
            fi
            if [ "$sfb_distinct" -ge 0 ] 2>/dev/null; then
                echo "DETAIL SF_ARM_B afterReplay distinctIds=$sfb_distinct delta=$delta"
                # AT RISK is measured against what the CLIENT SENT, not against the server's own
                # committed frontier. `F == C` only says the server kept what it had COMMITTED; it
                # is silent about rows the client sent that the server never committed at all --
                # exactly the set store-and-forward protects. Judging on F vs C printed
                # "server lost nothing" beside a measured replay of 532,000 rows.
                sent=$(grep -oE '^sent=[0-9]+' "$DB/_qwp_progress" 2>/dev/null | head -1 | cut -d= -f2)
                : "${sent:=-1}"
                if [ "$sent" -lt 0 ] 2>/dev/null; then
                    echo "DETAIL SF_INDETERMINATE no sent= in _qwp_progress; cannot say what was at risk"
                elif [ "$sfa_distinct" -lt "$sent" ] 2>/dev/null; then
                    # The server alone held LESS than the client had sent. This is the boundary
                    # that actually tests the pairing, so the client must have put something back.
                    atrisk=$(( sent - sfa_distinct ))
                    shortfall=$(( sent - sfb_distinct ))
                    if [ "$delta" -gt 0 ]; then
                        echo "DETAIL SF_REPLAY_PROVEN sent=$sent serverAlone=$sfa_distinct atRisk=$atrisk"
                        echo "DETAIL SF_REPLAY_PROVEN client replayed $delta rows; endToEndShortfall=$shortfall"
                        # A residual shortfall is EXPECTED, not a failure: sf_durability=periodic
                        # means the client's own buffer is durable only to its last sync, so rows
                        # sent after it died with the client's disk. Reported so the size of that
                        # window is visible rather than assumed to be zero.
                    else
                        # THE FAILURE THIS ARM EXISTS TO CATCH: rows the client sent were missing
                        # from the server, and the client put NONE of them back. Overrides pass B's
                        # verdict, which would otherwise read as a pass.
                        line="DURABILITY_FAILURE qwp-sf: $atrisk rows the client sent were absent after recovery (sent=$sent serverAlone=$sfa_distinct) and the client replayed NOTHING — store-and-forward did not close the gap"
                    fi
                else
                    # Legitimate: the server already held everything the client had sent, so the
                    # replay had nothing to do. Reported, never counted as proof of the mechanism.
                    echo "DETAIL SF_NOT_DEMONSTRATED server alone already held everything sent (sent=$sent serverAlone=$sfa_distinct); replay had nothing to recover"
                fi
            fi
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
        # There is no separate product ORACLE, and there should not be: the arm varies the
        # ARTIFACT, and a measurement that changes with the thing being measured cannot compare
        # them. The product arm runs this same reference oracle with --server=product, which is
        # what lib/arms.sh emits. Reaching this branch means a caller sent the workload arm here.
        echo "LOUD_FAILURE: --arm=product is not an oracle; the product arm uses --arm=reference --server=product (see lib/arms.sh arm_verify_flags)"
        ;;

    *)
        echo "LOUD_FAILURE: unknown arm $ARM"
        ;;
esac
