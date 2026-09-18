#!/usr/bin/env bash
# guest/verify.sh --arm=reference --mode=MODE [--server=classpath|product]
#
# Runs the arm's oracle and prints exactly one verdict line on stdout. Anything else is
# classified UNPARSEABLE upstream and keeps the run's disks.
#
# --arm names the oracle, not the workload that produced the data, and `reference` is the only
# oracle there is. Every other arm is that oracle plus modifiers (--qwp / --qwp-sf / --server),
# which is what lib/arms.sh emits.
#
# The reference oracle delegates to CrashVerifier, which runs the production recovery triple
# (RecoveryCoordinator.recover() -> notifyWalTxnRepublisher -> drainWalQueue), so the path under
# test is the real one.
set -euo pipefail

ARM=reference
MODE=adaptive
WINDOW=0
EPOCH=1000
# The verifier runs in its own JVM, so every -D the oracle depends on must be passed again here.
# CrashIngestWriter.SIBLING_TABLE reads `sibling.table` statically; omit it and the verifier
# skips the sibling table entirely while still reporting a pass.
SIBLING="${QDB_SIBLING_TABLE:-false}"
# Restart the engine under a different global commit mode than the data was written under.
# Empty means the same mode. See CrashVerifier's recover.as.
RECOVER_AS="${QDB_RECOVER_AS:-}"
MATVIEW="${QDB_MAT_VIEW:-false}"
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"
QWP="${QDB_QWP:-false}"
# The qwp-sf oracle: identity and contiguity over distinct ids, duplicates counted rather than
# fatal because store-and-forward replay is at-least-once, plus a bar that the durable-ack
# channel was live. qwp-sf sets both this and --qwp, since the server-side checks still apply.
QWPSF="${QDB_QWP_SF:-false}"
REBASE="${QDB_REBASE:-false}"
SFREPLAY="${QDB_SF_REPLAY:-false}"
# Which server binary runs during verification: `classpath` (benchmarks.jar) or `product` (the
# shipped tarball through questdb.sh). CrashVerifier itself is unchanged either way, or verdicts
# stop being comparable across arms; this switches only the server that performs recovery and
# serves the store-and-forward replay.
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
        # NOT_EVALUATED, not LOUD_FAILURE: an unknown flag means the harness called its own
        # oracle wrongly, so nothing was measured and this says nothing about the product.
        *) echo "NOT_EVALUATED: verify.sh unknown argument $a"; exit 0 ;;
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

# Name a missing or truncated jar for what it is; otherwise it surfaces as a bare
# ClassNotFoundException, which reads as a packaging fault rather than an artifact the cut lost.
if [ ! -s "$JAR" ]; then
    echo "LOUD_FAILURE: $JAR missing or empty after the cut — shipped artifacts were not durable"
    exit 0
fi

case "$SERVER" in
    classpath) ;;
    product)
        # shellcheck source=product-dist.sh
        source /opt/vmcrash/guest/product-dist.sh
        # NOT_EVALUATED rather than a product finding, because the tarball never sat on the
        # crashed data device: it is staged on the guest's boot overlay and re-shipped to the
        # replay VM after the reboot, so every unpack failure is a transfer or staging fault of
        # the rig's. A genuinely broken release assembly stays covered: it fails identically at
        # every boundary, and the premise assertion at server start convicts the artifact itself.
        product_dist_unpack || { echo "NOT_EVALUATED: product distribution unusable in the guest"; exit 0; }
        ;;
    # Harness misuse again: lib/arms.sh chooses this value, never a user.
    *) echo "NOT_EVALUATED: verify.sh unknown --server=$SERVER"; exit 0 ;;
esac

case "$ARM" in
    reference)
        # -Dgroup.window.us and -Depoch.interval.ms pick CrashVerifier's bar: W=0 demands zero
        # loss (F >= C), W>0 applies the RPO bar (F >= Wm, loss confined to the window). Omit
        # them and a W>0 run is graded against the stricter W=0 bar, which turns a legitimate
        # RPO-bounded loss into a DURABILITY_FAILURE.
        #
        # Store-and-forward replay runs two passes over the same boundary. The server's RPO
        # window may legitimately discard txns above Wm while the client still holds them in its
        # sf buffer, so pass A measures what the server alone kept and pass B measures the state
        # after a restarted client replays. The delta between them is the only number that
        # separates "the client put the rows back" from "the server never lost any". The order is
        # the deployment order: the server recovers first, the client reconnects afterwards.
        #
        # Results come from a file, never from the shared stdout: the engine logs to the same
        # stream, and a spliced line yields a garbage count and a false verdict.
        read_result() {  # FILE KEY -> value, or -1
            local v
            v=$(grep -oE "^$2=-?[0-9]+$" "$1" 2>/dev/null | head -1 | cut -d= -f2)
            echo "${v:--1}"
        }

        # ---- PRODUCT RECOVERY PASS ---------------------------------------------------------
        # The shipped artifact must open the crashed root first, as an operator restarting a
        # machine would, so that its recovery is the one under test. CrashVerifier would otherwise
        # recover the database itself and the arm would prove only that the shipped server wrote
        # the data. QDB_PRODUCT_RECOVERY_PASS=false skips the pass and the arm's central claim
        # with it, so the skip is reported rather than silent.
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
                # Record the premise on the passing path too: its only other trace is a guest-side
                # log that dies with the VM.
                echo "DETAIL PRODUCT_PREMISE $PRODUCT_PREMISE"
                # Wait for the WAL to drain: the apply job runs behind the HTTP endpoint, so a row
                # count read as soon as SQL answers measures a recovery still in progress. `|| true`
                # keeps an early boundary, where the table does not exist yet, from killing the
                # script under `set -euo pipefail` before it can emit a verdict.
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
                # The database must be released before the oracle opens it, or the verifier cannot
                # start and the boundary yields no verdict.
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
                -DcommitMode="$MODE" ${QDB_WAL_TABLE:+-Dwal.table="$QDB_WAL_TABLE"} \
                -Dgroup.window.us="$WINDOW" -Depoch.interval.ms="$EPOCH" \
                -Dsibling.table="$SIBLING" -Drecover.as="$RECOVER_AS" \
                -Dmat.view="$MATVIEW" -Drebase="$REBASE" \
                -Dschema.profile="$PROFILE" -Dqwp="$QWP" -Dqwp.sf="$QWPSF" \
                -Dresult.file="$pares" \
                org.questdb.CrashVerifier "$DB" >/mnt/qdb/verify-armA.log 2>&1 || true
            sfa_distinct=$(read_result "$pares" distinctIds)
            sfa_f=$(read_result "$pares" F)
            sfa_c=$(read_result "$pares" C)
            echo "DETAIL SF_ARM_A serverAlone distinctIds=$sfa_distinct F=$sfa_f C=$sfa_c"
            # Pass A opened this root and the replay server opens it again, so a still-held lock
            # stops it starting. Wait for the JVM to be gone rather than assuming the shell's
            # return implies it.
            for _ in $(seq 1 40); do
                pgrep -f '[C]rashVerifier' >/dev/null 2>&1 || break
                sleep 0.25
            done
        fi

        if [ "$SFREPLAY" = "true" ] || [ "$SFREPLAY" = "compare" ]; then
            # The replay server must be the arm's own binary. If the product arm's client
            # reconnected to the classpath server, one verdict would cover the shipped artifact on
            # the write path and a different binary on the replay path.
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
            # Stop the server so the verifier can open the database itself; a live writer holds
            # locks it needs. The product branch stops the shipped way (questdb.sh stop), which
            # waits for the process to be gone instead of sleeping.
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
        # `|| rc=$?` is required: a non-zero exit is how this oracle signals a bad verdict and how
        # a JVM crash signals 134, and under `set -e` it would kill the script before it emits one.
        rc=0
        vbres="/mnt/qdb/verify-armB.properties"; rm -f "$vbres"
        # Every -D must appear on both passes: they are separate JVMs and CrashVerifier re-reads
        # each flag from scratch. Drop -Dwal.table here and the verifier grades a WAL table with
        # the non-WAL oracle, which never runs the recovery triple, and reports a pass.
        java $QDB_JVM -cp "$JAR" \
                -Dresult.file="$vbres" \
                -DcommitMode="$MODE" \
                ${QDB_WAL_TABLE:+-Dwal.table="$QDB_WAL_TABLE"} \
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
            # No verdict. Diagnose why rather than emitting a stray line, and keep it a failure:
            # a boundary the oracle cannot evaluate is a finding, not something to skip past.
            why="exit=$rc"
            if [ "$rc" -gt 128 ]; then
                why="$why killed-by-signal-$((rc - 128))"
            fi
            [ -s "$verr" ] && why="$why stderr='$(tail -c 900 "$verr" | tr '\n' ' ')'"
            [ ! -s "$vout" ] && [ ! -s "$verr" ] && why="$why produced-no-output-at-all"
            ls /tmp/hs_err_pid*.log >/dev/null 2>&1 && why="$why jvm-crash-log-present"
            [ -d "$DB" ] || why="$why db-root-absent"
            # The rig's finding, not the product's: still red, but it must not page the product
            # owner for a JVM the agent killed.
            line="NOT_EVALUATED: verifier produced no verdict ($why)"
        fi
        # The delta between the two passes is what the client put back.
        if [ "$SFREPLAY" = "compare" ] && [ "${sfa_distinct:--1}" -ge 0 ] 2>/dev/null; then
            sfb_distinct=$(read_result "$vbres" distinctIds)
            if [ "$sfb_distinct" -ge 0 ] 2>/dev/null; then
                delta=$(( sfb_distinct - sfa_distinct ))
                # Pass B verifies pass A's data plus whatever the client replayed, so distinct ids
                # can only rise. A negative delta means the numbers did not parse, which is a
                # finding about the harness rather than a durability verdict.
                if [ "$delta" -lt 0 ]; then
                    echo "DETAIL SF_INDETERMINATE armA=$sfa_distinct armB=$sfb_distinct delta=$delta"
                    line="NOT_EVALUATED qwp-sf: impossible negative replay delta ($delta) — the oracle's own numbers did not parse, so this boundary was not evaluated"
                    sfb_distinct=-1
                fi
            fi
            if [ "$sfb_distinct" -ge 0 ] 2>/dev/null; then
                echo "DETAIL SF_ARM_B afterReplay distinctIds=$sfb_distinct delta=$delta"
                # At-risk rows are measured against what the client sent, not the server's own
                # committed frontier: `F == C` says only that the server kept what it committed
                # and is silent about rows it never committed, which is the set sf protects.
                sent=$(grep -oE '^sent=[0-9]+' "$DB/_qwp_progress" 2>/dev/null | head -1 | cut -d= -f2)
                : "${sent:=-1}"
                if [ "$sent" -lt 0 ] 2>/dev/null; then
                    echo "DETAIL SF_INDETERMINATE no sent= in _qwp_progress; cannot say what was at risk"
                elif [ "$sfa_distinct" -lt "$sent" ] 2>/dev/null; then
                    # The server alone held less than the client had sent, so this is the
                    # boundary that tests the pairing: the client must put something back.
                    atrisk=$(( sent - sfa_distinct ))
                    shortfall=$(( sent - sfb_distinct ))
                    if [ "$delta" -gt 0 ]; then
                        echo "DETAIL SF_REPLAY_PROVEN sent=$sent serverAlone=$sfa_distinct atRisk=$atrisk"
                        echo "DETAIL SF_REPLAY_PROVEN client replayed $delta rows; endToEndShortfall=$shortfall"
                        # A residual shortfall is expected: sf_durability=periodic makes the
                        # client's buffer durable only to its last sync, so rows sent after it
                        # died with the client's disk. Reported so that window's size is visible.
                    else
                        # The failure this arm exists to catch: rows the client sent were missing
                        # and the client replayed none of them. Overrides pass B's verdict, which
                        # would otherwise read as a pass.
                        line="DURABILITY_FAILURE qwp-sf: $atrisk rows the client sent were absent after recovery (sent=$sent serverAlone=$sfa_distinct) and the client replayed NOTHING — store-and-forward did not close the gap"
                    fi
                else
                    # Legitimate: the server already held everything the client had sent, so the
                    # replay had nothing to do. Reported, never counted as proof of the mechanism.
                    echo "DETAIL SF_NOT_DEMONSTRATED server alone already held everything sent (sent=$sent serverAlone=$sfa_distinct); replay had nothing to recover"
                fi
            fi
        fi

        # Emit the verifier's full output, prefixed, before the verdict. Callers take the verdict
        # with `tail -1` or `grep -m1`, so prefixed detail lines cannot be mistaken for it and the
        # evidence survives instead of being truncated away.
        if [ "${QDB_VERIFY_FULL_OUTPUT:-true}" = "true" ]; then
            sed 's/^/DETAIL /' "$vout" 2>/dev/null || true
            [ -s "$verr" ] && sed 's/^/DETAIL-ERR /' "$verr" 2>/dev/null || true
        fi
        rm -f "$vout" "$verr"

        # A cut before the table was durably created makes CrashVerifier report "table does not
        # exist" as a LOUD_FAILURE, which is a legitimate outcome if nothing was ever committed.
        # Committed rows with no table is real data loss, so reclassify only when the durable
        # watermark says nothing was committed; otherwise the failure stands.
        case "$line" in
            LOUD_FAILURE*"table does not exist"*)
                # `|| true` is required: an early cut has no _progress by definition, and under
                # `set -euo pipefail` the failed head would kill the script before it prints a
                # verdict, which the caller then classifies UNPARSEABLE.
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
        # There is no separate product oracle: the arm varies the artifact, and a measurement that
        # changes with the thing being measured cannot compare them. Reaching this branch means a
        # caller sent the workload arm to the oracle, so nothing was measured.
        echo "NOT_EVALUATED: --arm=product is not an oracle; the product arm uses --arm=reference --server=product (see lib/arms.sh arm_verify_flags)"
        ;;

    *)
        # Same class as above: an arm the harness knows and this case statement does not.
        echo "NOT_EVALUATED: unknown arm $ARM"
        ;;
esac
