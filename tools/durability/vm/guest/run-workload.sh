#!/usr/bin/env bash
# guest/run-workload.sh --arm=reference|product --mode=MODE [--window-us=W] [--epoch-ms=N] [--max-rows=N]
#
# Starts the arm's workload against the DB root on the flakey device. Blocks;
# the controller cuts it short.
#
#   reference — CrashIngestWriter with the engine embedded. Captures C and Wm
#               (localDurableSeqTxn) in-process, which works today.
#   product   — the real server plus a wire client. W=0 ONLY: the client-side
#               LOCAL durable-ack frontier is WIP, so at W>0 this arm cannot
#               observe Wm and therefore cannot enforce the RPO bar. The
#               controller rejects W>0 for this arm; the guard here is a second
#               line of defence in case it is invoked directly.
set -euo pipefail

ARM=reference
MODE=adaptive
WINDOW=0
EPOCH=1000
# Must NOT be reachable within the cut window. At ~83k rows/s the old 2,000,000
# cap completed in ~24s, while cuts are drawn out to 30s -- so late iterations
# cut an IDLE, already-finished system and returned a guaranteed DURABLE that
# proved nothing. Raising the cap costs neither time nor disk: the cut always
# fires first, so only ~30s of rows are ever actually written.
# 100M suits the in-process writer. The QWP arm sustains ~12M rows/s (batched frames, a
# dedicated server), finishes 100M INSIDE the ~8s recording window, and then trips the liveness
# assertion -- a completed workload is exactly the vacuous cell that assertion exists to reject.
# The qwp branch raises it below; a workload that ends early tests a quiescent database.
ROWS=100000000
PROFILE="${QDB_SCHEMA_PROFILE:-bitmap}"   # bitmap|posting|covering|none

for a in "$@"; do
    case "$a" in
        --arm=*)       ARM="${a#*=}" ;;
        --mode=*)      MODE="${a#*=}" ;;
        --window-us=*) WINDOW="${a#*=}" ;;
        --epoch-ms=*)  EPOCH="${a#*=}" ;;
        --max-rows=*)  ROWS="${a#*=}" ;;
        *) echo "run-workload: unknown argument $a" >&2; exit 64 ;;
    esac
done

JAR=/opt/vmcrash/benchmarks.jar
DB=/mnt/qdb/db

# Validate BEFORE any side effect, so a rejected cell fails with the reason
# rather than with a filesystem error from setup it should never have reached.
case "$ARM" in
    reference) ;;
    qwp)
        # Same reasoning as the product arm's W=0 restriction, for the same underlying reason:
        # the client can observe only the COMMIT-ack frontier. Under SYNC a commit fsyncs before
        # the ack, so that frontier IS durable and the no-loss bar holds. Under ADAPTIVE W>0 it
        # runs ahead of durable, and asserting no-loss from it would either over-claim or report
        # phantom loss. Durable ack over QWP is refused by an OSS server outright, so there is no
        # way to observe the real durable frontier here today.
        # ADAPTIVE is now supported: the client records the SERVER's localDurableSeqTxn from
        # wal_tables(), the same frontier the reference arm reads from SeqTxnTracker, so the
        # F >= Wm bar applies. (Previously refused: without the local durable-ack tier the client
        # could only see COMMIT acks, which equal durability at W=0 only.)
        :
        ;;
    product)
        if [ "$WINDOW" -gt 0 ]; then
            echo "run-workload: product arm supports W=0 only (got W=$WINDOW)." >&2
            echo "  The client-side LOCAL durable-ack frontier is WIP, so Wm cannot be" >&2
            echo "  observed and the RPO bar cannot be enforced at W>0." >&2
            exit 64
        fi
        echo "run-workload: product arm is implemented by Task 6b; not yet available" >&2
        exit 64
        ;;
    *) echo "run-workload: unknown arm $ARM" >&2; exit 64 ;;
esac

mkdir -p "$DB"

# Same flag set core/pom.xml uses. Without --add-exports ...jdk.internal.vm the
# worker continuation class fails to initialise and QuestDB runs DEGRADED with
# dead workers, which would silently invalidate the whole run.
QDB_JVM="--enable-native-access=ALL-UNNAMED \
--sun-misc-unsafe-memory-access=allow \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.time.zone=ALL-UNNAMED \
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"

case "$ARM" in
    reference)
        exec java $QDB_JVM -cp "$JAR" \
            -DcommitMode="$MODE" \
            -Dgroup.window.us="$WINDOW" \
            -Depoch.interval.ms="$EPOCH" \
            -Dmax.rows="$ROWS" \
            -Dschema.profile="$PROFILE" \
            -Dsibling.table="${QDB_SIBLING_TABLE:-false}" \
            -Dddl.every.rows="${QDB_DDL_EVERY_ROWS:--1}" \
            -Drebase.at.rows="${QDB_REBASE_AT_ROWS:--1}" -Dmat.view="${QDB_MAT_VIEW:-false}" \
            org.questdb.CrashIngestWriter "$DB" > /mnt/qdb/writer.log 2>&1
        ;;

    qwp)
        # QWP arm: a REAL server plus a REAL WebSocket client, so the cut lands on the wire
        # protocol's write path -- frame decode, ingress buffering, server-side commit -- none of
        # which the embedded-engine arm touches. The server owns the engine; the client only speaks
        # the protocol, exactly as a deployment does.
        #
        # NOTE ON THE WATERMARK: durable ack is NOT an OSS capability (the server refuses the
        # upgrade: "server does not support durable ack"), so the client records the COMMIT-ack
        # frontier. That is a durable frontier only under commitMode=SYNC, where a commit fsyncs
        # before acking. The verifier applies the no-loss bar accordingly; asserting it under
        # ADAPTIVE W>0 would claim a guarantee the acked frontier does not carry.
        # ServerMain takes configuration from QDB_-prefixed ENV VARS (QDB_CAIRO_COMMIT_MODE etc),
        # derived from the property path -- NOT from -D system properties. -DcommitMode is the
        # harness's own convention for CrashIngestWriter and the SERVER IGNORES IT.
        #
        # This was not a harmless slip: the server ran at CommitMode.DEFAULT = NOSYNC while the
        # sweep reported "mode=sync", and the resulting 4.6M zeroed rows out of 57M -- correct,
        # documented NOSYNC behaviour after a power cut -- looked exactly like a durability defect.
        # Control clean, reproduced twice, coherent mechanism, and still wrong, because the PREMISE
        # was never checked. The assertion below exists so that can never be assumed again.
        # EDITION. `ent` runs the enterprise server instead of OSS. Two differences the harness
        # must handle, neither of which the OSS arm can ever exercise:
        #   * ENT is NOT a fat jar -- it needs entlib/ (jar-jni + the OSS engine jar), or it dies
        #     with io/questdb/jar/jni/LoadException.
        #   * ACL is ON by default (acl.enabled=true, admin/quest), so every request needs
        #     credentials where OSS needs none.
        if [ "${QDB_EDITION:-oss}" = "ent" ]; then
            SERVER_CP="/opt/vmcrash/questdb-enterprise.jar:/opt/vmcrash/entlib/*:$JAR"
            SERVER_MAIN="com.questdb.EntServerMain"
        else
            SERVER_CP="$JAR"
            SERVER_MAIN="io.questdb.ServerMain"
        fi
        env QDB_CAIRO_COMMIT_MODE="$MODE" \
            QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW="${WINDOW}us" \
            QDB_CAIRO_ADAPTIVE_EPOCH_INTERVAL="${EPOCH}ms" \
            java $QDB_JVM -cp "$SERVER_CP" \
            $SERVER_MAIN -d "$(dirname "$DB")" > /mnt/qdb/server.log 2>&1 &
        echo "qwp: server started pid=$!" >> /mnt/qdb/writer.log
        # Wait for HTTP before ingesting: connecting to a half-started server fails the upgrade
        # and the arm would report a connection error as if it were a durability finding.
        CURL_AUTH=""
        [ "${QDB_EDITION:-oss}" = "ent" ] && CURL_AUTH="-u ${QDB_ENT_USER:-admin}:${QDB_ENT_PASSWORD:-quest}"
        # Require a DATASET, not merely an HTTP response: under ENT an unauthenticated request
        # answers 401 and `curl >/dev/null && ...` would report the server as up.
        for _ in $(seq 1 120); do
            curl -s $CURL_AUTH "http://localhost:9000/exec?query=select%201" 2>/dev/null | grep -q dataset && break
            sleep 0.5
        done
        # ASSERT THE EDITION. A silent fallback to OSS would report enterprise coverage that was
        # never exercised -- the same failure shape as a run labelled sync while serving nosync.
        if [ "${QDB_EDITION:-oss}" = "ent" ]; then
            edition=$(curl -s $CURL_AUTH -G http://localhost:9000/exec --data-urlencode "query=select build()" 2>/dev/null || true)
            case "$edition" in
                *Enterprise*) echo "qwp: server is $(echo "$edition" | grep -oE 'QuestDB Enterprise [A-Za-z]+')" >> /mnt/qdb/writer.log ;;
                *) echo "run-workload: QDB_EDITION=ent but the server is not enterprise -- refusing." >&2
                   echo "run-workload: build() said: $edition" >> /mnt/qdb/writer.log
                   exit 64 ;;
            esac
        fi
        # ASSERT THE PREMISE. The no-loss oracle is sound only if the server really is in the
        # requested mode; a label is not evidence. Refuse to ingest otherwise -- a run that tests
        # a different mode than it reports is worse than no run.
        # $CURL_AUTH: under ENT this query is authenticated like every other. Without it the 401
        # yields no grep match, the pipeline returns non-zero, and under `set -e` the ASSIGNMENT
        # kills the script silently -- the client never launched and the run failed with no output
        # at all. `|| true` so a probe that cannot answer degrades to "unknown" rather than
        # aborting the run; the comparison below already tolerates an empty value.
        actual_mode=$(curl -s $CURL_AUTH -G http://localhost:9000/exec \
            --data-urlencode "query=select value from (show parameters) where property_path = 'cairo.commit.mode'" \
            2>/dev/null | grep -oE '\[\["[a-zA-Z]+"\]\]' | grep -oE '[a-zA-Z]+' | head -1 || true)
        echo "qwp: server reports cairo.commit.mode=$actual_mode (requested $MODE)" >> /mnt/qdb/writer.log
        if [ -n "$actual_mode" ] && [ "$(echo "$actual_mode" | tr A-Z a-z)" != "$(echo "$MODE" | tr A-Z a-z)" ]; then
            echo "run-workload: server is in '$actual_mode' but the run claims '$MODE' -- refusing." >&2
            echo "run-workload: server is in '$actual_mode' but the run claims '$MODE' -- refusing." >> /mnt/qdb/writer.log
            exit 64
        fi
        exec java $QDB_JVM -cp "$JAR" \
            -Dqwp.addr=localhost:9000 \
            -Dqwp.durable.ack="${QDB_QWP_DURABLE_ACK:-local}" \
            -Dqwp.user="$([ "${QDB_EDITION:-oss}" = ent ] && echo "${QDB_ENT_USER:-admin}" || echo "")" \
            -Dqwp.password="$([ "${QDB_EDITION:-oss}" = ent ] && echo "${QDB_ENT_PASSWORD:-quest}" || echo "")" \
            -Dqwp.sf.dir="${QDB_QWP_SF_DIR:-/mnt/qdb/sf}" \
            -Dqwp.sf.durability="${QDB_QWP_SF_DURABILITY:-periodic}" \
            -Dqwp.batch="${QDB_QWP_BATCH:-1000}" \
            -Dmax.rows="${QDB_QWP_ROWS:-2000000000}" \
            org.questdb.QwpCrashIngestClient "$DB" >> /mnt/qdb/writer.log 2>&1
        ;;

esac
