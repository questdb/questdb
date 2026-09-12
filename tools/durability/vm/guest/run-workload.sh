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
            -Dsibling.table="${QDB_SIBLING_TABLE:-false}" \            -Dddl.every.rows="${QDB_DDL_EVERY_ROWS:--1}" \
            -Drebase.at.rows="${QDB_REBASE_AT_ROWS:--1}" -Dmat.view="${QDB_MAT_VIEW:-false}" \
            org.questdb.CrashIngestWriter "$DB" > /mnt/qdb/writer.log 2>&1
        ;;

esac
