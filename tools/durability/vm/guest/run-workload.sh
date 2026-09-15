#!/usr/bin/env bash
# guest/run-workload.sh --arm=reference|qwp|qwp-sf|product --mode=MODE [--window-us=W] [--epoch-ms=N] [--max-rows=N]
#
# Starts the arm's workload against the DB root on the flakey device. Blocks;
# the controller cuts it short.
#
#   reference — CrashIngestWriter with the engine embedded. Captures C and Wm
#               (localDurableSeqTxn) in-process, which works today.
#   qwp/qwp-sf— a real classpath-launched server plus the real WebSocket client.
#   product   — the SHIPPED release tarball, started by the real questdb.sh
#               launcher, plus the same client qwp-sf uses. W>0 IS SUPPORTED:
#               the client's LOCAL durable-ack tier supplies the durable
#               frontier, so the RPO bar applies here exactly as it does to
#               qwp-sf. (It was W=0-only while that tier was unbuilt; the tier
#               landed with issues/17 and was measured against a module-launched
#               server before this arm was wired up.)
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
    qwp-sf)
        # THE STORE-AND-FORWARD ARM. Additive: it shares the qwp arm's server and client binaries
        # and changes only what the CLIENT is asked to guarantee.
        #
        #   qwp     the SERVER tracks and recovers. The client is a plain producer; its watermark
        #           is polled from the server's wal_tables(). Bar: F >= Wm.
        #   qwp-sf  the server does all of the above AND the client requests the LOCAL durable-ack
        #           tier, holds everything not yet locally-durable-acked in its store-and-forward
        #           buffer, and replays it after the server comes back. Bar: F >= Wm, PLUS the ack
        #           channel was live, PLUS the replay refills what the server's RPO window dropped.
        #
        # The two are kept separate rather than folded behind a flag because their ORACLES differ:
        # store-and-forward replay is at-least-once, so duplicate ids are correct here and are
        # SILENT_CORRUPTION everywhere else. One shared oracle would have to either fail this arm
        # on correct behaviour or weaken the bar for every other arm.
        #
        # Runs on OSS: LocalDurableAckRegistry is the default registry and grants
        # DurabilityTier.LOCAL out of the box. Only `replicated` needs enterprise.
        :
        ;;
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
        # THE SHIPPED-ARTIFACT ARM. Identical to qwp-sf in every respect except which server
        # binary runs and how it is launched -- see guest/product-dist.sh for why that is a
        # runtime-configuration difference rather than a packaging detail.
        #
        # ENT is refused rather than quietly downgraded: the enterprise distribution is a
        # different artifact that this arm does not build or ship, and running the OSS tarball
        # under an `ent` label is the same false-green shape as a sync-labelled nosync run.
        if [ "${QDB_EDITION:-oss}" = "ent" ]; then
            echo "run-workload: arm=product has no enterprise distribution to run (QDB_EDITION=ent)." >&2
            echo "  The ENT tarball is not built by this harness; use --arm=qwp-sf for ENT coverage." >&2
            exit 64
        fi
        # shellcheck source=product-dist.sh
        source /opt/vmcrash/guest/product-dist.sh
        product_dist_unpack || exit 64
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
        # -Dwal.table decouples the TABLE KIND from the commit mode, which is what makes the
        # barrier control (WAL table + NOSYNC, no durability barrier) expressible at all. The
        # default below is computed by CrashIngestWriter itself from the commit mode and
        # reproduces the historical routing, so an unset QDB_WAL_TABLE changes nothing.
        # guest/verify.sh MUST pass the same value, or the verifier grades a WAL table with the
        # non-WAL oracle.
        exec java $QDB_JVM -cp "$JAR" \
            -DcommitMode="$MODE" \
            ${QDB_WAL_TABLE:+-Dwal.table="$QDB_WAL_TABLE"} \
            -Dgroup.window.us="$WINDOW" \
            -Depoch.interval.ms="$EPOCH" \
            -Dmax.rows="$ROWS" \
            -Dschema.profile="$PROFILE" \
            -Dwitness.fsync="${QDB_WITNESS_FSYNC:-true}" \
            -Dsibling.table="${QDB_SIBLING_TABLE:-false}" \
            -Dddl.every.rows="${QDB_DDL_EVERY_ROWS:--1}" \
            -Drebase.at.rows="${QDB_REBASE_AT_ROWS:--1}" -Dmat.view="${QDB_MAT_VIEW:-false}" \
            org.questdb.CrashIngestWriter "$DB" > /mnt/qdb/writer.log 2>&1
        ;;

    qwp|qwp-sf|product)
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
        CURL_AUTH=""
        [ "${QDB_EDITION:-oss}" = "ent" ] && CURL_AUTH="-u ${QDB_ENT_USER:-admin}:${QDB_ENT_PASSWORD:-quest}"
        if [ "$ARM" = product ]; then
            # THE SHIPPED LAUNCHER STARTS THE SERVER, and it backgrounds the JVM itself -- so no
            # trailing `&` here, and no hand-built java line. Reconstructing the launcher's
            # command line would test our reading of questdb.sh instead of questdb.sh.
            product_server_start "$(dirname "$DB")" "$MODE" "$WINDOW" "$EPOCH" /mnt/qdb/server.log \
                || { echo "run-workload: shipped server failed to start; see /mnt/qdb/server.log" >&2; exit 64; }
            echo "product: server started via $PRODUCT_DIST_DIR/questdb.sh (dist $(product_dist_version))" >> /mnt/qdb/writer.log
            # ASSERT THE ARTIFACT before a single row is ingested -- see product-dist.sh.
            product_server_assert /mnt/qdb/writer.log \
                || { echo "run-workload: product premise assertion failed; refusing to ingest" >&2; exit 64; }
        else
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
            # Require a DATASET, not merely an HTTP response: under ENT an unauthenticated request
            # answers 401 and `curl >/dev/null && ...` would report the server as up.
            for _ in $(seq 1 120); do
                curl -s $CURL_AUTH "http://localhost:9000/exec?query=select%201" 2>/dev/null | grep -q dataset && break
                sleep 0.5
            done
        fi
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
        # qwp-sf AND product FORCE the local tier and an SF buffer on the crashed device; they are
        # what those arms ARE, so they are not left to an environment variable that could be unset
        # without anyone noticing. The plain qwp arm keeps whatever was asked for (default off).
        # Messages below name $ARM rather than a literal: the product arm reaches this block too,
        # and a refusal that names the wrong arm sends the reader to the wrong script.
        # THE LOCAL DURABLE-ACK TIER EXISTS ONLY FOR ADAPTIVE TABLES. WalWriter guards the whole
        # durable-ack bookkeeping with `if (commitMode == CommitMode.ADAPTIVE)`, so under SYNC the
        # commit is fdatasync'd but localDurableSeqTxn never advances, LocalDurableAckRegistry
        # returns -1, and the server emits no STATUS_LOCAL_DURABLE_ACK frames at all.
        #
        #   qwp-sf   REFUSED outside adaptive. The tier is the entire arm; without it there is
        #            nothing left that the plain qwp arm does not already cover.
        #   product  DEGRADED to the plain-qwp contract, loudly. This arm is about the ARTIFACT,
        #            and "the shipped server writes, crashes and recovers under SYNC" is still a
        #            real and wanted cell -- but it must not be reported as ack-channel coverage.
        #
        # Refuse HERE, before the device work, rather than 20,000 rows in: the client's own
        # "channel is dead" guard fires late and reaches the caller as "workload was not running",
        # which names the symptom and blames the wrong layer.
        if { [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; } \
           && [ "$(echo "$MODE" | tr A-Z a-z)" != adaptive ]; then
            if [ "$ARM" = qwp-sf ]; then
                echo "run-workload: arm=qwp-sf cannot run at mode=$MODE." >&2
                echo "  The LOCAL durable-ack tier is advanced only on the ADAPTIVE commit path" >&2
                echo "  (WalWriter: 'if (commitMode == CommitMode.ADAPTIVE)'), so the server emits no" >&2
                echo "  durable-ack frames and the arm's bar cannot be evaluated. Use --arm=qwp." >&2
                exit 64
            fi
            echo "product: mode=$MODE has no LOCAL durable-ack tier; running the PLAIN QWP contract" >&2
            echo "product: DEGRADED to plain qwp (tier=off, no store-and-forward) because mode=$MODE" >> /mnt/qdb/writer.log
            echo "product:   the artifact claim stands; the ack-channel claim is NOT made for this cell" >> /mnt/qdb/writer.log
            PRODUCT_DEGRADED=1
        fi

        # The guard below keys on the DEGRADE, not on the tier's value. Keying on
        # `QDB_QWP_DURABLE_ACK != off` would have silently swallowed the case it exists to catch:
        # a qwp-sf run asked for with the tier turned off but WITHOUT QDB_QWP_DEFANG_ACK=1, which
        # must be refused rather than quietly demoted to a qwp run under an sf label. It would
        # also have skipped the DEFANGED negative control's warning banner, leaving a run that is
        # REQUIRED to fail looking like an ordinary one.
        if { [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; } && [ "${PRODUCT_DEGRADED:-0}" != 1 ]; then
            QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}"
            case "$QWP_TIER" in
                *local*) ;;
                *)
                    # QDB_QWP_DEFANG_ACK=1 -- THE NEGATIVE CONTROL, and nothing else. Same role as
                    # QDB_CUT_DROP_WRITES=0 in arm-cut.sh: deliberately break the thing the arm is
                    # built on, and require the oracle to NOTICE. Without it, "the ack channel bar
                    # passes" is unfalsifiable, and an oracle that cannot fail is decoration.
                    #
                    # The verifier must report DURABILITY_FAILURE here, because the client holds
                    # nothing on the strength of an ack it never asked for. Never set in a real run.
                    if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
                        echo "run-workload: WARNING — $ARM DEFANGED (tier='$QWP_TIER'); the sweep MUST fail" >&2
                        echo "$ARM: DEFANGED negative control, tier=$QWP_TIER" >> /mnt/qdb/writer.log
                    else
                        echo "run-workload: arm=$ARM requires a tier including 'local' (got '$QWP_TIER')" >&2
                        echo "run-workload: that is the whole arm; refusing rather than running a qwp arm under an sf label" >&2
                        exit 64
                    fi
                    ;;
            esac
            # sf_dir on the CRASHED device on purpose: the client's buffer must take the same
            # power cut as the server's WAL, or the pairing is never actually tested.
            # `periodic` is the strongest IMPLEMENTED durability (`flush`/`append` parse but are
            # not implemented), so the client's own guarantee extends only to its last sync.
            QWP_SF_DIR="${QDB_QWP_SF_DIR:-/mnt/qdb/sf}"
            mkdir -p "$QWP_SF_DIR"
            echo "$ARM: tier=$QWP_TIER sf_dir=$QWP_SF_DIR durability=${QDB_QWP_SF_DURABILITY:-periodic}" >> /mnt/qdb/writer.log
        else
            QWP_TIER="${QDB_QWP_DURABLE_ACK:-off}"
            QWP_SF_DIR="${QDB_QWP_SF_DIR:-/mnt/qdb/sf}"
        fi
        exec java $QDB_JVM -cp "$JAR" \
            -Dqwp.addr=localhost:9000 \
            -Dqwp.durable.ack="$QWP_TIER" \
            -Dqwp.user="$([ "${QDB_EDITION:-oss}" = ent ] && echo "${QDB_ENT_USER:-admin}" || echo "")" \
            -Dqwp.password="$([ "${QDB_EDITION:-oss}" = ent ] && echo "${QDB_ENT_PASSWORD:-quest}" || echo "")" \
            -Dqwp.sf.dir="$QWP_SF_DIR" \
            -Dqwp.sf.durability="${QDB_QWP_SF_DURABILITY:-periodic}" \
            -Dqwp.batch="${QDB_QWP_BATCH:-1000}" \
            -Dmax.rows="${QDB_QWP_ROWS:-2000000000}" \
            org.questdb.QwpCrashIngestClient "$DB" >> /mnt/qdb/writer.log 2>&1
        ;;

esac
