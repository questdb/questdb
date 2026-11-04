#!/usr/bin/env bash
# guest/run-workload.sh --arm=reference|qwp|qwp-sf|product --mode=MODE [--window-us=W] [--epoch-ms=N] [--max-rows=N]
#
# Starts the arm's workload against the DB root on the recorded device. Blocks;
# the controller cuts it short.
#
#   reference — CrashIngestWriter with the engine embedded. Captures C and Wm
#               (localDurableSeqTxn) in-process.
#   qwp/qwp-sf— a real classpath-launched server plus the real WebSocket client.
#   product   — the shipped release tarball, started by the real questdb.sh
#               launcher, plus the same client qwp-sf uses.
set -euo pipefail

ARM=reference
MODE=adaptive
WINDOW=0
EPOCH=1000
# The cap must stay out of reach for the length of the cut window. A workload that finishes
# first leaves the cut landing on an idle database, which returns a DURABLE that proves nothing.
# Raising the cap costs neither time nor disk, because the cut always fires first. The qwp
# branch raises it again: that arm ingests far faster and would otherwise finish inside the
# recording window.
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
        # The store-and-forward arm shares the qwp arm's server and client binaries and changes
        # only what the client is asked to guarantee: it requests the local durable-ack tier,
        # holds everything not yet locally-durable-acked, and replays it once the server is back.
        # Bar: F >= Wm, plus a live ack channel, plus a replay that refills the server's RPO gap.
        #
        # Kept separate from qwp rather than folded behind a flag because the oracles differ.
        # Store-and-forward replay is at-least-once, so duplicate ids are correct here and are
        # SILENT_CORRUPTION in every other arm; one shared oracle would have to fail this arm on
        # correct behaviour or weaken the bar everywhere else.
        #
        # Runs on OSS: LocalDurableAckRegistry is the default registry and grants
        # DurabilityTier.LOCAL out of the box. Only `replicated` needs enterprise.
        :
        ;;
    qwp)
        # The client records the server's localDurableSeqTxn from wal_tables(), the same frontier
        # the reference arm reads from SeqTxnTracker, so the F >= Wm bar applies at any window.
        :
        ;;
    product)
        # The shipped-artifact arm is qwp-sf with a different server binary and launcher; see
        # guest/product-dist.sh.
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
        # -Dwal.table decouples the table kind from the commit mode, which is what makes the
        # barrier control (WAL table + NOSYNC, no durability barrier) expressible at all. An unset
        # QDB_WAL_TABLE changes nothing: CrashIngestWriter then derives the kind from the commit
        # mode. guest/verify.sh must pass the same value, or the verifier grades a WAL table with
        # the non-WAL oracle.
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
        # A real server plus a real WebSocket client, so the cut lands on the wire protocol's
        # write path -- frame decode, ingress buffering, server-side commit -- none of which the
        # embedded-engine arm touches. The server owns the engine; the client only speaks the
        # protocol, exactly as a deployment does.
        #
        # ServerMain takes configuration from QDB_-prefixed environment variables
        # (QDB_CAIRO_COMMIT_MODE and friends), derived from the property path. It ignores -D
        # system properties, which are the harness's own convention for CrashIngestWriter only.
        # A server left on the default commit mode while the sweep reports another one produces
        # correct behaviour for the mode it actually ran, which reads exactly like a defect.
        #
        # Edition. `ent` runs the enterprise server, which differs in two ways the OSS arm never
        # exercises: it is not a fat jar and needs entlib/ (jar-jni plus the OSS engine jar) or it
        # dies with io/questdb/jar/jni/LoadException, and ACL is on by default (admin/quest), so
        # every request needs credentials.
        CURL_AUTH=""
        [ "${QDB_EDITION:-oss}" = "ent" ] && CURL_AUTH="-u ${QDB_ENT_USER:-admin}:${QDB_ENT_PASSWORD:-quest}"
        if [ "$ARM" = product ]; then
            # The shipped launcher backgrounds the JVM itself, so no trailing `&` and no
            # hand-built java line: reconstructing the launcher's command would test our reading
            # of questdb.sh instead of questdb.sh.
            product_server_start "$(dirname "$DB")" "$MODE" "$WINDOW" "$EPOCH" /mnt/qdb/server.log \
                || { echo "run-workload: shipped server failed to start; see /mnt/qdb/server.log" >&2; exit 64; }
            echo "product: server started via $PRODUCT_DIST_DIR/questdb.sh (dist $(product_dist_version))" >> /mnt/qdb/writer.log
            # Assert the artifact before a single row is ingested -- see product-dist.sh.
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
        # Assert the edition. A silent fallback to OSS would report enterprise coverage that was
        # never exercised.
        if [ "${QDB_EDITION:-oss}" = "ent" ]; then
            edition=$(curl -s $CURL_AUTH -G http://localhost:9000/exec --data-urlencode "query=select build()" 2>/dev/null || true)
            case "$edition" in
                *Enterprise*) echo "qwp: server is $(echo "$edition" | grep -oE 'QuestDB Enterprise [A-Za-z]+')" >> /mnt/qdb/writer.log ;;
                *) echo "run-workload: QDB_EDITION=ent but the server is not enterprise -- refusing." >&2
                   echo "run-workload: build() said: $edition" >> /mnt/qdb/writer.log
                   exit 64 ;;
            esac
        fi
        # Assert the premise: the no-loss oracle is sound only if the server really is in the
        # requested mode, and a label is not evidence. A run that tests a different mode than it
        # reports is worse than no run.
        #
        # $CURL_AUTH is required under ENT like every other query; without it the 401 yields no
        # grep match and `set -e` kills the script on the assignment, with no output at all.
        # `|| true` keeps the failure here, where it can be reported, rather than in an abort with
        # no output -- and an unanswerable probe is itself a refusal, below: a premise that cannot
        # be checked is not a premise that held.
        actual_mode=$(curl -s $CURL_AUTH -G http://localhost:9000/exec \
            --data-urlencode "query=select value from (show parameters) where property_path = 'cairo.commit.mode'" \
            2>/dev/null | grep -oE '\[\["[a-zA-Z]+"\]\]' | grep -oE '[a-zA-Z]+' | head -1 || true)
        echo "qwp: server reports cairo.commit.mode=$actual_mode (requested $MODE)" >> /mnt/qdb/writer.log
        if [ -z "$actual_mode" ]; then
            echo "run-workload: the server did not report cairo.commit.mode; premise unverified -- refusing." >&2
            echo "run-workload: the server did not report cairo.commit.mode; premise unverified -- refusing." >> /mnt/qdb/writer.log
            exit 64
        fi
        if [ "$(echo "$actual_mode" | tr A-Z a-z)" != "$(echo "$MODE" | tr A-Z a-z)" ]; then
            echo "run-workload: server is in '$actual_mode' but the run claims '$MODE' -- refusing." >&2
            echo "run-workload: server is in '$actual_mode' but the run claims '$MODE' -- refusing." >> /mnt/qdb/writer.log
            exit 64
        fi
        # qwp-sf and product force the local tier and an SF buffer on the crashed device, because
        # that is what those arms are; leaving it to an environment variable would let a run drop
        # the tier unnoticed. Plain qwp keeps whatever was asked for. Messages name $ARM rather
        # than a literal, since the product arm reaches this block too.
        #
        # The local durable-ack tier exists only for adaptive tables: WalWriter guards the
        # durable-ack bookkeeping with `if (commitMode == CommitMode.ADAPTIVE)`, so under SYNC the
        # commit is fdatasync'd but localDurableSeqTxn never advances, LocalDurableAckRegistry
        # returns -1, and the server emits no STATUS_LOCAL_DURABLE_ACK frames.
        #
        #   qwp-sf   refused outside adaptive. The tier is the entire arm; without it nothing is
        #            left that plain qwp does not already cover.
        #   product  degraded to the plain-qwp contract, loudly. The artifact claim still holds
        #            under SYNC, but it must not be reported as ack-channel coverage.
        #
        # Refuse here rather than thousands of rows in: the client's own dead-channel guard fires
        # late and reaches the caller as "workload was not running", blaming the wrong layer.
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

        # This guard keys on the degrade, not on the tier's value. Keying on the tier would
        # swallow the case it exists to catch -- a qwp-sf run asked for with the tier off but
        # without QDB_QWP_DEFANG_ACK=1, which must be refused rather than demoted to a qwp run
        # under an sf label -- and would skip the defanged control's warning banner.
        if { [ "$ARM" = qwp-sf ] || [ "$ARM" = product ]; } && [ "${PRODUCT_DEGRADED:-0}" != 1 ]; then
            QWP_TIER="${QDB_QWP_DURABLE_ACK:-local}"
            case "$QWP_TIER" in
                *local*)
                    # The label must match the teeth. A run asked for as the negative control but
                    # still holding a live durable-ack tier would pass while every report marks it
                    # required-to-fail.
                    if [ "${QDB_QWP_DEFANG_ACK:-0}" = "1" ]; then
                        echo "run-workload: QDB_QWP_DEFANG_ACK=1 but tier='$QWP_TIER' is still live" >&2
                        echo "run-workload: set QDB_QWP_DURABLE_ACK=off to defang; refusing a labelled control that is not one" >&2
                        exit 64
                    fi
                    ;;
                *)
                    # QDB_QWP_DEFANG_ACK=1 is the negative control and nothing else: it breaks the
                    # thing the arm is built on and requires the oracle to notice. Without it,
                    # "the ack channel bar passes" is unfalsifiable. The verifier must report
                    # DURABILITY_FAILURE here, because the client holds nothing on the strength of
                    # an ack it never asked for. Never set in a real run.
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
            # sf_dir sits on the crashed device on purpose: the client's buffer must take the same
            # power cut as the server's WAL, or the pairing is never actually tested. `periodic` is
            # the strongest implemented durability (`flush`/`append` parse but are not
            # implemented), so the client's own guarantee extends only to its last sync.
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
