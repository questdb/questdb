#!/usr/bin/env bash
# guest/product-dist.sh — the SHIPPED distribution, in the guest. Source this; do not execute it.
#
# The product arm runs the release tarball (core/src/main/assembly/no-jre.xml ->
# questdb-<ver>-no-jre-bin.tar.gz) through its real launcher. That matters because the shipped
# launcher and the harness's other arms start DIFFERENT RUNTIME CONFIGURATIONS, not merely
# different jars:
#
#   every other arm        java -cp benchmarks.jar io.questdb.ServerMain
#                          --enable-native-access=ALL-UNNAMED --add-opens=...=ALL-UNNAMED
#                          -> unnamed module, classpath, out of a JMH shade-jar
#
#   questdb.sh (shipped)   java -p questdb.jar -m io.questdb/io.questdb.ServerMain
#                          --enable-native-access=io.questdb --add-opens=...=io.questdb
#                          -> NAMED JPMS MODULE, module path, the assembled artifact
#
# Reflection access, native-access enforcement, resource loading and native-library extraction
# all follow the module, so a regression in any of them is invisible to every other arm.
#
# BOTH run-workload.sh AND verify.sh need this: the arm writes with the shipped server and must
# also RECOVER with it. One definition, sourced by both, for the same reason lib/arms.sh exists
# -- the two callers of the arm vocabulary drifted apart three times before it did (issues/04).

PRODUCT_DIST_TGZ="${QDB_PRODUCT_DIST_TGZ:-/opt/vmcrash/questdb-dist.tar.gz}"
PRODUCT_DIST_DIR="${QDB_PRODUCT_DIST_DIR:-/opt/vmcrash/dist}"
# /opt/vmcrash, NOT /mnt/qdb. The distribution is an INSTRUMENT, not data under test: unpacking
# it onto the recorded device would add its writes to the log-writes stream and manufacture
# crash boundaries that only exist because the harness is there (the fault issues/08 measured
# for the witness file, at ~10.5% of boundaries).
PRODUCT_JAVA_HOME="${QDB_PRODUCT_JAVA_HOME:-/opt/jdk}"

# product_dist_unpack -> unpack the tarball, idempotently, and record its version.
#
# Reports the MISSING TARBALL as itself. The same fault surfaced as `ClassNotFoundException`
# in an earlier arm and read like a packaging bug in the product, when the artifact had simply
# never been shipped into the guest.
product_dist_unpack() {
    if [ ! -s "$PRODUCT_DIST_TGZ" ]; then
        echo "product-dist: $PRODUCT_DIST_TGZ is missing or empty -- the release tarball was never shipped into the guest" >&2
        return 64
    fi
    if [ ! -x "$PRODUCT_DIST_DIR/questdb.sh" ]; then
        rm -rf "$PRODUCT_DIST_DIR"
        mkdir -p "$PRODUCT_DIST_DIR"
        # --strip-components=1: the tarball's single top-level directory carries the version,
        # which is captured below BEFORE it is stripped. Unpacking with the version in the path
        # would make every downstream path version-dependent for no gain.
        tar -C "$PRODUCT_DIST_DIR" --strip-components=1 -xzf "$PRODUCT_DIST_TGZ"
        tar -tzf "$PRODUCT_DIST_TGZ" \
            | head -1 | cut -d/ -f1 | sed -e 's/^questdb-//' -e 's/-no-jre-bin$//' \
            > "$PRODUCT_DIST_DIR/.dist-version"
    fi
    # The launcher AND the two scripts it calls. print-hello.sh is invoked by questdb.sh on the
    # normal start path; env.sh carries JAVA_MAIN, without which the launcher starts nothing.
    # A partially shipped distribution must fail here, loudly, not halfway through a boot.
    local f
    for f in questdb.sh env.sh print-hello.sh questdb.jar; do
        [ -s "$PRODUCT_DIST_DIR/$f" ] || {
            echo "product-dist: $PRODUCT_DIST_DIR/$f missing from the unpacked distribution" >&2
            return 64
        }
    done
    [ -x "$PRODUCT_DIST_DIR/questdb.sh" ] || {
        echo "product-dist: questdb.sh is not executable -- the assembly's fileMode was lost in transit" >&2
        return 64
    }
    return 0
}

# product_dist_version -> the version string from the tarball's top-level directory.
product_dist_version() {
    cat "$PRODUCT_DIST_DIR/.dist-version" 2>/dev/null || echo "unknown"
}

# product_server_start ROOT MODE WINDOW_US EPOCH_MS LOGFILE
#
# Starts the shipped server through the shipped launcher and returns when it answers SQL.
#
# CONFIGURATION IS BY ENVIRONMENT, exactly as for the qwp arms: ServerMain reads QDB_-prefixed
# env vars derived from the property path. -DcommitMode is the harness's own convention for
# CrashIngestWriter and the SERVER IGNORES IT -- a slip that once ran the server at NOSYNC while
# the sweep reported sync, and made correct NOSYNC loss look like a durability defect.
#
# JAVA_HOME is REQUIRED by the no-jre distribution: with it unset the launcher exits 55 with
# "JAVA_HOME is undefined". The guest's JDK lives at /opt/jdk (build-image.sh).
product_server_start() {
    local root="$1" mode="$2" window="$3" epoch="$4" logfile="$5"
    env JAVA_HOME="$PRODUCT_JAVA_HOME" \
        QDB_CAIRO_COMMIT_MODE="$mode" \
        QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW="${window}us" \
        QDB_CAIRO_ADAPTIVE_EPOCH_INTERVAL="${epoch}ms" \
        bash "$PRODUCT_DIST_DIR/questdb.sh" start -d "$root" >>"$logfile" 2>&1
    # The launcher backgrounds the JVM itself and returns once the banner is printed, so a
    # successful return is NOT a running server. Wait for a DATASET, not merely for an HTTP
    # response: a half-started server answers the port before it can answer SQL, and connecting
    # then fails the QWP upgrade -- which the arm would report as a durability finding.
    local i
    for i in $(seq 1 120); do
        curl -s "http://localhost:9000/exec?query=select%201" 2>/dev/null | grep -q dataset && return 0
        sleep 0.5
    done
    echo "product-dist: server did not answer SQL within 60s; see $logfile" >&2
    return 1
}

# product_server_assert LOGFILE -> refuse unless the RUNNING server really is the shipped
# artifact, started the shipped way.
#
# THIS IS THE ARM'S PREMISE, and it is asserted rather than assumed for the same reason the qwp
# arm asserts its mode and its edition: a silent fallback to a classpath-launched server would
# report product coverage that never happened, and nothing downstream could tell the difference
# -- both servers write byte-identical data.
#
# Two independent checks, because each can pass while the other fails:
#   1. the JVM command line runs a MODULE MAIN (-m io.questdb/io.questdb.ServerMain). This is
#      the runtime configuration the arm exists to cover. A `-cp` fallback fails here.
#   2. build() reports the COMMIT HASH baked into the tarball's manifest at assembly time. This
#      is what ties the running server to the artifact built from THIS tree; a stale unpacked
#      distribution left over from an earlier run fails here while passing check 1.
#
# On success it sets PRODUCT_PREMISE to a one-line summary, so the CALLER can put the evidence
# where the evidence is kept. A passing check that writes only to a guest-side log the sweep
# never archives cannot be audited from the run's output -- and "the check ran" would then be
# something a reader has to take on trust, which is the thing this harness refuses to do.
PRODUCT_PREMISE=""
product_server_assert() {
    local logfile="$1"
    local cmdline
    # The bracket idiom, for the reason lib/arms.sh documents at length: pgrep -f matches whole
    # command lines, and the shell running this check carries the pattern in its OWN cmdline.
    cmdline=$(pgrep -af '[Q]uestDB-Runtime' | head -1 || true)
    case "$cmdline" in
        *"-m io.questdb/io.questdb.ServerMain"*) ;;
        "") echo "product-dist: no server process matching the launcher's process label is running" >&2
            return 64 ;;
        *)  echo "product-dist: the running server is NOT module-launched -- refusing." >&2
            echo "product-dist: cmdline was: $cmdline" >&2
            return 64 ;;
    esac
    echo "product: module-launched, cmdline ok" >>"$logfile"

    local want got
    # python3, NOT unzip: the guest image installs dmsetup, e2fsprogs and xfsprogs and nothing
    # else, so `unzip` is absent and the pipeline returned an empty hash. That failed CLOSED --
    # the arm refused to ingest rather than skipping the check -- which is the only reason this
    # was a two-minute diagnosis instead of a run that silently proved less than it claimed.
    # python3 is already a hard dependency of the harness (guest/replay-log.py).
    #
    # Manifest continuation lines are unfolded before matching: MANIFEST.MF wraps at 72 bytes
    # with a leading space, and a hash that happened to wrap would otherwise read as absent.
    want=$(python3 - "$PRODUCT_DIST_DIR/questdb.jar" <<'PY' 2>/dev/null || true
import sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    text = z.read('META-INF/MANIFEST.MF').decode('utf-8', 'replace')
text = text.replace('\r\n', '\n').replace('\r', '\n').replace('\n ', '')
for line in text.split('\n'):
    if line.lower().startswith('build-commit-hash:'):
        print(line.split(':', 1)[1].strip())
        break
PY
)
    got=$(curl -s -G http://localhost:9000/exec --data-urlencode "query=select build()" 2>/dev/null || true)
    echo "product: dist version=$(product_dist_version) manifest commit=${want:-<none>}" >>"$logfile"
    echo "product: build() = $got" >>"$logfile"
    if [ -z "$want" ]; then
        # No hash to compare against is a HARNESS fault, not a product one, and it must not be
        # silently downgraded to "assume it matches" -- that would make the check decoration.
        echo "product-dist: questdb.jar has no Build-Commit-Hash in its manifest; cannot tie the running server to the shipped artifact" >&2
        return 64
    fi
    case "$got" in
        *"$want"*) ;;
        *) echo "product-dist: the running server does not report the tarball's commit hash ($want) -- refusing." >&2
           echo "product-dist: build() said: $got" >&2
           return 64 ;;
    esac
    PRODUCT_PREMISE="moduleLaunched=yes dist=$(product_dist_version) commit=$want"
    return 0
}

# product_server_stop -> stop it the way an operator would, and do not return until it is gone.
#
# `questdb.sh stop` sends SIGTERM, polls, and escalates to SIGKILL. The POLLING MATTERS: the
# next step opens the same database, and a still-running server holds the lock. That exact race
# left 2 of 7 boundaries with no verdict at all in the first qwp-sf compare run (issues/19).
product_server_stop() {
    bash "$PRODUCT_DIST_DIR/questdb.sh" stop >/dev/null 2>&1 || true
    local i
    for i in $(seq 1 60); do
        pgrep -f '[Q]uestDB-Runtime' >/dev/null 2>&1 || return 0
        sleep 0.5
    done
    echo "product-dist: server still running after questdb.sh stop" >&2
    return 1
}
