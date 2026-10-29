#!/usr/bin/env bash
# guest/product-dist.sh — the shipped distribution, in the guest. Source this; do not execute it.
#
# The product arm runs the release tarball (core/src/main/assembly/no-jre.xml ->
# questdb-<ver>-no-jre-bin.tar.gz) through its real launcher, which is a different runtime
# configuration and not merely a different jar:
#
#   every other arm        java -cp benchmarks.jar io.questdb.ServerMain
#                          --enable-native-access=ALL-UNNAMED --add-opens=...=ALL-UNNAMED
#
#   questdb.sh (shipped)   java -p questdb.jar -m io.questdb/io.questdb.ServerMain
#                          --enable-native-access=io.questdb --add-opens=...=io.questdb
#
# Reflection access, native-access enforcement, resource loading and native-library extraction
# all follow the module, so a regression in any of them is invisible to every other arm.
# run-workload.sh and verify.sh both source this: the arm writes with the shipped server and
# must also recover with it.

PRODUCT_DIST_TGZ="${QDB_PRODUCT_DIST_TGZ:-/opt/vmcrash/questdb-dist.tar.gz}"
PRODUCT_DIST_DIR="${QDB_PRODUCT_DIST_DIR:-/opt/vmcrash/dist}"
# /opt/vmcrash, not /mnt/qdb. The distribution is an instrument, not data under test: unpacking
# it onto the recorded device would add its writes to the log-writes stream and manufacture crash
# boundaries that exist only because the harness is there.
PRODUCT_JAVA_HOME="${QDB_PRODUCT_JAVA_HOME:-/opt/jdk}"

# product_dist_unpack -> unpack the tarball, idempotently, and record its version. A missing
# tarball is reported as itself; downstream it surfaces as a class-loading failure that reads
# like a packaging bug in the product.
product_dist_unpack() {
    if [ ! -s "$PRODUCT_DIST_TGZ" ]; then
        echo "product-dist: $PRODUCT_DIST_TGZ is missing or empty -- the release tarball was never shipped into the guest" >&2
        return 64
    fi
    if [ ! -x "$PRODUCT_DIST_DIR/questdb.sh" ]; then
        rm -rf "$PRODUCT_DIST_DIR"
        mkdir -p "$PRODUCT_DIST_DIR"
        # --strip-components=1: the tarball's single top-level directory carries the version,
        # captured below before it is stripped, so downstream paths stay version-independent.
        tar -C "$PRODUCT_DIST_DIR" --strip-components=1 -xzf "$PRODUCT_DIST_TGZ"
        tar -tzf "$PRODUCT_DIST_TGZ" \
            | head -1 | cut -d/ -f1 | sed -e 's/^questdb-//' -e 's/-no-jre-bin$//' \
            > "$PRODUCT_DIST_DIR/.dist-version"
    fi
    # questdb.sh invokes print-hello.sh on the normal start path, and env.sh carries JAVA_MAIN,
    # without which the launcher starts nothing. A partial distribution fails here, not halfway
    # through a boot.
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
# ServerMain reads QDB_-prefixed env vars derived from the property path and ignores the -D
# properties the harness uses for CrashIngestWriter, so a server configured the -D way silently
# serves a different mode than the sweep reports. JAVA_HOME is required by the no-jre
# distribution: unset, the launcher exits 55. The guest's JDK lives at /opt/jdk (build-image.sh).
product_server_start() {
    local root="$1" mode="$2" window="$3" epoch="$4" logfile="$5"
    env JAVA_HOME="$PRODUCT_JAVA_HOME" \
        QDB_CAIRO_COMMIT_MODE="$mode" \
        QDB_CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW="${window}us" \
        QDB_CAIRO_ADAPTIVE_EPOCH_INTERVAL="${epoch}ms" \
        bash "$PRODUCT_DIST_DIR/questdb.sh" start -d "$root" >>"$logfile" 2>&1
    # The launcher backgrounds the JVM and returns once the banner is printed, so a successful
    # return is not a running server. Wait for a dataset, not merely an HTTP response: a
    # half-started server answers the port before it can answer SQL, and connecting then fails
    # the QWP upgrade, which the arm would report as a durability finding.
    local i
    for i in $(seq 1 120); do
        curl -s "http://localhost:9000/exec?query=select%201" 2>/dev/null | grep -q dataset && return 0
        sleep 0.5
    done
    echo "product-dist: server did not answer SQL within 60s; see $logfile" >&2
    return 1
}

# product_server_assert LOGFILE -> refuse unless the running server really is the shipped
# artifact, started the shipped way.
#
# This is the arm's premise. A silent fallback to a classpath-launched server would report
# product coverage that never happened, and both servers write byte-identical data, so nothing
# downstream could tell. Two independent checks, because each can pass while the other fails:
#   1. the command line runs a module main (-m io.questdb/io.questdb.ServerMain); a `-cp`
#      fallback fails here.
#   2. build() reports the commit hash baked into the tarball's manifest, tying the server to
#      the artifact built from this tree; a stale unpacked distribution fails here only.
#
# On success it sets PRODUCT_PREMISE so the caller can archive the evidence.
PRODUCT_PREMISE=""
product_server_assert() {
    local logfile="$1"
    local cmdline
    # The bracket idiom keeps pgrep -f from matching the shell running this check, which carries
    # the pattern in its own command line.
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
    # python3, not unzip: unzip is absent from the guest image, python3 is already a hard
    # dependency. Manifest continuation lines are unfolded before matching, because MANIFEST.MF
    # wraps at 72 bytes with a leading space and a wrapped hash would read as absent.
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
        # No hash to compare against is a harness fault. Downgrading it to "assume it matches"
        # would make the check decoration.
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
# `questdb.sh stop` sends SIGTERM, polls, escalates to SIGKILL. The polling matters: the next
# step opens the same database, and a still-running server holds the lock.
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
