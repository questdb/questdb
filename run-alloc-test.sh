#!/bin/bash
#
# ILP Allocation Profiling Test Script
#
# This script helps run the allocation profiling test for ILP protocols.
#
# Usage:
#   ./run-alloc-test.sh server              - Start QuestDB server
#   ./run-alloc-test.sh client [options]    - Run test client
#   ./run-alloc-test.sh profile [options]   - Run with async-profiler
#   ./run-alloc-test.sh jfr [options]       - Run with Java Flight Recorder
#   ./run-alloc-test.sh compare [options]   - Compare all protocols
#
# Options (passed to client):
#   --protocol=PROTOCOL      Protocol: ilp-tcp, ilp-http, ilpv4-http, ilpv4-websocket
#   --host=HOST              Server host (default: localhost)
#   --port=PORT              Server port
#   --rows=N                 Total rows to send
#   --batch=N                Auto-flush after N rows
#   --flush-bytes=N          Auto-flush after N bytes (WebSocket only)
#   --flush-interval-ms=N    Auto-flush after N ms (WebSocket only)
#   --in-flight-window=N     Max batches awaiting server ACK (WebSocket only, default: 8)
#   --send-queue=N           Max batches waiting to send (WebSocket only, default: 16)
#   --warmup=N               Warmup rows
#   --report=N               Report interval
#   --no-warmup              Skip warmup
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_VERSION="9.2.4-SNAPSHOT"
MAIN_JAR="$SCRIPT_DIR/core/target/questdb-${JAR_VERSION}.jar"
TEST_JAR="$SCRIPT_DIR/core/target/questdb-${JAR_VERSION}-tests.jar"
QDB_ROOT="$SCRIPT_DIR/qdb-alloc-test"

# Check if JARs exist
check_jars() {
    if [ ! -f "$MAIN_JAR" ]; then
        echo "ERROR: Main JAR not found: $MAIN_JAR"
        echo "Run: mvn clean package -DskipTests -pl core"
        exit 1
    fi

    if [ ! -f "$TEST_JAR" ]; then
        echo "ERROR: Test JAR not found: $TEST_JAR"
        echo "Run: mvn test-compile -pl core"
        exit 1
    fi
}

# Extract protocol from args for port defaulting
get_protocol_from_args() {
    for arg in "$@"; do
        if [[ "$arg" == --protocol=* ]]; then
            echo "${arg#--protocol=}"
            return
        fi
    done
    echo "ilpv4-http"  # default
}

case "$1" in
    server)
        check_jars
        echo "Starting QuestDB server..."
        echo "Data directory: $QDB_ROOT"
        echo ""
        mkdir -p "$QDB_ROOT"
        java -p "$MAIN_JAR" \
             -m io.questdb/io.questdb.ServerMain \
             -d "$QDB_ROOT"
        ;;

    client)
        check_jars
        shift  # remove 'client' from args

        # Check for --debug flag
        DEBUG_FLAG=""
        CLIENT_ARGS=()
        for arg in "$@"; do
            if [[ "$arg" == "--debug" ]]; then
                DEBUG_FLAG="-Debug"
            else
                CLIENT_ARGS+=("$arg")
            fi
        done

        echo "Running ILP test client..."
        java -cp "$MAIN_JAR:$TEST_JAR" $DEBUG_FLAG \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "${CLIENT_ARGS[@]}"
        ;;

    profile)
        check_jars
        shift  # remove 'profile' from args

        # Check for async-profiler
        if [ -z "$ASYNC_PROFILER_HOME" ]; then
            # Try common locations
            for dir in /opt/async-profiler ~/async-profiler /usr/local/async-profiler; do
                if [ -d "$dir" ]; then
                    ASYNC_PROFILER_HOME="$dir"
                    break
                fi
            done
        fi

        if [ -z "$ASYNC_PROFILER_HOME" ] || [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/jvm-profiling-tools/async-profiler"
            echo ""
            echo "Alternative: Use JFR (Java Flight Recorder):"
            echo "  $0 jfr [options]"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-alloc-profile-${PROTOCOL}.jfr"
        echo "Running ILP test client with allocation profiling..."
        echo "Output: $PROFILE_OUTPUT"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=alloc,file=$PROFILE_OUTPUT" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "$@"
        echo ""
        echo "Profile saved to: $PROFILE_OUTPUT"
        echo "View with: $ASYNC_PROFILER_HOME/bin/jfr2flame $PROFILE_OUTPUT alloc-flame-${PROTOCOL}.html"
        ;;

    jfr)
        check_jars
        shift  # remove 'jfr' from args

        PROTOCOL=$(get_protocol_from_args "$@")
        JFR_OUTPUT="$SCRIPT_DIR/ilp-alloc-${PROTOCOL}.jfr"
        echo "Running ILP test client with JFR allocation profiling..."
        echo "Output: $JFR_OUTPUT"
        echo ""
        java -XX:StartFlightRecording=filename="$JFR_OUTPUT",settings=profile \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "$@"
        echo ""
        echo "JFR recording saved to: $JFR_OUTPUT"
        echo "View with: jfr print --events jdk.ObjectAllocationInNewTLAB,jdk.ObjectAllocationOutsideTLAB $JFR_OUTPUT"
        echo "Or open in JDK Mission Control (jmc)"
        ;;

    compare)
        check_jars
        shift  # remove 'compare' from args

        echo "Running comparison test across all protocols..."
        echo "Make sure QuestDB server is running!"
        echo ""

        for protocol in ilp-tcp ilp-http ilpv4-http ilpv4-websocket; do
            echo "=========================================="
            echo "Testing: $protocol"
            echo "=========================================="
            java -cp "$MAIN_JAR:$TEST_JAR" \
                 io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
                 --protocol="$protocol" "$@"
            echo ""
        done
        ;;

    *)
        echo "ILP Allocation Profiling Test"
        echo ""
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  server              Start QuestDB server"
        echo "  client [options]    Run test client"
        echo "  profile [options]   Run with async-profiler allocation tracking"
        echo "  jfr [options]       Run with Java Flight Recorder"
        echo "  compare [options]   Run all 4 protocols and compare"
        echo ""
        echo "Options:"
        echo "  --debug                  Enable debug logging"
        echo "  --protocol=PROTOCOL      Protocol: ilp-tcp, ilp-http, ilpv4-http, ilpv4-websocket (default: ilpv4-http)"
        echo "  --host=HOST              Server host (default: localhost)"
        echo "  --port=PORT              Server port (default: 9009 for TCP, 9000 for HTTP/WebSocket)"
        echo "  --rows=N                 Total rows to send (default: 80000000)"
        echo "  --batch=N                Auto-flush after N rows (default: 10000)"
        echo "  --flush-bytes=N          Auto-flush after N bytes (WebSocket only, default: 1MB)"
        echo "  --flush-interval-ms=N    Auto-flush after N ms (WebSocket only, default: 100ms)"
        echo "  --in-flight-window=N     Max batches awaiting server ACK (WebSocket only, default: 8)"
        echo "  --send-queue=N           Max batches waiting to send (WebSocket only, default: 16)"
        echo "  --warmup=N               Warmup rows (default: 100000)"
        echo "  --report=N               Report progress every N rows (default: 1000000)"
        echo "  --no-warmup              Skip warmup phase"
        echo ""
        echo "Examples:"
        echo "  Terminal 1: $0 server"
        echo "  Terminal 2: $0 client --protocol=ilpv4-http --rows=1000000 --batch=5000"
        echo "  Terminal 2: $0 client --protocol=ilpv4-websocket --rows=100000 --no-warmup"
        echo "  Terminal 2: $0 jfr --protocol=ilpv4-http --rows=10000000"
        echo "  Terminal 2: $0 compare --rows=1000000 --batch=10000"
        ;;
esac
