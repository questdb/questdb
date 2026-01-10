#!/bin/bash
#
# ILP Allocation Profiling Test Script
#
# This script helps run the allocation profiling test for ILP protocols.
#
# Usage:
#   ./run-alloc-test.sh server              - Start QuestDB server
#   ./run-alloc-test.sh client <protocol>   - Run test client
#   ./run-alloc-test.sh profile <protocol>  - Run with async-profiler
#   ./run-alloc-test.sh jfr <protocol>      - Run with Java Flight Recorder
#
# Protocols:
#   ilp-tcp     - Old ILP text protocol over TCP (port 9009)
#   ilp-http    - Old ILP text protocol over HTTP (port 9000)
#   ilpv4-tcp   - New ILPv4 binary protocol over TCP (port 9009)
#   ilpv4-http  - New ILPv4 binary protocol over HTTP (port 9000)
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

# Get default port for protocol
get_default_port() {
    local protocol=$1
    case "$protocol" in
        ilp-http|ilpv4-http)
            echo "9000"
            ;;
        *)
            echo "9009"
            ;;
    esac
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
        PROTOCOL=${2:-ilpv4-tcp}
        HOST=${3:-localhost}
        PORT=${4:-$(get_default_port "$PROTOCOL")}
        ROWS=${5:-10000000}
        echo "Running ILP test client..."
        echo "Protocol: $PROTOCOL"
        echo "Host: $HOST"
        echo "Port: $PORT"
        echo "Rows: $ROWS"
        echo ""
        java -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "$PROTOCOL" "$HOST" "$PORT" "$ROWS"
        ;;

    profile)
        check_jars
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
            echo "  $0 jfr <protocol> [rows]"
            exit 1
        fi

        PROTOCOL=${2:-ilpv4-tcp}
        HOST=${3:-localhost}
        PORT=${4:-$(get_default_port "$PROTOCOL")}
        ROWS=${5:-10000000}
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-alloc-profile-${PROTOCOL}.jfr"
        echo "Running ILP test client with allocation profiling..."
        echo "Protocol: $PROTOCOL"
        echo "Host: $HOST"
        echo "Port: $PORT"
        echo "Rows: $ROWS"
        echo "Output: $PROFILE_OUTPUT"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=alloc,file=$PROFILE_OUTPUT" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "$PROTOCOL" "$HOST" "$PORT" "$ROWS"
        echo ""
        echo "Profile saved to: $PROFILE_OUTPUT"
        echo "View with: $ASYNC_PROFILER_HOME/bin/jfr2flame $PROFILE_OUTPUT alloc-flame-${PROTOCOL}.html"
        ;;

    jfr)
        check_jars
        # Use Java Flight Recorder (built into JDK)
        PROTOCOL=${2:-ilpv4-tcp}
        HOST=${3:-localhost}
        PORT=${4:-$(get_default_port "$PROTOCOL")}
        ROWS=${5:-10000000}
        JFR_OUTPUT="$SCRIPT_DIR/ilp-alloc-${PROTOCOL}.jfr"
        echo "Running ILP test client with JFR allocation profiling..."
        echo "Protocol: $PROTOCOL"
        echo "Host: $HOST"
        echo "Port: $PORT"
        echo "Rows: $ROWS"
        echo "Output: $JFR_OUTPUT"
        echo ""
        java -XX:StartFlightRecording=filename="$JFR_OUTPUT",settings=profile \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
             "$PROTOCOL" "$HOST" "$PORT" "$ROWS"
        echo ""
        echo "JFR recording saved to: $JFR_OUTPUT"
        echo "View with: jfr print --events jdk.ObjectAllocationInNewTLAB,jdk.ObjectAllocationOutsideTLAB $JFR_OUTPUT"
        echo "Or open in JDK Mission Control (jmc)"
        ;;

    compare)
        check_jars
        # Run all 4 protocols and compare throughput
        ROWS=${2:-1000000}
        echo "Running comparison test across all protocols..."
        echo "Rows per protocol: $ROWS"
        echo ""
        echo "Make sure QuestDB server is running!"
        echo ""

        for protocol in ilp-tcp ilp-http ilpv4-tcp ilpv4-http; do
            echo "=========================================="
            echo "Testing: $protocol"
            echo "=========================================="
            PORT=$(get_default_port "$protocol")
            java -cp "$MAIN_JAR:$TEST_JAR" \
                 io.questdb.test.cutlass.line.tcp.v4.IlpV4AllocationTestClient \
                 "$protocol" localhost "$PORT" "$ROWS"
            echo ""
        done
        ;;

    *)
        echo "ILP Allocation Profiling Test"
        echo ""
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  server                              Start QuestDB server"
        echo "  client <protocol> [host] [port] [rows]"
        echo "                                      Run test client"
        echo "  profile <protocol> [host] [port] [rows]"
        echo "                                      Run with async-profiler allocation tracking"
        echo "  jfr <protocol> [host] [port] [rows]"
        echo "                                      Run with Java Flight Recorder"
        echo "  compare [rows]                      Run all 4 protocols and compare"
        echo ""
        echo "Protocols:"
        echo "  ilp-tcp      Old ILP text protocol over TCP (default port: 9009)"
        echo "  ilp-http     Old ILP text protocol over HTTP (default port: 9000)"
        echo "  ilpv4-tcp    New ILPv4 binary protocol over TCP (default port: 9009)"
        echo "  ilpv4-http   New ILPv4 binary protocol over HTTP (default port: 9000)"
        echo ""
        echo "Examples:"
        echo "  Terminal 1: $0 server"
        echo "  Terminal 2: $0 client ilpv4-tcp localhost 9009 1000000"
        echo "  Terminal 2: $0 jfr ilpv4-http localhost 9000 10000000"
        echo "  Terminal 2: $0 compare 1000000"
        ;;
esac
