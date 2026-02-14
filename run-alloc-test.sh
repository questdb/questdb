#!/bin/bash
#
# ILP Allocation Profiling Test Script
#
# This script helps run the allocation profiling test for ILP protocols.
#
# Usage:
#   ./run-alloc-test.sh server              - Start QuestDB server
#   ./run-alloc-test.sh client [options]    - Run test client
#   ./run-alloc-test.sh profile [options]   - Run with async-profiler (allocation)
#   ./run-alloc-test.sh cpu [options]       - CPU profiling (find hot methods)
#   ./run-alloc-test.sh wall [options]      - Wall-clock profiling (find I/O waits)
#   ./run-alloc-test.sh lock [options]      - Lock contention profiling
#   ./run-alloc-test.sh jfr [options]       - Run with Java Flight Recorder
#   ./run-alloc-test.sh compare [options]   - Compare all protocols
#
# Server-side profiling (attach to running server):
#   ./run-alloc-test.sh server-cpu [options]   - Profile server CPU during client test
#   ./run-alloc-test.sh server-wall [options]  - Profile server wall-clock
#   ./run-alloc-test.sh server-alloc [options] - Profile server allocations
#   ./run-alloc-test.sh server-lock [options]  - Profile server lock contention
#
# Options (passed to client):
#   --protocol=PROTOCOL      Protocol: ilp-tcp, ilp-http, qwp-websocket
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
QDB_ROOT="$SCRIPT_DIR/qdb-alloc-test"

# Find JARs dynamically (don't rely on specific version)
find_jars() {
    # Find main JAR (questdb-*.jar but not -tests.jar or -sources.jar)
    MAIN_JAR=$(find "$SCRIPT_DIR/core/target" -maxdepth 1 -name "questdb-*.jar" \
               ! -name "*-tests.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" \
               2>/dev/null | head -1)

    # Find test JAR
    TEST_JAR=$(find "$SCRIPT_DIR/core/target" -maxdepth 1 -name "questdb-*-tests.jar" \
               2>/dev/null | head -1)
}

# Check if JARs exist
check_jars() {
    find_jars

    if [ -z "$MAIN_JAR" ] || [ ! -f "$MAIN_JAR" ]; then
        echo "ERROR: Main JAR not found in $SCRIPT_DIR/core/target/"
        echo "Run: mvn clean package -DskipTests -pl core"
        exit 1
    fi

    if [ -z "$TEST_JAR" ] || [ ! -f "$TEST_JAR" ]; then
        echo "ERROR: Test JAR not found in $SCRIPT_DIR/core/target/"
        echo "Run: mvn test-compile -pl core"
        exit 1
    fi

    echo "Using JARs:"
    echo "  Main: $MAIN_JAR"
    echo "  Test: $TEST_JAR"
}

# Extract protocol from args for port defaulting
get_protocol_from_args() {
    for arg in "$@"; do
        if [[ "$arg" == --protocol=* ]]; then
            echo "${arg#--protocol=}"
            return
        fi
    done
    echo "qwp-websocket"  # default
}

# Find QuestDB server PID by checking port 9000
find_questdb_pid() {
    # Try lsof first (most reliable)
    local pid=$(lsof -ti:9000 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    # Fallback: try jps to find ServerMain
    pid=$(jps -l 2>/dev/null | grep 'io.questdb.ServerMain' | awk '{print $1}' | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    return 1
}

# Check async-profiler installation
check_asprof() {
    ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"
    ASPROF="$ASYNC_PROFILER_HOME/bin/asprof"

    if [ ! -x "$ASPROF" ]; then
        echo "ERROR: asprof not found at: $ASPROF"
        echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
        exit 1
    fi
}

case "$1" in
    server)
        check_jars
        shift  # remove 'server' from args

        # Check for --debug flag
        DEBUG_FLAG=""
        for arg in "$@"; do
            if [[ "$arg" == "--debug" ]]; then
                DEBUG_FLAG="-Debug"
            fi
        done

        echo "Starting QuestDB server..."
        echo "Data directory: $QDB_ROOT"
        echo ""
        mkdir -p "$QDB_ROOT"
        java -p "$MAIN_JAR" $DEBUG_FLAG \
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
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "${CLIENT_ARGS[@]}"
        ;;

    profile)
        check_jars
        shift  # remove 'profile' from args

        # async-profiler location
        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-alloc-profile-${PROTOCOL}.collapsed"
        echo "Running ILP test client with allocation profiling..."
        echo "Output: $PROFILE_OUTPUT (collapsed stacks format)"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=alloc,alloc=1k,file=$PROFILE_OUTPUT,collapsed" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@"
        echo ""
        echo "Profile saved to: $PROFILE_OUTPUT"
        echo ""
        echo "Top allocation sites:"
        sort -t' ' -k2 -rn "$PROFILE_OUTPUT" | head -30
        ;;

    cpu)
        check_jars
        shift  # remove 'cpu' from args

        # async-profiler location
        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-cpu-profile-${PROTOCOL}.jfr"
        echo "Running ILP test client with CPU profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=cpu,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@"
        echo ""
        echo "CPU profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ilp-cpu-flame.html"
        ;;

    wall)
        check_jars
        shift  # remove 'wall' from args

        # async-profiler location
        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-wall-profile-${PROTOCOL}.jfr"
        echo "Running ILP test client with wall-clock profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures time spent waiting (I/O, locks, sleep) in addition to CPU time."
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=wall,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@"
        echo ""
        echo "Wall-clock profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ilp-wall-flame.html"
        ;;

    lock)
        check_jars
        shift  # remove 'lock' from args

        # async-profiler location
        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/ilp-lock-profile-${PROTOCOL}.jfr"
        echo "Running ILP test client with lock contention profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures thread contention on synchronized blocks and locks."
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=lock,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@"
        echo ""
        echo "Lock contention profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ilp-lock-flame.html"
        ;;

    server-cpu)
        # Profile SERVER CPU while running client test
        check_jars
        check_asprof
        shift  # remove 'server-cpu' from args

        # Find server PID
        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/server-cpu-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER CPU during client test..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo ""

        # Start profiler on server (specify output file at start time)
        "$ASPROF" start -e cpu -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        # Run client test
        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@" || true  # Don't fail if client errors

        # Stop profiler (file is written automatically)
        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server CPU profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o server-cpu-flame.html"
        ;;

    server-wall)
        # Profile SERVER wall-clock while running client test
        check_jars
        check_asprof
        shift  # remove 'server-wall' from args

        # Find server PID
        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/server-wall-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER wall-clock during client test..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures time spent waiting (I/O, locks, sleep) in addition to CPU time."
        echo ""

        # Start profiler on server (specify output file at start time)
        "$ASPROF" start -e wall -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        # Run client test
        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@" || true  # Don't fail if client errors

        # Stop profiler (file is written automatically)
        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server wall-clock profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o server-wall-flame.html"
        ;;

    server-alloc)
        # Profile SERVER allocations while running client test
        check_jars
        check_asprof
        shift  # remove 'server-alloc' from args

        # Find server PID
        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/server-alloc-profile-${PROTOCOL}.collapsed"
        echo "Profiling SERVER allocations during client test..."
        echo "Output: $PROFILE_OUTPUT (collapsed stacks format)"
        echo ""

        # Start profiler on server (alloc=1k means track allocations >= 1KB)
        "$ASPROF" start -e alloc --alloc 1k -o collapsed -f "$PROFILE_OUTPUT" "$SERVER_PID"

        # Run client test
        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@" || true  # Don't fail if client errors

        # Stop profiler (file is written automatically)
        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server allocation profile saved to: $PROFILE_OUTPUT"
        echo ""
        echo "Top allocation sites:"
        sort -t' ' -k2 -rn "$PROFILE_OUTPUT" | head -30
        ;;

    server-lock)
        # Profile SERVER lock contention while running client test
        check_jars
        check_asprof
        shift  # remove 'server-lock' from args

        # Find server PID
        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/server-lock-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER lock contention during client test..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures thread contention on synchronized blocks and locks."
        echo ""

        # Start profiler on server (specify output file at start time)
        "$ASPROF" start -e lock -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        # Run client test
        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
             "$@" || true  # Don't fail if client errors

        # Stop profiler (file is written automatically)
        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server lock contention profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o server-lock-flame.html"
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
             io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
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

        for protocol in ilp-tcp ilp-http qwp-websocket; do
            echo "=========================================="
            echo "Testing: $protocol"
            echo "=========================================="
            java -cp "$MAIN_JAR:$TEST_JAR" \
                 io.questdb.test.cutlass.line.tcp.v4.QwpAllocationTestClient \
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
        echo "  profile [options]   Run with async-profiler allocation tracking (collapsed format)"
        echo "  cpu [options]       Run with CPU profiling - find hot methods (JFR format)"
        echo "  wall [options]      Run with wall-clock profiling - find I/O waits and blocking (JFR format)"
        echo "  lock [options]      Run with lock contention profiling - find thread contention (JFR format)"
        echo "  jfr [options]       Run with Java Flight Recorder"
        echo "  compare [options]   Run all 3 protocols and compare"
        echo ""
        echo "Server-side profiling (attach to running server):"
        echo "  server-cpu [options]     Profile SERVER CPU during client test"
        echo "  server-wall [options]    Profile SERVER wall-clock during client test"
        echo "  server-alloc [options]   Profile SERVER allocations during client test"
        echo "  server-lock [options]    Profile SERVER lock contention during client test"
        echo ""
        echo "Options:"
        echo "  --debug                  Enable debug logging"
        echo "  --protocol=PROTOCOL      Protocol: ilp-tcp, ilp-http, qwp-websocket (default: qwp-websocket)"
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
        echo "  Terminal 2: $0 client --protocol=qwp-websocket --rows=1000000 --batch=5000"
        echo "  Terminal 2: $0 client --protocol=qwp-websocket --rows=100000 --no-warmup"
        echo "  Terminal 2: $0 profile --rows=100000000 --warmup=1000000  # client allocation profiling"
        echo "  Terminal 2: $0 cpu --rows=100000000 --warmup=1000000      # client CPU hotspots"
        echo "  Terminal 2: $0 wall --rows=100000000 --warmup=1000000     # client I/O and blocking"
        echo "  Terminal 2: $0 lock --rows=100000000 --warmup=1000000     # client lock contention"
        echo "  Terminal 2: $0 jfr --protocol=qwp-websocket --rows=10000000"
        echo "  Terminal 2: $0 compare --rows=1000000 --batch=10000"
        echo ""
        echo "Server-side profiling examples:"
        echo "  Terminal 1: $0 server"
        echo "  Terminal 2: $0 server-cpu --rows=100000000 --warmup=1000000    # profile server CPU"
        echo "  Terminal 2: $0 server-alloc --rows=100000000 --warmup=1000000  # profile server allocations"
        echo "  Terminal 2: $0 server-wall --rows=100000000 --warmup=1000000   # profile server I/O waits"
        echo "  Terminal 2: $0 server-lock --rows=100000000 --warmup=1000000   # profile server lock contention"
        ;;
esac
