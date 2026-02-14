#!/bin/bash
#
# STAC Benchmark Ingestion Test Script
#
# Tests ingestion performance for a STAC-like NYSE quotes table.
# Models 8512 unique 4-letter ticker symbols with quote data
# (bid, ask, volume, exchange, market flag).
#
# Usage:
#   ./run-stac-bench.sh server              - Start QuestDB server
#   ./run-stac-bench.sh create-table        - Create the STAC quotes table
#   ./run-stac-bench.sh client [options]    - Run test client
#   ./run-stac-bench.sh profile [options]   - Run with async-profiler (allocation)
#   ./run-stac-bench.sh cpu [options]       - CPU profiling (find hot methods)
#   ./run-stac-bench.sh wall [options]      - Wall-clock profiling (find I/O waits)
#   ./run-stac-bench.sh lock [options]      - Lock contention profiling
#   ./run-stac-bench.sh jfr [options]       - Run with Java Flight Recorder
#   ./run-stac-bench.sh compare [options]   - Compare all protocols
#
# Server-side profiling (attach to running server):
#   ./run-stac-bench.sh server-cpu [options]   - Profile server CPU during client test
#   ./run-stac-bench.sh server-wall [options]  - Profile server wall-clock
#   ./run-stac-bench.sh server-alloc [options] - Profile server allocations
#   ./run-stac-bench.sh server-lock [options]  - Profile server lock contention
#
# Options (passed to client):
#   --protocol=PROTOCOL      Protocol: ilp-tcp, ilp-http, qwp-websocket
#   --host=HOST              Server host (default: localhost)
#   --port=PORT              Server port
#   --table=TABLE            Table name (default: q)
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
QDB_ROOT="$SCRIPT_DIR/qdb-stac-bench"
CLIENT_CLASS="io.questdb.test.cutlass.line.tcp.v4.StacBenchmarkClient"
PROFILE_PREFIX="stac"

# Default host/port for REST API (table creation)
QDB_HOST="${QDB_HOST:-localhost}"
QDB_HTTP_PORT="${QDB_HTTP_PORT:-9000}"

# Find JARs dynamically (don't rely on specific version)
find_jars() {
    MAIN_JAR=$(find "$SCRIPT_DIR/core/target" -maxdepth 1 -name "questdb-*.jar" \
               ! -name "*-tests.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" \
               2>/dev/null | head -1)

    TEST_JAR=$(find "$SCRIPT_DIR/core/target" -maxdepth 1 -name "questdb-*-tests.jar" \
               2>/dev/null | head -1)
}

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

get_protocol_from_args() {
    for arg in "$@"; do
        if [[ "$arg" == --protocol=* ]]; then
            echo "${arg#--protocol=}"
            return
        fi
    done
    echo "qwp-websocket"
}

get_table_from_args() {
    for arg in "$@"; do
        if [[ "$arg" == --table=* ]]; then
            echo "${arg#--table=}"
            return
        fi
    done
    echo "q"
}

find_questdb_pid() {
    local pid=$(lsof -ti:${QDB_HTTP_PORT} 2>/dev/null | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    pid=$(jps -l 2>/dev/null | grep 'io.questdb.ServerMain' | awk '{print $1}' | head -1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return 0
    fi

    return 1
}

check_asprof() {
    ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"
    ASPROF="$ASYNC_PROFILER_HOME/bin/asprof"

    if [ ! -x "$ASPROF" ]; then
        echo "ERROR: asprof not found at: $ASPROF"
        echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
        exit 1
    fi
}

create_table() {
    local table_name="${1:-q}"
    local host="${2:-$QDB_HOST}"
    local port="${3:-$QDB_HTTP_PORT}"

    echo "Creating STAC quotes table '${table_name}' on ${host}:${port}..."

    # Drop table if exists, then create with correct schema
    local drop_query="DROP TABLE IF EXISTS ${table_name};"
    local create_query="CREATE TABLE ${table_name} (
    s SYMBOL,
    x CHAR,
    b FLOAT,
    a FLOAT,
    v SHORT,
    w SHORT,
    m BOOLEAN,
    T TIMESTAMP
) timestamp(T)
  PARTITION BY DAY WAL;"

    # Execute DROP
    local response
    response=$(curl -s -w "\n%{http_code}" -G "http://${host}:${port}/exec" \
        --data-urlencode "query=${drop_query}" 2>&1)
    local http_code=$(echo "$response" | tail -1)
    local body=$(echo "$response" | head -n -1)

    if [ "$http_code" != "200" ]; then
        echo "Warning: DROP TABLE returned HTTP $http_code (may not exist yet)"
        echo "Response: $body"
    fi

    # Execute CREATE
    response=$(curl -s -w "\n%{http_code}" -G "http://${host}:${port}/exec" \
        --data-urlencode "query=${create_query}" 2>&1)
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" = "200" ]; then
        echo "Table '${table_name}' created successfully."
        echo ""
        echo "Schema:"
        echo "  s SYMBOL, x CHAR, b FLOAT, a FLOAT,"
        echo "  v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP"
        echo "  PARTITION BY DAY WAL"
    else
        echo "ERROR: CREATE TABLE returned HTTP $http_code"
        echo "Response: $body"
        exit 1
    fi
}

case "$1" in
    server)
        check_jars
        shift

        DEBUG_FLAG=""
        for arg in "$@"; do
            if [[ "$arg" == "--debug" ]]; then
                DEBUG_FLAG="-Debug"
            fi
        done

        echo "Starting QuestDB server for STAC benchmark..."
        echo "Data directory: $QDB_ROOT"
        echo ""
        mkdir -p "$QDB_ROOT"
        java -p "$MAIN_JAR" $DEBUG_FLAG \
             -m io.questdb/io.questdb.ServerMain \
             -d "$QDB_ROOT"
        ;;

    create-table)
        shift
        TABLE=$(get_table_from_args "$@")
        create_table "$TABLE" "$QDB_HOST" "$QDB_HTTP_PORT"
        ;;

    client)
        check_jars
        shift

        DEBUG_FLAG=""
        CLIENT_ARGS=()
        for arg in "$@"; do
            if [[ "$arg" == "--debug" ]]; then
                DEBUG_FLAG="-Debug"
            else
                CLIENT_ARGS+=("$arg")
            fi
        done

        echo "Running STAC benchmark client..."
        java -cp "$MAIN_JAR:$TEST_JAR" $DEBUG_FLAG \
             $CLIENT_CLASS \
             "${CLIENT_ARGS[@]}"
        ;;

    profile)
        check_jars
        shift

        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-alloc-profile-${PROTOCOL}.collapsed"
        echo "Running STAC benchmark client with allocation profiling..."
        echo "Output: $PROFILE_OUTPUT (collapsed stacks format)"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=alloc,alloc=1k,file=$PROFILE_OUTPUT,collapsed" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@"
        echo ""
        echo "Profile saved to: $PROFILE_OUTPUT"
        echo ""
        echo "Top allocation sites:"
        sort -t' ' -k2 -rn "$PROFILE_OUTPUT" | head -30
        ;;

    cpu)
        check_jars
        shift

        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-cpu-profile-${PROTOCOL}.jfr"
        echo "Running STAC benchmark client with CPU profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=cpu,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@"
        echo ""
        echo "CPU profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-cpu-flame.html"
        ;;

    wall)
        check_jars
        shift

        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-wall-profile-${PROTOCOL}.jfr"
        echo "Running STAC benchmark client with wall-clock profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures time spent waiting (I/O, locks, sleep) in addition to CPU time."
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=wall,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@"
        echo ""
        echo "Wall-clock profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-wall-flame.html"
        ;;

    lock)
        check_jars
        shift

        ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-/home/jara/devel/tools/async-profiler-4.2.1-linux-x64}"

        if [ ! -f "$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so" ]; then
            echo "ERROR: async-profiler not found at: $ASYNC_PROFILER_HOME"
            echo "Set ASYNC_PROFILER_HOME or install from: https://github.com/async-profiler/async-profiler"
            exit 1
        fi

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-lock-profile-${PROTOCOL}.jfr"
        echo "Running STAC benchmark client with lock contention profiling..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures thread contention on synchronized blocks and locks."
        echo ""
        java -agentpath:"$ASYNC_PROFILER_HOME/lib/libasyncProfiler.so=start,event=lock,file=$PROFILE_OUTPUT,jfr" \
             -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@"
        echo ""
        echo "Lock contention profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASYNC_PROFILER_HOME/bin/asprof --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-lock-flame.html"
        ;;

    server-cpu)
        check_jars
        check_asprof
        shift

        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-server-cpu-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER CPU during STAC benchmark..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo ""

        "$ASPROF" start -e cpu -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@" || true

        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server CPU profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-server-cpu-flame.html"
        ;;

    server-wall)
        check_jars
        check_asprof
        shift

        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-server-wall-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER wall-clock during STAC benchmark..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures time spent waiting (I/O, locks, sleep) in addition to CPU time."
        echo ""

        "$ASPROF" start -e wall -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@" || true

        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server wall-clock profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-server-wall-flame.html"
        ;;

    server-alloc)
        check_jars
        check_asprof
        shift

        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-server-alloc-profile-${PROTOCOL}.collapsed"
        echo "Profiling SERVER allocations during STAC benchmark..."
        echo "Output: $PROFILE_OUTPUT (collapsed stacks format)"
        echo ""

        "$ASPROF" start -e alloc --alloc 1k -o collapsed -f "$PROFILE_OUTPUT" "$SERVER_PID"

        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@" || true

        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server allocation profile saved to: $PROFILE_OUTPUT"
        echo ""
        echo "Top allocation sites:"
        sort -t' ' -k2 -rn "$PROFILE_OUTPUT" | head -30
        ;;

    server-lock)
        check_jars
        check_asprof
        shift

        SERVER_PID=$(find_questdb_pid || true)
        if [ -z "$SERVER_PID" ]; then
            echo "ERROR: QuestDB server not found. Start it first with: $0 server"
            exit 1
        fi
        echo "Found QuestDB server PID: $SERVER_PID"

        PROTOCOL=$(get_protocol_from_args "$@")
        PROFILE_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-server-lock-profile-${PROTOCOL}.jfr"
        echo "Profiling SERVER lock contention during STAC benchmark..."
        echo "Output: $PROFILE_OUTPUT (JFR format)"
        echo "This captures thread contention on synchronized blocks and locks."
        echo ""

        "$ASPROF" start -e lock -o jfr -f "$PROFILE_OUTPUT" "$SERVER_PID"

        echo "Running client test..."
        java -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@" || true

        "$ASPROF" stop "$SERVER_PID"

        echo ""
        echo "Server lock contention profile saved to: $PROFILE_OUTPUT"
        echo "View with: jfr print $PROFILE_OUTPUT | head -100"
        echo "Or open in JDK Mission Control (jmc)"
        echo "Or convert to flamegraph: $ASPROF --jfrstackdepth 512 convert $PROFILE_OUTPUT -o ${PROFILE_PREFIX}-server-lock-flame.html"
        ;;

    jfr)
        check_jars
        shift

        PROTOCOL=$(get_protocol_from_args "$@")
        JFR_OUTPUT="$SCRIPT_DIR/${PROFILE_PREFIX}-${PROTOCOL}.jfr"
        echo "Running STAC benchmark client with JFR allocation profiling..."
        echo "Output: $JFR_OUTPUT"
        echo ""
        java -XX:StartFlightRecording=filename="$JFR_OUTPUT",settings=profile \
             -cp "$MAIN_JAR:$TEST_JAR" \
             $CLIENT_CLASS \
             "$@"
        echo ""
        echo "JFR recording saved to: $JFR_OUTPUT"
        echo "View with: jfr print --events jdk.ObjectAllocationInNewTLAB,jdk.ObjectAllocationOutsideTLAB $JFR_OUTPUT"
        echo "Or open in JDK Mission Control (jmc)"
        ;;

    compare)
        check_jars
        shift

        echo "Running STAC benchmark comparison across all protocols..."
        echo "Make sure QuestDB server is running!"
        echo ""

        for protocol in ilp-tcp ilp-http qwp-websocket; do
            echo "=========================================="
            echo "Testing: $protocol"
            echo "=========================================="
            java -cp "$MAIN_JAR:$TEST_JAR" \
                 $CLIENT_CLASS \
                 --protocol="$protocol" "$@"
            echo ""
        done
        ;;

    *)
        echo "STAC Benchmark Ingestion Test"
        echo ""
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  server                Start QuestDB server"
        echo "  create-table          Create the STAC quotes table (must be done before ingestion)"
        echo "  client [options]      Run test client"
        echo "  profile [options]     Run with async-profiler allocation tracking (collapsed format)"
        echo "  cpu [options]         Run with CPU profiling - find hot methods (JFR format)"
        echo "  wall [options]        Run with wall-clock profiling - find I/O waits and blocking (JFR format)"
        echo "  lock [options]        Run with lock contention profiling - find thread contention (JFR format)"
        echo "  jfr [options]         Run with Java Flight Recorder"
        echo "  compare [options]     Run all 3 protocols and compare"
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
        echo "  --table=TABLE            Table name (default: q)"
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
        echo "Table schema (created by 'create-table' command):"
        echo "  CREATE TABLE q ("
        echo "      s SYMBOL, x CHAR, b FLOAT, a FLOAT,"
        echo "      v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP"
        echo "  ) timestamp(T) PARTITION BY DAY WAL;"
        echo ""
        echo "Environment variables:"
        echo "  QDB_HOST                 QuestDB host for table creation (default: localhost)"
        echo "  QDB_HTTP_PORT            QuestDB HTTP port for table creation (default: 9000)"
        echo "  ASYNC_PROFILER_HOME      Path to async-profiler installation"
        echo ""
        echo "Quick start:"
        echo "  Terminal 1: $0 server"
        echo "  Terminal 2: $0 create-table"
        echo "  Terminal 2: $0 client --rows=10000000"
        echo ""
        echo "Examples:"
        echo "  $0 client --protocol=qwp-websocket --rows=80000000 --batch=10000"
        echo "  $0 client --protocol=ilp-tcp --rows=10000000"
        echo "  $0 cpu --rows=80000000 --warmup=1000000"
        echo "  $0 server-cpu --rows=80000000 --warmup=1000000"
        echo "  $0 compare --rows=10000000 --batch=10000"
        ;;
esac