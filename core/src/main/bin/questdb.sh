#!/usr/bin/env bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2026 QuestDB
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
################################################################################

function read_link {
    f=$(readlink $1)
    if [ "$f" != "" ]; then

        if [[ "$f" != /* ]]; then
            f="$(dirname $1)/$f"
            f="$(cd $(dirname ${f}); pwd)/$(basename ${f})"
        fi

        n=$(read_link ${f})
        if [ "$n" != "" ]; then
             f=${n};
        fi
    fi
    echo "$f"
}

LINK=$(read_link $0)
if [ "$LINK" != "" ]; then
    BASE=$(dirname ${LINK})
else
    BASE=$(dirname $0)
fi

source "$BASE/env.sh"

export QDB_PROCESS_LABEL="QuestDB-Runtime-66535"
export QDB_MAX_STOP_ATTEMPTS=60;
export QDB_OS=`uname`

# Async profiler configuration
export PROFILER_EVENT="${PROFILER_EVENT:-cpu,wall}"           # cpu, alloc, lock, wall, itimer
export PROFILER_INTERVAL="${PROFILER_INTERVAL:-5ms}"          # sampling interval
export PROFILER_ALLOC_INTERVAL="${PROFILER_ALLOC_INTERVAL:-512k}"
export PROFILER_LOCK_THRESHOLD="${PROFILER_LOCK_THRESHOLD:-10ms}"
export PROFILE_NAME="${PROFILE_NAME:-"profile-%n{48}.jfr"}"     # %n{MAX} = seq#
export PROFILER_LOOP="${PROFILER_LOOP:-30m}"
export PROFILER_EXTRA_OPTS="${PROFILER_EXTRA_OPTS:-}"         # comma-separated, no spaces

# NOTE:
# - PROFILER_EVENT accepts comma-separated tokens (no spaces). Any spaces will be stripped at runtime.
# - PROFILER_EXTRA_OPTS must be a comma-separated list without spaces (e.g., "threads,cstack=vm").

case $QDB_OS in
   Darwin|FreeBSD)
       export PS_CMD="ps aux"
       if [ -d "/usr/local/var/questdb" ] || [ "$(id -u)" = "0" ]; then
           export QDB_DEFAULT_ROOT="/usr/local/var/questdb"
       else
           export QDB_DEFAULT_ROOT="$HOME/.questdb"
       fi
       ;;
   *)
       export PS_CMD="ps -ef"
       export QDB_DEFAULT_ROOT="$HOME/.questdb"
       ;;
esac

function usage {
    echo "Usage: $0 start|status|stop|recover [-f] [-n] [-p] [-d path] [-t tag] [-- agent-params]"
    echo "       $0 profile [-t tag] -- [profiler-args]"
    echo ""
    echo "Commands:"
    echo "  start    Start QuestDB server"
    echo "  status   Check if QuestDB is running"
    echo "  stop     Stop QuestDB server"
    echo "  recover  Start offline recovery mode"
    echo "  profile  Profile a running QuestDB instance with async-profiler"
    echo ""
    echo "Options:"
    echo "  -f    Force overwrite public directory"
    echo "  -n    Disable HUP handler"
    echo "  -p    Enable async profiler at startup - continuous profiling"
    echo "  -d    Set QuestDB root directory"
    echo "  -t    Set process tag for identification"
    echo ""
    echo "Profiling modes:"
    echo "  1. Attach to running instance (profile command):"
    echo "     - Uses asprof CLI tool to attach to an already running QuestDB"
    echo "     - Pass asprof command-line arguments after '--'"
    echo "     Example: $0 profile -t mydb -- -e cpu -d 30 -f /tmp/profile.html"
    echo ""
    echo "  2. Continuous profiling (start -p):"
    echo "     - Starts QuestDB with profiler agent loaded from startup"
    echo "     - Pass JVM agent parameters after '--'"
    echo "     Example: $0 start -p -- start,event=cpu,file=/tmp/profile.jfr,interval=10ms"
    echo
    exit 55
}

function recover {
    export_java

    JAVA_LIB="$BASE/questdb.jar"
    RECOVERY_MAIN="io.questdb/io.questdb.RecoveryMain"

    LIB_DIR_CANDIDATE="$BASE/../lib"
    if [ -d "$LIB_DIR_CANDIDATE" ]; then
        if command -v realpath >/dev/null 2>&1; then
            LIB_DIR=$(realpath "$LIB_DIR_CANDIDATE")
        else
            LIB_DIR=$(cd "$LIB_DIR_CANDIDATE" && pwd)
        fi

        if [ -r "$LIB_DIR" ]; then
            LIB_DIR_PROP="-Dquestdb.libs.dir=${LIB_DIR}"
        else
            echo "Warning: Library directory '${LIB_DIR}' found, but it is not readable." >&2
        fi
    fi

    JAVA_OPTS="
    ${LIB_DIR_PROP}
    -ea -Dnoebug
    ${JVM_PREPEND}
    "

    ${JAVA} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${RECOVERY_MAIN} -d ${QDB_ROOT}
}

function export_pid {
    export QDB_PID=`${PS_CMD} | grep ${QDB_PROCESS_LABEL} | grep -v grep | tr -s " " | cut -d " " -f 2`
}

function export_java {

    # check if this is our runtime
    if [ -f "$BASE/java" ]; then
        export QDB_PACKAGE=withjre
        export JAVA="$BASE/java"
    else
      export QDB_PACKAGE=withoutjre
      if [ "$JAVA_HOME" = "" -a -e /usr/libexec/java_home ]; then
          JAVA_HOME=$(/usr/libexec/java_home -v 11)
      fi

      # check that JAVA_HOME is defined
      if [ "$JAVA_HOME" = "" ]; then
          echo "JAVA_HOME is undefined"
          exit 55
      fi

      # check that Java binary is executable
      export JAVA=${JAVA_HOME}/bin/java
    fi

    if [ ! -x "$JAVA" ]; then
        echo "$JAVA is not executable"
        exit 55;
    fi

    echo "JAVA: $JAVA"
}

function export_jemalloc() {
    local arch=$(uname -m)

    # auto-enable jemalloc on Linux x86_64 for open source edition with bundled JRE
    # if QDB_JEMALLOC is not explicitly set
    if [[ -z "${QDB_JEMALLOC+x}" ]]; then
        if [[ "$QDB_OS" == "Linux" && "$arch" == "x86_64" && "$JAVA_MAIN" == *"io.questdb"* && "$QDB_PACKAGE" == "withjre" ]]; then
            QDB_JEMALLOC="true"
        fi
    fi

    if [[ "$QDB_JEMALLOC" = "true" ]]; then
      if [[ "$QDB_OS" != "Linux" ]]; then
          echo "Error: QDB_JEMALLOC is enabled but jemalloc is only supported on Linux (detected OS: $QDB_OS)"
          echo "QuestDB works with the default system allocator on this platform."
          echo ""
          echo "To disable jemalloc, either:"
          echo "  - Unset the environment variable: unset QDB_JEMALLOC"
          echo "  - Set the environment variable to false: export QDB_JEMALLOC=false"
          echo "  - Remove or comment out 'export QDB_JEMALLOC=true' from your env.sh or shell profile"
          exit 55
      fi
      jemalloc_so=$(ls $BASE/libjemalloc.so* 2>/dev/null | head -1)
      if [[ -r "${jemalloc_so}" ]]; then
          export QDB_JEMALLOC_LIB=${jemalloc_so}
          echo "Using jemalloc"
      else
          echo "Error: QDB_JEMALLOC is enabled but jemalloc library not found in ${BASE}"
          echo "Your QuestDB distribution may not include jemalloc."
          echo "QuestDB works with the default system allocator too."
          echo ""
          echo "To disable jemalloc, either:"
          echo "  - Unset the environment variable: unset QDB_JEMALLOC"
          echo "  - Set the environment variable to false: export QDB_JEMALLOC=false"
          echo "  - Remove or comment out 'export QDB_JEMALLOC=true' from your env.sh or shell profile"
          exit 55
      fi
    fi
}

function export_args {

    export QDB_OVERWRITE_PUBLIC=""
    export QDB_DISABLE_HUP_HANDLER=""
    export QDB_CONTAINER_MODE=""
    export QDB_PROFILING_ENABLED=""
    export QDB_ROOT=${QDB_DEFAULT_ROOT}

    while [[ $# -gt 0 ]]; do
        key="$1"

        case ${key} in
            -f)
                export QDB_OVERWRITE_PUBLIC="-f"
                ;;
            -n)
                export QDB_DISABLE_HUP_HANDLER="-n"
                ;;
            -c)
                export QDB_CONTAINER_MODE="-c"
                ;;
            -p)
                export QDB_PROFILING_ENABLED="true"
                ;;
            -d)
                if [[ $# -eq 1 ]]; then
                    echo "Expected: -d <path>"
                    exit 55
                fi
                export QDB_ROOT="$2"
                shift
                ;;
            -t)
                if [[ $# -eq 1 ]]; then
                    echo "Expected: -t <tag>"
                    exit 55
                fi
                export QDB_PROCESS_LABEL="QuestDB-Runtime-$2"
                shift
                ;;
            *)
                echo "Unexpected option: $key"
                usage
                ;;
        esac
        shift
    done
}

function start {

    export_pid

    if [[ "${QDB_PID}" != "" ]]; then
        echo "Already running. PID: ${QDB_PID}"
        exit 55
    fi

    export_java
    export_jemalloc

    # Check for async profiler if profiling is enabled
    if [ "$QDB_PROFILING_ENABLED" = "true" ]; then
        LIB_DIR_CANDIDATE="$BASE/../lib"
        if [ -d "$LIB_DIR_CANDIDATE" ]; then
            if command -v realpath >/dev/null 2>&1; then
                PROFILER_LIB_DIR=$(realpath "$LIB_DIR_CANDIDATE")
            else
                # fallback for systems without 'realpath'
                PROFILER_LIB_DIR=$(cd "$LIB_DIR_CANDIDATE" && pwd)
            fi
        else
            echo "Error: Library directory not found: ${LIB_DIR_CANDIDATE}"
            echo "Profiling was requested with -p flag but the lib directory is missing."
            exit 55
        fi

        ASYNC_PROFILER_LIB="$PROFILER_LIB_DIR/libasyncProfiler.so"
        if [ ! -f "$ASYNC_PROFILER_LIB" ]; then
            echo "Error: Async profiler library not found at: $ASYNC_PROFILER_LIB"
            echo "Profiling was requested with -p flag but the profiler library is missing."
            exit 55
        fi
        echo "Async profiler enabled: $ASYNC_PROFILER_LIB"
    fi

    # create root directory if it does not exist
    if [ ! -d "$QDB_ROOT" ]; then
        echo "Created QuestDB ROOT directory: $QDB_ROOT"
        mkdir -p "${QDB_ROOT}"
    fi

    QDB_LOG="${QDB_ROOT}/log"
    mkdir -p "${QDB_LOG}"

    if [ "$QDB_PROFILING_ENABLED" = "true" ]; then
        QDB_PROFILES="${QDB_ROOT}/profiles"
        mkdir -p "${QDB_PROFILES}"
        echo "Profile output directory: ${QDB_PROFILES}"
    fi

    JAVA_LIB="$BASE/questdb.jar"

    # if we are a full platform-specific distribution
    # we should load binary libraries from the lib directory instead of copying them to a temporary directory
    # how? we set a system property that points to the lib directory
    # and the Java code will try load libraries from there
    LIB_DIR_CANDIDATE="$BASE/../lib"
    if [ -d "$LIB_DIR_CANDIDATE" ]; then
        # resolve the absolute path to the lib directory
        if command -v realpath >/dev/null 2>&1; then
            LIB_DIR=$(realpath "$LIB_DIR_CANDIDATE")
        else
            # fallback for systems without 'realpath'
            LIB_DIR=$(cd "$LIB_DIR_CANDIDATE" && pwd)
        fi

        if [ -r "$LIB_DIR" ]; then
            LIB_DIR_PROP="-Dquestdb.libs.dir=${LIB_DIR}"
        else
            echo "Warning: Library directory '${LIB_DIR}' found, but it is not readable." >&2
        fi
    fi

    PROFILER_AGENT=""
    if [ "$QDB_PROFILING_ENABLED" = "true" ]; then
        if [ "$PROFILER_CUSTOM_PARAMS" != "" ]; then
            # Use custom parameters provided via command line
            AGENT_PARAMS="$PROFILER_CUSTOM_PARAMS"
            echo "Using custom profiler parameters: ${AGENT_PARAMS}"
        else
            # Build parameters from environment variables
            PROFILE_FILE="${QDB_PROFILES}/${PROFILE_NAME}"

            # Normalize events: strip all whitespace to safely handle "cpu, wall" inputs
            PROFILER_EVENT_NOSPACE="${PROFILER_EVENT//[[:space:]]/}"
            AGENT_PARAMS="start,event=${PROFILER_EVENT_NOSPACE}"
            AGENT_PARAMS="${AGENT_PARAMS},file=${PROFILE_FILE}"
            AGENT_PARAMS="${AGENT_PARAMS},jfr"  # Enable JFR format output
            AGENT_PARAMS="${AGENT_PARAMS},loop=${PROFILER_LOOP}"

            if [[ ",$PROFILER_EVENT_NOSPACE," == *",cpu,"* ]] || [[ ",$PROFILER_EVENT_NOSPACE," == *",wall,"* ]] || [[ ",$PROFILER_EVENT_NOSPACE," == *",itimer,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},interval=${PROFILER_INTERVAL}"
            fi
            if [[ ",$PROFILER_EVENT_NOSPACE," == *",alloc,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},alloc=${PROFILER_ALLOC_INTERVAL}"
            fi
            if [[ ",$PROFILER_EVENT_NOSPACE," == *",lock,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},lock=${PROFILER_LOCK_THRESHOLD}"
            fi

            if [ -n "$PROFILER_EXTRA_OPTS" ]; then
                if [[ "$PROFILER_EXTRA_OPTS" == *" "* ]]; then
                    echo "Error: PROFILER_EXTRA_OPTS must be comma-separated without spaces." >&2
                    exit 55
                fi
                AGENT_PARAMS="${AGENT_PARAMS},${PROFILER_EXTRA_OPTS}"
            fi

            echo "Profiler parameters: ${AGENT_PARAMS}"
            echo "Continuous profiling enabled: new profile every ${PROFILER_LOOP}"
        fi

        PROFILER_AGENT="-agentpath:${ASYNC_PROFILER_LIB}=${AGENT_PARAMS}"
    fi

    JAVA_OPTS="
    -D$QDB_PROCESS_LABEL
    -Dcontainerized=$([ "${QDB_CONTAINER_MODE}" != '' ] && echo "true" || echo "false" )
    ${LIB_DIR_PROP}
    -ea -Dnoebug
    -XX:ErrorFile=${QDB_ROOT}/db/hs_err_pid+%p.log
    -XX:+UnlockExperimentalVMOptions
    -XX:+AlwaysPreTouch
    -XX:+UseParallelGC
    ${PROFILER_AGENT}
    ${JVM_PREPEND}
    "

    if [ "$QDB_OS" == "Darwin" ]; then
        # JVM on MacOS has its own max open files limit, set to 10240
        # This limit can be removed by passing the -XX:-MaxFDLimit option
        # However, if this built-in limit is removed, the JVM starts to use the soft limit as if it was the hard limit,
        # so we should set the soft limit to the same value as the hard limit
        ulimit -n $(ulimit -H -n)
        JAVA_OPTS="$JAVA_OPTS 
        -XX:-MaxFDLimit
        "
    fi

    DATE=`date +%Y-%m-%dT%H-%M-%S`
    HELLO_FILE=${QDB_ROOT}/hello.txt
    rm ${HELLO_FILE} 2> /dev/null

    # Set LD_PRELOAD only for the Java process, not the shell
    JAVA_CMD="${JAVA}"
    if [ -n "${QDB_JEMALLOC_LIB}" ]; then
        JAVA_CMD="env LD_PRELOAD=${QDB_JEMALLOC_LIB} ${JAVA}"
    fi

    if [ "${QDB_CONTAINER_MODE}" != "" ]; then
        ${JAVA_CMD} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1
    elif [ "${QDB_DISABLE_HUP_HANDLER}" = "" ]; then
        ${JAVA_CMD} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1 &
        $BASE/print-hello.sh ${HELLO_FILE}
    else
        $BASE/print-hello.sh ${HELLO_FILE} &
        ${JAVA_CMD} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} ${QDB_DISABLE_HUP_HANDLER} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1
    fi
}

function query {
    export_pid

    if [[ "${QDB_PID}" = "" ]]; then
        echo "Not running"
    else
        echo "PID: ${QDB_PID}"
    fi
}

function stop {
    export_pid
    if [[ "${QDB_PID}" = "" ]]; then
        echo "Not running"
        exit 55
    fi

    OUR_PID=${QDB_PID}

    echo "Stopping ${OUR_PID}"

    count=${QDB_MAX_STOP_ATTEMPTS}
    while [ "${QDB_PID}" != "" ] && [ ${count} -gt 0 ]; do
        kill ${QDB_PID}
        sleep 0.5
        export_pid
        count=$((count-1))
    done

    if [[ "${QDB_PID}" != "" ]]; then
        kill -9 ${QDB_PID}
        echo "Something is wrong. Process does not stop. Killing..."
        export_pid
    fi

    if [[ "${QDB_PID}" != "" ]]; then
        echo "Cannot stop ${QDB_PID}. Check permissions."
    else
        echo "Stopped ${OUR_PID}"
    fi
}

function profile {
    export_pid

    if [[ "${QDB_PID}" = "" ]]; then
        echo "Not running"
        exit 55
    fi

    # Find asprof binary and library
    LIB_DIR_CANDIDATE="$BASE/../lib"
    if [ -d "$LIB_DIR_CANDIDATE" ]; then
        if command -v realpath >/dev/null 2>&1; then
            PROFILER_LIB_DIR=$(realpath "$LIB_DIR_CANDIDATE")
        else
            # fallback for systems without 'realpath'
            PROFILER_LIB_DIR=$(cd "$LIB_DIR_CANDIDATE" && pwd)
        fi
    else
        echo "Error: Library directory not found: $LIB_DIR_CANDIDATE"
        exit 55
    fi

    ASPROF_BIN="$PROFILER_LIB_DIR/asprof"
    ASYNC_PROFILER_LIB="$PROFILER_LIB_DIR/libasyncProfiler.so"

    if [ ! -f "$ASPROF_BIN" ]; then
        echo "Error: asprof not found at: $ASPROF_BIN"
        echo "Please ensure async-profiler binaries are installed in the lib directory."
        exit 55
    fi

    if [ ! -x "$ASPROF_BIN" ]; then
        echo "Error: asprof is not executable: $ASPROF_BIN"
        exit 55
    fi

    if [ ! -f "$ASYNC_PROFILER_LIB" ]; then
        echo "Error: Async profiler library not found at: $ASYNC_PROFILER_LIB"
        echo "Please ensure async-profiler is installed."
        exit 55
    fi

    echo "Attaching profiler to QuestDB PID: ${QDB_PID}"
    echo "Using: $ASPROF_BIN"

    # Execute asprof with all profiler arguments passed after '--'
    # PROFILER_ARGS is an array set during arg parsing
    "${ASPROF_BIN}" "${PROFILER_ARGS[@]}" "${QDB_PID}"
}

if [[ $# -gt 0 ]]; then
    command=$1
    shift

    # Special handling for profile command
    if [[ "${command}" = "profile" ]]; then
        # Parse questdb.sh arguments until we hit '--'
        PROFILER_ARGS=()
        while [[ $# -gt 0 ]]; do
            key="$1"
            case ${key} in
                -t)
                    if [[ $# -eq 1 ]]; then
                        echo "Expected: -t <tag>"
                        exit 55
                    fi
                    export QDB_PROCESS_LABEL="QuestDB-Runtime-$2"
                    shift
                    shift
                    ;;
                --)
                    # Everything after '--' goes to asprof
                    shift
                    PROFILER_ARGS=("$@")
                    break
                    ;;
                *)
                    echo "Unexpected option for profile: $key"
                    echo "Use: $0 profile [-t tag] -- [profiler-args]"
                    exit 55
                    ;;
            esac
        done
        profile
    # Special handling for start command to capture custom profiler parameters
    elif [[ "${command}" = "start" ]]; then
        # Look for -- delimiter in start arguments
        PROFILER_CUSTOM_PARAMS=""
        START_ARGS=()

        while [[ $# -gt 0 ]]; do
            if [[ "$1" = "--" ]]; then
                shift
                PROFILER_CUSTOM_PARAMS="$@"
                break
            else
                START_ARGS+=("$1")
                shift
            fi
        done

        # Process normal start arguments
        export_args "${START_ARGS[@]}"
        start
    elif [[ "${command}" = "recover" ]]; then
        export_args $@
        recover
    else
        # Normal argument parsing for other commands
        export_args $@

        case ${command} in
           status)
            query
            ;;
           stop)
            stop
            ;;
           *)
            usage
            ;;
        esac
    fi
else
    usage
fi
