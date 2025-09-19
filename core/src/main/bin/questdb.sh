#!/usr/bin/env bash

################################################################################
#     ___                  _   ____  ____
#    / _ \ _   _  ___  ___| |_|  _ \| __ )
#   | | | | | | |/ _ \/ __| __| | | |  _ \
#   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#    \__\_\\__,_|\___||___/\__|____/|____/
#
#  Copyright (c) 2014-2019 Appsicle
#  Copyright (c) 2019-2024 QuestDB
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
export PROFILER_EVENT="cpu,wall"              # Event to profile: cpu, alloc, lock, wall, itimer
export PROFILER_INTERVAL="5ms"          # Sampling interval
export PROFILER_ALLOC_INTERVAL="512k"    # Allocation profiling interval (for alloc event)
export PROFILER_LOCK_THRESHOLD="10ms"     # Lock profiling threshold (for lock event)
export PROFILE_NAME="profile-%n{48}.jfr"     # Profile file name; %n{MAX} is expanded as a sequence number
export PROFILER_LOOP="30m"                # Loop duration for continuous profiling (e.g., 1h, 30m, 86400s)
export PROFILER_EXTRA_OPTS=""             # Additional profiler options

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
    echo "Usage: $0 start|status|stop [-f] [-n] [-p] [-d path] [-t tag]"
    echo "  -f    Force overwrite public directory"
    echo "  -n    Disable HUP handler"
    echo "  -p    Enable async profiler (requires libasyncProfiler.so)"
    echo "  -d    Set QuestDB root directory"
    echo "  -t    Set process tag for identification"
    echo
    exit 55
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
    if [[ "$QDB_JEMALLOC" = "true" ]]; then
      jemalloc_so=$(ls $BASE/libjemalloc*)
      if [[ "$QDB_OS" != "FreeBSD" && -r "${jemalloc_so}" ]]; then
          if [[ "$QDB_OS" == "Darwin" ]]; then
              export DYLD_INSERT_LIBRARIES=${jemalloc_so}
          else
              export LD_PRELOAD=${jemalloc_so}
          fi
          echo "Using jemalloc"
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
            echo "Error: Library directory not found: $LIB_DIR_CANDIDATE"
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
        mkdir -p ${QDB_ROOT}
    fi

    QDB_LOG=${QDB_ROOT}/log
    mkdir -p ${QDB_LOG}

    if [ "$QDB_PROFILING_ENABLED" = "true" ]; then
        QDB_PROFILES=${QDB_ROOT}/profiles
        mkdir -p ${QDB_PROFILES}
        echo "Profile output directory: $QDB_PROFILES"
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
        PROFILE_FILE="${QDB_PROFILES}/${PROFILE_NAME}"

        AGENT_PARAMS="start,event=${PROFILER_EVENT}"
        AGENT_PARAMS="${AGENT_PARAMS},file=${PROFILE_FILE}"
        AGENT_PARAMS="${AGENT_PARAMS},jfr"  # Enable JFR format output
        AGENT_PARAMS="${AGENT_PARAMS},loop=${PROFILER_LOOP}"

        if [[ ",$PROFILER_EVENT," == *",cpu,"* ]] || [[ ",$PROFILER_EVENT," == *",wall,"* ]] || [[ ",$PROFILER_EVENT," == *",itimer,"* ]]; then
            AGENT_PARAMS="${AGENT_PARAMS},interval=${PROFILER_INTERVAL}"
        fi
        if [[ ",$PROFILER_EVENT," == *",alloc,"* ]]; then
            AGENT_PARAMS="${AGENT_PARAMS},alloc=${PROFILER_ALLOC_INTERVAL}"
        fi
        if [[ ",$PROFILER_EVENT," == *",lock,"* ]]; then
            AGENT_PARAMS="${AGENT_PARAMS},lock=${PROFILER_LOCK_THRESHOLD}"
        fi

        if [ "$PROFILER_EXTRA_OPTS" != "" ]; then
            AGENT_PARAMS="${AGENT_PARAMS},${PROFILER_EXTRA_OPTS}"
        fi

        PROFILER_AGENT="-agentpath:${ASYNC_PROFILER_LIB}=${AGENT_PARAMS}"
        echo "Profiler parameters: ${AGENT_PARAMS}"
        echo "Continuous profiling enabled: new profile every ${PROFILER_LOOP}"
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
    if [ "${QDB_CONTAINER_MODE}" != "" ]; then
        ${JAVA} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1
    elif [ "${QDB_DISABLE_HUP_HANDLER}" = "" ]; then
        ${JAVA} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1 &
        $BASE/print-hello.sh ${HELLO_FILE}
    else
        $BASE/print-hello.sh ${HELLO_FILE} &
        ${JAVA} ${JAVA_OPTS} -p ${JAVA_LIB} -m ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} ${QDB_DISABLE_HUP_HANDLER} > "${QDB_LOG}/stdout-${DATE}.txt" 2>&1
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

if [[ $# -gt 0 ]]; then
    command=$1
    shift

    export_args $@

    case ${command} in
       start)
        start
        ;;
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
else
    usage
fi
