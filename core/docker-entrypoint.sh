#!/usr/bin/env bash
set -Eeo pipefail
export QDB_PACKAGE=${QDB_PACKAGE:-docker}

QUESTDB_DATA_DIR=${QUESTDB_DATA_DIR:-"/var/lib/questdb"}
IGNORE_DATA_ROOT_MOUNT_CHECK=${IGNORE_DATA_ROOT_MOUNT_CHECK:-"false"}
RUN_AS_ROOT=${RUN_AS_ROOT:-"false"}
DO_CHOWN=${DO_CHOWN:-"true"}
QUESTDB_UID="${QUESTDB_UID:-"$(id -u questdb)"}"
QUESTDB_GID="${QUESTDB_GID:-"$(id -g questdb)"}"
JAVA_COMMAND="/app/bin/java"
QUESTDB_LIBS_DIR_PROP="-Dquestdb.libs.dir=/app/lib/"

# Async profiler configuration
PROFILER_ENABLED="${PROFILER_ENABLED:-false}"
PROFILER_CUSTOM_PARAMS="${PROFILER_CUSTOM_PARAMS:-}"   # Override all params if set
PROFILER_EVENT="${PROFILER_EVENT:-cpu,wall}"           # cpu, alloc, lock, wall, itimer
PROFILER_INTERVAL="${PROFILER_INTERVAL:-5ms}"          # sampling interval
PROFILER_ALLOC_INTERVAL="${PROFILER_ALLOC_INTERVAL:-512k}"
PROFILER_LOCK_THRESHOLD="${PROFILER_LOCK_THRESHOLD:-10ms}"
PROFILE_NAME="${PROFILE_NAME:-profile-%n{48}.jfr}"     # %n{MAX} = seq#
PROFILER_LOOP="${PROFILER_LOOP:-30m}"
PROFILER_EXTRA_OPTS="${PROFILER_EXTRA_OPTS:-}"         # comma-separated, no spaces
PROFILER_OUTPUT_DIR="${PROFILER_OUTPUT_DIR:-${QUESTDB_DATA_DIR}/profiles}"

# directories inside QUESTDB_DATA_DIR that we will chown
DEFAULT_LOCAL_DIRS=${DEFAULT_LOCAL_DIRS:-"/conf /public /db /.checkpoint /snapshot"}
# Add profiles directory to chown list only if profiler is enabled
if [ "$PROFILER_ENABLED" = "true" ]; then
    DEFAULT_LOCAL_DIRS="${DEFAULT_LOCAL_DIRS} /profiles"
fi
array=( ${DEFAULT_LOCAL_DIRS} )
read -ra LOCALDIRS < <( echo -n "( "; printf -- "-ipath ${QUESTDB_DATA_DIR}%s* -o " "${array[@]:0:$((${#array[@]} - 1))}"; echo -n "-ipath ${QUESTDB_DATA_DIR}${array[@]: -1}*"; echo " )";)

# backwards compatibility with previous versions
if [ ${IGNORE_FIND_AND_OWN_DIR+x} ]
then
    DO_CHOWN=$IGNORE_FIND_AND_OWN_DIR
fi

find_and_own_dir() {
    local USER=$1
    local GROUP=$2
    [ $(stat --format '%u:%g' ${QUESTDB_DATA_DIR}) == "$USER:$GROUP" ] || chown "$USER:$GROUP" ${QUESTDB_DATA_DIR}
    find ${QUESTDB_DATA_DIR} "${LOCALDIRS[@]}" \( ! -user $USER -o ! -group $GROUP \) -exec chown $USER:$GROUP '{}' \;
}

# Temporary only
# Most of the users will have the data mounted under /root/.questdb as default
# we will run as root for them until they change the mount to /var/lib/questdb or something else
if [ "$IGNORE_DATA_ROOT_MOUNT_CHECK" = "false" ] && mount | grep "/root/.questdb" -q; then
    echo "Found /root/.questdb mount, overwriting QUESTDB_DATA_DIR"
    QUESTDB_DATA_DIR="/root/.questdb"
fi

# Check if on-demand JVM arguments are provided through environment variable
if [ -n "$JVM_PREPEND" ]; then
    echo "Found on-demand JVM arguments: $JVM_PREPEND, prepending to JVM args"
    JAVA_COMMAND="$JAVA_COMMAND $JVM_PREPEND"
fi

# Setup async profiler if enabled
PROFILER_AGENT=""
if [ "$PROFILER_ENABLED" = "true" ]; then
    ASYNC_PROFILER_LIB="/app/lib/libasyncProfiler.so"
    if [ ! -f "$ASYNC_PROFILER_LIB" ]; then
        echo "Warning: Async profiler library not found at: $ASYNC_PROFILER_LIB"
        echo "Profiling was requested but the profiler library is missing. Continuing without profiling."
    else
        echo "Async profiler enabled: $ASYNC_PROFILER_LIB"

        # Create profiles directory if it doesn't exist
        mkdir -p "${PROFILER_OUTPUT_DIR}"
        echo "Profile output directory: ${PROFILER_OUTPUT_DIR}"

        if [ -n "$PROFILER_CUSTOM_PARAMS" ]; then
            # Use custom parameters provided via environment variable
            AGENT_PARAMS="$PROFILER_CUSTOM_PARAMS"
            echo "Using custom profiler parameters: ${AGENT_PARAMS}"
        else
            # Build profiler agent parameters from individual settings
            PROFILE_FILE="${PROFILER_OUTPUT_DIR}/${PROFILE_NAME}"

            # Normalize events: strip all whitespace to safely handle "cpu, wall" inputs
            PROFILER_EVENT_NOSPACE="${PROFILER_EVENT//[[:space:]]/}"
            AGENT_PARAMS="start,event=${PROFILER_EVENT_NOSPACE}"
            AGENT_PARAMS="${AGENT_PARAMS},file=${PROFILE_FILE}"
            AGENT_PARAMS="${AGENT_PARAMS},jfr"  # Enable JFR format output
            AGENT_PARAMS="${AGENT_PARAMS},loop=${PROFILER_LOOP}"

            # Add interval for CPU/wall/itimer events
            if [[ ",$PROFILER_EVENT_NOSPACE," == *",cpu,"* ]] || [[ ",$PROFILER_EVENT_NOSPACE," == *",wall,"* ]] || [[ ",$PROFILER_EVENT_NOSPACE," == *",itimer,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},interval=${PROFILER_INTERVAL}"
            fi
            # Add alloc interval for allocation profiling
            if [[ ",$PROFILER_EVENT_NOSPACE," == *",alloc,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},alloc=${PROFILER_ALLOC_INTERVAL}"
            fi
            # Add lock threshold for lock profiling
            if [[ ",$PROFILER_EVENT_NOSPACE," == *",lock,"* ]]; then
                AGENT_PARAMS="${AGENT_PARAMS},lock=${PROFILER_LOCK_THRESHOLD}"
            fi

            # Add extra options if provided
            if [ -n "$PROFILER_EXTRA_OPTS" ]; then
                if [[ "$PROFILER_EXTRA_OPTS" == *" "* ]]; then
                    echo "Error: PROFILER_EXTRA_OPTS must be comma-separated without spaces."
                    exit 1
                fi
                AGENT_PARAMS="${AGENT_PARAMS},${PROFILER_EXTRA_OPTS}"
            fi

            echo "Profiler parameters: ${AGENT_PARAMS}"
            echo "Continuous profiling enabled: new profile every ${PROFILER_LOOP}"
        fi

        PROFILER_AGENT="-agentpath:${ASYNC_PROFILER_LIB}=${AGENT_PARAMS}"
    fi
fi

# Check if arguments are provided in the configuration file
if [ $# -eq 0 ]; then
    echo "No arguments found in the configuration, start with default arguments"
    set -- $JAVA_COMMAND $QUESTDB_LIBS_DIR_PROP -ea -Dnoebug -XX:+UseParallelGC ${PROFILER_AGENT} -XX:ErrorFile=${QUESTDB_DATA_DIR}/db/hs_err_pid+%p.log -Dout=${QUESTDB_DATA_DIR}/conf/log.conf -m io.questdb/io.questdb.ServerMain -d ${QUESTDB_DATA_DIR} -f
else
    if [ "${1:0:1}" = '-' ]; then
        echo "Found config arguments $@"
        set -- $JAVA_COMMAND $QUESTDB_LIBS_DIR_PROP ${PROFILER_AGENT} "$@"
    elif [ "$1" = "/app/bin/java" ]; then
        echo "Java binary argument found in command, ignoring on-demand JVM arguments, start with fully-customized arguments"
        # we still need to set the libs dir property
        # so that the Java code can find the native libraries
        java_bin=$1
        shift
        set -- "$java_bin" $QUESTDB_LIBS_DIR_PROP "$@"
    fi
fi

if [ "$(id -u)" = '0' ] && [ "${QUESTDB_DATA_DIR%/}" != "/root/.questdb" ] && [ "$RUN_AS_ROOT" = "false" ] ; then
    if [ "$DO_CHOWN" = "true" ]; then
        echo "Checking data directory ownership"
        find_and_own_dir $QUESTDB_UID $QUESTDB_GID
    fi

    if [ -x "$(command -v gosu)" ] ; then
      echo "Running as questdb user"
      exec gosu $QUESTDB_UID:$QUESTDB_GID "$@"
    fi
fi

echo "Running as $(id -un 2>/dev/null) user"
exec "$@"
