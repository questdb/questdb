#!/bin/bash

export QDB_PROCESS_LABEL="QuestDB-Runtime-66535"
export QDB_MAX_STOP_ATTEMPTS=5;
export QDB_OS=`uname`

case `uname` in
   Darwin)
       export PID_FIELD=3
       export QDB_DEFAULT_ROOT="/usr/local/var/questdb"
       ;;
   *)
       export PID_FIELD=2
       export QDB_DEFAULT_ROOT="$HOME/.questdb"
       ;;
esac

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

function usage {
    echo "Usage: $0 start|status|stop [-f] [-d path]"
    echo
    exit 55
}

function export_pid {
    export QDB_PID=`ps -ef | grep ${QDB_PROCESS_LABEL} | grep -v grep | tr -s " " | cut -d " " -f ${PID_FIELD}`
}

function export_java {

    if [ "$JAVA_HOME" = "" -a -e /usr/libexec/java_home ]; then
        JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
    fi

    # check that JAVA_HOME is defined
    if [ "$JAVA_HOME" = "" ]; then
        echo "JAVA_HOME is undefined"
        exit 55
    fi

    # check that Java binary is executable
    export JAVA=${JAVA_HOME}/bin/java
    if [ ! -x "$JAVA" ]; then
        echo "$JAVA is not executable"
        exit 55;
    fi
}

function export_args {

    export QDB_OVERWRITE_PUBLIC=""
    export QDB_DISABLE_HUP_HANDLER=""
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

    # create root directory if it does not exist
    if [ ! -d "$QDB_ROOT" ]; then
        echo "Created QuestDB ROOT directory: $QDB_ROOT"
        mkdir -p ${QDB_ROOT}
    fi

    QDB_LOG=${QDB_ROOT}/log
    mkdir -p ${QDB_LOG}

    LINK=$(read_link $0)
    if [ "$LINK" != "" ]; then
        BASE=$(dirname ${LINK})
    else
        BASE=$(dirname $0)
    fi

    JAVA_LIB="$BASE/questdb.jar"

    JAVA_OPTS="
    -D$QDB_PROCESS_LABEL
    -ea -Dnoebug
    -XX:+PrintGCApplicationStoppedTime
    -XX:+PrintSafepointStatistics
    -XX:PrintSafepointStatisticsCount=1
    -XX:+UseParNewGC
    -XX:+UseConcMarkSweepGC
    -XX:+PrintGCDetails
    -XX:+PrintGCTimeStamps
    -XX:+PrintGCDateStamps
    -XX:+UnlockDiagnosticVMOptions
    -XX:GuaranteedSafepointInterval=90000000
    -XX:-UseBiasedLocking
    -XX:BiasedLockingStartupDelay=0"

    JAVA_MAIN="com.questdb.BootstrapMain"
    DATE=`date +%Y-%m-%d:%H:%M:%S`

    if [ "${QDB_DISABLE_HUP_HANDLER}" = "" ]; then
        ${JAVA} ${JAVA_OPTS} -cp ${JAVA_LIB} ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} > ${QDB_LOG}/stdout-${DATE}.txt &
        sleep 0.5
    else
        ${JAVA} ${JAVA_OPTS} -cp ${JAVA_LIB} ${JAVA_MAIN} -d ${QDB_ROOT} ${QDB_OVERWRITE_PUBLIC} ${QDB_DISABLE_HUP_HANDLER} > ${QDB_LOG}/stdout-${DATE}.txt
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

    count=${QDB_MAX_STOP_ATTEMPTS}
    while [ "${QDB_PID}" != "" ] && [ ${count} -gt 0 ]; do
        kill ${QDB_PID}
        sleep 0.5
        export_pid
        count=$((count-1))
    done

    if [[ "${QDB_PID}" != "" ]]; then
        kill -9 ${QDB_PID}
        echo "Something is wrong. Process does not stop. Killing.."
        export_pid
    fi

    if [[ "${QDB_PID}" != "" ]]; then
        echo "Cannot stop ${QDB_PID}. Check permissions."
    else
        echo "Stopped ${OUR_PID}"
    fi
}

function banner {
    echo ''
    echo '  ___                  _   ____  ____'
    echo ' / _ \ _   _  ___  ___| |_|  _ \| __ )'
    echo '| | | | | | |/ _ \/ __| __| | | |  _ \'
    echo '| |_| | |_| |  __/\__ \ |_| |_| | |_) |'
    echo ' \__\_\\__,_|\___||___/\__|____/|____/'
    echo '                       www.questdb.org'
    echo
}

banner

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
