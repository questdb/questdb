#!/bin/sh

export QDB_PROCESS_LABEL="QuestDB-Runtime-66535"
export QDB_MAX_STOP_ATTEMPTS=5;

function start {
    # check that JAVA_HOME is defined
    if [ "$JAVA_HOME" = "" ]; then
        echo "JAVA_HOME is undefined"
        exit 55
    fi

    # check that Java binary is executable
    JAVA=${JAVA_HOME}/bin/java
    if [ ! -x "$JAVA" ]; then
        echo "$JAVA is not executable"
        exit 55;
    fi

    # check the state of root directory
    if [ "$1" = "" ]; then
        QDB_ROOT="qdbroot"
    else
        QDB_ROOT="$1"
    fi

    # create root directory if it does not exist
    if [ ! -d "$QDB_ROOT" ]; then
        echo "Created QuestDB ROOT directory: $QDB_ROOT"
        mkdir -p ${QDB_ROOT}
    fi

    QDB_LOG=${QDB_ROOT}/log
    mkdir -p ${QDB_LOG}

    JAVA_LIB="`dirname $0`/questdb.jar"

    JAVA_OPTS="
    -D$QDB_PROCESS_LABEL
    -da -Dnoebug
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

    ${JAVA} ${JAVA_OPTS} -cp ${JAVA_LIB} ${JAVA_MAIN} ${QDB_ROOT} -f > ${QDB_LOG}/stdout.txt &
    sleep 0.5
}

function export_pid {
    export QDB_PID=`ps -ef | grep ${QDB_PROCESS_LABEL} | grep -v grep | tr -s " " | cut -d " " -f 3`
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
    echo ' \__\_\\\__,_|\___||___/\__|____/|____/'
    echo
}

function usage {
    echo "$0 start|stop|query [-f]"
    exit 55
}

banner

if [[ $# -gt 0 ]]; then
    command=$1
    shift

    case ${command} in
       start)
        start
        ;;
       query)
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
