#!/usr/bin/env bash
set -Eeo pipefail
export QDB_PACKAGE=${QDB_PACKAGE:-docker}

QUESTDB_DATA_DIR=${QUESTDB_DATA_DIR:-"/var/lib/questdb"}
IGNORE_DATA_ROOT_MOUNT_CHECK=${IGNORE_DATA_ROOT_MOUNT_CHECK:-"false"}
IGNORE_FIND_AND_OWN_DIR=${IGNORE_FIND_AND_OWN_DIR:-"false"}
RUN_AS_ROOT=${RUN_AS_ROOT:-"false"}

find_and_own_dir() {
    if [ "$IGNORE_FIND_AND_OWN_DIR" = "false" ]; then
        find "$1" \( ! -user questdb -o ! -group questdb \) -exec chown questdb:questdb '{}' +
    fi
}


# Temporary only
# Most of the users will have the data mounted under /root/.questdb as default
# we will run as root for them until they change the mount to /var/lib/questdb or someting else
if [ "$IGNORE_DATA_ROOT_MOUNT_CHECK" = "false" ] && mount | grep "/root/.questdb" -q; then
    echo "Found /root/.questdb mount, overwriting QUESTDB_DATA_DIR"
    QUESTDB_DATA_DIR="/root/.questdb"
fi

if [ $# -eq 0 ]; then
    echo "No arguments found, start with default arguments"
    set -- /app/bin/java -Dout=conf/log.conf -m io.questdb/io.questdb.ServerMain -d ${QUESTDB_DATA_DIR} -f
else
    if [ "${1:0:1}" = '-' ]; then
        echo "Found config arguments $@"
        set -- /app/bin/java "$@"
    elif [ "$1" = "/app/bin/java" ]; then
        echo "Java binary arguments found, Non default arguments config run"
        set -- "$@"
    fi
fi

if [ "$(id -u)" = '0' ] && [ "${QUESTDB_DATA_DIR%/}" != "/root/.questdb" ] && [ "$RUN_AS_ROOT" = "false" ] ; then
    echo "Running as questdb user"
    find_and_own_dir ${QUESTDB_DATA_DIR}
    exec gosu questdb "$@"
fi

echo "Running as $(id -un) user"
cd  ${QUESTDB_DATA_DIR}
exec "$@"