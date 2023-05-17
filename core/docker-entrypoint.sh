#!/usr/bin/env bash
set -Eeo pipefail
export QDB_PACKAGE=${QDB_PACKAGE:-docker}

QUESTDB_DATA_DIR=${QUESTDB_DATA_DIR:-"/var/lib/questdb"}
IGNORE_DATA_ROOT_MOUNT_CHECK=${IGNORE_DATA_ROOT_MOUNT_CHECK:-"false"}
RUN_AS_ROOT=${RUN_AS_ROOT:-"false"}
DO_CHOWN=${DO_CHOWN:-"true"}
QUESTDB_UID="${QUESTDB_UID:-"$(id -u questdb)"}"
QUESTDB_GID="${QUESTDB_GID:-"$(id -g questdb)"}"


# directories inside QUESTDB_DATA_DIR that we will chown
DEFAULT_LOCAL_DIRS=${DEFAULT_LOCAL_DIRS:-"/conf /public /db /snapshot"}
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

if [ $# -eq 0 ]; then
    echo "No arguments found, start with default arguments"
    set -- /app/bin/java -XX:ErrorFile=${QUESTDB_DATA_DIR}/db/hs_err_pid+%p.log -Dout=${QUESTDB_DATA_DIR}/conf/log.conf -m io.questdb/io.questdb.ServerMain -d ${QUESTDB_DATA_DIR} -f
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
    if [ "$DO_CHOWN" = "true" ]; then
        find_and_own_dir $QUESTDB_UID $QUESTDB_GID
    fi
    exec gosu $QUESTDB_UID:$QUESTDB_GID "$@"
fi

echo "Running as $(id -un 2>/dev/null) user"
exec "$@"
