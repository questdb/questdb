#!/usr/bin/env bash
set -Eeo pipefail
export QDB_PACKAGE=${QDB_PACKAGE:-docker}

QUESTDB_DATA_DIR=${QUESTDB_DATA_DIR:-"/var/lib/questdb"}
IGNORE_DATA_ROOT_MOUNT_CHECK=${IGNORE_DATA_ROOT_MOUNT_CHECK:-"false"}
RUN_AS_ROOT=${RUN_AS_ROOT:-"false"}
DO_CHOWN=${DO_CHOWN:-"true"}
QUESTDB_UID="${QUESTDB_UID:-"$(id -u questdb)"}"
QUESTDB_GID="${QUESTDB_GID:-"$(id -g questdb)"}"
TEMPFILE="$(mktemp)"
HELP_TEMPFILE="$(mktemp)"


# directories inside QUESTDB_DATA_DIR that we will chown
DEFAULT_LOCAL_DIRS=${DEFAULT_LOCAL_DIRS:-"/conf /public /db /snapshot"}
array=( ${DEFAULT_LOCAL_DIRS} )
read -ra LOCALDIRS < <( echo -n "( "; printf -- "-ipath ${QUESTDB_DATA_DIR}%s* -o " "${array[@]:0:$((${#array[@]} - 1))}"; echo -n "-ipath ${QUESTDB_DATA_DIR}${array[@]: -1}*"; echo " )";)


# backwards compatibility with previous versions
if [ ${IGNORE_FIND_AND_OWN_DIR+x} ]
then
    DO_CHOWN=$IGNORE_FIND_AND_OWN_DIR
fi

cat << EOF > $HELP_TEMPFILE
!!! The following files/directories have been found not to be owned by the questdb user/group, which may cause QuestDB to fail to start. !!!

To allow the questdb container image to change the file ownership for you, please run the container with the -e DO_CHOWN=true option.
This will only work if you don't use --user argument when running the container image
For example, you can use the command:
  docker run -it -v /local-questdb-path:/var/lib/questdb -e DO_CHOWN=true questdb/questdb


Alternatively, you can change the file ownership yourself.
QuestDB usually runs under the questdb user with user id 10001 and group id 10001.
To change the file ownership to questdb user, you can run the command chown 10001:10001 /local-questdb-path -R


If you are running QuestDB on your local machine under a normal user (e.g., user id 1000) and you don't want to change the file permissions, 
you can run the questdb container image with the --user $(id -u):$(id -g) option. 
For example, you can use the command:
  docker run -it -v /local-questdb-path:/var/lib/questdb --user $(id -u):$(id -g) questdb/questdb
Or use environment variables QUESTDB_UID and QUESTDB_GID that should have the same effect:
  docker run -it -v /local-questdb-path:/var/lib/questdb -e QUESTDB_UID=$(id -u) -e QUESTDB_GID=$(id -u) questdb/questdb

If you want to run QuestDB as root, you can add the RUN_AS_ROOT environment variable when running questdb. 
For example, you can use the command:
  docker run -it -v /local-questdb-path:/var/lib/questdb -e RUN_AS_ROOT=true questdb/questdb

EOF


find_files_and_dirs_not_owned_by_user() {
    local USER=$1
    local GROUP=$2
    # check if QUESTDB_DATA_DIR directory is owned by $USER:$GROUP
    [ $(stat --format '%u:%g' ${QUESTDB_DATA_DIR}) == "$USER:$GROUP" ] || echo ${QUESTDB_DATA_DIR} > $TEMPFILE
    find ${QUESTDB_DATA_DIR} "${LOCALDIRS[@]}" \( ! -user $USER -o ! -group $GROUP \) >> $TEMPFILE
    grep . $TEMPFILE > /dev/null 2>&1 && cat $HELP_TEMPFILE && cat $TEMPFILE || echo -n
    echo
}

find_and_own_dir() {
    local USER=$1
    local GROUP=$2
    find_files_and_dirs_not_owned_by_user $USER $GROUP
    while read -r line; do chown $USER:$GROUP $line; done < $TEMPFILE
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

if [ "$(id -u)" != '0' ] ; then
    find_files_and_dirs_not_owned_by_user $(id -u 2>/dev/null) $(id -g 2>/dev/null)
fi
echo "Running as $(id -un 2>/dev/null) user"
exec "$@"
