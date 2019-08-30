/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

#include <winsock2.h>
#include <stddef.h>
#include "select.h"
#include "errno.h"

JNIEXPORT jint JNICALL Java_io_questdb_network_SelectAccessor_select
        (JNIEnv *e, jclass cl, jlong readfds, jlong writefds, jlong exceptfds) {
    struct timeval tv = {0, 0};
    int n = select(0, (fd_set *) readfds, (fd_set *) writefds, (fd_set *) exceptfds, &tv);
    if (n != 0) {
        SaveLastError();
    }
    return n;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_SelectAccessor_arrayOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct fd_set, fd_array[0]);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_SelectAccessor_countOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct fd_set, fd_count);
}

