/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <winsock2.h>
#include <stddef.h>
#include "select.h"
#include "errno.h"

JNIEXPORT jint JNICALL Java_io_questdb_network_SelectAccessor_select
        (JNIEnv *e, jclass cl, jlong readfds, jlong writefds, jlong exceptfds, jint timeout) {
    int tv_sec = timeout / 1000;
    struct timeval tv = {tv_sec, (timeout - tv_sec * 1000) * 1000};
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

