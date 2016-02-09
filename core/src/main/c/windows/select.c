/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

#include <winsock2.h>
#include <stddef.h>
#include "select.h"

JNIEXPORT jint JNICALL Java_com_nfsdb_net_http_Win32SelectDispatcher_select
        (JNIEnv *e, jclass cl, jlong readfds, jlong writefds, jlong exceptfds) {
    struct timeval tv = {0, 0};
    int n = select(0, (fd_set *) readfds, (fd_set *) writefds, (fd_set *) exceptfds, &tv);
    return n;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_http_Win32SelectDispatcher_arrayOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct fd_set, fd_array[0]);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_http_Win32SelectDispatcher_countOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct fd_set, fd_count);
}

