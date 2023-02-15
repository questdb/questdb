/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <stddef.h>
#include <unistd.h>
#include "jni.h"

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getEvfiltRead
        (JNIEnv *e, jclass cl) {
    return EVFILT_READ;
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getEvfiltWrite
        (JNIEnv *e, jclass cl) {
    return EVFILT_WRITE;
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getSizeofKevent
        (JNIEnv *e, jclass cl) {
    return (short) sizeof(struct kevent);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getFdOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, ident);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getFilterOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, filter);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getDataOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, udata);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getFlagsOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, flags);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getEvAdd
        (JNIEnv *e, jclass cl) {
    return EV_ADD;
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getEvOneshot
        (JNIEnv *e, jclass cl) {
    return EV_ONESHOT;
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_KqueueAccessor_getEvDelete
        (JNIEnv *e, jclass cl) {
    return EV_DELETE;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_KqueueAccessor_kqueue
        (JNIEnv *e, jclass cl) {
    return kqueue();
}

JNIEXPORT jint JNICALL Java_io_questdb_network_KqueueAccessor_kevent
        (JNIEnv *e, jclass cl, jint kq, jlong changelist, jint nChanges, jlong eventlist, jint nEvents) {
    struct timespec dontBlock = {0, 0};
    return (jint) kevent(kq, (const struct kevent *) changelist, nChanges, (struct kevent *) eventlist, nEvents,
                         &dontBlock);
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_KqueueAccessor_pipe
        (JNIEnv *e, jclass cl) {
    int fds[2];
    int res = pipe2(fds, O_NONBLOCK);
    if (res < 0) {
        return res;
    }
    return (jlong) fds[0] << 32 | (jlong) fds[1];
}

JNIEXPORT jint JNICALL Java_io_questdb_network_KqueueAccessor_readPipe
        (JNIEnv *e, jclass cl, jint fd) {
    char buf[1];
    ssize_t s;
    s = read((int) fd, &buf[0], 1);
    if (s != 1) {
        return -1;
    }
    return (jint) buf[0];
}

JNIEXPORT jint JNICALL Java_io_questdb_network_KqueueAccessor_writePipe
        (JNIEnv *e, jclass cl, jint fd) {
    char buf[1];
    ssize_t s;
    buf[0] = 1;
    s = write((int) fd, &buf[0], 1);
    if (s != 1) {
        return -1;
    }
    return 0;
}
