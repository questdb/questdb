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

#include <fcntl.h>
#include <sys/event.h>
#include <sys/time.h>
#include <stddef.h>
#include <unistd.h>
#include <stdlib.h>
#include "jni.h"
#include "../share/sysutil.h"

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvfiltRead
        (JNIEnv *e, jclass cl) {
    return EVFILT_READ;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvfiltWrite
        (JNIEnv *e, jclass cl) {
    return EVFILT_WRITE;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvfiltVnode
        (JNIEnv *e, jclass cl) {
    return EVFILT_VNODE;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteDelete
        (JNIEnv *e, jclass cl) {
    return NOTE_DELETE;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteWrite
        (JNIEnv *e, jclass cl) {
    return NOTE_WRITE;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteExtend
        (JNIEnv *e, jclass cl) {
    return NOTE_EXTEND;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteAttrib
        (JNIEnv *e, jclass cl) {
    return NOTE_ATTRIB;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteLink
        (JNIEnv *e, jclass cl) {
    return NOTE_LINK;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteRename
        (JNIEnv *e, jclass cl) {
    return NOTE_RENAME;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getNoteRevoke
        (JNIEnv *e, jclass cl) {
    return NOTE_REVOKE;
}
JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getSizeofKevent
        (JNIEnv *e, jclass cl) {
    return (short) sizeof(struct kevent);
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getFdOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, ident);
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getFilterOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, filter);
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getDataOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, udata);
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getFlagsOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, flags);
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvAdd
        (JNIEnv *e, jclass cl) {
    return EV_ADD;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvOneshot
        (JNIEnv *e, jclass cl) {
    return EV_ONESHOT;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvDelete
        (JNIEnv *e, jclass cl) {
    return EV_DELETE;
}

JNIEXPORT jshort JNICALL Java_io_questdb_KqueueAccessor_getEvClear
        (JNIEnv *e, jclass cl) {
    return EV_CLEAR;
}

JNIEXPORT jint JNICALL Java_io_questdb_KqueueAccessor_kqueue
        (JNIEnv *e, jclass cl) {
    return kqueue();
}

JNIEXPORT jint JNICALL Java_io_questdb_KqueueAccessor_kevent
        (JNIEnv *e, jclass cl, jint kq, jlong changelist, jint nChanges, jlong eventlist, jint nEvents, jint timeout_msec) {
    long MILLION = 1000 * 1000;
    long BILLION = 1000 * MILLION;

    long remaining_timeout_nsec = timeout_msec * MILLION;
    int res = -1;
    do {
        int tv_sec = remaining_timeout_nsec / BILLION;
        struct timespec _timeout = { tv_sec, remaining_timeout_nsec - tv_sec * BILLION };
        clock_t start = clock();
        res = kevent(
                kq, (const struct kevent *) changelist,
                nChanges,
                (struct kevent *) eventlist,
                nEvents,
                &_timeout
        );
        if (timeout_msec != 0) {
            clock_t end = clock();
            long elapsed_nsec = (end - start) * BILLION / CLOCKS_PER_SEC;
            remaining_timeout_nsec -= elapsed_nsec;
        }
    } while ( res == -1 && errno == EINTR && remaining_timeout_nsec >= 0 );
    return res;
}

JNIEXPORT jint JNICALL Java_io_questdb_KqueueAccessor_keventRegister
        (JNIEnv *e, jclass cl, jint kq, jlong changelist, jint nChanges) {
    return (jint) kevent(
            kq,
            (const struct kevent *) changelist,
            nChanges,
            NULL,
            0,
            NULL
    );
}

JNIEXPORT jint JNICALL Java_io_questdb_KqueueAccessor_keventGetBlocking
        (JNIEnv *e, jclass cl, jint kq, jlong eventList, jint nEvents) {
    int res;
    RESTARTABLE(kevent(kq, NULL, 0, (struct kevent *) eventList, nEvents, NULL), res);
    return (jint) res;
}

JNIEXPORT jlong JNICALL Java_io_questdb_KqueueAccessor_evtAlloc
    (JNIEnv *e, jclass cl, jlong ident, jint filter, jint flags, jint fflags, jlong data) {
    struct kevent *event = malloc(sizeof(struct kevent));
    if (!event) {
        return (jlong) 0;
    }
    EV_SET(event, ident, filter, flags, fflags, data, NULL);
    return (jlong) event;
}

JNIEXPORT void JNICALL Java_io_questdb_KqueueAccessor_evtFree
        (JNIEnv *e, jclass cl, jlong event) {
    free((void *) event);
}
