/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

#include <sys/event.h>
#include <sys/time.h>
#include <stddef.h>
#include "jni.h"

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getEvfiltRead
        (JNIEnv *e, jclass cl) {
    return EVFILT_READ;
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getEvfiltWrite
        (JNIEnv *e, jclass cl) {
    return EVFILT_WRITE;
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getSizeofKevent
        (JNIEnv *e, jclass cl) {
    return (short) sizeof(struct kevent);
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getFdOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, ident);
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getFilterOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, filter);
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getDataOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, udata);
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getFlagsOffset
        (JNIEnv *e, jclass cl) {
    return (short) offsetof(struct kevent, flags);
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getEvAdd
        (JNIEnv *e, jclass cl) {
    return EV_ADD;
}

JNIEXPORT jshort JNICALL Java_com_questdb_network_KqueueAccessor_getEvOneshot
        (JNIEnv *e, jclass cl) {
    return EV_ONESHOT;
}


JNIEXPORT jint JNICALL Java_com_questdb_network_KqueueAccessor_kqueue
        (JNIEnv *e, jclass cl) {
    return kqueue();
}

JNIEXPORT jint JNICALL Java_com_questdb_network_KqueueAccessor_kevent
        (JNIEnv *e, jclass cl, jint kq, jlong changelist, jint nChanges, jlong eventlist, jint nEvents) {
    struct timespec dontBlock = {0, 0};
    return (jint) kevent(kq, (const struct kevent *) changelist, nChanges, (struct kevent *) eventlist, nEvents,
                         &dontBlock);
}



