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

#include "epoll.h"
#include <sys/epoll.h>
#include <stddef.h>


JNIEXPORT jlong JNICALL Java_com_nfsdb_net_Epoll_epollCreate
        (JNIEnv *e, jclass cl) {
    return epoll_create1(0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_epollCtl
        (JNIEnv *e, jclass cl, jlong epfd, jint op, jlong fd, jlong event) {
    return epoll_ctl((int) epfd, op, (int) fd, (struct epoll_event *) event);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_epollWait
        (JNIEnv *e, jclass cl, jlong epfd, jlong eventPtr, jint eventCount, jint timeout) {
    return epoll_wait((int) epfd, (struct epoll_event *) eventPtr, eventCount, timeout);
}


JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Epoll_getDataOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct epoll_event, data);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Epoll_getEventsOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct epoll_event, events);
}

JNIEXPORT jshort JNICALL Java_com_nfsdb_net_Epoll_getEventSize
        (JNIEnv *e, jclass cl) {
    return sizeof(struct epoll_event);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_getEPOLLIN
        (JNIEnv *e, jclass cl) {
    return EPOLLIN;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_getEPOLLET
        (JNIEnv *e, jclass cl) {
    return EPOLLET;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_getEPOLLOUT
        (JNIEnv *e, jclass cl) {
    return EPOLLOUT;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_getEPOLLONESHOT
        (JNIEnv *e, jclass cl) {
    return EPOLLONESHOT;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_net_Epoll_getCtlAdd
        (JNIEnv *e, jclass cl) {
    return EPOLL_CTL_ADD;
}
