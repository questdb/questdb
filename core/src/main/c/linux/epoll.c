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

#include <jni.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <stddef.h>
#include "../share/sysutil.h"


JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_epollCreate
        (JNIEnv *e, jclass cl) {
    return epoll_create1(0);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_epollCtl
        (JNIEnv *e, jclass cl, jint epfd, jint op, jint fd, jlong event) {
    return epoll_ctl((int) epfd, op, (int) fd, (struct epoll_event *) event);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_epollWait
        (JNIEnv *e, jclass cl, jint epfd, jlong eventPtr, jint eventCount, jint timeout) {
    int res;
    RESTARTABLE(epoll_wait((int) epfd, (struct epoll_event *) eventPtr, eventCount, timeout), res);
    return res;
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_EpollAccessor_getDataOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct epoll_event, data);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_EpollAccessor_getEventsOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct epoll_event, events);
}

JNIEXPORT jshort JNICALL Java_io_questdb_network_EpollAccessor_getEventSize
        (JNIEnv *e, jclass cl) {
    return sizeof(struct epoll_event);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getEPOLLIN
        (JNIEnv *e, jclass cl) {
    return EPOLLIN;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getEPOLLET
        (JNIEnv *e, jclass cl) {
    return EPOLLET;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getEPOLLOUT
        (JNIEnv *e, jclass cl) {
    return EPOLLOUT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getEPOLLONESHOT
        (JNIEnv *e, jclass cl) {
    return EPOLLONESHOT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getCtlAdd
        (JNIEnv *e, jclass cl) {
    return EPOLL_CTL_ADD;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getCtlMod
        (JNIEnv *e, jclass cl) {
    return EPOLL_CTL_MOD;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_EpollAccessor_getCtlDel
        (JNIEnv *e, jclass cl) {
    return EPOLL_CTL_DEL;
}
