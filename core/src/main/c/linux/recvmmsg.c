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


#define _GNU_SOURCE
#include "jni.h"
#include <sys/socket.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>


JNIEXPORT jint JNICALL Java_io_questdb_network_Net_recvmmsg
        (JNIEnv *e, jclass cl, jint fd, jlong msgvec, jint vlen) {
    struct timespec timeout;
    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    return recvmmsg((int) fd, (struct mmsghdr *) msgvec, (unsigned int) vlen, MSG_DONTWAIT, &timeout);
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_msgHeaders
        (JNIEnv *e, jclass cl, jint blockSize, jint blockCount) {
    struct mmsghdr *msgs = malloc(sizeof(struct mmsghdr) * blockCount);
    struct iovec *iovecs = malloc(sizeof(struct iovec) * blockCount);
    void *buf = malloc(((size_t) blockSize * (size_t) blockCount));

    memset(msgs, 0, sizeof(struct mmsghdr) * blockCount);
    for (int i = 0; i < blockCount; i++) {
        iovecs[i].iov_base = buf;
        iovecs[i].iov_len = (size_t) blockSize;
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
        buf += blockSize;
    }

    return (jlong) msgs;
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_getMsgHeaderSize
        (JNIEnv *e, jclass cl) {
    return sizeof(struct mmsghdr);
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_getMsgHeaderBufferAddressOffset
        (JNIEnv *e, jclass cl) {
    return offsetof(struct mmsghdr, msg_hdr) + offsetof(struct msghdr, msg_iov);
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_getMsgHeaderBufferLengthOffset
        (JNIEnv *e, jclass cl) {
    return (jint) offsetof(struct mmsghdr, msg_len);
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_freeMsgHeaders
        (JNIEnv *e, jclass cl, jlong address) {
    struct mmsghdr *msgs = (struct mmsghdr *) address;
    free(msgs[0].msg_hdr.msg_iov->iov_base);
    free(msgs[0].msg_hdr.msg_iov);
    free(msgs);
}
