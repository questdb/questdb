/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 ******************************************************************************/

/** The code you provided contains a lot of repeated patterns and boilerplate code for JNI (Java Native Interface)
methods. I can refactor it to reduce redundancy while keeping the same functionality. Here's a more concise version **/

#include <jni.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/utsname.h>
#include <liburing.h>

JNIEXPORT jstring JNICALL Java_io_questdb_std_IOUringAccessor_kernelVersion(JNIEnv *e, jclass cl) {
    struct utsname u;
    uname(&u);
    return (*e)->NewStringUTF(e, u.release);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_IOUringAccessor_create(JNIEnv *e, jclass cl, jint capacity) {
    struct io_uring *ring = malloc(sizeof(struct io_uring));
    if (io_uring_queue_init(capacity, ring, 0) < 0) {
        free(ring);
        return -1;
    }
    return (jlong) ring;
}

JNIEXPORT void JNICALL Java_io_questdb_std_IOUringAccessor_close(JNIEnv *e, jclass cl, jlong ptr) {
    struct io_uring *ring = (struct io_uring *) ptr;
    io_uring_queue_exit(ring);
    free(ring);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_IOUringAccessor_submit(JNIEnv *e, jclass cl, jlong ptr) {
    return (jint) io_uring_submit((struct io_uring *) ptr);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_IOUringAccessor_submitAndWait(JNIEnv *e, jclass cl, jlong ptr, jint waitNr) {
    return (jint) io_uring_submit_and_wait((struct io_uring *) ptr, (int) waitNr);
}

#define GET_OFFSET(fn_name, struct_name, member) \
    JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_##fn_name(JNIEnv *e, jclass cl) { \
        return (jshort) offsetof(struct struct_name, member); \
    }

GET_OFFSET(getRingFdOffset, io_uring, ring_fd)
GET_OFFSET(getSqOffset, io_uring, sq)
GET_OFFSET(getSqKheadOffset, io_uring_sq, khead)
GET_OFFSET(getSqKtailOffset, io_uring_sq, ktail)
GET_OFFSET(getSqKringMaskOffset, io_uring_sq, kring_mask)
GET_OFFSET(getSqKringEntriesOffset, io_uring_sq, kring_entries)
GET_OFFSET(getSqSqesOffset, io_uring_sq, sqes)
GET_OFFSET(getSqSqeHeadOffset, io_uring_sq, sqe_head)
GET_OFFSET(getSqSqeTailOffset, io_uring_sq, sqe_tail)
GET_OFFSET(getCqOffset, io_uring, cq)
GET_OFFSET(getCqKheadOffset, io_uring_cq, khead)
GET_OFFSET(getCqKtailOffset, io_uring_cq, ktail)
GET_OFFSET(getCqKringMaskOffset, io_uring_cq, kring_mask)
GET_OFFSET(getCqKringEntriesOffset, io_uring_cq, kring_entries)
GET_OFFSET(getCqCqesOffset, io_uring_cq, cqes)
GET_OFFSET(getSqeSize, io_uring_sqe, )
GET_OFFSET(getSqeOpcodeOffset, io_uring_sqe, opcode)
GET_OFFSET(getSqeFDOffset, io_uring_sqe, fd)
GET_OFFSET(getSqeOffOffset, io_uring_sqe, off)
GET_OFFSET(getSqeAddrOffset, io_uring_sqe, addr)
GET_OFFSET(getSqeLenOffset, io_uring_sqe, len)
GET_OFFSET(getSqeUserDataOffset, io_uring_sqe, user_data)
GET_OFFSET(getCqeSize, io_uring_cqe, )
GET_OFFSET(getCqeUserDataOffset, io_uring_cqe, user_data)
GET_OFFSET(getCqeResOffset, io_uring_cqe, res)

#undef GET_OFFSET
