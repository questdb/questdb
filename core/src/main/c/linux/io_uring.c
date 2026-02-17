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
#include <stdlib.h>
#include <stddef.h>
#include <syscall.h>
#include <sys/utsname.h>
#include <liburing.h>


JNIEXPORT jstring JNICALL Java_io_questdb_std_IOUringAccessor_kernelVersion
        (JNIEnv *e, jclass cl) {
    struct utsname u;
    uname(&u);

    jstring result = (*e)->NewStringUTF(e, u.release);
    return result;
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_IOUringAccessor_create
        (JNIEnv *e, jclass cl, jint capacity) {
    struct io_uring *ring = malloc(sizeof(struct io_uring));
    int ret = io_uring_queue_init(capacity, ring, 0);
    if (ret < 0) {
        free(ring);
        return (jlong) ret;
    }
    return (jlong) ring;
}

JNIEXPORT void JNICALL Java_io_questdb_std_IOUringAccessor_close
        (JNIEnv *e, jclass cl, jlong ptr) {
    struct io_uring *ring = (struct io_uring *) ptr;
    io_uring_queue_exit(ring);
    free(ring);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_IOUringAccessor_submit
        (JNIEnv *e, jclass cl, jlong ptr) {
    struct io_uring *ring = (struct io_uring *) ptr;
    return (jint) io_uring_submit(ring);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_IOUringAccessor_submitAndWait
        (JNIEnv *e, jclass cl, jlong ptr, jint waitNr) {
    struct io_uring *ring = (struct io_uring *) ptr;
    return (jint) io_uring_submit_and_wait(ring, (int) waitNr);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getRingFdOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring, ring_fd);
}

// io_uring_sq

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring, sq);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqKheadOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, khead);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqKtailOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, ktail);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqKringMaskOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, kring_mask);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqKringEntriesOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, kring_entries);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqSqesOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, sqes);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqSqeHeadOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, sqe_head);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqSqeTailOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sq, sqe_tail);
}

// io_uring_cq

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring, cq);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqKheadOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cq, khead);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqKtailOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cq, ktail);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqKringMaskOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cq, kring_mask);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqKringEntriesOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cq, kring_entries);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqCqesOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cq, cqes);
}

// io_uring_sqe

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeSize
        (JNIEnv *e, jclass cl) {
    return sizeof(struct io_uring_sqe);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeOpcodeOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, opcode);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeFDOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, fd);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeOffOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, off);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeAddrOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, addr);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeLenOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, len);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getSqeUserDataOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_sqe, user_data);
}

// io_uring_cqe

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqeSize
        (JNIEnv *e, jclass cl) {
    return sizeof(struct io_uring_cqe);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqeUserDataOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cqe, user_data);
}

JNIEXPORT jshort JNICALL Java_io_questdb_std_IOUringAccessor_getCqeResOffset
        (JNIEnv *e, jclass cl) {
    return (jshort) offsetof(struct io_uring_cqe, res);
}
