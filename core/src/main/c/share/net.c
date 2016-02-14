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

#include <jni.h>
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/errno.h>
#include "net.h"

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_socketTcp
        (JNIEnv *e, jobject cl, jboolean blocking) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd > 0 && !blocking) {
        if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
            close(fd);
            return -1;
        }

        int oni = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *) &oni, sizeof(oni)) < 0) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

JNIEXPORT jboolean JNICALL Java_com_nfsdb_misc_Net_bind
        (JNIEnv *e, jobject cl, jlong fd, jint address, jint port) {
    struct sockaddr_in addr;

    addr.sin_family = AF_INET;

    addr.sin_addr.s_addr = htonl(address);
    addr.sin_port = htons(port);

    return (jboolean) (bind((int) fd, (struct sockaddr *) &addr, sizeof(addr)) == 0);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_accept
        (JNIEnv *e, jobject cl, jlong fd) {
    return accept((int) fd, NULL, NULL);
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Net_listen
        (JNIEnv *e, jclass cl, jlong fd, jint backlog) {
    listen((int) fd, backlog);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_send
        (JNIEnv *e, jclass cl, jlong fd, jlong ptr, jint len) {
    return (jint) send((int) fd, (const void *) ptr, (size_t) len, 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_recv
        (JNIEnv *e, jclass cl, jlong fd, jlong ptr, jint len) {
    return (jint) recv((int) fd, (void *) ptr, (size_t) len, 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jlong fd) {
    int flags;


    if ((flags = fcntl((int) fd, F_GETFL, 0)) < 0) {
        return flags;
    }


    if ((flags = fcntl((int) fd, F_SETFL, flags | O_NONBLOCK)) < 0) {
        return flags;
    }

    return 0;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setSndBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((int) fd, SOL_SOCKET, SO_SNDBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((int) fd, SOL_SOCKET, SO_RCVBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_getEof
        (JNIEnv *e, jclass cl) {
    return ENOENT;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return EWOULDBLOCK;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_getPeerIP
        (JNIEnv *e, jclass cl, jlong fd) {
    struct sockaddr peer;
    socklen_t nameLen = sizeof(peer);

    if (getpeername((int) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return inet_addr(inet_ntoa(((struct sockaddr_in *)&peer)->sin_addr));
        } else {
            return -2;
        }
    }
    return -1;

}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_getPeerPort
        (JNIEnv *e, jclass cl, jlong fd) {
    struct sockaddr peer;
    socklen_t nameLen = sizeof(peer);

    if (getpeername((int) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohs(((struct sockaddr_in *)&peer)->sin_port);
        } else {
            return -2;
        }
    }
    return -1;
}
