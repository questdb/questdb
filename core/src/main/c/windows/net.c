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
#include <ws2tcpip.h>
#include "../share/net.h"

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_socketTcp
        (JNIEnv *e, jclass cl, jboolean blocking) {

    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s && !blocking) {
        u_long mode = 1;
        if (ioctlsocket(s, FIONBIO, &mode) != 0) {
            closesocket(s);
            return -1;
        }
    }
    return (jlong) s;
}

JNIEXPORT jboolean JNICALL Java_com_nfsdb_misc_Net_bind
        (JNIEnv *e, jclass cl, jlong fd, jint address, jint port) {

    // int ip address to string
    struct in_addr ip_addr;
    ip_addr.s_addr = (u_long) address;
    inet_ntoa(ip_addr);

    // port to string
    char p[16];
    itoa(port, p, 10);

    // hints for bind
    struct addrinfo hints;
    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_PASSIVE;

    // populate addrinfo
    struct addrinfo *addr;
    if (getaddrinfo(inet_ntoa(ip_addr), p, &hints, &addr) != 0) {
        return FALSE;
    }

    return (jboolean) (bind((SOCKET) fd, addr->ai_addr, (int) addr->ai_addrlen) == 0);
}

JNIEXPORT void JNICALL Java_com_nfsdb_misc_Net_listen
        (JNIEnv *e, jclass cl, jlong fd, jint backlog) {
    listen((SOCKET) fd, backlog);
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_accept
        (JNIEnv *e, jclass cl, jlong fd) {
    return (jlong) accept((SOCKET) fd, NULL, 0);
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jlong fd) {
    u_long mode = 1;
    return ioctlsocket((SOCKET) fd, FIONBIO, &mode);
}

jint convert_error(int n) {
    if (n > 0) {
        return (jint) n;
    }

    switch (n) {
        case 0:
            return com_nfsdb_misc_Net_EPEERDISCONNECT;
        default:
            if (WSAGetLastError() == WSAEWOULDBLOCK) {
                return com_nfsdb_misc_Net_ERETRY;
            } else {
                return com_nfsdb_misc_Net_EOTHERDISCONNECT;
            }
    }
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_recv
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(recv((SOCKET) fd, (char *) addr, len, 0));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_send
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(send((SOCKET) fd, (const char *) addr, len, 0));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setSndBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_SNDBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_RCVBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_available
        (JNIEnv *e, jclass cl, jlong fd) {
    unsigned long avail;

    ioctlsocket((SOCKET) fd, FIONREAD, &avail);
    return avail;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_getEof
        (JNIEnv *e, jclass cl) {
    return  WSAENOTCONN;
}

JNIEXPORT jint JNICALL Java_com_nfsdb_misc_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return WSAEWOULDBLOCK;
}

JNIEXPORT jlong JNICALL Java_com_nfsdb_misc_Net_getPeerIP
        (JNIEnv *e, jclass cl, jlong fd) {

    struct sockaddr peer;
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
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
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohs(((struct sockaddr_in *)&peer)->sin_port);
        } else {
            return -2;
        }
    }
    return -1;
}
