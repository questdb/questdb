/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

#include <winsock2.h>
#include <ws2tcpip.h>
#include "../share/net.h"

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Net_socketTcp
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

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Net_bind
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

JNIEXPORT void JNICALL Java_com_questdb_misc_Net_listen
        (JNIEnv *e, jclass cl, jlong fd, jint backlog) {
    listen((SOCKET) fd, backlog);
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Net_accept
        (JNIEnv *e, jclass cl, jlong fd) {
    return (jlong) accept((SOCKET) fd, NULL, 0);
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_configureNonBlocking
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
            return com_questdb_misc_Net_EPEERDISCONNECT;
        default:
            if (WSAGetLastError() == WSAEWOULDBLOCK) {
                return com_questdb_misc_Net_ERETRY;
            } else {
                return com_questdb_misc_Net_EOTHERDISCONNECT;
            }
    }
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_recv
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(recv((SOCKET) fd, (char *) addr, len, 0));
}

JNIEXPORT jboolean JNICALL Java_com_questdb_misc_Net_isDead
        (JNIEnv *e, jclass cl, jlong fd) {
    int c;
    return (jboolean) (recv((SOCKET) fd, (char *) &c, 1, 0) == 0);
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_send
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(send((SOCKET) fd, (const char *) addr, len, 0));
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_setSndBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_SNDBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_RCVBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Net_available
        (JNIEnv *e, jclass cl, jlong fd) {
    unsigned long avail;

    ioctlsocket((SOCKET) fd, FIONREAD, &avail);
    return avail;
}

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return WSAEWOULDBLOCK;
}

JNIEXPORT jlong JNICALL Java_com_questdb_misc_Net_getPeerIP
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

JNIEXPORT jint JNICALL Java_com_questdb_misc_Net_getPeerPort
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
