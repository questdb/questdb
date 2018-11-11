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

#include <winsock2.h>
#include <ws2tcpip.h>
#include "../share/net.h"
#include "errno.h"

JNIEXPORT jlong JNICALL Java_com_questdb_std_Net_socketTcp
        (JNIEnv *e, jclass cl, jboolean blocking) {

    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s && !blocking) {
        u_long mode = 1;
        if (ioctlsocket(s, FIONBIO, &mode) != 0) {
            SaveLastError();
            closesocket(s);
            return -1;
        }
    } else {
        SaveLastError();
    }
    return (jlong) s;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Net_socketUdp0
        (JNIEnv *e, jclass cl) {
    SOCKET s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == INVALID_SOCKET) {
        return -1;
    }

    u_long mode = 1;
    if (ioctlsocket(s, FIONBIO, &mode) != 0) {
        SaveLastError();
        closesocket(s);
        return -1;
    }
    return s;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_getEWouldBlock
        (JNIEnv *e, jclass cl) {
    return EWOULDBLOCK;
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Net_sockaddr
        (JNIEnv *e, jclass cl, jint address, jint port) {
    struct sockaddr_in *addr = calloc(1, sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl((u_long) address);
    addr->sin_port = htons((u_short) port);
    return (jlong) addr;
}

JNIEXPORT void JNICALL Java_com_questdb_std_Net_freeSockAddr
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        free((void *) address);
    }
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Net_bindTcp
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
        SaveLastError();
        return FALSE;
    }

    return (jboolean) (bind((SOCKET) fd, addr->ai_addr, (int) addr->ai_addrlen) == 0);
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Net_join
        (JNIEnv *e, jclass cl, jlong fd, jint bindAddress, jint groupAddress) {
    struct ip_mreq_source imr;
    imr.imr_multiaddr.s_addr = htonl((u_long) groupAddress);
    imr.imr_sourceaddr.s_addr = 0;
    imr.imr_interface.s_addr = htonl((u_long) bindAddress);
    if (setsockopt((SOCKET) fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, (char *) &imr, sizeof(imr)) < 0) {
        SaveLastError();
        return FALSE;
    }
    return TRUE;
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Net_bindUdp
        (JNIEnv *e, jclass cl, jlong fd, jint address, jint port) {

    struct sockaddr_in RecvAddr;
    ZeroMemory(&RecvAddr, sizeof(RecvAddr));

    RecvAddr.sin_family = AF_INET;
    RecvAddr.sin_addr.s_addr = htonl((u_long) address);
    RecvAddr.sin_port = htons((u_short) port);

    if (bind((SOCKET) fd, (SOCKADDR *) &RecvAddr, sizeof(RecvAddr)) == 0) {
        return TRUE;
    }

    SaveLastError();
    return FALSE;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_connect
        (JNIEnv *e, jclass cl, jlong fd, jlong sockAddr) {
    return connect((SOCKET) fd, (const struct sockaddr *) sockAddr, sizeof(struct sockaddr));
}

JNIEXPORT void JNICALL Java_com_questdb_std_Net_listen
        (JNIEnv *e, jclass cl, jlong fd, jint backlog) {
    listen((SOCKET) fd, backlog);
}

JNIEXPORT jlong JNICALL Java_com_questdb_std_Net_accept
        (JNIEnv *e, jclass cl, jlong fd) {
    return (jlong) accept((SOCKET) fd, NULL, 0);
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jlong fd) {
    u_long mode = 1;
    return ioctlsocket((SOCKET) fd, FIONBIO, &mode);
}

jint convert_error(int n) {
    SaveLastError();
    if (n > 0) {
        return (jint) n;
    }

    switch (n) {
        case 0:
            return com_questdb_std_Net_EPEERDISCONNECT;
        default:
            if (WSAGetLastError() == WSAEWOULDBLOCK) {
                return com_questdb_std_Net_ERETRY;
            } else {
                return com_questdb_std_Net_EOTHERDISCONNECT;
            }
    }
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_recv
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(recv((SOCKET) fd, (char *) addr, len, 0));
}

JNIEXPORT jboolean JNICALL Java_com_questdb_std_Net_isDead
        (JNIEnv *e, jclass cl, jlong fd) {
    int c;
    return (jboolean) (recv((SOCKET) fd, (char *) &c, 1, 0) == 0);
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_send
        (JNIEnv *e, jclass cl, jlong fd, jlong addr, jint len) {
    return convert_error(send((SOCKET) fd, (const char *) addr, len, 0));
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_sendTo
        (JNIEnv *e, jclass cl, jlong fd, jlong ptr, jint len, jlong sockaddr) {
    return (jint) sendto((SOCKET) fd, (const void *) ptr, len, 0, (const struct sockaddr *) sockaddr,
                         sizeof(struct sockaddr_in));
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_configureNoLinger
        (JNIEnv *e, jclass cl, jlong fd) {
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = 0;
    return setsockopt((SOCKET) (int) fd, SOL_SOCKET, SO_LINGER, (const char *) &sl, sizeof(struct linger));
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_setSndBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_SNDBUF, (char *) &sz, sizeof(sz));
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jlong fd, jint size) {
    jint sz = size;
    return setsockopt((SOCKET) fd, SOL_SOCKET, SO_RCVBUF, (char *) &sz, sizeof(sz));
}


JNIEXPORT jint JNICALL Java_com_questdb_std_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return WSAEWOULDBLOCK;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_getPeerIP
        (JNIEnv *e, jclass cl, jlong fd) {

    struct sockaddr peer;
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohl(((struct sockaddr_in *) &peer)->sin_addr.s_addr);
        }
        return -2;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_com_questdb_std_Net_getPeerPort
        (JNIEnv *e, jclass cl, jlong fd) {

    struct sockaddr peer;
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohs(((struct sockaddr_in *) &peer)->sin_port);
        } else {
            return -2;
        }
    }
    return -1;
}
