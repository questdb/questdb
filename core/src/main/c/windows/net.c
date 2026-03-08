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

#include <winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>
#include "../share/net.h"
#include "errno.h"

int get_int_sockopt(SOCKET fd, int level, int opt) {
    int value = 0;
    socklen_t len = sizeof(value);
    int result = getsockopt(fd, level, opt, (char *) &value, &len);
    if (result == SOCKET_ERROR) {
        SaveLastError();
        return result;
    }
    return value;
}

int set_int_sockopt(SOCKET fd, int level, int opt, DWORD value) {
    int result = setsockopt(fd, level, opt, (const char *) &value, sizeof(value));
    if (result == SOCKET_ERROR) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setKeepAlive0
        (JNIEnv *e, jclass cl, jint fd, jint idle_sec) {
    struct tcp_keepalive keepaliveParams;
    DWORD ret = 0;
    keepaliveParams.onoff = 1;
    keepaliveParams.keepaliveinterval = keepaliveParams.keepalivetime = idle_sec * 1000;
    if (WSAIoctl(fd, SIO_KEEPALIVE_VALS, &keepaliveParams, sizeof(keepaliveParams), NULL, 0, &ret, NULL, NULL) < 0) {
        SaveLastError();
        return -1;
    }
    return fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_socketTcp0
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
    return (jint) s;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_socketUdp0
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
    return (jint) s;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getEWouldBlock
        (JNIEnv *e, jclass cl) {
    return EWOULDBLOCK;
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_sockaddr0
        (JNIEnv *e, jclass cl, jint address, jint port) {
    struct sockaddr_in *addr = calloc(1, sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl((u_long) address);
    addr->sin_port = htons((u_short) port);
    return (jlong) addr;
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_getAddrInfo0
        (JNIEnv *e, jclass cl, jlong host, jint port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *addr = NULL;

    char _port[32];
    itoa(port, _port, 10);
    errno_t gai_err_code = getaddrinfo((const char *) host, (const char *) &_port, &hints, &addr);

    if (gai_err_code == 0) {
        return (jlong) addr;
    }

    SaveLastError();
    return -1;
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_freeSockAddr0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        free((void *) address);
    }
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_freeAddrInfo0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        freeaddrinfo((void *) address);
    }
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_bindTcp
        (JNIEnv *e, jclass cl, jint fd, jint address, jint port) {

    // int ip address to string
    struct in_addr ip_addr;
    ip_addr.s_addr = htonl((u_long) address);
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

    const int result = bind((SOCKET) fd, addr->ai_addr, (int) addr->ai_addrlen);
    if (result != 0) {
        SaveLastError();
        return FALSE;
    }
    return TRUE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_join
        (JNIEnv *e, jclass cl, jint fd, jint bindAddress, jint groupAddress) {
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

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_bindUdp
        (JNIEnv *e, jclass cl, jint fd, jint ipv4Address, jint port) {

    struct sockaddr_in RecvAddr;
    ZeroMemory(&RecvAddr, sizeof(RecvAddr));

    RecvAddr.sin_family = AF_INET;
    RecvAddr.sin_addr.s_addr = htonl((u_long) ipv4Address);
    RecvAddr.sin_port = htons((u_short) port);

    if (bind((SOCKET) fd, (SOCKADDR *) &RecvAddr, sizeof(RecvAddr)) == 0) {
        return TRUE;
    }

    SaveLastError();
    return FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_connect
        (JNIEnv *e, jclass cl, jint fd, jlong sockAddr) {
    jint res = connect((SOCKET) fd, (const struct sockaddr *) sockAddr, sizeof(struct sockaddr));
    if (res < 0) {
        SaveLastError();
    }
    return res;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_connectAddrInfo
        (JNIEnv *e, jclass cl, jint fd, jlong lpAddrInfo) {
    struct addrinfo *addr = (struct addrinfo *) lpAddrInfo;
    jint res = connect((SOCKET) fd,
                        addr->ai_addr,
                        (int) addr->ai_addrlen
    );
    if (res < 0) {
        SaveLastError();
    }
    return res;
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_listen
        (JNIEnv *e, jclass cl, jint fd, jint backlog) {
    listen((SOCKET) fd, backlog);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_shutdown
        (JNIEnv *e, jclass cl, jint fd, jint how) {
    return shutdown((SOCKET) fd, how);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_accept0
        (JNIEnv *e, jclass cl, jint fd) {
    // cast to jint makes variable signed, otherwise < 0 comparison does not work
    SOCKET sock = accept((SOCKET) fd, NULL, 0);
    if (sock == INVALID_SOCKET) {
        SaveLastError();
    }
    return sock;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jint fd) {
    u_long mode = 1;
    jint res = ioctlsocket((SOCKET) fd, FIONBIO, &mode);
    if (res < 0) {
        SaveLastError();
    }
    return res;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_recv
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jint len) {
    const int n = recv((SOCKET) fd, (char *) addr, len, 0);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (WSAGetLastError() == WSAEWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    SaveLastError();
    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_peek
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jint len) {
    const int n = recv((SOCKET) fd, (char *) addr, len, MSG_PEEK);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (WSAGetLastError() == WSAEWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    SaveLastError();
    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_isDead
        (JNIEnv *e, jclass cl, jint fd) {
    int c;
    return (jboolean) (recv((SOCKET) fd, (char *) &c, 1, 0) < 1);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_send
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jint len) {
    const int n = send((SOCKET) fd, (const char *) addr, len, 0);
    if (n > -1) {
        return n;
    }

    if (WSAGetLastError() == WSAEWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    SaveLastError();
    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_sendTo
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len, jlong sockaddr) {
    int result = sendto((SOCKET) fd, (const void *) ptr, len, 0, (const struct sockaddr *) sockaddr,
                        sizeof(struct sockaddr_in));
    if (result != len) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_configureLinger
        (JNIEnv *e, jclass cl, jint fd, jint seconds) {
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = seconds;

    int result = setsockopt((SOCKET) (int) fd, SOL_SOCKET, SO_LINGER, (const char *) &sl, sizeof(struct linger));
    if (result == SOCKET_ERROR) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setSndBuf
        (JNIEnv *e, jclass cl, jint fd, jint size) {
    return set_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_SNDBUF, size);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jint fd, jint size) {
    return set_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_RCVBUF, size);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return WSAEWOULDBLOCK;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getRcvBuf
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_RCVBUF);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getSndBuf
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_SNDBUF);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setTcpNoDelay
        (JNIEnv *e, jclass cl, jint fd, jboolean noDelay) {
    return set_int_sockopt((SOCKET) fd, IPPROTO_TCP, TCP_NODELAY, noDelay);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastInterface
        (JNIEnv *e, jclass cl, jint fd, jint ipv4address) {
    struct in_addr address;
    address.s_addr = htonl((u_long) ipv4address);
    int result = setsockopt((SOCKET) fd, IPPROTO_IP, IP_MULTICAST_IF, (const char *) &address, sizeof(address));
    if (result == SOCKET_ERROR) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastTtl
        (JNIEnv *e, jclass cl, jint fd, jint ttl) {
    DWORD lTTL = ttl;
    int result = setsockopt((SOCKET) fd, IPPROTO_IP, IP_MULTICAST_TTL, (char *) &lTTL, sizeof(lTTL));
    if (result == SOCKET_ERROR) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setReuseAddress
        (JNIEnv *e, jclass cl, jint fd) {
    return set_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_REUSEADDR, 1);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setReusePort
        (JNIEnv *e, jclass cl, jint fd) {
    // windows does not support SO_REUSEPORT
    return set_int_sockopt((SOCKET) fd, SOL_SOCKET, SO_REUSEADDR, 1);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastLoop
        (JNIEnv *e, jclass cl, jint fd, jboolean loop) {
    int result = setsockopt((SOCKET) fd, IPPROTO_IP, IP_MULTICAST_LOOP, (const char *) &loop, sizeof(loop));
    if (result == SOCKET_ERROR) {
        SaveLastError();
    }
    return result;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getTcpNoDelay
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((SOCKET) fd, IPPROTO_TCP, TCP_NODELAY);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getPeerIP
        (JNIEnv *e, jclass cl, jint fd) {

    struct sockaddr peer;
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohl(((struct sockaddr_in *) &peer)->sin_addr.s_addr);
        }
        return -2;
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getPeerPort
        (JNIEnv *e, jclass cl, jint fd) {

    struct sockaddr peer;
    int nameLen = sizeof(peer);

    if (getpeername((SOCKET) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohs(((struct sockaddr_in *) &peer)->sin_port);
        } else {
            return -2;
        }
    }
    SaveLastError();
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_resolvePort
        (JNIEnv *e, jclass cl, jint fd) {
    struct sockaddr_in resolved_addr;
    memset(&resolved_addr, 0, sizeof(resolved_addr));
    socklen_t resolved_addr_len = sizeof(resolved_addr);
    if (getsockname(
            fd,
            (struct sockaddr *)&resolved_addr,
            &resolved_addr_len) == -1)
        return -1;
    return ntohs(resolved_addr.sin_port);
}
