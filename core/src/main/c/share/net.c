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
#include <sys/socket.h>
#include <sys/fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <string.h>
#include "net.h"
#include <netdb.h>
#include "sysutil.h"
#ifndef __APPLE__
#include <sys/un.h>
#endif

jint handleEintrInConnect(jint fd, int result);

int set_int_sockopt(int fd, int level, int opt, int value) {
    return setsockopt(fd, level, opt, &value, sizeof(value));
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setKeepAlive0
        (JNIEnv *e, jclass cl, jint fd, jint idle_sec) {
    if (set_int_sockopt(fd, SOL_SOCKET, SO_KEEPALIVE, 1) < 0) {
        return -1;
    }
#if defined(__linux__) || defined(__FreeBSD__)
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, idle_sec) < 0) {
        return -1;
    }
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, idle_sec) < 0) {
        return -1;
    }
#endif
#ifdef __APPLE__
    if (set_int_sockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, idle_sec) < 0) {
        return -1;
    }
#endif
    return fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_socketTcp0
        (JNIEnv *e, jclass cl, jboolean blocking) {
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

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_socketUdp0
        (JNIEnv *e, jclass cl) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);

    if (fd > 0 && fcntl(fd, F_SETFL, O_NONBLOCK) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_sockaddr0
        (JNIEnv *e, jclass cl, jint address, jint port) {
    struct sockaddr_in *addr = calloc(1, sizeof(struct sockaddr_in));
    addr->sin_family = AF_INET;
    addr->sin_addr.s_addr = htonl((uint32_t) address);
    addr->sin_port = htons((uint16_t) port);
    return (jlong) addr;
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_freeSockAddr0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        free((void *) address);
    }
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_shutdown
        (JNIEnv *e, jclass cl, jint fd, jint how) {
    return shutdown((int) fd, how);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_sendTo
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len, jlong sockaddr) {
    return (jint) sendto((int) fd, (const void *) ptr, (size_t) len, 0, (const struct sockaddr *) sockaddr,
                         sizeof(struct sockaddr_in));
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_bindTcp
        (JNIEnv *e, jobject cl, jint fd, jint address, jint port) {
    struct sockaddr_in addr;

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl((uint32_t) address);
    addr.sin_port = htons((uint16_t) port);

    return (jboolean) (bind((int) fd, (struct sockaddr *) &addr, sizeof(addr)) == 0);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_bindUdp
        (JNIEnv *e, jobject cl, jint fd, jint ipv4Address, jint port) {
    return Java_io_questdb_network_Net_bindTcp(e, cl, fd, ipv4Address, port);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_join
        (JNIEnv *e, jclass cl, jint fd, jint bindAddress, jint groupAddress) {
    struct ip_mreq mreq;
    mreq.imr_interface.s_addr = htonl((uint32_t) bindAddress);
    mreq.imr_multiaddr.s_addr = htonl((uint32_t) groupAddress);
    return (jboolean) (setsockopt((int) fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0 ? JNI_FALSE
                                                                                                    : JNI_TRUE);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_accept0
        (JNIEnv *e, jobject cl, jint fd) {
    return accept((int) fd, NULL, NULL);
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_listen
        (JNIEnv *e, jclass cl, jint fd, jint backlog) {
    listen((int) fd, backlog);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_send
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(send((int) fd, (const void *) ptr, (size_t) len, 0), n);
    if (n > -1) {
        return n;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_recv
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(recv((int) fd, (void *) ptr, (size_t) len, 0), n);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_peek
        (JNIEnv *e, jclass cl, jint fd, jlong ptr, jint len) {
    ssize_t n;
    RESTARTABLE(recv((int) fd, (void *) ptr, (size_t) len, MSG_PEEK), n);
    if (n > 0) {
        return n;
    }

    if (n == 0) {
        return com_questdb_network_Net_EOTHERDISCONNECT;
    }

    if (errno == EWOULDBLOCK) {
        return com_questdb_network_Net_ERETRY;
    }

    return com_questdb_network_Net_EOTHERDISCONNECT;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_network_Net_isDead
        (JNIEnv *e, jclass cl, jint fd) {
    int c;
    ssize_t res;
    RESTARTABLE(recv((int) fd, &c, 1, 0), res);
    return (jboolean) (res < 1);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_configureNonBlocking
        (JNIEnv *e, jclass cl, jint fd) {
    int flags;

    if ((flags = fcntl((int) fd, F_GETFL, 0)) < 0) {
        return flags;
    }

    if ((flags = fcntl((int) fd, F_SETFL, flags | O_NONBLOCK)) < 0) {
        return flags;
    }

    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_configureLinger
        (JNIEnv *e, jclass cl, jint fd, jint seconds) {
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = seconds;
    return setsockopt((int) fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
}

JNIEXPORT jint handleEintrInConnect(jint fd, int result) {
    if (result == -1 && errno == EINTR) {
        // Connection was interrupted but continues in background
        // Wait for it to complete using select()
        fd_set writefds, exceptfds;
        struct timeval timeout;

        FD_ZERO(&writefds);
        FD_ZERO(&exceptfds);
        FD_SET(fd, &writefds);
        FD_SET(fd, &exceptfds);

        // Set a reasonable timeout (e.g., 30 seconds)
        timeout.tv_sec = 30;
        timeout.tv_usec = 0;

        int select_result = select(fd + 1, NULL, &writefds, &exceptfds, &timeout);

        if (select_result > 0) {
            if (FD_ISSET(fd, &exceptfds)) {
                // Exception occurred
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error != 0) {
                    errno = error;
                }
                return -1;
            } else if (FD_ISSET(fd, &writefds)) {
                // Socket is writable, check for connection error
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0) {
                    if (error == 0) {
                        return 0; // Success
                    } else {
                        errno = error;
                        return -1;
                    }
                }
                return -1;
            }
        } else if (select_result == 0) {
            // Timeout
            errno = ETIMEDOUT;
            return -1;
        } else {
            // select() failed
            return -1;
        }
    }

    return result;
}

jint JNICALL Java_io_questdb_network_Net_connect
        (JNIEnv *e, jclass cl, jint fd, jlong sockAddr) {
    int result;

    struct sockaddr *addr = (struct sockaddr *) sockAddr;
    socklen_t addrlen;

    switch (addr->sa_family) {
        case AF_INET:
            addrlen = sizeof(struct sockaddr_in);
            break;
        case AF_INET6:
            addrlen = sizeof(struct sockaddr_in6);
            break;
#ifndef __APPLE__
            case AF_UNIX:
                addrlen = sizeof(struct sockaddr_un);
                break;
#endif
        default:
            return -2;
    }

    result = connect((int) fd, (const struct sockaddr *) sockAddr, addrlen);
    return handleEintrInConnect(fd, result);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setSndBuf
        (JNIEnv *e, jclass cl, jint fd, jint size) {
    return set_int_sockopt((int) fd, SOL_SOCKET, SO_SNDBUF, size);
}

int get_int_sockopt(int fd, int level, int opt) {
    int value = 0;
    socklen_t len = sizeof(value);
    int result = getsockopt(fd, level, opt, &value, &len);
    if (result == 0) {
        return value;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastTtl
        (JNIEnv *e, jclass cl, jint fd, jint ttl) {
    u_char lTTL = ttl;
    int result = setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, (char *) &lTTL, sizeof(lTTL));
    if (result == 0) {
        return result;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getSndBuf
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((int) fd, SOL_SOCKET, SO_SNDBUF);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setRcvBuf
        (JNIEnv *e, jclass cl, jint fd, jint size) {
    return set_int_sockopt((int) fd, SOL_SOCKET, SO_RCVBUF, size);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getRcvBuf
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((int) fd, SOL_SOCKET, SO_RCVBUF);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastInterface
        (JNIEnv *e, jclass cl, jint fd, jint ipv4address) {
    struct in_addr address;
    address.s_addr = (in_addr_t) htonl((__uint32_t) ipv4address);
    return setsockopt((int) fd, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address));
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setMulticastLoop
        (JNIEnv *e, jclass cl, jint fd, jboolean loop) {
    return setsockopt((int) fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setReuseAddress
        (JNIEnv *e, jclass cl, jint fd) {
    int optval = 1;
    return setsockopt((int) fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setReusePort
        (JNIEnv *e, jclass cl, jint fd) {
    int optval = 1;
    return setsockopt((int) fd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_setTcpNoDelay
        (JNIEnv *e, jclass cl, jint fd, jboolean noDelay) {
    return set_int_sockopt((int) fd, IPPROTO_TCP, TCP_NODELAY, noDelay);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getTcpNoDelay
        (JNIEnv *e, jclass cl, jint fd) {
    return get_int_sockopt((int) fd, IPPROTO_TCP, TCP_NODELAY);
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getEwouldblock
        (JNIEnv *e, jclass cl) {
    return EWOULDBLOCK;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getPeerIP
        (JNIEnv *e, jclass cl, jint fd) {
    struct sockaddr peer;
    socklen_t nameLen = sizeof(peer);

    if (getpeername((int) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohl(((struct sockaddr_in *) &peer)->sin_addr.s_addr);
        }
        return -2;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_getPeerPort
        (JNIEnv *e, jclass cl, jint fd) {
    struct sockaddr peer;
    socklen_t nameLen = sizeof(peer);

    if (getpeername((int) fd, &peer, &nameLen) == 0) {
        if (peer.sa_family == AF_INET) {
            return ntohs(((struct sockaddr_in *) &peer)->sin_port);
        } else {
            return -2;
        }
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_connectAddrInfo
        (JNIEnv *e, jclass cl, jint fd, jlong lpAddrInfo) {
    struct addrinfo *addr = (struct addrinfo *) lpAddrInfo;
    int result;

    result = connect((int) fd, addr->ai_addr, (int) addr->ai_addrlen);
    return handleEintrInConnect(fd, result);
}

JNIEXPORT void JNICALL Java_io_questdb_network_Net_freeAddrInfo0
        (JNIEnv *e, jclass cl, jlong address) {
    if (address != 0) {
        freeaddrinfo((void *) address);
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_network_Net_getAddrInfo0
        (JNIEnv *e, jclass cl, jlong host, jint port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICSERV;
    struct addrinfo *addr = NULL;

    char _port[13];
    snprintf(_port, sizeof(_port) / sizeof(_port[0]), "%d", port);
    int gai_err_code = getaddrinfo((const char *) host, (const char *) &_port, &hints, &addr);

    if (gai_err_code == 0) {
        return (jlong) addr;
    }
    return -1;
}

JNIEXPORT jint JNICALL Java_io_questdb_network_Net_resolvePort
        (JNIEnv *e, jclass cl, jint fd) {
    struct sockaddr_in resolved_addr;
    memset(&resolved_addr, 0, sizeof(resolved_addr));
    socklen_t resolved_addr_len = sizeof(resolved_addr);
    if (getsockname(
            fd,
            (struct sockaddr *) &resolved_addr,
            &resolved_addr_len) == -1)
        return -1;
    return ntohs(resolved_addr.sin_port);
}
