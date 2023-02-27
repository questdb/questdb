/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.network;

import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StdoutSink;

import java.util.concurrent.atomic.AtomicInteger;

public final class Net {

    @SuppressWarnings("unused")
    public static final int EOTHERDISCONNECT = -2;
    @SuppressWarnings("unused")
    public static final int EPEERDISCONNECT = -1;
    @SuppressWarnings("unused")
    public static final int ERETRY = 0;
    public static final int EWOULDBLOCK;
    public static final long MMSGHDR_BUFFER_ADDRESS_OFFSET;
    public static final long MMSGHDR_BUFFER_LENGTH_OFFSET;
    public static final long MMSGHDR_SIZE;
    public static final int SHUT_WR = 1;

    private static final AtomicInteger ADDR_INFO_COUNTER = new AtomicInteger();
    private static final AtomicInteger SOCK_ADDR_COUNTER = new AtomicInteger();

    private Net() {
    }

    /**
     * Aborts blocking accept() call. On Darwin and Windows
     * this method simply closes the underlying file descriptor.
     * On Linux this method calls shutdown().
     * <p>
     * Once accept() exists it is by convention liable for
     * closing its own file descriptor.
     *
     * @param fd file descriptor
     * @return 0 when call was successful and -1 otherwise. In case of
     * error errno() would return error code.
     */
    public static native int abortAccept(int fd);

    public static int accept(int serverFd) {
        return Files.bumpFileCount(accept0(serverFd));
    }

    public static void appendIP4(CharSink sink, long ip) {
        sink.put((ip >> 24) & 0xff).put('.')
                .put((ip >> 16) & 0xff).put('.')
                .put((ip >> 8) & 0xff).put('.')
                .put(ip & 0xff);
    }

    public native static boolean bindTcp(int fd, int ipv4address, int port);

    public static boolean bindTcp(int fd, CharSequence ipv4address, int port) {
        return bindTcp(fd, parseIPv4(ipv4address), port);
    }

    public native static boolean bindUdp(int fd, int ipv4Address, int port);

    public static void bumpFdCount(int fd) {
        Files.bumpFileCount(fd);
    }

    public static int close(int fd) {
        return Files.close(fd);
    }

    public native static int configureLinger(int fd, int seconds);

    public static int configureNoLinger(int fd) {
        return configureLinger(fd, 0);
    }

    public static native int configureNonBlocking(int fd);

    public native static int connect(int fd, long sockaddr);

    public native static int connectAddrInfo(int fd, long lpAddrInfo);

    public static void dump(long buffer, int len) {
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                Numbers.appendHex(StdoutSink.INSTANCE, Unsafe.getUnsafe().getByte(buffer + i) & 0xff);
            }
            StdoutSink.INSTANCE.put('\n');
            StdoutSink.INSTANCE.flush();
        }
    }

    public static void freeAddrInfo(long pAddrInfo) {
        if (pAddrInfo != 0) {
            ADDR_INFO_COUNTER.decrementAndGet();
        }
        freeAddrInfo0(pAddrInfo);
    }

    public static native void freeMsgHeaders(long msgHeaders);

    public static void freeSockAddr(long sockaddr) {
        if (sockaddr != 0) {
            SOCK_ADDR_COUNTER.decrementAndGet();
        }
        freeSockAddr0(sockaddr);
    }

    public static long getAddrInfo(LPSZ host, int port) {
        return getAddrInfo(host.address(), port);
    }

    public static long getAddrInfo(CharSequence host, int port) {
        try (Path p = new Path().of(host).$()) {
            return getAddrInfo(p, port);
        }
    }

    public static long getAddrInfo(long lpszHost, int port) {
        long addrInfo = getAddrInfo0(lpszHost, port);
        if (addrInfo != -1) {
            ADDR_INFO_COUNTER.incrementAndGet();
        }
        return addrInfo;
    }

    public static int getAllocatedAddrInfoCount() {
        return ADDR_INFO_COUNTER.get();
    }

    public static int getAllocatedSockAddrCount() {
        return SOCK_ADDR_COUNTER.get();
    }

    public static long getMMsgBuf(long msgPtr) {
        return Unsafe.getUnsafe().getLong(Unsafe.getUnsafe().getLong(msgPtr + MMSGHDR_BUFFER_ADDRESS_OFFSET));
    }

    public static int getMMsgBufLen(long msgPtr) {
        return Unsafe.getUnsafe().getInt(msgPtr + MMSGHDR_BUFFER_LENGTH_OFFSET);
    }

    public native static int getPeerIP(int fd);

    public native static int getPeerPort(int fd);

    public native static int getRcvBuf(int fd);

    public native static int getSndBuf(int fd);

    public native static int getTcpNoDelay(int fd);

    /**
     * This method reads 1 byte (or none if the socket is non-blocking and there is no data).
     * If there is no error (EOF ?) then it returns false
     * If there is an error (EOF ?) then it returns true
     *
     * @param fd network file descriptor
     * @return check the description
     */
    public static native boolean isDead(int fd);

    public static boolean join(int fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return join(fd, parseIPv4(bindIPv4Address), parseIPv4(groupIPv4Address));
    }

    public native static boolean join(int fd, int bindIPv4Address, int groupIPv4Address);

    public native static void listen(int fd, int backlog);

    public static native long msgHeaders(int blockSize, int count);

    public static int parseIPv4(CharSequence ipv4Address) {
        int ip = 0;
        int count = 0;
        int lo = 0;
        int hi;
        try {
            while ((hi = Chars.indexOf(ipv4Address, lo, '.')) > -1) {
                int n = Numbers.parseInt(ipv4Address, lo, hi);
                ip = (ip << 8) | n;
                count++;
                lo = hi + 1;
            }

            if (count != 3) {
                throw NetworkError.instance(0, "invalid address [").put(ipv4Address).put(']');
            }

            return (ip << 8) | Numbers.parseInt(ipv4Address, lo, ipv4Address.length());
        } catch (NumericException e) {
            throw NetworkError.instance(0, "invalid address [").put(ipv4Address).put(']');
        }
    }

    public static native int peek(int fd, long ptr, int len);

    public static native int recv(int fd, long ptr, int len);

    public static native int recvmmsg(int fd, long msgvec, int vlen);

    public native static int resolvePort(int fd);

    public static native int send(int fd, long ptr, int len);

    public native static int sendTo(int fd, long ptr, int len, long sockaddr);

    public native static int setMulticastInterface(int fd, int ipv4address);

    public native static int setMulticastLoop(int fd, boolean loop);

    public native static int setMulticastTtl(int fd, int ttl);

    public native static int setRcvBuf(int fd, int size);

    public native static int setReuseAddress(int fd);

    public native static int setReusePort(int fd);

    public native static int setSndBuf(int fd, int size);

    public native static int setTcpNoDelay(int fd, boolean noDelay);

    public native static int shutdown(int fd, int how);

    public static long sockaddr(CharSequence ipv4address, int port) {
        return sockaddr(parseIPv4(ipv4address), port);
    }

    public static long sockaddr(int ipv4address, int port) {
        SOCK_ADDR_COUNTER.incrementAndGet();
        return sockaddr0(ipv4address, port);
    }

    public static int socketTcp(boolean blocking) {
        return Files.bumpFileCount(socketTcp0(blocking));
    }

    public static int socketUdp() {
        return Files.bumpFileCount(socketUdp0());
    }

    private native static int accept0(int serverFd);

    private static native void freeAddrInfo0(long pAddrInfo);

    private static native void freeSockAddr0(long sockaddr);

    private static native long getAddrInfo0(long lpszHost, int port);

    private native static int getEwouldblock();

    private static native long getMsgHeaderBufferAddressOffset();

    private static native long getMsgHeaderBufferLengthOffset();

    private static native long getMsgHeaderSize();

    private native static long sockaddr0(int ipv4address, int port);

    private native static int socketTcp0(boolean blocking);

    private native static int socketUdp0();

    static {
        Os.init();
        EWOULDBLOCK = getEwouldblock();
        if (Os.isLinux()) {
            MMSGHDR_SIZE = getMsgHeaderSize();
            MMSGHDR_BUFFER_ADDRESS_OFFSET = getMsgHeaderBufferAddressOffset();
            MMSGHDR_BUFFER_LENGTH_OFFSET = getMsgHeaderBufferLengthOffset();
        } else {
            MMSGHDR_SIZE = -1L;
            MMSGHDR_BUFFER_ADDRESS_OFFSET = -1L;
            MMSGHDR_BUFFER_LENGTH_OFFSET = -1L;
        }
    }
}
