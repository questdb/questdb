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

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StdoutSink;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.std.Files.toOsFd;

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
    private static final Log LOG = LogFactory.getLog(Net.class);
    private static final AtomicInteger SOCK_ADDR_COUNTER = new AtomicInteger();
    // TCP KeepAlive not meant to be configurable. It's a last resort measure to disable/change keepalive if the default
    // value causes problems in some environments. If it does not cause problems then this option should be removed after a few releases.
    // It's not exposed as PropertyKey, because it would become a supported and hard to remove API.
    private static final int TCP_KEEPALIVE_SECONDS = Integer.getInteger("questdb.unsupported.tcp.keepalive.seconds", 30);

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
    public static int abortAccept(long fd) {
        return abortAccept(toOsFd(fd));
    }

    public static long accept(long serverFd) {
        return Files.createUniqueFd(accept0(toOsFd(serverFd)));
    }

    public static void appendIP4(CharSink<?> sink, long ip) {
        sink.put((ip >> 24) & 0xff).putAscii('.')
                .put((ip >> 16) & 0xff).putAscii('.')
                .put((ip >> 8) & 0xff).putAscii('.')
                .put(ip & 0xff);
    }

    public static boolean bindTcp(long fd, int ipv4address, int port) {
        return bindTcp(toOsFd(fd), ipv4address, port);
    }

    public static boolean bindTcp(long fd, CharSequence ipv4address, int port) {
        return bindTcp(fd, parseIPv4(ipv4address), port);
    }

    public static boolean bindUdp(long fd, int ipv4Address, int port) {
        return bindUdp(toOsFd(fd), ipv4Address, port);
    }

    public static int close(long fd) {
        return Files.close(fd);
    }

    public static void configureKeepAlive(long fd) {
        if (TCP_KEEPALIVE_SECONDS < 0 || fd < 0) {
            return;
        }
        if (setKeepAlive0(toOsFd(fd), TCP_KEEPALIVE_SECONDS) < 0) {
            int errno = Os.errno();
            LOG.error().$("could not set tcp keepalive [fd=").$(fd).$(", errno=").$(errno).I$();
        }
    }

    public static int configureLinger(long fd, int seconds) {
        return configureLinger(toOsFd(fd), seconds);
    }

    public static int configureNoLinger(long fd) {
        return configureLinger(toOsFd(fd), 0);
    }

    public static int configureNonBlocking(long fd) {
        return configureNonBlocking(toOsFd(fd));
    }

    public static int connect(long fd, long sockaddr) {
        return connect(toOsFd(fd), sockaddr);
    }

    public static int connectAddrInfo(long fd, long lpAddrInfo) {
        return connectAddrInfo(toOsFd(fd), lpAddrInfo);
    }

    public static void dump(long buffer, int len) {
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                Numbers.appendHex(StdoutSink.INSTANCE, Unsafe.getUnsafe().getByte(buffer + i) & 0xff);
            }
            StdoutSink.INSTANCE.put('\n');
            StdoutSink.INSTANCE.flush();
        }
    }

    @SuppressWarnings("unused")
    public static void dumpAscii(long buffer, int len) {
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                char c = (char) (Unsafe.getUnsafe().getByte(buffer + i) & 0xff);
                switch (c) {
                    case '\r':
                        System.out.print("\\r");
                        break;
                    case '\n':
                        System.out.print("\\n");
                        System.out.print(c);
                        break;
                    default:
                        System.out.print(c);
                        break;
                }
            }
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
        return getAddrInfo(host.ptr(), port);
    }

    public static long getAddrInfo(CharSequence host, int port) {
        try (Path p = new Path()) {
            return getAddrInfo(p.of(host).$(), port);
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

    public static int getPeerIP(long fd) {
        return getPeerIP(toOsFd(fd));
    }

    public static int getRcvBuf(long fd) {
        return getRcvBuf(toOsFd(fd));
    }

    public static int getSndBuf(long fd) {
        return getSndBuf(toOsFd(fd));
    }

    public static int getTcpNoDelay(long fd) {
        return getTcpNoDelay(toOsFd(fd));
    }

    public static void init() {
        // no-op
    }

    /**
     * This method reads 1 byte (or none if the socket is non-blocking and there is no data).
     * If there is no error (EOF ?) then it returns false
     * If there is an error (EOF ?) then it returns true
     *
     * @param fd network file descriptor
     * @return check the description
     */
    public static boolean isDead(long fd) {
        return isDead(toOsFd(fd));
    }

    public static boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return join(toOsFd(fd), parseIPv4(bindIPv4Address), parseIPv4(groupIPv4Address));
    }

    public static boolean join(long fd, int bindIPv4Address, int groupIPv4Address) {
        return join(toOsFd(fd), bindIPv4Address, groupIPv4Address);
    }

    public static void listen(long fd, int backlog) {
        listen(toOsFd(fd), backlog);
    }

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

    public static int peek(long fd, long ptr, int len) {
        return peek(toOsFd(fd), ptr, len);
    }

    public static int recv(long fd, long ptr, int len) {
        return recv(toOsFd(fd), ptr, len);
    }

    public static int recvmmsg(long fd, long msgvec, int vlen) {
        return recvmmsg(toOsFd(fd), msgvec, vlen);
    }

    public static int resolvePort(long fd) {
        return resolvePort(toOsFd(fd));
    }

    public static int send(long fd, long ptr, int len) {
        return send(toOsFd(fd), ptr, len);
    }

    public static int sendTo(long fd, long ptr, int len, long sockaddr) {
        return sendTo(toOsFd(fd), ptr, len, sockaddr);
    }

    public static int setMulticastInterface(long fd, int ipv4address) {
        return setMulticastInterface(toOsFd(fd), ipv4address);
    }

    public static int setMulticastLoop(long fd, boolean loop) {
        return setMulticastLoop(toOsFd(fd), loop);
    }

    public static int setMulticastTtl(long fd, int ttl) {
        return setMulticastTtl(toOsFd(fd), ttl);
    }

    public static int setRcvBuf(long fd, int size) {
        return setRcvBuf(toOsFd(fd), size);
    }

    public static int setReuseAddress(long fd) {
        return setReuseAddress(toOsFd(fd));
    }

    public static int setReusePort(long fd) {
        return setReusePort(toOsFd(fd));
    }

    public static int setSndBuf(long fd, int size) {
        return setSndBuf(toOsFd(fd), size);
    }

    public static int setTcpNoDelay(long fd, boolean noDelay) {
        return setTcpNoDelay(toOsFd(fd), noDelay);
    }

    public static int shutdown(long fd, int how) {
        return shutdown(toOsFd(fd), how);
    }

    public static long sockaddr(CharSequence ipv4address, int port) {
        return sockaddr(parseIPv4(ipv4address), port);
    }

    public static long sockaddr(int ipv4address, int port) {
        SOCK_ADDR_COUNTER.incrementAndGet();
        return sockaddr0(ipv4address, port);
    }

    public static long socketTcp(boolean blocking) {
        return Files.createUniqueFd(socketTcp0(blocking));
    }

    public static long socketUdp() {
        return Files.createUniqueFd(socketUdp0());
    }

    private static native int abortAccept(int fd);

    private native static int accept0(int serverFd);

    private native static boolean bindTcp(int fd, int ipv4address, int port);

    private native static boolean bindUdp(int fd, int ipv4Address, int port);

    private native static int configureLinger(int fd, int seconds);

    private static native int configureNonBlocking(int fd);

    private native static int connect(int fd, long sockaddr);

    private native static int connectAddrInfo(int fd, long lpAddrInfo);

    private static native void freeAddrInfo0(long pAddrInfo);

    private static native void freeSockAddr0(long sockaddr);

    private static native long getAddrInfo0(long lpszHost, int port);

    private static native int getEwouldblock();

    private static native long getMsgHeaderBufferAddressOffset();

    private static native long getMsgHeaderBufferLengthOffset();

    private static native long getMsgHeaderSize();

    private native static int getPeerIP(int fd);

    private native static int getPeerPort(int fd);

    private native static int getRcvBuf(int fd);

    private native static int getSndBuf(int fd);

    private native static int getTcpNoDelay(int fd);

    private static native boolean isDead(int fd);

    private native static boolean join(int fd, int bindIPv4Address, int groupIPv4Address);

    private native static void listen(int fd, int backlog);

    private static native int peek(int fd, long ptr, int len);

    private static native int recv(int fd, long ptr, int len);

    private static native int recvmmsg(int fd, long msgvec, int vlen);

    private native static int resolvePort(int fd);

    private static native int send(int fd, long ptr, int len);

    private native static int sendTo(int fd, long ptr, int len, long sockaddr);

    private static native int setKeepAlive0(int fd, int seconds);

    private native static int setMulticastInterface(int fd, int ipv4address);

    private native static int setMulticastLoop(int fd, boolean loop);

    private native static int setMulticastTtl(int fd, int ttl);

    private native static int setRcvBuf(int fd, int size);

    private native static int setReuseAddress(int fd);

    private native static int setReusePort(int fd);

    private native static int setSndBuf(int fd, int size);

    private native static int setTcpNoDelay(int fd, boolean noDelay);

    private native static int shutdown(int fd, int how);

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
