/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.str.StdoutSink;

public final class Net {

    public static final long MMSGHDR_SIZE;
    public static final long MMSGHDR_BUFFER_ADDRESS_OFFSET;
    public static final long MMSGHDR_BUFFER_LENGTH_OFFSET;

    public static final int EWOULDBLOCK;
    @SuppressWarnings("unused")
    public static final int ERETRY = 0;
    @SuppressWarnings("unused")
    public static final int EPEERDISCONNECT = -1;
    @SuppressWarnings("unused")
    public static final int EOTHERDISCONNECT = -2;
    public static final int SHUT_WR = 1;

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
    public static native int abortAccept(long fd);

    public static long accept(long fd) {
        return Files.bumpFileCount(accept0(fd));
    }

    public static void appendIP4(CharSink sink, long ip) {
        sink.put((ip >> 24) & 0xff).put('.')
                .put((ip >> 16) & 0xff).put('.')
                .put((ip >> 8) & 0xff).put('.')
                .put(ip & 0xff);
    }

    public native static boolean bindTcp(long fd, int ipv4address, int port);

    public static boolean bindTcp(long fd, CharSequence ipv4address, int port) {
        return bindTcp(fd, parseIPv4(ipv4address), port);
    }

    public native static boolean bindUdp(long fd, int ipv4Address, int port);

    public static int close(long fd) {
        return Files.close(fd);
    }

    public native static int configureNoLinger(long fd);

    public static native int configureNonBlocking(long fd);

    public native static long connect(long fd, long sockaddr);

    public static void dump(long buffer, int len) {
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                Numbers.appendHex(StdoutSink.INSTANCE, Unsafe.getUnsafe().getByte(buffer + i) & 0xff);
            }
            StdoutSink.INSTANCE.put('\n');
            StdoutSink.INSTANCE.flush();
        }
    }

    public static void dumpAscii(long buffer, int len) {
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                StdoutSink.INSTANCE.put((char) Unsafe.getUnsafe().getByte(buffer + i));
            }
            StdoutSink.INSTANCE.put('\n');
            StdoutSink.INSTANCE.flush();
        }
    }

    public static native void freeMsgHeaders(long msgHeaders);

    public native static void freeSockAddr(long sockaddr);

    public static long getMMsgBuf(long msgPtr) {
        return Unsafe.getUnsafe().getLong(Unsafe.getUnsafe().getLong(msgPtr + MMSGHDR_BUFFER_ADDRESS_OFFSET));
    }

    public static int getMMsgBufLen(long msgPtr) {
        return Unsafe.getUnsafe().getInt(msgPtr + MMSGHDR_BUFFER_LENGTH_OFFSET);
    }

    public native static int getPeerIP(long fd);

    public native static int getPeerPort(long fd);

    public native static int getRcvBuf(long fd);

    public native static int getSndBuf(long fd);

    public native static int getTcpNoDelay(long fd);

    /**
     * This method reads 1 byte (or none if the socket is non blocking and there is no data).
     * If there is no error (EOF ?) then it returns false
     * If there is an error (EOF ?) then it returns true
     *
     * @param fd network file descriptor
     * @return check the description
     */
    public static native boolean isDead(long fd);

    public static boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return join(fd, parseIPv4(bindIPv4Address), parseIPv4(groupIPv4Address));
    }

    public native static boolean join(long fd, int bindIPv4Address, int groupIPv4Address);

    public native static void listen(long fd, int backlog);

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
                throw NetworkError.instance(0, "invalid address [").put(ipv4Address);
            }

            return (ip << 8) | Numbers.parseInt(ipv4Address, lo, ipv4Address.length());
        } catch (NumericException e) {
            throw NetworkError.instance(0, "invalid address [").put(ipv4Address);
        }
    }

    public static native int peek(long fd, long ptr, int len);

    public static native int recv(long fd, long ptr, int len);

    public static native int recvmmsg(long fd, long msgvec, int vlen);

    public static native int send(long fd, long ptr, int len);

    public native static int sendTo(long fd, long ptr, int len, long sockaddr);

    public native static int setMulticastInterface(long fd, int ipv4address);

    public native static int setMulticastLoop(long fd, boolean loop);

    public native static int setMulticastTtl(long fd, int ttl);

    public native static int setRcvBuf(long fd, int size);

    public native static int setReuseAddress(long fd);

    public native static int setReusePort(long fd);

    public native static int setSndBuf(long fd, int size);

    public native static int setTcpNoDelay(long fd, boolean noDelay);

    public native static int shutdown(long fd, int how);

    public static long sockaddr(CharSequence ipv4address, int port) {
        return sockaddr(parseIPv4(ipv4address), port);
    }

    public native static long sockaddr(int ipv4address, int port);

    public static long socketTcp(boolean blocking) {
        return Files.bumpFileCount(socketTcp0(blocking));
    }

    public static long socketUdp() {
        return Files.bumpFileCount(socketUdp0());
    }

    private native static long accept0(long fd);

    private native static long socketTcp0(boolean blocking);

    private native static long socketUdp0();

    private static native long getMsgHeaderSize();

    private static native long getMsgHeaderBufferAddressOffset();

    private static native long getMsgHeaderBufferLengthOffset();

    private native static int getEwouldblock();

    static {
        Os.init();
        EWOULDBLOCK = getEwouldblock();
        if (Os.type == Os.LINUX_AMD64) {
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
