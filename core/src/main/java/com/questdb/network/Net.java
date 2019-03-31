/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.network;

import com.questdb.std.*;
import com.questdb.std.str.CharSink;

public final class Net {

    public static final long MMSGHDR_SIZE;
    public static final long MMSGHDR_BUFFER_ADDRESS_OFFSET;
    public static final long MMSGHDR_BUFFER_LENGTH_OFFSET;

    public static final int EWOULDBLOCK;
    public static final int ERETRY = 0;
    public static final int EPEERDISCONNECT = -1;
    @SuppressWarnings("unused")
    public static final int EOTHERDISCONNECT = -2;

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
        long acceptedFd = accept0(fd);
        if (acceptedFd != -1L) {
            Files.bumpFileCount();
        }
        return acceptedFd;
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

    public native static boolean bindUdp(long fd, int port);

    public static int close(long fd) {
        return Files.close(fd);
    }

    public native static int configureNoLinger(long fd);

    public static native int configureNonBlocking(long fd);

    public native static int connect(long fd, long sockaddr);

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

    public static native int recv(long fd, long ptr, int len);

    public static native int recvmmsg(long fd, long msgvec, int vlen);

    public static native int send(long fd, long ptr, int len);

    public native static int sendTo(long fd, long ptr, int len, long sockaddr);

    public native static int setMulticastInterface(long fd, int ipv4address);

    public native static int setMulticastLoop(long fd, boolean loop);

    public native static int setRcvBuf(long fd, int size);

    public native static int setReuseAddress(long fd);

    public native static int setReusePort(long fd);

    public native static int setSndBuf(long fd, int size);

    public native static int setTcpNoDelay(long fd, boolean noDelay);

    public static long sockaddr(CharSequence ipv4address, int port) {
        return sockaddr(parseIPv4(ipv4address), port);
    }

    public native static long sockaddr(int ipv4address, int port);

    public static long socketTcp(boolean blocking) {
        final long fd = socketTcp0(blocking);
        if (fd != -1L) {
            Files.bumpFileCount();
        }
        return fd;
    }

    public static long socketUdp() {
        long fd = socketUdp0();
        if (fd != -1L) {
            Files.bumpFileCount();
        }
        return fd;
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
        if (Os.type == Os.LINUX) {
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
