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

package com.nfsdb.misc;

import com.nfsdb.ex.NetworkError;
import com.nfsdb.ex.NumericException;
import com.nfsdb.io.sink.CharSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class Net {

    public static final int EWOULDBLOCK;

    public static final int ERETRY = 0;
    public static final int EPEERDISCONNECT = -1;
    @SuppressWarnings("unused")
    public static final int EOTHERDISCONNECT = -2;

    private Net() {
    }

    public native static long accept(long fd);

    public static void appendIP4(CharSink sink, long ip) {
        sink.put(ip & 0xff).put('.').put((ip >> 8) & 0xff).put('.').put((ip >> 16) & 0xff).put('.').put((ip >> 24) & 0xff);
    }

    public native static long available(long fd);

    public native static boolean bind(long fd, int address, int port);

    public static boolean bind(long fd, CharSequence address, int port) {
        return bind(fd, parseIPv4(address), port);
    }

    public static native int configureNonBlocking(long fd);

    public native static long getPeerIP(long fd);

    public native static int getPeerPort(long fd);

    public native static void listen(long fd, int backlog);

    public static native int recv(long fd, long ptr, int len);

    public static native int send(long fd, long ptr, int len);

    public native static int setRcvBuf(long fd, int size);

    public native static int setSndBuf(long fd, int size);

    public native static long socketTcp(boolean blocking);

    private native static int getEwouldblock();

    @SuppressFBWarnings("LEST_LOST_EXCEPTION_STACK_TRACE")
    private static int parseIPv4(CharSequence address) {
        int ip = 0;
        int count = 0;
        int lo = 0;
        int hi;
        try {
            while ((hi = Chars.indexOf(address, lo, '.')) > -1) {
                int n = Numbers.parseInt(address, lo, hi);
                ip = (ip << 8) | n;
                count++;
                lo = hi + 1;
            }

            if (count != 3) {
                throw new NetworkError("Invalid ip address: " + address);
            }

            return (ip << 8) | Numbers.parseInt(address, lo, address.length());
        } catch (NumericException e) {
            throw new NetworkError("Invalid ip address: " + address);
        }
    }

    static {
        EWOULDBLOCK = getEwouldblock();
    }
}
