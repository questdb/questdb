/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.utils;

import com.nfsdb.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.exceptions.JournalNetworkException;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public final class ByteBuffers {

    //    private static final int[] multipliers = new int[]{1, 3, 5, 7, 9, 11, 13};
    private static final int[] multipliers = new int[]{1, 3};

    private ByteBuffers() {
    }

    public static void copy(ByteBuffer from, WritableByteChannel to) throws JournalNetworkException {
        try {
            if (to.write(from) < 1) {
                throw new JournalNetworkException("Write to closed channel");
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public static int copy(ByteBuffer from, WritableByteChannel to, long count) throws JournalNetworkException {
        if (count < 1 || !from.hasRemaining()) {
            return 0;
        }

        int result;

        try {
            if (count >= from.remaining()) {
                if ((result = to.write(from)) < 1) {
                    throw new JournalNetworkException("Write to closed channel");
                }
                return result;
            }

            int limit = from.limit();
            try {
                from.limit((int) (from.position() + count));
                if ((result = to.write(from)) < 1) {
                    throw new JournalNetworkException("Write to closed channel");
                }

                return result;
            } finally {
                from.limit(limit);
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public static int copy(ReadableByteChannel from, ByteBuffer to) throws JournalNetworkException {
        try {
            int r = to.remaining();
            int target = r;
            while (target > 0) {
                int result = from.read(to);
                if (result == -1) {
                    throw new JournalDisconnectedChannelException();
                }
                target -= result;
            }
            return r;
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public static int copy(ReadableByteChannel from, ByteBuffer to, long count) throws JournalNetworkException {
        return count < to.remaining() ? copy0(from, to, count) : copy(from, to);
    }

    public static void copy(ByteBuffer from, ByteBuffer to) {
        int x = from.remaining();
        int y = to.remaining();
        int d = x < y ? x : y;
        if ((from instanceof DirectBuffer) && (to instanceof DirectBuffer)) {
            Unsafe.getUnsafe().copyMemory(getAddress(from) + from.position(), getAddress(to) + to.position(), d);
            from.position(from.position() + d);
            to.position(to.position() + d);
        } else {
            to.put(from);
        }
    }

    public static long getAddress(ByteBuffer buffer) {
        return ((DirectBuffer) buffer).address();
    }

    public static int getBitHint(int recSize, int recCount) {
//                return Math.min(30, 32 - Integer.numberOfLeadingZeros(recSize * recCount));
        long target = ((long) recSize) * recCount;
        long minDeviation = Long.MAX_VALUE;
        int resultBits = 0;
        for (int i = 0; i < multipliers.length; i++) {
            int m = multipliers[i];
            int bits = Math.min(30, 32 - Integer.numberOfLeadingZeros(recSize * recCount / m));
            long actual = (1 << bits) * m;

            long deviation;
            if (target / actual > multipliers[multipliers.length - 1]) {
                return bits;
            }

            if (actual <= target) {
                deviation = 100 + ((target % actual) * 100 / (1 << bits));
            } else {
                deviation = (actual * 100) / target;
            }
            if (deviation < minDeviation) {
                minDeviation = deviation;
                resultBits = bits;
            }
        }
        return resultBits;
    }

    public static long getMaxMappedBufferSize(long channelSize) {
        long max = Os.getSystemMemory() / 4;
        max = max > Integer.MAX_VALUE ? Integer.MAX_VALUE : max;
        return channelSize > max ? max : channelSize;
    }

    public static void putStr(ByteBuffer buffer, CharSequence value) {
        int p = buffer.position();
        for (int i = 0; i < value.length(); i++) {
            buffer.putChar(p, value.charAt(i));
            p += 2;
        }
        buffer.position(p);
    }

    public static void putStringDW(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putInt(0);
        } else {
            buffer.putInt(value.length());
            putStr(buffer, value);
        }
    }

    public static void putStringW(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putChar((char) 0);
        } else {
            buffer.putChar((char) value.length());
            putStr(buffer, value);
        }
    }

    /**
     * Releases ByteBuffer if possible. Call semantics should be as follows:
     * <p/>
     * ByteBuffer buffer = ....
     * <p/>
     * buffer = release(buffer);
     *
     * @param buffer direct byte buffer
     * @return null if buffer is released or same buffer if release is not possible.
     */
    public static <T extends ByteBuffer> T release(final T buffer) {
        if (buffer instanceof DirectBuffer) {
            ((DirectBuffer) buffer).cleaner().clean();
            return null;
        }
        return buffer;
    }

    private static int copy0(ReadableByteChannel from, ByteBuffer to, long count) throws JournalNetworkException {
        int result = 0;
        int limit = to.limit();
        try {
            to.limit((int) (to.position() + count));
            try {
                result = from.read(to);
            } catch (IOException e) {
                throw new JournalNetworkException(e);
            }
            if (result == -1) {
                throw new JournalDisconnectedChannelException();
            }
        } finally {
            to.limit(limit);
        }
        return result;
    }


}
