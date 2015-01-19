/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.utils;

import com.nfsdb.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.exceptions.JournalNetworkException;
import sun.misc.Cleaner;
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
            if (to.write(from) <= 0) {
                throw new JournalNetworkException("Write to closed channel");
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public static int copy(ByteBuffer from, WritableByteChannel to, int count) throws JournalNetworkException {
        if (count <= 0 || !from.hasRemaining()) {
            return 0;
        }

        int result;

        try {
            if (count >= from.remaining()) {
                if ((result = to.write(from)) <= 0) {
                    throw new JournalNetworkException("Write to closed channel");
                }
                return result;
            }

            int limit = from.limit();
            try {
                from.limit(from.position() + count);
                if ((result = to.write(from)) <= 0) {
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
            int result = from.read(to);
            if (result == -1) {
                throw new JournalDisconnectedChannelException();
            }
            return result;
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    public static int copy(ReadableByteChannel from, ByteBuffer to, int count) throws JournalNetworkException {
        if (count >= to.remaining()) {
            return copy(from, to);
        }

        int result = 0;
        int limit = to.limit();
        try {
            to.limit(to.position() + count);
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

    /**
     * Releases ByteBuffer is possible. Call semantics should be as follows:
     * <p/>
     * ByteBuffer buffer = ....
     * <p/>
     * buffer = release(buffer);
     *
     * @param buffer direct byte buffer
     * @return null if buffer is released or same buffer if release is not possible.
     */
    public static <T extends ByteBuffer> T release(final T buffer) {
        if (buffer != null) {
            if (buffer instanceof DirectBuffer) {
                Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
                if (cleaner != null) {
                    cleaner.clean();
                    return null;
                }
            }
        }
        return buffer;
    }

    public static void putStringW(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putChar((char) 0);
        } else {
            buffer.putChar((char) value.length());
            putStr(buffer, value);
        }
    }

    public static void putStringDW(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putInt(0);
        } else {
            buffer.putInt(value.length());
            putStr(buffer, value);
        }
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

    public static int copy(ByteBuffer from, ByteBuffer to) {
        int result = from.remaining();
        if ((from instanceof DirectBuffer) && (to instanceof DirectBuffer)) {
            Unsafe.getUnsafe().copyMemory(((DirectBuffer) from).address() + from.position(), ((DirectBuffer) to).address() + to.position(), result);
            from.position(from.position() + result);
            to.position(to.position() + result);
        } else {
            to.put(from);
        }
        return result;
    }

    public static void copy(ByteBuffer from, ByteBuffer to, int count) {

        if (count <= 0) {
            return;
        }

        if (count > to.remaining()) {
            count = to.remaining();
        }

        if (from.remaining() <= count) {
            copy(from, to);
            return;
        }

        int limit = from.limit();
        try {
            from.limit(from.position() + count);
            copy(from, to);
        } finally {
            from.limit(limit);
        }
    }

    public static void putStr(ByteBuffer buffer, String value) {
        int p = buffer.position();
        for (int i = 0; i < value.length(); i++) {
            buffer.putChar(p, value.charAt(i));
            p += 2;
        }
        buffer.position(p);
    }
}
