/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.utils;

import com.nfsdb.journal.exceptions.JournalDisconnectedChannelException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public final class ByteBuffers {

    private static final int[] multipliers = new int[]{1, 3, 5, 7, 9, 11, 13};

    private ByteBuffers() {
    }

    public static void copy(ByteBuffer from, WritableByteChannel to) throws JournalNetworkException {
        copy(from, to, from.remaining());
    }

    public static int copy(ByteBuffer from, WritableByteChannel to, int count) throws JournalNetworkException {
        int result = 0;
        if (to != null) {
            int limit = from.limit();
            try {
                if (from.remaining() > count) {
                    from.limit(from.position() + count);
                }
                result = from.remaining();
                if (result > 0) {
                    try {
                        int n = to.write(from);
                        if (n <= 0) {
                            throw new JournalNetworkException("Write to closed channel");
                        }
                    } catch (IOException e) {
                        throw new JournalNetworkException(e);
                    }
                }
            } finally {
                from.limit(limit);
            }
        }
        return result;
    }

    public static void copy(ReadableByteChannel from, ByteBuffer to) throws JournalNetworkException {
        copy(from, to, to.remaining());
    }

    public static int copy(ReadableByteChannel from, ByteBuffer to, int count) throws JournalNetworkException {
        int result = 0;
        if (to != null) {
            int limit = to.limit();
            try {
                if (to.remaining() > count) {
                    to.limit(to.position() + count);
                }
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
        }
        return result;
    }

    public static void copy(ByteBuffer from, ByteBuffer to) {
        copy(from, to, to == null ? 0 : to.remaining());
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

    public static int copy(ByteBuffer from, ByteBuffer to, long count) {
        int result = 0;
        if (to != null && to.remaining() > 0) {
            int limit = from.limit();
            try {
                int c = count < to.remaining() ? (int) count : to.remaining();
                if (from.remaining() > c) {
                    from.limit(from.position() + c);
                }
                result = from.remaining();
                to.put(from);
            } finally {
                from.limit(limit);
            }
        }
        return result;
    }

    public static int copy(ByteBuffer from, int fromPos, ByteBuffer to, int toPos, int count) {
        if (to != null && to.limit() > toPos) {
            int toRem = to.limit() - toPos;
            int fromRem = from.limit() - fromPos;

            int c = fromRem < count ? fromRem : count;
            c = toRem < c ? toRem : c;
            for (int i = 0; i < c; i++) {
                to.put(toPos + i, from.get(fromPos + i));
            }
            return c;
        }
        return 0;
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
