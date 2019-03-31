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

package com.questdb.std;

import com.questdb.network.Net;
import com.questdb.std.ex.*;
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

    public static void copyGreedyNonBlocking(ReadableByteChannel channel, ByteBuffer to, final int retryCount) throws IOException {
        try {
            int r = to.remaining();
            int target = r;
            int retriesRemaining = retryCount;
            while (target > 0) {
                int result = channel.read(to);

                // disconnected
                if (result == -1) {
                    throw DisconnectedChannelException.INSTANCE;
                }

                if (result == 0 && --retriesRemaining < 0) {
                    if (target == r) {
                        throw SlowReadableChannelException.INSTANCE;
                    }
                    break;
                }

                target -= result;
            }
        } finally {
            to.flip();
        }
    }

    public static void copyNonBlocking(ByteBuffer from, WritableByteChannel channel, final int retryCount)
            throws DisconnectedChannelException, SlowWritableChannelException {
        int target = from.remaining();
        int retriesRemaining = retryCount;
        while (target > 0) {
            int result;
            try {
                result = channel.write(from);
            } catch (SlowWritableChannelException e) {
                throw e;
            } catch (IOException e) {
                throw DisconnectedChannelException.INSTANCE;
            }

            if (result > 0) {
                target -= result;
                continue;
            }

            if (result == Net.ERETRY) {
                if (--retriesRemaining < 0) {
                    throw SlowWritableChannelException.INSTANCE;
                }
            } else {
                throw DisconnectedChannelException.INSTANCE;
            }
        }
    }

    public static void copyNonBlocking(NetworkChannel channel, ByteBuffer to, final int retryCount)
            throws DisconnectedChannelException, SlowReadableChannelException, EndOfChannelException {
        int r = to.remaining();
        int target = r;
        int retriesRemaining = retryCount;

        OUT:
        while (target > 0) {
            int result;
            try {
                result = channel.read(to);
            } catch (IOException e) {
                throw DisconnectedChannelException.INSTANCE;
            }

            if (result > 0) {
                target -= result;
                continue;
            }

            switch (result) {
                case Net.ERETRY:
                    if (target < r) {
                        break OUT;
                    }
                    if (--retriesRemaining < 0) {
                        throw SlowReadableChannelException.INSTANCE;
                    }
                    break;
                case Net.EPEERDISCONNECT:
                    throw EndOfChannelException.INSTANCE;
                default:
                    throw DisconnectedChannelException.INSTANCE;
            }
        }
    }

    @SuppressWarnings("unused")
    public static void dump(ByteBuffer b) {
        int p = b.position();
        while (b.hasRemaining()) {
            System.out.print((char) b.get());
        }
        b.position(p);
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
            int bits = Math.min(30, Numbers.msb(recSize * recCount / m));
//            int bits = Math.min(30, 32 - Integer.numberOfLeadingZeros(recSize * recCount / m));
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

    public static boolean isDirect(ByteBuffer buf) {
        return buf instanceof DirectBuffer;
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
     * <p>
     * ByteBuffer buffer = ....
     * <p>
     * buffer = release(buffer);
     *
     * @param <T>    ByteBuffer subclass
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
        int result;
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
