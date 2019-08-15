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

import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;

public class Rnd {
    private static final long mask = (1L << 48) - 1;
    private static final double DOUBLE_UNIT = 0x1.0p-53; // 1.0 / (1L << 53)
    private static final float FLOAT_UNIT = 1 / ((float) (1 << 24));
    private final StringSink sink = new StringSink();
    private long s0;
    private long s1;

    public Rnd(long s0, long s1) {
        reset(s0, s1);
    }

    public Rnd() {
        reset();
    }

    public boolean nextBoolean() {
        return nextLong(1) != 0;
    }

    public byte nextByte() {
        return (byte) nextLong();
    }

    public byte[] nextBytes(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (nextPositiveInt() % 25 + 66);
        }
        return bytes;
    }

    public char nextChar() {
        return (char) (nextPositiveInt() % 25 + 66);
    }

    public void nextChars(final long address, int len) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + i * 2, nextChar());
        }
    }

    public CharSequence nextChars(int len) {
        sink.clear();
        nextChars(sink, len);
        return sink;
    }

    public void nextChars(CharSink sink, int len) {
        for (int i = 0; i < len; i++) {
            sink.put((char) (nextPositiveInt() % 25 + 66));
        }
    }

    @Deprecated
    public double nextDouble() {
        return (nextLong(26) << 27 + nextLong(27)) * DOUBLE_UNIT;
    }

    public double nextDouble2() {
        return (((long) (nextIntForDouble(26)) << 27) + nextIntForDouble(27)) * DOUBLE_UNIT;
    }

    @Deprecated
    public float nextFloat() {
        return nextLong(24) * FLOAT_UNIT;
    }

    public float nextFloat2() {
        return nextIntForDouble(24) * FLOAT_UNIT;
    }

    public int nextInt() {
        return (int) nextLong();
    }

    public long nextLong() {
        long l1 = s0;
        long l0 = s1;
        s0 = l0;
        l1 ^= l1 << 23;
        return (s1 = l1 ^ l0 ^ (l1 >> 17) ^ (l0 >> 26)) + l0;
    }

    public int nextPositiveInt() {
        int n = (int) nextLong();
        return n > 0 ? n : -n;
    }

    public long nextPositiveLong() {
        long l = nextLong();
        return l > 0 ? l : -l;
    }

    public short nextShort() {
        return (short) nextLong();
    }

    public String nextString(int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (nextPositiveInt() % 25 + 66);
        }
        return new String(chars);
    }

    public final void reset(long s0, long s1) {
        this.s0 = s0;
        this.s1 = s1;
    }

    public final void reset() {
        reset(0xdeadbeef, 0xdee4c0ed);
    }

    public void syncWith(Rnd other) {
        this.s0 = other.s0;
        this.s1 = other.s1;
    }

    private int nextIntForDouble(int bits) {
        return (int) ((nextLong() & mask) >>> (48 - bits));
    }

    private long nextLong(int bits) {
        return nextLong() >>> (64 - bits);
    }
}
