/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std;

import io.questdb.cairo.GeoHashes;
import io.questdb.std.str.*;

import java.util.Collections;
import java.util.List;

public class Rnd {
    private static final double DOUBLE_UNIT = 0x1.0p-53; // 1.0 / (1L << 53)
    private static final float FLOAT_UNIT = 1 / ((float) (1 << 24));
    private static final long mask = (1L << 48) - 1;
    private final StringSink sink = new StringSink();
    private long s0;
    private long s1;

    public Rnd(long s0, long s1) {
        reset(s0, s1);
    }

    public Rnd() {
        reset();
    }

    public static void main(String[] args) {
        Rnd rnd = new Rnd();
        Utf8StringSink utf8sink = new Utf8StringSink();
        rnd.nextUtf8Str(512, utf8sink);

        StringSink utf16sink = new StringSink();
        if (!Utf8s.utf8ToUtf16(utf8sink, utf16sink)) {
            throw new RuntimeException();
        }
        System.out.println(utf16sink);
    }

    public long getSeed0() {
        return s0;
    }

    public long getSeed1() {
        return s1;
    }

    public boolean nextBoolean() {
        return nextLong() >>> (64 - 1) != 0;
    }

    public byte nextByte() {
        return (byte) nextLong();
    }

    //returns random bytes between 'B' and 'Z' for legacy reasons
    public byte[] nextBytes(int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (nextPositiveInt() % 25 + 'B');
        }
        return bytes;
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public void nextBytes(byte[] bytes) {
        int len = bytes.length;
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (nextPositiveInt() % 25 + 'B');
        }
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public char nextChar() {
        return (char) (nextPositiveInt() % 25 + 'B');
    }

    public void nextChars(final long address, int len) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putChar(address + i * 2L, nextChar());
        }
    }

    public CharSequence nextChars(int len) {
        sink.clear();
        nextChars(sink, len);
        return sink;
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public void nextChars(Utf16Sink sink, int len) {
        for (int i = 0; i < len; i++) {
            sink.put((char) (nextPositiveInt() % 25 + 66));
        }
    }

    public double nextDouble() {
        return (((long) (nextIntForDouble(26)) << 27) + nextIntForDouble(27)) * DOUBLE_UNIT;
    }

    public float nextFloat() {
        return nextIntForDouble(24) * FLOAT_UNIT;
    }

    public long nextGeoHash(int bits) {
        double x = nextDouble() * 180.0 - 90.0;
        double y = nextDouble() * 360.0 - 180.0;
        try {
            return GeoHashes.fromCoordinatesDeg(x, y, bits);
        } catch (NumericException e) {
            // Should never happen
            return GeoHashes.NULL;
        }
    }

    public byte nextGeoHashByte(int bits) {
        return (byte) nextGeoHash(bits);
    }

    public int nextGeoHashInt(int bits) {
        return (int) nextGeoHash(bits);
    }

    public long nextGeoHashLong(int bits) {
        return nextGeoHash(bits);
    }

    public short nextGeoHashShort(int bits) {
        return (short) nextGeoHash(bits);
    }

    public int nextInt() {
        return (int) nextLong();
    }

    public int nextInt(int boundary) {
        return nextPositiveInt() % boundary;
    }

    public long nextLong(long boundary) {
        return nextPositiveLong() % boundary;
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
        return n > 0 ? n : (n == Integer.MIN_VALUE ? Integer.MAX_VALUE : -n);
    }

    public long nextPositiveLong() {
        long l = nextLong();
        return l > 0 ? l : (l == Long.MIN_VALUE ? Long.MAX_VALUE : -l);
    }

    public short nextShort() {
        return (short) nextLong();
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public String nextString(int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (nextPositiveInt() % 25 + 66);
        }
        return new String(chars);
    }

    public void nextUtf8AsciiStr(int len, Utf8Sink sink) {
        for (int i = 0; i < len; i++) {
            sink.putAscii((char) (32 + nextPositiveInt() % (127 - 32)));
        }
    }

    // https://stackoverflow.com/questions/1319022/really-good-bad-utf-8-example-test-data
    public void nextUtf8Str(int len, Utf8Sink sink) {
        for (int i = 0; i < len; i++) {
            // 5 is the exclusive upper limit for up to how many UTF8 bytes per character we generate
            int byteCount = Math.max(1, nextInt(5));
            switch (byteCount) {
                case 1:
                    sink.putAscii((char) (32 + nextPositiveInt() % (127 - 32)));
                    break;
                case 2:
                    while (true) {
                        // first byte of two-byte character, it has to start with 110xxxxx
                        final byte b1 = nextUtf8Byte(0xe0, 0xc0);
                        final byte b2 = nextUtf8ContinuationByte();

                        // rule out 0xc1 since 0xC0 and 0xC1 can't appear in valid UTF8 as the only characters
                        // that could be encoded by those are minimally encoded as single byte characters
                        if ((b1 & 30) == 0) {
                            continue;
                        }
                        sink.put(b1).put(b2);
                        break;
                    }
                    break;
                case 3:
                    while (true) {
                        // first byte of 3-byte character, it has to start with 1110xxxx
                        final byte b1 = nextUtf8Byte(0xf0, 0xe0);
                        final byte b2 = nextUtf8ContinuationByte();
                        final byte b3 = nextUtf8ContinuationByte();
                        final char c = Utf8s.utf8ToChar(b1, b2, b3);

                        // we might end up with surrogate, which we have to re-generate
                        if (Character.isSurrogate(c)) {
                            continue;
                        }
                        sink.put(b1).put(b2).put(b3);
                        break;
                    }
                    break;
                case 4:
                    // first byte of 4-byte character, it has to start with 11110xxx
                    while (true) {
                        final byte b1 = nextUtf8Byte(0xf8, 0xf0);
                        // remaining bytes start with continuation 10xxxxxx
                        final byte b2 = nextUtf8ContinuationByte();
                        final byte b3 = nextUtf8ContinuationByte();
                        final byte b4 = nextUtf8ContinuationByte();
                        if (Character.isSupplementaryCodePoint(Utf8s.getUtf8Codepoint(b1, b2, b3, b4))) {
                            sink.put(b1).put(b2).put(b3).put(b4);
                            break;
                        }
                    }
                    break;
                default:
                    assert false;
                    break;
            }
        }
    }

    public final void reset(long s0, long s1) {
        this.s0 = s0;
        this.s1 = s1;
    }

    public final void reset() {
        reset(0xdeadbeef, 0xdee4c0ed);
    }

    public void shuffle(List<?> list) {
        for (int i = 1, n = list.size(); i < n; i++) {
            int swapTarget = nextInt(i + 1);
            Collections.swap(list, i, swapTarget);
        }
    }

    public void syncWith(Rnd other) {
        this.s0 = other.s0;
        this.s1 = other.s1;
    }

    private int nextIntForDouble(int bits) {
        return (int) ((nextLong() & mask) >>> (48 - bits));
    }

    private byte nextUtf8Byte(int wipe, int set) {
        while (true) {
            int k = nextInt();
            k &= ~wipe;
            k &= 0xff;
            if (k != 0) {
                k |= set;
                return (byte) k;
            }
        }
    }

    private byte nextUtf8ContinuationByte() {
        return nextUtf8Byte(0xc0, 0x80);
    }
}
