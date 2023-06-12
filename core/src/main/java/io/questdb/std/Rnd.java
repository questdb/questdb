/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

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

    /**
     * Creates a set to be used for generating fixed numbers from 0 to valueCount with variable, non-uniform
     * probability. The lower numbers have the highest probability.
     *
     * @param valueCount         number of values to generate probabilities for
     * @param diminishingScale   the scale at which probability should be diminishing
     * @param initialProbability the initial probability, the one assigned to the first value
     * @return an of ints. To generate number with calculated probabilities use value=set[rnd.nextInt(valueCount)]
     */
    public static int[] computeDiminishingFrequencyDistribution(int valueCount, double diminishingScale, double initialProbability) {

        assert diminishingScale < 1 && diminishingScale > 0;

        double lastProbability = initialProbability / diminishingScale;
        double[] probabilities = new double[valueCount];
        for (int i = 0; i < valueCount; i++) {
            final double prob = lastProbability * diminishingScale;
            probabilities[i] = prob;
            lastProbability = prob;
        }


        // find out scale of last probability
        double minProb = probabilities[valueCount - 1];
        int distScale = 1;
        while (((int) minProb / 100) == 0) {
            minProb *= 10;
            distScale *= 10;
        }

        // compute distribution set size
        int ccyDistSize = 0;
        for (int i = 0; i < valueCount; i++) {
            ccyDistSize += (int) (distScale * probabilities[i] / 100);
        }

        int[] ccyDist = new int[ccyDistSize];

        int x = 0;
        for (int i = 0; i < valueCount; i++) {
            final int len = (int) (distScale * probabilities[i] / 100);
            assert len > 0;
            int n = Math.min(x + len, ccyDistSize);
            for (; x < n; x++) {
                ccyDist[x] = i;
            }
        }
        return ccyDist;
    }

    public String[] createValues(int count, int len) {
        String[] ccy = new String[count];
        for (int i = 0; i < count; i++) {
            ccy[i] = nextString(len);
        }
        return ccy;
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

    //returns random bytes between 'B' and 'Z' for legacy reasons
    public void nextBytes(byte[] bytes) {
        int len = bytes.length;
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) (nextPositiveInt() % 25 + 'B');
        }
    }

    //returns random bytes between 'B' and 'Z' for legacy reasons
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

    //returns random bytes between 'B' and 'Z' for legacy reasons
    public void nextChars(CharSink sink, int len) {
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

    //returns random bytes between 'B' and 'Z' for legacy reasons
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
        reset(0xdeadbeefL, 0xdee4c0edL);
    }

    public void syncWith(Rnd other) {
        this.s0 = other.s0;
        this.s1 = other.s1;
    }

    private int nextIntForDouble(int bits) {
        return (int) ((nextLong() & mask) >>> (48 - bits));
    }
}
