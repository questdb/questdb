/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.geohash;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class GeoHashNativeTest {
    static final double lat = 31.23;
    static final double lon = 121.473;
    static final StringSink sink = new StringSink();


    private static double rnd_double(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    private static long rnd_geohash(int size) throws NumericException {
        double x = rnd_double(-90, 90);
        double y = rnd_double(-180, 180);
        return GeoHashes.fromCoordinates(x, y, size * 5);
    }

    @Test
    public void testFromStringNl() throws NumericException {
        final long gh = GeoHashes.fromCoordinates(lat, lon, 8 * 5);
        sink.clear();
        GeoHashes.toString(gh, 8, sink);
        final long gh1 = GeoHashes.fromStringNl(sink);
        Assert.assertEquals(gh, gh1);
    }

    @Test(expected = NumericException.class)
    public void testFromString() throws NumericException {
        sink.clear();
        sink.put("@s");
        GeoHashes.fromString(sink.toString(), 2);
    }

    @Test
    public void testFromCoordinates() throws NumericException {
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 5), GeoHashes.fromBitString("11100"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 2 * 5), GeoHashes.fromBitString("1110011001"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 3 * 5), GeoHashes.fromBitString("111001100111100"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 4 * 5), GeoHashes.fromBitString("11100110011110000011"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 5 * 5), GeoHashes.fromBitString("1110011001111000001111000"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 6 * 5), GeoHashes.fromBitString("111001100111100000111100010001"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 7 * 5), GeoHashes.fromBitString("11100110011110000011110001000110001"));
        Assert.assertEquals(GeoHashes.fromCoordinates(lat, lon, 8 * 5), GeoHashes.fromBitString("1110011001111000001111000100011000111111"));
    }

    @Test
    public void testToHash() throws NumericException {
        final long gh = GeoHashes.fromCoordinates(lat, lon, 8 * 5);
        final long ghz = GeoHashes.toHashWithSize(gh, 8);
        Assert.assertEquals(gh, GeoHashes.toHash(ghz));
        Assert.assertEquals(0, GeoHashes.hashSize(gh));
        Assert.assertEquals(8, GeoHashes.hashSize(ghz));
    }

    @Test
    public void testBitmask() {
        for (int i = 0; i < 64; i++) {
            final long bm = GeoHashes.bitmask(1, i);
            Assert.assertEquals(1L << i, bm);
        }
        Assert.assertEquals(7L << 5, GeoHashes.bitmask(3, 5));
    }

    @Test
    public void testFromStringToBits() throws NumericException {
        final int cap = 12;
        DirectLongList bits = new DirectLongList(cap * 2); // hash and mask
        CharSequenceHashSet strh = new CharSequenceHashSet();
        for (int i = 0; i < cap; i++) {
            final int prec = (i % 3) + 3;
            final long h = rnd_geohash(prec);
            sink.clear();
            GeoHashes.toString(h, prec, sink);
            strh.add(sink);
        }
        GeoHashes.fromStringToBits(strh, ColumnType.geohashWithPrecision(cap*5), bits);
        for (int i = 0; i < bits.size() / 2; i += 2) {
            final long b = bits.get(i);
            final long m = bits.get(i + 1);
            Assert.assertEquals(b, b & m);
        }
    }

    @Test
    public void testFromStringToBitsInvalidNull() {
        final int cap = 12;
        DirectLongList bits = new DirectLongList(cap * 2); // hash and mask
        CharSequenceHashSet strh = new CharSequenceHashSet();
        strh.add("");
        strh.add(null);
        strh.add("$invalid");
        strh.add("questdb1234567890");

        GeoHashes.fromStringToBits(strh, ColumnType.geohashWithPrecision(cap*5), bits);
        Assert.assertEquals(0, bits.size());
    }

    @Test
    public void testFromStringToBitsSingle() {
        final int cap = 12;
        DirectLongList bits = new DirectLongList(cap * 2); // hash and mask
        CharSequenceHashSet strh = new CharSequenceHashSet();
        strh.add("questdb");

        GeoHashes.fromStringToBits(strh, ColumnType.geohashWithPrecision(cap*5), bits);
        Assert.assertEquals(2, bits.size());
    }

    @Test
    public void testFromStringToBitsInvalidStrings() {
        final int cap = 12;
        DirectLongList bits = new DirectLongList(cap * 2); // hash and mask
        CharSequenceHashSet strh = new CharSequenceHashSet();
        strh.add("");
        strh.add("a medium sized banana");
        GeoHashes.fromStringToBits(strh, ColumnType.geohashWithPrecision(cap*5), bits);
        Assert.assertEquals(0, bits.size());
    }

    @Test
    public void testSlideFoundBlocks() {
        int keyCount = 20;

        DirectLongList rows = new DirectLongList(keyCount);
        rows.extend(keyCount);

        GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

        final int workerCount = 5;

        final long chunkSize = (keyCount + workerCount - 1) / workerCount;
        final int taskCount = (int) ((keyCount + chunkSize - 1) / chunkSize);

        final long argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
        try {
            for (long i = 0; i < taskCount; ++i) {
                final long klo = i * chunkSize;
                final long khi = Long.min(klo + chunkSize, keyCount);
                final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, klo);
                LatestByArguments.setKeyHi(argsAddress, khi);
                LatestByArguments.setRowsSize(argsAddress, 0);

                // 0, 2, 4, 0, 2 ...
                // zero, half, full
                long sz = (i % 3) * 2;
                LatestByArguments.setFilteredSize(argsAddress, sz);
            }
            final long rowCount = GeoHashNative.slideFoundBlocks(argumentsAddress, taskCount);
            Assert.assertEquals(8, rowCount);

            Assert.assertEquals(4, rows.get(0));
            Assert.assertEquals(5, rows.get(1));
            Assert.assertEquals(8, rows.get(2));
            Assert.assertEquals(9, rows.get(3));
            Assert.assertEquals(10, rows.get(4));
            Assert.assertEquals(11, rows.get(5));
            Assert.assertEquals(16, rows.get(6));
            Assert.assertEquals(17, rows.get(7));

        } finally {
            rows.close();
            LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
        }

    }

    @Test
    public void testIota() {
        final long N = 511;
        final long K = 42;
        try (DirectLongList list = new DirectLongList(N)) {
            list.setPos(list.getCapacity());
            for (int i = 1; i < N; i++) {
                GeoHashNative.iota(list.getAddress(), i, K);
                for (int j = 0; j < i; j++) {
                    Assert.assertEquals(j + K, list.get(j));
                }
            }
        }
    }

    @Test
    public void testLatLon() throws NumericException {
        String expected = "24 -> s\n" +
                "789 -> sp\n" +
                "25248 -> sp0\n" +
                "807941 -> sp05\n" +
                "25854114 -> sp052\n" +
                "827331676 -> sp052w\n" +
                "26474613641 -> sp052w9\n" +
                "847187636514 -> sp052w92\n" +
                "27110004368469 -> sp052w92p\n" +
                "867520139791009 -> sp052w92p1\n" +
                "27760644473312309 -> sp052w92p1p\n" +
                "888340623145993896 -> sp052w92p1p8\n";

        final int maxGeoHashSizeChars = 12;
        String[] expectedStr = new String[maxGeoHashSizeChars];
        long[] expectedHash = new long[maxGeoHashSizeChars];
        StringSink everything = new StringSink();

        for (int precision = 1; precision <= maxGeoHashSizeChars; precision++) {
            int numBits = precision * 5;
            long hash = GeoHashes.fromCoordinates(39.982, 0.024, numBits);
            sink.clear();
            GeoHashes.toString(hash, precision, sink);
            expectedStr[precision - 1] = sink.toString();
            expectedHash[precision - 1] = hash;
            everything.put(expectedHash[precision - 1]).put(" -> ").put(expectedStr[precision - 1]).put('\n');
        }

        for (int i = 0; i < maxGeoHashSizeChars; i++) {
            final long gh = GeoHashes.fromStringNl(expectedStr[i]);
            Assert.assertEquals(expectedHash[i], gh);
            sink.clear();
            GeoHashes.toString(gh, expectedStr[i].length(), sink);
            Assert.assertEquals(expectedStr[i], sink.toString());
        }
        Assert.assertEquals(expected, everything.toString());
    }

    @Test
    public void testToString() {
        assertSuccess(-1, Integer.MIN_VALUE, "", toString);
        assertSuccess(-1L, Integer.MAX_VALUE, "", toString);
        assertSuccess(27760644473312309L, 11, "sp052w92p1p", toString);
    }

    @Test
    public void testToStringInvalidSizeInChars() {
        boolean assertsEnabled = false;
        assert assertsEnabled = true; // Test only when assertions enabled
        if (assertsEnabled) {
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.toString(-0, 0, sink));
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.toString(-0, 31, sink));
        }
    }

    @Test
    public void testToBitStringNull() {
        assertSuccess(-1, Integer.MIN_VALUE, "", toBitString);
        assertSuccess(-1L, Integer.MAX_VALUE, "", toBitString);
    }

    @Test
    public void testToBitString() throws NumericException {
        long hash = 27760644473312309L;
        String expected = "1100010101000000010100010111000100100010101010000110101";
        Assert.assertEquals(expected, Long.toBinaryString(hash));
        assertSuccess(hash, expected.length(), expected, toBitString);
        Assert.assertEquals(hash, GeoHashes.fromBitString(expected));
    }

    @Test
    public void tstsRndD() {
        Rnd r = new Rnd();
        for(int i = 0; i < 1000; i++) {
            double d = r.nextDouble();
            Assert.assertTrue(Double.toString(d), Math.abs(d) < 1.0);
        }
    }

    @Test
    public void testFromBitStringInvalid() {
        CharSequence tooLongBitString = Chars.repeat("1", 61);
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromBitString(tooLongBitString));
    }

    @Test
    public void testFromBitStringInvalid2() {
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromBitString("a"));
    }

    @Test
    public void testToBitStringInvalidSizeInBits() {
        boolean assertsEnable = false;
        assert assertsEnable = true; // Test only when assertions enabled
        if (assertsEnable) {
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.toBitString(-0, 0, sink));
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.toBitString(-31, 0, sink));
        }
    }

    @Test
    public void testFromStringNull() throws NumericException {
        Assert.assertEquals(GeoHashes.fromStringNl(null), GeoHashes.NULL);
        Assert.assertEquals(GeoHashes.fromStringNl(""), GeoHashes.NULL);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testFromCoordinatesEdge() {
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromCoordinates(-91, 0, 10));
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromCoordinates(0, 190, 10));
    }

    @FunctionalInterface
    private interface StringConverter {
        void convert(long hash, int size, CharSink sink) throws NumericException;
    }

    private static final StringConverter toString = GeoHashes::toString;
    private static final StringConverter toBitString = GeoHashes::toBitString;

    private static void assertSuccess(long hash, int size, String message, StringConverter converter) {
        try {
            sink.clear();
            converter.convert(hash, size, sink);
            Assert.assertEquals(message, sink.toString());
        } catch (IllegalArgumentException | NumericException err) {
            Assert.fail();
        }
    }
}
