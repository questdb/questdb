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

import io.questdb.griffin.engine.table.LatestByArguments;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.DirectLongList;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class GeoHashNativeTest {
    static final double lat = 31.23;
    static final double lon = 121.473;

    private static double rnd_double(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    private static long rnd_geohash(int size) {
        double x = rnd_double(-90, 90);
        double y = rnd_double(-180, 180);
        return GeoHashNative.fromCoordinates(x, y, size * 5);
    }

    @Test
    public void testFromString() {
        final long gh = GeoHashNative.fromCoordinates(lat, lon, 8 * 5);
        final CharSequence ghStr = GeoHashNative.toString(gh, 8);
        final long gh1 = GeoHashNative.fromString(ghStr);
        Assert.assertEquals(gh, gh1);
    }

    @Test
    public void testFromCoordinates() {
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 1 * 5), GeoHashNative.fromBitString("11100"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 2 * 5), GeoHashNative.fromBitString("1110011001"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 3 * 5), GeoHashNative.fromBitString("111001100111100"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 4 * 5), GeoHashNative.fromBitString("11100110011110000011"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 5 * 5), GeoHashNative.fromBitString("1110011001111000001111000"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 6 * 5), GeoHashNative.fromBitString("111001100111100000111100010001"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 7 * 5), GeoHashNative.fromBitString("11100110011110000011110001000110001"));
        Assert.assertEquals(GeoHashNative.fromCoordinates(lat, lon, 8 * 5), GeoHashNative.fromBitString("1110011001111000001111000100011000111111"));
    }

    @Test
    public void testBitmask() {
        for (int i = 0; i < 64; i++) {
            final long bm = GeoHashNative.bitmask(1, i);
            Assert.assertEquals(1L << i, bm);
        }
        Assert.assertEquals(7L << 5, GeoHashNative.bitmask(3, 5));
    }

    @Test
    public void testToHash() {
        final long ghz = GeoHashNative.fromCoordinates(lat, lon, 8 * 5);
        final long gh = GeoHashNative.toHash(ghz);
        final long ghz1 = GeoHashNative.toHashWithSize(gh, 8);
        Assert.assertEquals(gh, GeoHashNative.toHash(ghz1));
        Assert.assertEquals(8, GeoHashNative.hashSize(ghz));
        Assert.assertEquals(0, GeoHashNative.hashSize(gh));
        Assert.assertEquals(8, GeoHashNative.hashSize(ghz1));
    }

    @Test
    public void testFromStringToBits() {
        final int cap = 12;
        DirectLongList bits = new DirectLongList(cap * 2); // hash and mask
        CharSequenceHashSet strh = new CharSequenceHashSet();
        for (int i = 0; i < cap; i++) {
            final int prec = (i % 3) + 3;
            final long h = rnd_geohash(prec);
            strh.add(GeoHashNative.toString(h, prec));
        }
        GeoHashNative.fromStringToBits(strh, bits);
        for (int i = 0; i < bits.size() / 2; i += 2) {
            final long b = bits.get(i);
            final long m = bits.get(i + 1);
            Assert.assertEquals(b, b & m);
        }
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
    public void testLatLon() {
        String expected = "1:5: 1152921504606847000 -> s\n" +
                "2:10: 2305843009213694741 -> sp\n" +
                "3:15: 3458764513820566176 -> sp0\n" +
                "4:20: 4611686018428195845 -> sp05\n" +
                "5:25: 5764607523060088994 -> sp052\n" +
                "6:30: 6917529028468413532 -> sp052w\n" +
                "7:35: 8070450558722542473 -> sp052w9\n" +
                "8:40: -9223371189667139294 -> sp052w92\n" +
                "9:45: -8070423422243560363 -> sp052w92p\n" +
                "10:50: -6916661507501290847 -> sp052w92p1\n" +
                "11:55: -5736846878560922571 -> sp052w92p1p\n" +
                "12:60: -3723345395281394008 -> sp052w92p1p8\n";
        StringSink sink = new StringSink();
        for (int precision = 1; precision <= 12; precision++) {
            int numBits = precision * 5;
            long hashz = GeoHashNative.fromCoordinates(39.982, 0.024, numBits);
            String location = GeoHashNative.toString(hashz, precision);
            Assert.assertEquals(location, GeoHashNative.toString(hashz));
            sink.put(String.format("%d:%d: %d -> %s%n", precision, numBits, hashz, location));
        }
        Assert.assertEquals(expected, sink.toString());
    }
}
