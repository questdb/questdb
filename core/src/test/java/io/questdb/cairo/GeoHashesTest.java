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

package io.questdb.cairo;

import io.questdb.std.Misc;
import io.questdb.std.NumericException;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashesTest {

    @Test
    public void testBitsPrecision() {
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(ColumnType.GEOHASH));
        Assert.assertEquals(0, GeoHashes.getBitsPrecision(ColumnType.GEOHASH));

        int geohashCol = ColumnType.geohashWithPrecision(42);
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(geohashCol));
        Assert.assertEquals(42, GeoHashes.getBitsPrecision(geohashCol));
        geohashCol = ColumnType.geohashWithPrecision(24);
        Assert.assertEquals(24, GeoHashes.getBitsPrecision(geohashCol));
    }

    @Test
    public void testBitsPrecisionNeg() {
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(ColumnType.GEOHASH));
        Assert.assertEquals(0, GeoHashes.getBitsPrecision(ColumnType.GEOHASH));

        int geohashCol = GeoHashes.setBitsPrecision(ColumnType.GEOHASH, -5);
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(geohashCol));
        Assert.assertEquals(-5, GeoHashes.getBitsPrecision(geohashCol));
        geohashCol = GeoHashes.setBitsPrecision(geohashCol, 24);
        Assert.assertEquals(24, GeoHashes.getBitsPrecision(geohashCol));
        geohashCol = GeoHashes.setBitsPrecision(geohashCol, -12);
        Assert.assertEquals(-12, GeoHashes.getBitsPrecision(geohashCol));
    }

    @Test
    public void testStorageSize() {
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(ColumnType.GEOHASH));
        Assert.assertEquals(0, GeoHashes.getBitsPrecision(ColumnType.GEOHASH));

        int geohashCol = ColumnType.geohashWithPrecision(42);
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(geohashCol));
        Assert.assertEquals(64 / 8, GeoHashes.sizeOf(geohashCol));
        Assert.assertEquals(3, GeoHashes.pow2SizeOf(geohashCol));

        geohashCol = ColumnType.geohashWithPrecision(24);
        Assert.assertEquals(32 / 8, GeoHashes.sizeOf(geohashCol));
        Assert.assertEquals(2, GeoHashes.pow2SizeOf(geohashCol));

        geohashCol = ColumnType.geohashWithPrecision(13);
        Assert.assertEquals(16 / 8, GeoHashes.sizeOf(geohashCol));
        Assert.assertEquals(1, GeoHashes.pow2SizeOf(geohashCol));

        geohashCol = ColumnType.geohashWithPrecision(7);
        Assert.assertEquals(1, GeoHashes.sizeOf(geohashCol));
        Assert.assertEquals(0, GeoHashes.pow2SizeOf(geohashCol));
    }

    @Test
    public void testStorageSizeWithNull() {
        for (int i = 1; i < 61; i++) {
            int geohashCol = ColumnType.geohashWithPrecision(i);
            if (i < 8) {
                Assert.assertEquals(1, GeoHashes.sizeOf(geohashCol));
                Assert.assertEquals(0, GeoHashes.pow2SizeOf(geohashCol));
            } else if (i < 16) {
                Assert.assertEquals(2, GeoHashes.sizeOf(geohashCol));
                Assert.assertEquals(1, GeoHashes.pow2SizeOf(geohashCol));
            } else if (i < 32) {
                Assert.assertEquals(4, GeoHashes.sizeOf(geohashCol));
                Assert.assertEquals(2, GeoHashes.pow2SizeOf(geohashCol));
            } else {
                Assert.assertEquals(8, GeoHashes.sizeOf(geohashCol));
                Assert.assertEquals(3, GeoHashes.pow2SizeOf(geohashCol));
            }
        }
    }

    @Test
    public void testGeoHashTypeName() {
        String expected = "GEOHASH(1b) -> 270 (1)\n" +
                "GEOHASH(2b) -> 526 (2)\n" +
                "GEOHASH(3b) -> 782 (3)\n" +
                "GEOHASH(4b) -> 1038 (4)\n" +
                "GEOHASH(1c) -> 1294 (5)\n" +
                "GEOHASH(6b) -> 1550 (6)\n" +
                "GEOHASH(7b) -> 1806 (7)\n" +
                "GEOHASH(8b) -> 2062 (8)\n" +
                "GEOHASH(9b) -> 2318 (9)\n" +
                "GEOHASH(2c) -> 2574 (10)\n" +
                "GEOHASH(11b) -> 2830 (11)\n" +
                "GEOHASH(12b) -> 3086 (12)\n" +
                "GEOHASH(13b) -> 3342 (13)\n" +
                "GEOHASH(14b) -> 3598 (14)\n" +
                "GEOHASH(3c) -> 3854 (15)\n" +
                "GEOHASH(16b) -> 4110 (16)\n" +
                "GEOHASH(17b) -> 4366 (17)\n" +
                "GEOHASH(18b) -> 4622 (18)\n" +
                "GEOHASH(19b) -> 4878 (19)\n" +
                "GEOHASH(4c) -> 5134 (20)\n" +
                "GEOHASH(21b) -> 5390 (21)\n" +
                "GEOHASH(22b) -> 5646 (22)\n" +
                "GEOHASH(23b) -> 5902 (23)\n" +
                "GEOHASH(24b) -> 6158 (24)\n" +
                "GEOHASH(5c) -> 6414 (25)\n" +
                "GEOHASH(26b) -> 6670 (26)\n" +
                "GEOHASH(27b) -> 6926 (27)\n" +
                "GEOHASH(28b) -> 7182 (28)\n" +
                "GEOHASH(29b) -> 7438 (29)\n" +
                "GEOHASH(6c) -> 7694 (30)\n" +
                "GEOHASH(31b) -> 7950 (31)\n" +
                "GEOHASH(32b) -> 8206 (32)\n" +
                "GEOHASH(33b) -> 8462 (33)\n" +
                "GEOHASH(34b) -> 8718 (34)\n" +
                "GEOHASH(7c) -> 8974 (35)\n" +
                "GEOHASH(36b) -> 9230 (36)\n" +
                "GEOHASH(37b) -> 9486 (37)\n" +
                "GEOHASH(38b) -> 9742 (38)\n" +
                "GEOHASH(39b) -> 9998 (39)\n" +
                "GEOHASH(8c) -> 10254 (40)\n" +
                "GEOHASH(41b) -> 10510 (41)\n" +
                "GEOHASH(42b) -> 10766 (42)\n" +
                "GEOHASH(43b) -> 11022 (43)\n" +
                "GEOHASH(44b) -> 11278 (44)\n" +
                "GEOHASH(9c) -> 11534 (45)\n" +
                "GEOHASH(46b) -> 11790 (46)\n" +
                "GEOHASH(47b) -> 12046 (47)\n" +
                "GEOHASH(48b) -> 12302 (48)\n" +
                "GEOHASH(49b) -> 12558 (49)\n" +
                "GEOHASH(10c) -> 12814 (50)\n" +
                "GEOHASH(51b) -> 13070 (51)\n" +
                "GEOHASH(52b) -> 13326 (52)\n" +
                "GEOHASH(53b) -> 13582 (53)\n" +
                "GEOHASH(54b) -> 13838 (54)\n" +
                "GEOHASH(11c) -> 14094 (55)\n" +
                "GEOHASH(56b) -> 14350 (56)\n" +
                "GEOHASH(57b) -> 14606 (57)\n" +
                "GEOHASH(58b) -> 14862 (58)\n" +
                "GEOHASH(59b) -> 15118 (59)\n" +
                "GEOHASH(12c) -> 15374 (60)\n";
        StringSink everything = new StringSink();
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            int type = ColumnType.geohashWithPrecision(b);
            String name = ColumnType.nameOf(type);
            everything.put(name)
                    .put(" -> ")
                    .put(type)
                    .put(" (")
                    .put(GeoHashes.getBitsPrecision(type))
                    .put(")\n");
        }
        Assert.assertEquals(expected, everything.toString());
        Assert.assertEquals("GEOHASH", ColumnType.nameOf(ColumnType.GEOHASH));
    }

    @Test(expected = NumericException.class)
    public void testFromStringNegativeStart() throws NumericException {
        GeoHashes.fromString("", -1, 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testFromStringNegativeParseLen() throws NumericException {
        GeoHashes.fromString("", 0, -1, 0);
    }

    @Test(expected = NumericException.class)
    public void testFromStringInvalidBitsPrecision1() throws NumericException {
        GeoHashes.fromString("123", 0, 3, -1);
    }

    @Test(expected = NumericException.class)
    public void testFromStringInvalidBitsPrecision2() throws NumericException {
        GeoHashes.fromString("123", 0, 3, GeoHashes.MAX_BITS_LENGTH + 1);
    }

    @Test(expected = NumericException.class)
    public void testFromStringNotEnoughChars() throws NumericException {
        GeoHashes.fromString("123", 0, 4, 15);
    }

    @Test(expected = NumericException.class)
    public void testFromStringNotEnoughBits() throws NumericException {
        GeoHashes.fromString("123", 0, 3, 16);
    }

    @Test(expected = NumericException.class)
    public void testFromStringOverMaxCharsLength() throws NumericException {
        GeoHashes.fromString("123456789abcde", 1, GeoHashes.MAX_STRING_LENGTH + 2, 0);
    }

    @Test(expected = NumericException.class)
    public void testFromStringShorterThanRequiredLength() throws NumericException {
        GeoHashes.fromString("123", 1, 7, 0);
    }

    @Test
    public void testFromNullYieldsNull() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromString(null, 1, 1, 0));
    }

    @Test
    public void testFromStringYieldsNullDueToZeroRequiredLen() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromString("''", 1, 0, 0));
    }

    @Test
    public void testFromStringJustOneChar() throws NumericException {
        Assert.assertEquals(24, GeoHashes.fromString("ast", 1, 1, 0));
    }

    @Test
    public void testFromStringIgnoreQuotes1() throws NumericException {
        Assert.assertEquals(27760644473312309L, GeoHashes.fromString("'sp052w92p1p'", 1, 11, 0));
    }

    @Test
    public void testFromStringIgnoreQuotes2() throws NumericException {
        Assert.assertEquals(27760644473312309L, GeoHashes.fromString("'sp052w92p1p'", 1, 11, 0));
    }

    @Test(expected = NumericException.class)
    public void testFromStringIBadChar() throws NumericException {
        Assert.assertEquals(27760644473312309L, GeoHashes.fromString("'sp05@w92p1p'", 1, 11, 0));
    }

    @Test
    public void testFromStringIgnoreQuotesTruncateChars() throws NumericException {
        Assert.assertEquals(807941, GeoHashes.fromString("'sp052w92p1p'", 1, 4, 0));
        StringSink sink = Misc.getThreadLocalBuilder();
        GeoHashes.toString(807941, 4, sink);
        Assert.assertEquals("sp05", sink.toString());
    }

    @Test
    public void testFromStringIgnoreQuotesTruncateBits() throws NumericException {
        Assert.assertEquals(807941, GeoHashes.fromString("'sp052w92p1p'", 1, 11, 20));
        StringSink sink = Misc.getThreadLocalBuilder();
        GeoHashes.toString(807941, 4, sink);
        Assert.assertEquals("sp05", sink.toString());
    }
}
