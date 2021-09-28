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

import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;


public class GeoHashesTest {

    private static final double lat = 31.23;
    private static final double lon = 121.473;
    private static final StringConverter toString = GeoHashes::appendChars;
    private static final StringConverter toBitString = GeoHashes::appendBinary;


    @Test
    public void testBitsPrecision() {
        int geoHashCol = ColumnType.getGeoHashTypeWithBits(42);
        Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(geoHashCol));
        Assert.assertEquals(42, ColumnType.getGeoHashBits(geoHashCol));
        geoHashCol = ColumnType.getGeoHashTypeWithBits(24);
        Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(geoHashCol));
        Assert.assertEquals(24, ColumnType.getGeoHashBits(geoHashCol));
    }

    @Test
    public void testGeoHashTypesValuesAreValid() {
        Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(ColumnType.GEOBYTE));
        Assert.assertEquals(0, ColumnType.getGeoHashBits(ColumnType.GEOBYTE));

        Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(ColumnType.GEOSHORT));
        Assert.assertEquals(0, ColumnType.getGeoHashBits(ColumnType.GEOSHORT));

        Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(ColumnType.GEOINT));
        Assert.assertEquals(0, ColumnType.getGeoHashBits(ColumnType.GEOINT));

        Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(ColumnType.GEOLONG));
        Assert.assertEquals(0, ColumnType.getGeoHashBits(ColumnType.GEOLONG));
    }

    @Test
    public void testStorageSize() {
        int geoHashType = ColumnType.getGeoHashTypeWithBits(42);
        Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(geoHashType));
        Assert.assertEquals(42, ColumnType.getGeoHashBits(geoHashType));

        geoHashType = ColumnType.getGeoHashTypeWithBits(16);
        Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(geoHashType));
        Assert.assertEquals(16, ColumnType.getGeoHashBits(geoHashType));

        geoHashType = ColumnType.getGeoHashTypeWithBits(8);
        Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(geoHashType));
        Assert.assertEquals(8, ColumnType.getGeoHashBits(geoHashType));

        geoHashType = ColumnType.getGeoHashTypeWithBits(7);
        Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(geoHashType));
        Assert.assertEquals(7, ColumnType.getGeoHashBits(geoHashType));
    }

    @Test
    public void testStorageSizeWithNull() {
        for (int i = 1; i < 61; i++) {
            final int type = ColumnType.getGeoHashTypeWithBits(i);
            if (i < 8) {
                Assert.assertEquals(ColumnType.GEOBYTE, ColumnType.tagOf(type));
            } else if (i < 16) {
                Assert.assertEquals(ColumnType.GEOSHORT, ColumnType.tagOf(type));
            } else if (i < 32) {
                Assert.assertEquals(ColumnType.GEOINT, ColumnType.tagOf(type));
            } else {
                Assert.assertEquals(ColumnType.GEOLONG, ColumnType.tagOf(type));
            }
        }
    }

    @Test
    public void testEncodeChar() {
        Assert.assertEquals(18, GeoHashes.encodeChar('k'));
        Assert.assertEquals(GeoHashes.BYTE_NULL, GeoHashes.encodeChar('o'));
        Assert.assertEquals(GeoHashes.BYTE_NULL, GeoHashes.encodeChar((char) 0x1FF));
        Assert.assertEquals(GeoHashes.BYTE_NULL, GeoHashes.encodeChar('ó'));
        Assert.assertEquals(GeoHashes.BYTE_NULL, GeoHashes.encodeChar('㤴'));

    }

    @Test
    public void testGeoHashTypeName() {
        String expected = "GEOHASH(1b) -> 65806 (1)\n" +
                "GEOHASH(2b) -> 66062 (2)\n" +
                "GEOHASH(3b) -> 66318 (3)\n" +
                "GEOHASH(4b) -> 66574 (4)\n" +
                "GEOHASH(1c) -> 66830 (5)\n" +
                "GEOHASH(6b) -> 67086 (6)\n" +
                "GEOHASH(7b) -> 67342 (7)\n" +
                "GEOHASH(8b) -> 67599 (8)\n" +
                "GEOHASH(9b) -> 67855 (9)\n" +
                "GEOHASH(2c) -> 68111 (10)\n" +
                "GEOHASH(11b) -> 68367 (11)\n" +
                "GEOHASH(12b) -> 68623 (12)\n" +
                "GEOHASH(13b) -> 68879 (13)\n" +
                "GEOHASH(14b) -> 69135 (14)\n" +
                "GEOHASH(3c) -> 69391 (15)\n" +
                "GEOHASH(16b) -> 69648 (16)\n" +
                "GEOHASH(17b) -> 69904 (17)\n" +
                "GEOHASH(18b) -> 70160 (18)\n" +
                "GEOHASH(19b) -> 70416 (19)\n" +
                "GEOHASH(4c) -> 70672 (20)\n" +
                "GEOHASH(21b) -> 70928 (21)\n" +
                "GEOHASH(22b) -> 71184 (22)\n" +
                "GEOHASH(23b) -> 71440 (23)\n" +
                "GEOHASH(24b) -> 71696 (24)\n" +
                "GEOHASH(5c) -> 71952 (25)\n" +
                "GEOHASH(26b) -> 72208 (26)\n" +
                "GEOHASH(27b) -> 72464 (27)\n" +
                "GEOHASH(28b) -> 72720 (28)\n" +
                "GEOHASH(29b) -> 72976 (29)\n" +
                "GEOHASH(6c) -> 73232 (30)\n" +
                "GEOHASH(31b) -> 73488 (31)\n" +
                "GEOHASH(32b) -> 73745 (32)\n" +
                "GEOHASH(33b) -> 74001 (33)\n" +
                "GEOHASH(34b) -> 74257 (34)\n" +
                "GEOHASH(7c) -> 74513 (35)\n" +
                "GEOHASH(36b) -> 74769 (36)\n" +
                "GEOHASH(37b) -> 75025 (37)\n" +
                "GEOHASH(38b) -> 75281 (38)\n" +
                "GEOHASH(39b) -> 75537 (39)\n" +
                "GEOHASH(8c) -> 75793 (40)\n" +
                "GEOHASH(41b) -> 76049 (41)\n" +
                "GEOHASH(42b) -> 76305 (42)\n" +
                "GEOHASH(43b) -> 76561 (43)\n" +
                "GEOHASH(44b) -> 76817 (44)\n" +
                "GEOHASH(9c) -> 77073 (45)\n" +
                "GEOHASH(46b) -> 77329 (46)\n" +
                "GEOHASH(47b) -> 77585 (47)\n" +
                "GEOHASH(48b) -> 77841 (48)\n" +
                "GEOHASH(49b) -> 78097 (49)\n" +
                "GEOHASH(10c) -> 78353 (50)\n" +
                "GEOHASH(51b) -> 78609 (51)\n" +
                "GEOHASH(52b) -> 78865 (52)\n" +
                "GEOHASH(53b) -> 79121 (53)\n" +
                "GEOHASH(54b) -> 79377 (54)\n" +
                "GEOHASH(11c) -> 79633 (55)\n" +
                "GEOHASH(56b) -> 79889 (56)\n" +
                "GEOHASH(57b) -> 80145 (57)\n" +
                "GEOHASH(58b) -> 80401 (58)\n" +
                "GEOHASH(59b) -> 80657 (59)\n" +
                "GEOHASH(12c) -> 80913 (60)\n";

        StringSink everything = new StringSink();
        for (int b = 1; b <= ColumnType.GEO_HASH_MAX_BITS_LENGTH; b++) {
            int type = ColumnType.getGeoHashTypeWithBits(b);
            String name = ColumnType.nameOf(type);
            everything.put(name)
                    .put(" -> ")
                    .put(type)
                    .put(" (")
                    .put(ColumnType.getGeoHashBits(type))
                    .put(")\n");
        }
        Assert.assertEquals(expected, everything.toString());
        Assert.assertEquals("GEOHASH", ColumnType.nameOf(ColumnType.GEOHASH));
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
    public void testFromCoordinates() throws NumericException {
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 5), GeoHashes.fromBitString("11100", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 2 * 5), GeoHashes.fromBitString("1110011001", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 3 * 5), GeoHashes.fromBitString("111001100111100", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 4 * 5), GeoHashes.fromBitString("11100110011110000011", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 5 * 5), GeoHashes.fromBitString("1110011001111000001111000", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 6 * 5), GeoHashes.fromBitString("111001100111100000111100010001", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 7 * 5), GeoHashes.fromBitString("11100110011110000011110001000110001", 0));
        Assert.assertEquals(GeoHashes.fromCoordinatesDeg(lat, lon, 8 * 5), GeoHashes.fromBitString("1110011001111000001111000100011000111111", 0));
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
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int precision = 1; precision <= maxGeoHashSizeChars; precision++) {
            int numBits = precision * 5;
            long hash = GeoHashes.fromCoordinatesDeg(39.982, 0.024, numBits);
            sink.clear();
            GeoHashes.appendChars(hash, precision, sink);
            expectedStr[precision - 1] = sink.toString();
            expectedHash[precision - 1] = hash;
            everything.put(expectedHash[precision - 1]).put(" -> ").put(expectedStr[precision - 1]).put('\n');
        }

        for (int i = 0; i < maxGeoHashSizeChars; i++) {
            final long gh = GeoHashes.fromString(expectedStr[i], 0, expectedStr[i].length());
            Assert.assertEquals(expectedHash[i], gh);
            sink.clear();
            GeoHashes.appendChars(gh, expectedStr[i].length(), sink);
            Assert.assertEquals(expectedStr[i], sink.toString());
        }
        Assert.assertEquals(expected, everything.toString());
    }

    @Test
    public void testFromCoordinatesEdge() {
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromCoordinatesDeg(-91, 0, 10));
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromCoordinatesDeg(0, 190, 10));
    }

    @Test
    public void testFromCoordinatesToFromStringMatch() throws NumericException {
        final long gh = GeoHashes.fromCoordinatesDeg(lat, lon, 8 * 5);
        StringSink sink = Misc.getThreadLocalBuilder();
        GeoHashes.appendChars(gh, 8, sink);
        final long gh1 = GeoHashes.fromString(sink, 0, 8);
        Assert.assertEquals(gh, gh1);
    }

    @Test
    public void testFromStringNl() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("123", 0, 0));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("", 0, 0));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("", 0, 1));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("''", 1, 0));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("''", 1, 0));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl(null, 0, 1));
    }

    @Test
    public void testFromStringTruncatingNl1() throws NumericException {
        for (int i = 5; i <= 60; i++) {
            Assert.assertNotEquals(
                    GeoHashes.fromStringTruncatingNl("123412341234", 0, 12, i - 1),
                    GeoHashes.fromStringTruncatingNl("123412341234", 0, 12, i));
        }
    }

    @Test(expected = NumericException.class)
    public void testFromStringTruncatingNl2() throws NumericException {
        GeoHashes.fromStringTruncatingNl("123", 0, 3, ColumnType.GEO_HASH_MAX_BITS_LENGTH + 1);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testFromStringNotEnoughChars() throws NumericException {
        GeoHashes.fromString("123", 0, 4);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testFromStringTruncatingNlNotEnoughChars() throws NumericException {
        GeoHashes.fromStringTruncatingNl("123", 0, 4, 15);
    }

    @Test(expected = NumericException.class)
    public void testFromStringTruncatingNlNotEnoughBits() throws NumericException {
        GeoHashes.fromStringTruncatingNl("123", 0, 3, 16);
    }

    @Test
    public void testFromStringOverMaxCharsLength() throws NumericException {
        Assert.assertEquals(
                GeoHashes.fromString("23456789bcde", 0, 12),
                GeoHashes.fromStringNl("!23456789bcde!", 1, GeoHashes.MAX_STRING_LENGTH + 2)
        );
    }

    @Test
    public void testFromStringTruncatingNlOverMaxCharsLength() throws NumericException {
        Assert.assertEquals(
                GeoHashes.fromStringTruncatingNl("23456789bcde", 0, 12, 0),
                GeoHashes.fromStringTruncatingNl("123456789bcdezz", 1, GeoHashes.MAX_STRING_LENGTH + 2, 0)
        );
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testFromStringShorterThanRequiredLength() throws NumericException {
        GeoHashes.fromString("123", 1, 7);
    }

    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testFromStringTruncatingNlShorterThanRequiredLength1() throws NumericException {
        GeoHashes.fromStringTruncatingNl("123", 1, 7, 0);
    }

    @Test
    public void testFromStringTruncatingNlShorterThanRequiredLength2() {
        testUnsafeFromStringTruncatingNl("123", (lo, hi) -> {
            try {
                Assert.assertEquals(0, GeoHashes.fromStringTruncatingNl(lo, hi + 7, 1));
            } catch (NumericException expected) {
                // beyond hi we will find whatever, very unlikely that it parses as a geohash char
            }
            return null;
        });
    }

    @Test
    public void testFromStringNlYieldsNullDueToZeroRequiredLen() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringNl("''", 1, 0));
    }

    @Test
    public void testFromStringTruncatingNlYieldsNullDueToZeroRequiredLen() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromStringTruncatingNl("''", 1, 1, 0));
    }

    @Test
    public void testFromStringJustOneChar() throws NumericException {
        Assert.assertEquals(0, GeoHashes.fromString("ast", 1, 1));
    }

    @Test
    public void testFromStringNlJustOneChar() throws NumericException {
        Assert.assertEquals(24, GeoHashes.fromStringNl("ast", 1, 1));
    }

    @Test
    public void testFromStringTruncatingNlJustOneChar() throws NumericException {
        Assert.assertEquals(24, GeoHashes.fromStringTruncatingNl("ast", 1, 2, 5));
    }

    @Test
    public void testFromStringIgnoreQuotes() throws NumericException {
        Assert.assertEquals(27760644473312309L, GeoHashes.fromString("'sp052w92p1p'", 1, 12));
    }

    @Test(expected = NumericException.class)
    public void testFromStringBadChar() throws NumericException {
        Assert.assertEquals(27760644473312309L, GeoHashes.fromString("'sp05@w92p1p'", 1, 12));
    }

    @Test
    public void testFromStringExcessChars1() throws NumericException {
        Assert.assertEquals(-8466588206747298559L, GeoHashes.fromString("'sp052w92p1p812'", 1, 14));
    }

    @Test
    public void testFromStringNlExcessChars() throws NumericException {
        Assert.assertEquals(888340623145993896L, GeoHashes.fromStringNl("'sp052w92p1p812'", 1, 14));
    }

    @Test
    public void testFromStringIgnoreQuotesTruncateChars() throws NumericException {
        Assert.assertEquals(807941, GeoHashes.fromString("'sp052w92p1p'", 1, 5));
        StringSink sink = Misc.getThreadLocalBuilder();
        GeoHashes.appendChars(807941, 4, sink);
        Assert.assertEquals("sp05", sink.toString());
    }

    @Test
    public void testFromStringTruncatingNlIgnoreQuotesTruncateBits1() throws NumericException {
        Assert.assertEquals(807941, GeoHashes.fromStringTruncatingNl("'sp052w92p1p'", 1, 11, 20));
        StringSink sink = Misc.getThreadLocalBuilder();
        GeoHashes.appendChars(807941, 4, sink);
        Assert.assertEquals("sp05", sink.toString());
    }

    @Test
    public void testFromStringTruncatingNlIgnoreQuotesTruncateBits2() {
        testUnsafeFromStringTruncatingNl("'sp052w92p1p'", (lo, hi) -> {
            try {
                Assert.assertEquals(807941, GeoHashes.fromStringTruncatingNl(lo + 1, lo + 5, 20));
            } catch (NumericException e) {
                Assert.fail();
            }
            return null;
        });
    }

    @Test
    public void testFromStringTruncatingNlIgnoreQuotesTruncateBits3() {
        testUnsafeFromStringTruncatingNl("23456789bcde", (lo0, hi0) -> {
            testUnsafeFromStringTruncatingNl("123456789bcdezz", (lo1, hi1) -> {
                try {
                    Assert.assertEquals(
                            GeoHashes.fromStringTruncatingNl(lo0, lo0 + 12, 4),
                            GeoHashes.fromStringTruncatingNl(lo1 + 1, lo1 + 13, 4));

                    Assert.assertNotEquals(
                            GeoHashes.fromStringTruncatingNl(lo0, lo0 + 12, 20),
                            GeoHashes.fromStringTruncatingNl(lo1 + 1, lo1 + 13, 25));
                } catch (NumericException e) {
                    Assert.fail();
                }
                return null;
            });
            return null;
        });
    }

    @Test
    public void testFromBitStringValid() throws NumericException {
        Assert.assertEquals(0, GeoHashes.fromBitString("", 0));
        Assert.assertEquals(0, GeoHashes.fromBitString("", 1));
        Assert.assertEquals(1, GeoHashes.fromBitString(
                "##000000000000000000000000000000000000000000000000000000000001", 2));
        Assert.assertEquals(1, GeoHashes.fromBitString(
                "##000000000000000000000000000000000000000000000000000000000001", 59));
    }

    @Test
    public void testFromBitStringNl() throws NumericException {
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromBitStringNl("", 0));
        Assert.assertEquals(GeoHashes.NULL, GeoHashes.fromBitStringNl("0011", 4));
        Assert.assertNotEquals(GeoHashes.NULL, GeoHashes.fromBitStringNl("0", 0));
        Assert.assertEquals(1, GeoHashes.fromBitStringNl( // same as empty string
                "##000000000000000000000000000000000000000000000000000000000000" + "1", 60));
        Assert.assertEquals(1, GeoHashes.fromBitStringNl(
                "##000000000000000000000000000000000000000000000000000000000000" + "1", 59));
        Assert.assertEquals(0, GeoHashes.fromBitStringNl( // truncates
                "##000000000000000000000000000000000000000000000000000000000000" + "1", 2));
    }

    @Test(expected = NumericException.class)
    public void testFromBitStringNotValid1() throws NumericException {
        GeoHashes.fromBitStringNl(" ", 0);
    }

    @Test(expected = NumericException.class)
    public void testFromBitStringNotValid2() throws NumericException {
        GeoHashes.fromBitStringNl("012", 0);
    }

    @Test(expected = NumericException.class)
    public void testFromString() throws NumericException {
        StringSink sink = Misc.getThreadLocalBuilder();
        sink.put("@s");
        GeoHashes.fromString(sink.toString(), 0, 2);
    }

    @Test
    public void testBuildNormalizedPrefixesAndMasks() throws NumericException {
        final int cap = 12;
        LongList bits = new LongList(cap * 2); // hash and mask
        int columnType = ColumnType.getGeoHashTypeWithBits(5 * cap);
        for (int i = 0; i < cap; i++) {
            final int prec = (i % 3) + 3;
            final long h = rnd_geohash(prec);
            int type = ColumnType.getGeoHashTypeWithBits(5 * prec);
            GeoHashes.addNormalizedGeoPrefix(h, type, columnType, bits);
        }

        for (int i = 0; i < bits.size() / 2; i += 2) {
            final long b = bits.get(i);
            final long m = bits.get(i + 1);
            Assert.assertEquals(b, b & m);
        }
    }

    @Test
    public void testPrefixPrecisionMismatch() throws NumericException {
        final int cap = 1;
        LongList bits = new LongList(cap * 2); // hash and mask
        final long h = rnd_geohash(5);
        final long p = rnd_geohash(7);

        int pType = ColumnType.getGeoHashTypeWithBits(5 * 7);
        int hType = ColumnType.getGeoHashTypeWithBits(5 * 5);
        try {
            GeoHashes.addNormalizedGeoPrefix(h, pType, hType, bits);
        } catch (NumericException ignored) {
        }
        Assert.assertEquals(0, bits.size());
    }

    @Test
    public void testFromBitStringTruncating() throws NumericException {
        CharSequence tooLongBitString = Chars.repeat("1", 61); // truncates
        long maxGeoHash = GeoHashes.fromString("zzzzzzzzzzzz", 0, 12);
        Assert.assertEquals(maxGeoHash, GeoHashes.fromBitString(tooLongBitString, 0));
    }

    @Test
    public void testFromBitStringInvalid() {
        Assert.assertThrows(NumericException.class, () -> GeoHashes.fromBitString("a", 0));
    }

    @Test
    public void testToFromBitString() throws NumericException {
        long hash = 27760644473312309L;
        String expected = "1100010101000000010100010111000100100010101010000110101";
        Assert.assertEquals(expected, Long.toBinaryString(hash));
        assertSuccess(hash, expected.length(), expected, toBitString);
        Assert.assertEquals(hash, GeoHashes.fromBitString(expected, 0));
    }

    @Test
    public void testToBitStringInvalidSizeInBits() {
        boolean assertsEnable = false;
        assert assertsEnable = true; // Test only when assertions enabled
        if (assertsEnable) {
            StringSink sink = Misc.getThreadLocalBuilder();
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.appendBinary(-0, 0, sink));
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.appendBinary(-31, 0, sink));
        }
    }

    @Test
    public void testToBitStringNull() {
        assertSuccess(-1, Integer.MIN_VALUE, "", toBitString);
        assertSuccess(-1L, Integer.MAX_VALUE, "", toBitString);
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
            StringSink sink = Misc.getThreadLocalBuilder();
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.appendChars(-0, 0, sink));
            Assert.assertThrows(AssertionError.class, () -> GeoHashes.appendChars(-0, 31, sink));
        }
    }

    @Test
    public void testIsValidChars1() {
        Rnd rnd = new Rnd();
        IntHashSet base32Chars = new IntHashSet(base32.length); // upper and lower case
        for (char ch : base32) {
            base32Chars.add(ch);
            Assert.assertTrue(GeoHashes.isValidChars("" + ch, 0));
        }
        for (int i = 0; i < 15000; i++) {
            char ch = (char) rnd.nextPositiveInt();
            Assert.assertEquals("dis one: " + ch, base32Chars.contains(ch), GeoHashes.isValidChars("" + ch, 0));
        }
    }

    @Test
    public void testIsValidChars2() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int len = 1; len <= 12; len++) {
            sink.clear();
            sink.put('@');
            rnd_geochars(rnd, sink, len);
            Assert.assertTrue(GeoHashes.isValidChars(sink, 1));
        }
    }

    @Test
    public void testIsValidCharsWhenIsNot() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int len = 1; len <= 12; len++) {
            sink.clear();
            rnd_geochars(rnd, sink, len);
            sink.put('@');
            Assert.assertFalse(GeoHashes.isValidChars(sink, 0));
        }
    }

    @Test
    public void testIsValidCharsOutOfBounds() {
        Assert.assertFalse(GeoHashes.isValidChars("", 0));
        Assert.assertFalse(GeoHashes.isValidChars("", 1));
        Assert.assertFalse(GeoHashes.isValidChars("s", 1));
    }

    @Test
    public void testIsValidBits1() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int len = 1; len <= 60; len++) {
            sink.clear();
            sink.put("##");
            rnd_geobits(rnd, sink, len);
            Assert.assertTrue(GeoHashes.isValidBits(sink, 2));
        }
    }

    @Test
    public void testIsValidBitsWhenIsNot() {
        Rnd rnd = new Rnd();
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int len = 1; len <= 60; len++) {
            sink.clear();
            rnd_geobits(rnd, sink, len);
            sink.put('@');
            Assert.assertFalse(GeoHashes.isValidBits(sink, 0));
        }
    }

    @Test
    public void testIsValidBitsOutOfBounds() {
        Assert.assertFalse(GeoHashes.isValidBits("", 0));
        Assert.assertFalse(GeoHashes.isValidBits("", 1));
        Assert.assertFalse(GeoHashes.isValidBits("1", 1));
    }

    private static void assertSuccess(long hash, int size, String message, StringConverter converter) {
        try {
            StringSink sink = Misc.getThreadLocalBuilder();
            converter.convert(hash, size, sink);
            Assert.assertEquals(message, sink.toString());
        } catch (IllegalArgumentException | NumericException err) {
            Assert.fail();
        }
    }

    @FunctionalInterface
    private interface StringConverter {
        void convert(long hash, int size, CharSink sink) throws NumericException;
    }

    private static final char[] base32 = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
            'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J',
            'K', 'M', 'N', 'P', 'Q', 'R', 'S', 'T',
            'U', 'V', 'W', 'X', 'Y', 'Z'
    };

    private static long rnd_geohash(int size) throws NumericException {
        double x = rnd_double(-90, 90);
        double y = rnd_double(-180, 180);
        return GeoHashes.fromCoordinatesDeg(x, y, size * 5);
    }

    private static double rnd_double(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    private static void rnd_geochars(Rnd rnd, StringSink sink, int len) {
        for (int i = 0; i < len; i++) {
            sink.put(base32[rnd.nextPositiveInt() % base32.length]);
        }
    }

    private static void rnd_geobits(Rnd rnd, StringSink sink, int len) {
        for (int i = 0; i < len; i++) {
            sink.put(rnd.nextBoolean() ? '1' : '0');
        }
    }

    private void testUnsafeFromStringTruncatingNl(CharSequence token, BiFunction<Long, Long, Void> code) {
        final int len = token.length();
        final long lo = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        final long hi = lo + len;
        try {
            sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
            for (long p = lo; p < hi; p++) {
                unsafe.putByte(p, (byte) token.charAt((int) (p - lo)));
            }
            code.apply(lo, hi);
        } finally {
            Unsafe.free(lo, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
