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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import org.junit.Assert;
import org.junit.Test;

public class LineTcpInsertGeoHashTest extends BaseLineTcpContextTest {

    @Test
    public void testByteSizedGeoHashes() throws Exception {
        assertGeoHash(5,
                "tracking geohash=\"9\" 1000000000\n" +
                        "tracking geohash=\"4\" 2000000000\n" +
                        "tracking geohash=\"j\" 3000000000\n" +
                        "tracking geohash=\"z\" 4000000000\n" +
                        "tracking geohash=\"h\" 5000000000\n",
                "geohash\ttimestamp\n" +
                        "9\t1970-01-01T00:00:01.000000Z\n" +
                        "4\t1970-01-01T00:00:02.000000Z\n" +
                        "j\t1970-01-01T00:00:03.000000Z\n" +
                        "z\t1970-01-01T00:00:04.000000Z\n" +
                        "h\t1970-01-01T00:00:05.000000Z\n");
    }

    @Test
    public void testByteSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(4,
                "tracking geohash=\"9\" 1000000000\n" +
                        "tracking geohash=\"4\" 2000000000\n" +
                        "tracking geohash=\"j\" 3000000000\n",
                "geohash\ttimestamp\n" +
                        "0100\t1970-01-01T00:00:01.000000Z\n" +
                        "0010\t1970-01-01T00:00:02.000000Z\n" +
                        "1000\t1970-01-01T00:00:03.000000Z\n");
    }

    @Test
    public void testByteSizedGeoHashesTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(4,
                "tracking carrots=\"9\" 1000000000\n" +
                        "tracking carrots=\"4\" 2000000000\n" +
                        "tracking carrots=\"j\" 3000000000\n",
                "geohash\ttimestamp\tcarrots\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4\n" +
                        "\t1970-01-01T00:00:03.000000Z\tj\n",
                "carrots");
    }

    @Test
    public void testByteSizedGeoHashesSeeminglyGoodLookingStringWhichIsTooLongToBeAGeoHash() throws Exception {
        assertGeoHash(4,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testByteSizedWrongCharGeoHashes() throws Exception {
        assertGeoHash(4,
                "tracking geohash=\"9@tralala\" 1000000000\n" +
                        "tracking geohash=\"4-12\" 2000000000\n" +
                        "tracking john=\"4-12\",activity=\"lion taming\" 2000000000\n" +
                        "tracking geohash=\"jurl\" 3000000000\n",
                "geohash\ttimestamp\tjohn\tactivity\n" +
                        "\t1970-01-01T00:00:01.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4-12\tlion taming\n" +
                        "\t1970-01-01T00:00:03.000000Z\t\t\n",
                "john", "activity");
    }

    @Test
    public void testShortSizedGeoHashes() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"9v1\" 1000000000\n" +
                        "tracking geohash=\"46s\" 2000000000\n" +
                        "tracking geohash=\"jnw\" 3000000000\n" +
                        "tracking geohash=\"zfu\" 4000000000\n" +
                        "tracking geohash=\"hp4\" 5000000000\n" +
                        "tracking geohash=\"wh4\" 6000000000\n" +
                        "tracking geohash=\"s2z\" 7000000000\n",
                "geohash\ttimestamp\n" +
                        "9v1\t1970-01-01T00:00:01.000000Z\n" +
                        "46s\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw\t1970-01-01T00:00:03.000000Z\n" +
                        "zfu\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashesTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(15,
                "tracking onions=\"9v1\" 1000000000\n" +
                        "tracking onions=\"46s\" 2000000000\n" +
                        "tracking onions=\"jnw\" 3000000000\n" +
                        "tracking geohash=\"zfu\" 4000000000\n" +
                        "tracking geohash=\"hp4\" 5000000000\n" +
                        "tracking onions=\"wh4\" 6000000000\n" +
                        "tracking mint=\"s2z\" 7000000000\n",
                "geohash\ttimestamp\tonions\tmint\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9v1\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t46s\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\tjnw\t\n" +
                        "zfu\t1970-01-01T00:00:04.000000Z\t\t\n" +
                        "hp4\t1970-01-01T00:00:05.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:06.000000Z\twh4\t\n" +
                        "\t1970-01-01T00:00:07.000000Z\t\ts2z\n",
                "onions", "mint");
    }

    @Test
    public void testShortSizedGeoHashesSeeminglyGoodLookingStringWhichIsTooLongToBeAGeoHash() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(13,
                "tracking geohash=\"9v1\" 1000000000\n" +
                        "tracking geohash=\"46s\" 2000000000\n" +
                        "tracking geohash=\"jnw\" 3000000000\n" +
                        "tracking geohash=\"zfu\" 4000000000\n" +
                        "tracking geohash=\"hp4\" 5000000000\n" +
                        "tracking geohash=\"wh4\" 6000000000\n" +
                        "tracking geohash=\"s2z\" 7000000000\n" +
                        "tracking geohash=\"1cj\" 8000000000\n" +
                        "tracking geohash=\"mmt\" 9000000000\n" +
                        "tracking geohash=\"71f\" 10000000000\n",
                "geohash\ttimestamp\n" +
                        "0100111011000\t1970-01-01T00:00:01.000000Z\n" +
                        "0010000110110\t1970-01-01T00:00:02.000000Z\n" +
                        "1000110100111\t1970-01-01T00:00:03.000000Z\n" +
                        "1111101110110\t1970-01-01T00:00:04.000000Z\n" +
                        "1000010101001\t1970-01-01T00:00:05.000000Z\n" +
                        "1110010000001\t1970-01-01T00:00:06.000000Z\n" +
                        "1100000010111\t1970-01-01T00:00:07.000000Z\n" +
                        "0000101011100\t1970-01-01T00:00:08.000000Z\n" +
                        "1001110011110\t1970-01-01T00:00:09.000000Z\n" +
                        "0011100001011\t1970-01-01T00:00:10.000000Z\n");
    }

    @Test
    public void testShortSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"9v\" 1000000000\n" +
                        "tracking geohash=\"46\" 2000000000\n" +
                        "tracking geohash=\"jn\" 3000000000\n" +
                        "tracking geohash=\"zf\" 4000000000\n" +
                        "tracking geohash=\"hp\",name=\"questdb\" 5000000000\n" +
                        "tracking geohash=\"wh\" 6000000000\n" +
                        "tracking geohash=\"s2\",name=\"neptune\" 7000000000\n" +
                        "tracking geohash=\"1c\" 8000000000\n" +
                        "tracking geohash=\"mm\" 9000000000\n" +
                        "tracking name=\"timeseries\",geohash=\"71\" 10000000000\n",
                "geohash\ttimestamp\tname\n" +
                        "\t1970-01-01T00:00:01.000000Z\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\t\n" +
                        "\t1970-01-01T00:00:04.000000Z\t\n" +
                        "\t1970-01-01T00:00:05.000000Z\tquestdb\n" +
                        "\t1970-01-01T00:00:06.000000Z\t\n" +
                        "\t1970-01-01T00:00:07.000000Z\tneptune\n" +
                        "\t1970-01-01T00:00:08.000000Z\t\n" +
                        "\t1970-01-01T00:00:09.000000Z\t\n" +
                        "\t1970-01-01T00:00:10.000000Z\ttimeseries\n",
                "name");
    }

    @Test
    public void testShortSizedWrongCharGeoHashes() throws Exception {
        assertGeoHash(13,
                "tracking geohash=\"9v1@\" 1000000000\n" +
                        "tracking geohash=\"@46s\" 2000000000\n" +
                        "tracking geohash=\"jLnw\" 3000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:03.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashes() throws Exception {
        assertGeoHash(30,
                "tracking geohash=\"9v1s8h\" 1000000000\n" +
                        "tracking geohash=\"46swgj\" 2000000000\n" +
                        "tracking geohash=\"jnw97u\" 3000000000\n" +
                        "tracking geohash=\"zfuqd3\" 4000000000\n" +
                        "tracking geohash=\"hp4muv\" 5000000000\n" +
                        "tracking geohash=\"wh4b6v\" 6000000000\n" +
                        "tracking geohash=\"s2z2fy\" 7000000000\n" +
                        "tracking geohash=\"1cjjwk\" 8000000000\n",
                "geohash\ttimestamp\n" +
                        "9v1s8h\t1970-01-01T00:00:01.000000Z\n" +
                        "46swgj\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw97u\t1970-01-01T00:00:03.000000Z\n" +
                        "zfuqd3\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4muv\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4b6v\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z2fy\t1970-01-01T00:00:07.000000Z\n" +
                        "1cjjwk\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashesTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(15,
                "tracking onions=\"9v1\" 1000000000\n" +
                        "tracking onions=\"46s\" 2000000000\n" +
                        "tracking onions=\"jnw\" 3000000000\n" +
                        "tracking geohash=\"zfu\",name=\"zfu\" 4000000000\n" +
                        "tracking geohash=\"hp4\" 5000000000\n" +
                        "tracking onions=\"wh4\" 6000000000\n" +
                        "tracking mint=\"s2z\" 7000000000\n",
                "geohash\ttimestamp\tonions\tname\tmint\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9v1\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t46s\t\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\tjnw\t\t\n" +
                        "zfu\t1970-01-01T00:00:04.000000Z\t\tzfu\t\n" +
                        "hp4\t1970-01-01T00:00:05.000000Z\t\t\t\n" +
                        "\t1970-01-01T00:00:06.000000Z\twh4\t\t\n" +
                        "\t1970-01-01T00:00:07.000000Z\t\t\ts2z\n",
                "onions", "mint", "name");
    }

    @Test
    public void testIntSizedGeoHashesSeeminglyGoodLookingStringWhichIsTooLongToBeAGeoHash() throws Exception {
        assertGeoHash(30,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(29,
                "tracking geohash=\"9v1s8h\" 1000000000\n" +
                        "tracking geohash=\"46swgj\" 2000000000\n" +
                        "tracking geohash=\"jnw97u\" 3000000000\n" +
                        "tracking geohash=\"zfuqd3\" 4000000000\n" +
                        "tracking geohash=\"hp4muv\" 5000000000\n" +
                        "tracking geohash=\"wh4b6v\" 6000000000\n" +
                        "tracking geohash=\"s2z2fy\" 7000000000\n" +
                        "tracking geohash=\"1cjjwk\" 8000000000\n",
                "geohash\ttimestamp\n" +
                        "01001110110000111000010001000\t1970-01-01T00:00:01.000000Z\n" +
                        "00100001101100011100011111000\t1970-01-01T00:00:02.000000Z\n" +
                        "10001101001110001001001111101\t1970-01-01T00:00:03.000000Z\n" +
                        "11111011101101010110011000001\t1970-01-01T00:00:04.000000Z\n" +
                        "10000101010010010011110101101\t1970-01-01T00:00:05.000000Z\n" +
                        "11100100000010001010001101101\t1970-01-01T00:00:06.000000Z\n" +
                        "11000000101111100010011101111\t1970-01-01T00:00:07.000000Z\n" +
                        "00001010111000110001111001001\t1970-01-01T00:00:08.000000Z\n");
    }

    @Test
    public void testIntSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(32,
                "tracking geohash=\"9v1s8\" 1000000000\n" +
                        "tracking geohash=\"46swg\" 2000000000\n" +
                        "tracking geohash=\"jnw97\" 3000000000\n" +
                        "tracking geohash=\"zfuqd\" 4000000000\n" +
                        "tracking name=\"hp4mu\" 5000000000\n" +
                        "tracking geohash=\"wh4b6\" 6000000000\n" +
                        "tracking geohash=\"s2z2f\" 7000000000\n" +
                        "tracking geohash=\"1cjjw\",name=\"\" 8000000000\n",
                "geohash\ttimestamp\tname\n" +
                        "\t1970-01-01T00:00:01.000000Z\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\t\n" +
                        "\t1970-01-01T00:00:04.000000Z\t\n" +
                        "\t1970-01-01T00:00:05.000000Z\thp4mu\n" +
                        "\t1970-01-01T00:00:06.000000Z\t\n" +
                        "\t1970-01-01T00:00:07.000000Z\t\n" +
                        "\t1970-01-01T00:00:08.000000Z\t\n");
    }

    @Test
    public void testIntSizedWrongCharGeoHashes() throws Exception {
        assertGeoHash(29,
                "tracking geohash=\"9v@1s8h\" 1000000000\n" +
                        "tracking geohash=\"46swLgj\" 2000000000\n" +
                        "tracking geohash=\"j+nw97u\" 3000000000\n" +
                        "tracking geohash=\"zf-uqd3\",composer=\"Mozart\" 4000000000\n",
                "geohash\ttimestamp\tcomposer\n" +
                        "\t1970-01-01T00:00:01.000000Z\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\t\n" +
                        "\t1970-01-01T00:00:04.000000Z\tMozart\n",
                "composer");
    }

    @Test
    public void testLongSizedGeoHashes() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"9v1s8hm7wpks\" 1000000000\n" +
                        "tracking geohash=\"46swgj10r88k\" 2000000000\n" +
                        "tracking geohash=\"jnw97u4yuquw\" 3000000000\n" +
                        "tracking geohash=\"zfuqd3bf8hbu\" 4000000000\n" +
                        "tracking geohash=\"hp4muv5tgg3q\" 5000000000\n" +
                        "tracking geohash=\"wh4b6vntdq1c\" 6000000000\n" +
                        "tracking geohash=\"s2z2fydsjq5n\" 7000000000\n",
                "geohash\ttimestamp\n" +
                        "9v1s8hm7wpks\t1970-01-01T00:00:01.000000Z\n" +
                        "46swgj10r88k\t1970-01-01T00:00:02.000000Z\n" +
                        "jnw97u4yuquw\t1970-01-01T00:00:03.000000Z\n" +
                        "zfuqd3bf8hbu\t1970-01-01T00:00:04.000000Z\n" +
                        "hp4muv5tgg3q\t1970-01-01T00:00:05.000000Z\n" +
                        "wh4b6vntdq1c\t1970-01-01T00:00:06.000000Z\n" +
                        "s2z2fydsjq5n\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongSizedWrongCharGeoHashes() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"9v1s8hm7wpks\" 1000000000\n" +
                        "tracking geohash=\"46swgj10r88k\" 2000000000\n" +
                        "tracking geohash=\"hp4m@v5tgg3q\" 5000000000\n" +
                        "tracking geohash=\"wh4b6vnt-q1c\" 6000000000\n",
                "geohash\ttimestamp\n" +
                        "9v1s8hm7wpks\t1970-01-01T00:00:01.000000Z\n" +
                        "46swgj10r88k\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n");
    }

    @Test
    public void testLongSizedGeoHashesSeeminglyGoodLookingStringWhichIsTooLongToBeAGeoHash() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"9v1s8hm7wpkssv1h\",item=\"book\" 1000000000\n",
                "geohash\ttimestamp\titem\n" +
                        "\t1970-01-01T00:00:01.000000Z\tbook\n");
    }

    @Test
    public void testLongSizedGeoHashesTruncating() throws Exception {
        assertGeoHash(57,
                "tracking geohash=\"9v1s8hm7wpks\" 1000000000\n" +
                        "tracking geohash=\"46swgj10r88k\" 2000000000\n" +
                        "tracking geohash=\"jnw97u4yuquw\" 3000000000\n" +
                        "tracking geohash=\"zfuqd3bf8hbu\" 4000000000\n" +
                        "tracking geohash=\"hp4muv5tgg3q\" 5000000000\n" +
                        "tracking geohash=\"wh4b6vntdq1c\" 6000000000\n" +
                        "tracking geohash=\"s2z2fydsjq5n\" 7000000000\n",
                "geohash\ttimestamp\n" +
                        "010011101100001110000100010000100110011111100101011001011\t1970-01-01T00:00:01.000000Z\n" +
                        "001000011011000111000111110001000010000010111010000100010\t1970-01-01T00:00:02.000000Z\n" +
                        "100011010011100010010011111010001001111011010101101101011\t1970-01-01T00:00:03.000000Z\n" +
                        "111110111011010101100110000011010100111001000100000101011\t1970-01-01T00:00:04.000000Z\n" +
                        "100001010100100100111101011011001011100101111011110001110\t1970-01-01T00:00:05.000000Z\n" +
                        "111001000000100010100011011011101001100101100101100000101\t1970-01-01T00:00:06.000000Z\n" +
                        "110000001011111000100111011110011001100010001101100010110\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongSizedGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"9v1s8hm7wpk\" 1000000000\n" +
                        "tracking geohash=\"46swgj10r88\" 2000000000\n" +
                        "tracking geohash=\"jnw97u4yuqu\" 3000000000\n" +
                        "tracking geohash=\"zfuqd3bf8hb\" 4000000000\n" +
                        "tracking geohash=\"hp4muv5tgg3\" 5000000000\n" +
                        "tracking geohash=\"wh4b6vntdq1\" 6000000000\n" +
                        "tracking geohash=\"s2z2fydsjq5\" 7000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:03.000000Z\n" +
                        "\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "\t1970-01-01T00:00:07.000000Z\n");
    }

    @Test
    public void testLongSizedGeoHashesTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(57,
                "tracking geohash=\"9v1s8hm7wpks\" 1000000000\n" +
                        "tracking herbs=\"46swgj10r88k\" 2000000000\n" +
                        "tracking herbs=\"jnw97u4yuquw\" 3000000000\n" +
                        "tracking herbs=\"zfuqd3bf8hbu\" 4000000000\n" +
                        "tracking herbs=\"hp4muv5tgg3q\" 5000000000\n" +
                        "tracking pepper=\"wh4b6vntdq1c\" 6000000000\n" +
                        "tracking geohash=\"s2z2fydsjq5n\" 7000000000\n",
                "geohash\ttimestamp\therbs\tpepper\n" +
                        "010011101100001110000100010000100110011111100101011001011\t1970-01-01T00:00:01.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t46swgj10r88k\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\tjnw97u4yuquw\t\n" +
                        "\t1970-01-01T00:00:04.000000Z\tzfuqd3bf8hbu\t\n" +
                        "\t1970-01-01T00:00:05.000000Z\thp4muv5tgg3q\t\n" +
                        "\t1970-01-01T00:00:06.000000Z\t\twh4b6vntdq1c\n" +
                        "110000001011111000100111011110011001100010001101100010110\t1970-01-01T00:00:07.000000Z\t\t\n",
                "herbs", "pepper");
    }

    @Test
    public void testInsertMissingGeoHashHasNoEffect() throws Exception {
        for (int b = 1; b <= GeoHashes.MAX_BITS_LENGTH; b++) {
            if (b > 1) {
                setUp();
            }
            assertGeoHash(b,
                    "tracking geohash= 1000000000\n",
                    "geohash\ttimestamp\n");
            tearDown();
        }
    }

    @Test
    public void testLongSizedWrongGeoHashes() throws Exception {
        assertGeoHash(57,
                "tracking geohash=\"@9v1s8hm@7wp\" 1000000000\n" +
                        "tracking geohash=\"46swgj1Lr88k\" 2000000000\n" +
                        "tracking geohash=\"jnw97u--uquw\" 3000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:03.000000Z\n");
    }

    @Test
    public void testInsertNullByteGeoHash() throws Exception {
        assertGeoHash(1,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testInsertNullShortGeoHash() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testInsertNullIntGeoHash() throws Exception {
        assertGeoHash(30,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Test
    public void testInsertNullLongGeoHash() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    private void assertGeoHash(int columnBits,
                               String inboundLines,
                               String expected) throws Exception {
        assertGeoHash(columnBits, inboundLines, expected, null);
    }

    private void assertGeoHash(int columnBits,
                               String inboundLines,
                               String expected,
                               String... expectedExtraStringColumns) throws Exception {
        runInContext(() -> {
            try (TableModel model = new TableModel(configuration, "tracking", PartitionBy.NONE)) {
                CairoTestUtils.create(model.col("geohash", ColumnType.geohashWithPrecision(columnBits)).timestamp());
            }
            recvBuffer = inboundLines;
            handleContextIO();
            waitForIOCompletion();
            closeContext();
            assertTable(expected, "tracking");
            if (expectedExtraStringColumns != null) {
                try (TableReader reader = new TableReader(configuration, "tracking")) {
                    TableReaderMetadata meta = reader.getMetadata();
                    Assert.assertEquals(2 + expectedExtraStringColumns.length, meta.getColumnCount());
                    for (String colName : expectedExtraStringColumns) {
                        Assert.assertEquals(ColumnType.STRING, meta.getColumnType(colName));
                    }
                }
            }
        });
    }
}
