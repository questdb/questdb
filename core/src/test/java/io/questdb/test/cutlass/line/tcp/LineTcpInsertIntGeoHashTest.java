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

package io.questdb.test.cutlass.line.tcp;

public class LineTcpInsertIntGeoHashTest extends BaseLineTcpInsertGeoHashTest {
    public LineTcpInsertIntGeoHashTest(WalMode walMode) {
        super(walMode);
    }

    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertGeoHash(30,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "9v1s8h\t1970-01-01T00:00:01.000000Z\n");
    }

    @Override
    public void testGeoHashes() throws Exception {
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

    @Override
    public void testGeoHashesNotEnoughPrecision() throws Exception {
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

    @Override
    public void testGeoHashesTruncating() throws Exception {
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

    @Override
    public void testNullGeoHash() throws Exception {
        assertGeoHash(30,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(29,
                "tracking onions=\"9v1\" 1000000000\n" +
                        "tracking onions=\"46s\" 2000000000\n" +
                        "tracking onions=\"jnw\" 3000000000\n" +
                        "tracking geohash=\"zfu123\",name=\"zfu123\" 4000000000\n" +
                        "tracking geohash=\"hp4567\" 5000000000\n" +
                        "tracking onions=\"wh4\" 6000000000\n" +
                        "tracking mint=\"s2z\" 7000000000\n",
                "geohash\ttimestamp\tonions\tname\tmint\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9v1\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t46s\t\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\tjnw\t\t\n" +
                        "11111011101101000001000100001\t1970-01-01T00:00:04.000000Z\t\tzfu123\t\n" +
                        "10000101010010000101001100011\t1970-01-01T00:00:05.000000Z\t\t\t\n" +
                        "\t1970-01-01T00:00:06.000000Z\twh4\t\t\n" +
                        "\t1970-01-01T00:00:07.000000Z\t\t\ts2z\n",
                "onions", "mint", "name");
    }

    @Override
    public void testWrongCharGeoHashes() throws Exception {
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
}
