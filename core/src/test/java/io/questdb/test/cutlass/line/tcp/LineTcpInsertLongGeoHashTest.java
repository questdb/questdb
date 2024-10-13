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

public class LineTcpInsertLongGeoHashTest extends BaseLineTcpInsertGeoHashTest {
    public LineTcpInsertLongGeoHashTest(WalMode walMode) {
        super(walMode);
    }

    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"9v1s8hm7wpkssv1h\",item=\"book\" 1000000000\n",
                "geohash\ttimestamp\titem\n" +
                        "9v1s8hm7wpks\t1970-01-01T00:00:01.000000Z\tbook\n");
    }

    @Override
    public void testGeoHashes() throws Exception {
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

    @Override
    public void testGeoHashesNotEnoughPrecision() throws Exception {
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

    @Override
    public void testGeoHashesTruncating() throws Exception {
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

    @Override
    public void testNullGeoHash() throws Exception {
        assertGeoHash(60,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
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

    @Override
    public void testWrongCharGeoHashes() throws Exception {
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
}
