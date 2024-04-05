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

import org.junit.Test;

public class LineTcpInsertByteGeoHashTest extends BaseLineTcpInsertGeoHashTest {
    public LineTcpInsertByteGeoHashTest(WalMode walMode) {
        super(walMode);
    }

    @Test
    public void test() throws Exception {
        assertGeoHash(10,
                "tracking geohash=,here=\"no\" 1000000000\n",
                "geohash\ttimestamp\there\n" +
                        "\t1970-01-01T00:00:01.000000Z\tno\n");
    }

    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertGeoHash(4,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "0100\t1970-01-01T00:00:01.000000Z\n");
    }

    @Override
    public void testGeoHashes() throws Exception {
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

    @Override
    public void testGeoHashesNotEnoughPrecision() {
        // TODO: there is no binary representation allowing to represent less than 5 bits
    }

    @Override
    public void testGeoHashesTruncating() throws Exception {
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
    public void testInsertNoValueByteGeoHash() throws Exception {
        assertGeoHash(5,
                "tracking geohash=,here=\"no\" 1000000000\n",
                "geohash\ttimestamp\there\n" +
                        "\t1970-01-01T00:00:01.000000Z\tno\n");
    }

    @Test
    public void testInsertNoValueIntGeoHash() throws Exception {
        assertGeoHash(30,
                "tracking geohash=,here=\"no\" 1000000000\n",
                "geohash\ttimestamp\there\n" +
                        "\t1970-01-01T00:00:01.000000Z\tno\n");
    }

    @Test
    public void testInsertNoValueLongGeoHash() throws Exception {
        assertGeoHash(60,
                "tracking geohash=,here=\"no\" 1000000000\n",
                "geohash\ttimestamp\there\n" +
                        "\t1970-01-01T00:00:01.000000Z\tno\n");
    }

    @Test
    public void testInsertNoValueShortGeoHash() throws Exception {
        assertGeoHash(10,
                "tracking geohash=,here=\"no\" 1000000000\n",
                "geohash\ttimestamp\there\n" +
                        "\t1970-01-01T00:00:01.000000Z\tno\n");
    }

    @Override
    public void testNullGeoHash() throws Exception {
        assertGeoHash(1,
                "tracking geohash=\"\" 1000000000\n",
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n");
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
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

    @Override
    public void testWrongCharGeoHashes() throws Exception {
        assertGeoHash(4,
                "tracking geohash=\"9@tralala\" 1000000000\n" +
                        "tracking geohash=\"4-12\" 2000000000\n" +
                        "tracking geohash=\"\",john=\"4-12\",activity=\"lion taming\" 2000000000\n" +
                        "tracking john=\"4-12\",geohash=\"\",activity=\"lion taming\" 2000000000\n" +
                        "tracking geohash=\"jurl\" 3000000000\n",
                "geohash\ttimestamp\tjohn\tactivity\n" +
                        "\t1970-01-01T00:00:01.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4-12\tlion taming\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4-12\tlion taming\n" +
                        "\t1970-01-01T00:00:03.000000Z\t\t\n",
                "john", "activity");
    }
}
