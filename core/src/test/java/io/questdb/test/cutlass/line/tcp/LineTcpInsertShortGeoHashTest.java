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

public class LineTcpInsertShortGeoHashTest extends BaseLineTcpInsertGeoHashTest {

    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"9v1s8hm7wpkssv1h\" 1000000000\n",
                """
                        geohash\ttimestamp
                        9v1\t1970-01-01T00:00:01.000000Z
                        """);
    }

    @Override
    public void testGeoHashes() throws Exception {
        assertGeoHash(15,
                """
                        tracking geohash="9v1" 1000000000
                        tracking geohash="46s" 2000000000
                        tracking geohash="jnw" 3000000000
                        tracking geohash="zfu" 4000000000
                        tracking geohash="hp4" 5000000000
                        tracking geohash="wh4" 6000000000
                        tracking geohash="s2z" 7000000000
                        """,
                """
                        geohash\ttimestamp
                        9v1\t1970-01-01T00:00:01.000000Z
                        46s\t1970-01-01T00:00:02.000000Z
                        jnw\t1970-01-01T00:00:03.000000Z
                        zfu\t1970-01-01T00:00:04.000000Z
                        hp4\t1970-01-01T00:00:05.000000Z
                        wh4\t1970-01-01T00:00:06.000000Z
                        s2z\t1970-01-01T00:00:07.000000Z
                        """);
    }

    @Override
    public void testGeoHashesNotEnoughPrecision() throws Exception {
        assertGeoHash(15,
                """
                        tracking geohash="9v" 1000000000
                        tracking geohash="46" 2000000000
                        tracking geohash="jn" 3000000000
                        tracking geohash="zf" 4000000000
                        tracking geohash="hp",name="questdb" 5000000000
                        tracking geohash="wh" 6000000000
                        tracking geohash="s2",name="neptune" 7000000000
                        tracking geohash="1c" 8000000000
                        tracking geohash="mm" 9000000000
                        tracking name="timeseries",geohash="71" 10000000000
                        """,
                """
                        geohash\ttimestamp\tname
                        \t1970-01-01T00:00:01.000000Z\t
                        \t1970-01-01T00:00:02.000000Z\t
                        \t1970-01-01T00:00:03.000000Z\t
                        \t1970-01-01T00:00:04.000000Z\t
                        \t1970-01-01T00:00:05.000000Z\tquestdb
                        \t1970-01-01T00:00:06.000000Z\t
                        \t1970-01-01T00:00:07.000000Z\tneptune
                        \t1970-01-01T00:00:08.000000Z\t
                        \t1970-01-01T00:00:09.000000Z\t
                        \t1970-01-01T00:00:10.000000Z\ttimeseries
                        """,
                "name");
    }

    @Override
    public void testGeoHashesTruncating() throws Exception {
        assertGeoHash(13,
                """
                        tracking geohash="9v1" 1000000000
                        tracking geohash="46s" 2000000000
                        tracking geohash="jnw" 3000000000
                        tracking geohash="zfu" 4000000000
                        tracking geohash="hp4" 5000000000
                        tracking geohash="wh4" 6000000000
                        tracking geohash="s2z" 7000000000
                        tracking geohash="1cj" 8000000000
                        tracking geohash="mmt" 9000000000
                        tracking geohash="71f" 10000000000
                        """,
                """
                        geohash\ttimestamp
                        0100111011000\t1970-01-01T00:00:01.000000Z
                        0010000110110\t1970-01-01T00:00:02.000000Z
                        1000110100111\t1970-01-01T00:00:03.000000Z
                        1111101110110\t1970-01-01T00:00:04.000000Z
                        1000010101001\t1970-01-01T00:00:05.000000Z
                        1110010000001\t1970-01-01T00:00:06.000000Z
                        1100000010111\t1970-01-01T00:00:07.000000Z
                        0000101011100\t1970-01-01T00:00:08.000000Z
                        1001110011110\t1970-01-01T00:00:09.000000Z
                        0011100001011\t1970-01-01T00:00:10.000000Z
                        """);
    }

    @Test
    public void testNullGeoHash() throws Exception {
        assertGeoHash(15,
                "tracking geohash=\"\" 1000000000\n",
                """
                        geohash\ttimestamp
                        \t1970-01-01T00:00:01.000000Z
                        """);
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
        assertGeoHash(15,
                """
                        tracking onions="9v1" 1000000000
                        tracking onions="46s" 2000000000
                        tracking onions="jnw" 3000000000
                        tracking geohash="zfu" 4000000000
                        tracking geohash="hp4" 5000000000
                        tracking onions="wh4" 6000000000
                        tracking mint="s2z" 7000000000
                        """,
                """
                        geohash\ttimestamp\tonions\tmint
                        \t1970-01-01T00:00:01.000000Z\t9v1\t
                        \t1970-01-01T00:00:02.000000Z\t46s\t
                        \t1970-01-01T00:00:03.000000Z\tjnw\t
                        zfu\t1970-01-01T00:00:04.000000Z\t\t
                        hp4\t1970-01-01T00:00:05.000000Z\t\t
                        \t1970-01-01T00:00:06.000000Z\twh4\t
                        \t1970-01-01T00:00:07.000000Z\t\ts2z
                        """,
                "onions", "mint");
    }

    @Override
    public void testWrongCharGeoHashes() throws Exception {
        assertGeoHash(13,
                """
                        tracking geohash="9v1@" 1000000000
                        tracking geohash="@46s" 2000000000
                        tracking geohash="jLnw" 3000000000
                        """,
                """
                        geohash\ttimestamp
                        \t1970-01-01T00:00:01.000000Z
                        \t1970-01-01T00:00:02.000000Z
                        \t1970-01-01T00:00:03.000000Z
                        """);
    }
}
