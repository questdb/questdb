/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cairo.ColumnType;

public class LineUdpInsertIntGeoHashTest extends LineUdpInsertGeoHashTest {
    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(20),
                "geohash\ttimestamp\n" +
                        "9v1s\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, "9v1s8hm7wpkssv1h").$(1_000_000_000)
        );
    }

    @Override
    public void testGeoHashes() throws Exception {
        assertGeoHash(30, 6, 8,
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
        assertGeoHash(32, 5, 8,
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:02.000000Z\n" +
                        "\t1970-01-01T00:00:03.000000Z\n" +
                        "\t1970-01-01T00:00:04.000000Z\n" +
                        "\t1970-01-01T00:00:05.000000Z\n" +
                        "\t1970-01-01T00:00:06.000000Z\n" +
                        "\t1970-01-01T00:00:07.000000Z\n" +
                        "\t1970-01-01T00:00:08.000000Z\n");
    }

    @Override
    public void testGeoHashesTruncating() throws Exception {
        assertGeoHash(29, 6, 8,
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
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(30),
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n" +
                        "\t1970-01-01T00:00:01.000000Z\n",
                sender -> {
                    sender.metric(tableName).field(targetColumnName, "").$(1_000_000_000);
                    sender.metric(tableName).field(targetColumnName, "null").$(1_000_000_000);
                }
        );
    }

    @Override
    public void testTableHasGeoHashMessageDoesNot() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(28),
                "geohash\ttimestamp\tcarrots\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4\n" +
                        "\t1970-01-01T00:00:03.000000Z\tj\n",
                sender -> {
                    sender.metric(tableName).field("carrots", "9").$(1000000000L);
                    sender.metric(tableName).field("carrots", "4").$(2000000000L);
                    sender.metric(tableName).field("carrots", "j").$(3000000000L);
                },
                "carrots"
        );
    }

    @Override
    public void testWrongCharGeoHashes() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(31),
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, "sp018*").$(1_000_000_000)
        );
    }
}
