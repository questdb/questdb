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

public class LineUdpInsertLongGeoHashTest extends LineUdpInsertGeoHashTest {
    @Override
    public void testExcessivelyLongGeoHashesAreTruncated() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(57),
                "geohash\ttimestamp\n" +
                        "010011101100001110000100010000100110011111100101011001011\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, "9v1s8hm7wpkssv1h").$(1_000_000_000)
        );
    }

    @Override
    public void testGeoHashes() throws Exception {
        assertGeoHash(60, 12, 7,
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
        assertGeoHash(60, 11, 7,
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
        assertGeoHash(57, 12, 7,
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
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(57),
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
                ColumnType.getGeoHashTypeWithBits(57),
                "geohash\ttimestamp\tcarrots\tonions\n" +
                        "\t1970-01-01T00:00:01.000000Z\t9\t\n" +
                        "\t1970-01-01T00:00:02.000000Z\t4\t\n" +
                        "\t1970-01-01T00:00:03.000000Z\tj\t\n" +
                        "\t1970-01-01T00:00:05.000000Z\t\they\n",
                sender -> {
                    sender.metric(tableName).field("carrots", "9").$(1000000000L);
                    sender.metric(tableName).field("carrots", "4").$(2000000000L);
                    sender.metric(tableName).field("carrots", "j").$(3000000000L);
                    sender.metric(tableName).field("onions", "hey").$(5000000000L);
                    sender.metric(tableName).field("carrots", "k").$(4000000000L);
                },
                "carrots", "onions"
        );
    }

    @Override
    public void testWrongCharGeoHashes() throws Exception {
        assertType(tableName,
                targetColumnName,
                ColumnType.getGeoHashTypeWithBits(58),
                "geohash\ttimestamp\n" +
                        "\t1970-01-01T00:00:01.000000Z\n",
                sender -> sender.metric(tableName).field(targetColumnName, "sp018sp0!18*").$(1_000_000_000)
        );
    }
}