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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QuestDBTestNode;
import org.junit.Ignore;
import org.junit.Test;

public class PartitionEncoderTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(PartitionEncoderTest.class);

    @Test
    @Ignore
    public void testEncodeExternal() {
        final String root2 = "/Users/alpel/temp/db";
        final QuestDBTestNode node2 = newNode(2, root2);
        nodes.remove(node2);

        try (
                Path path = new Path();
                PartitionEncoder partitionEncoder = new PartitionEncoder();
                TableReader reader = node2.getEngine().getReader("trades")
        ) {
            path.of(root2).concat("x.parquet").$();
            long start = System.nanoTime();
            partitionEncoder.encode(reader, 0, path);
            LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
        }
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 9;
            ddl("create table x as (select" +
                    " x id," +
//                    " rnd_boolean() a_boolean," +
//                    " rnd_byte() a_byte," +
//                    " rnd_short() a_short," +
//                    " rnd_char() a_char," +
//                    " rnd_int() an_int," +
//                    " rnd_long() a_long," +
//                    " rnd_float() a_float," +
//                    " rnd_double() a_double," +
//                    " rnd_symbol('a','b','c') a_symbol," +
//                    " rnd_geohash(4) a_geo_byte," +
//                    " rnd_geohash(8) a_geo_short," +
//                    " rnd_geohash(16) a_geo_int," +
//                    " rnd_geohash(32) a_geo_long," +
                    " rnd_str(null, 'hello', 'world', '!') a_string," +
//                    " rnd_bin(10, 20, 3) a_bin," +
//                    " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
//                    " rnd_ipv4('22.43.200.9/16', 2) a_ip," +
//                    " rnd_uuid4() a_uuid," +
//                    " rnd_long256() a_long256," +
//                    " to_long128(rnd_long(1, 13, 0), rnd_long(14, 26, 0)) a_long128," +
//                    " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
//                    " timestamp_sequence(500000000000, 600) a_ts," +
//                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + "))");
//                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");
//            assertQuery("a_ip\n" +
//                            "22.43.96.238\n" +
//                            "\n" +
//                            "22.43.173.21\n" +
//                            "22.43.250.138\n" +
//                            "\n" +
//                            "22.43.76.40\n" +
//                            "22.43.20.236\n" +
//                            "22.43.95.15\n" +
//                            "\n",
//                    "select * from x", null, null, true, true);

            try (
                    Path path = new Path();
                    PartitionEncoder partitionEncoder = new PartitionEncoder();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of("/Users/mtopol/Desktop").concat("x.parquet").$();
                long start = System.nanoTime();
                partitionEncoder.encode(reader, 0, path);
                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
            }
        });
    }

    @Test
    public void testWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x (ts timestamp) timestamp(ts) partition by hour");
            insert("insert into x values (0), (1000000)");
            ddl("alter table x add column a_boolean boolean");
            ddl("alter table x add column an_int int");
            ddl("alter table x add column a_long long");
            ddl("alter table x add column a_float float");
            ddl("alter table x add column a_double double");
            ddl("alter table x add column a_varchar varchar");
            ddl("alter table x add column a_str string");
            ddl("alter table x add column a_symbol symbol");
//            ddl("alter table x add column a_bin binary");
            insert("insert into x select " +
                    "(3597000000 + 1000000 * x)::timestamp, " +
                    "rnd_boolean(), " +
                    "rnd_int(10, 20, 1), " +
                    "rnd_long(50, 60, 1), " +
                    "rnd_float(1), " +
                    "rnd_double(1), " +
                    "rnd_varchar('x', 'y'), " +
                    "rnd_str('a', 'b'), " +
                    "rnd_symbol('i', 'j'), " +
//                    "rnd_bin() " +
                    "from long_sequence(7)");
//            assertQuery("ts\ta_varchar\ta_str\ta_symbol\n" +
//                            "1970-01-01T00:00:00.000000Z\t\t\t\n" +
//                            "1970-01-01T00:00:01.000000Z\t\t\t\n" +
//                            "1970-01-01T00:59:58.000000Z\tx\ta\tj\n" +
//                            "1970-01-01T00:59:59.000000Z\ty\tb\tj\n" +
//                            "1970-01-01T01:00:00.000000Z\tx\tb\ti\n" +
//                            "1970-01-01T01:00:01.000000Z\tx\ta\ti\n" +
//                            "1970-01-01T01:00:02.000000Z\tx\tb\tj\n" +
//                            "1970-01-01T01:00:03.000000Z\tx\ta\tj\n" +
//                            "1970-01-01T01:00:04.000000Z\ty\ta\tj\n",
//                    "select * from x", null, "ts", true, true);
            try (
                    Path path = new Path();
                    PartitionEncoder partitionEncoder = new PartitionEncoder();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of("/Users/mtopol/Desktop").concat("x0.parquet").$();
                long start = System.nanoTime();
                partitionEncoder.encode(reader, 0, path);
                path.of("/Users/mtopol/Desktop").concat("x1.parquet").$();
                partitionEncoder.encode(reader, 1, path);
                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
            }
        });
    }

    private static void printParsedBytes(String hexBytes) {
        System.out.println("\n");
        for (int i = hexBytes.length(); i > 0; i -= 2) {
            byte b = (byte) Integer.parseInt(hexBytes.substring(i - 2, i), 16);
            System.out.print(b + " ");
        }
        System.out.println("\n");
    }
}
