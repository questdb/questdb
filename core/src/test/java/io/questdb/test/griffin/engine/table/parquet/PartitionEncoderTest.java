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
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QuestDBTestNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.COMPRESSION_UNCOMPRESSED;

public class PartitionEncoderTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(PartitionEncoderTest.class);

    @Test
    public void testBadCompression() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(10)) timestamp(designated_ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                try {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encodeWithOptions(partitionDescriptor, path, 42, false, 0, 0, PartitionEncoder.PARQUET_VERSION_V1);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "Invalid value for CompressionCodec"));
                }
            }
        });
    }

    @Test
    public void testBadVersion() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(10)) timestamp(designated_ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                try {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encodeWithOptions(partitionDescriptor, path, COMPRESSION_UNCOMPRESSED, false, 0, 0, 42);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "Invalid value for Version"));
                }
            }
        });
    }

    @Test
    @Ignore
    public void testEncodeExternal() {
        final String root2 = "/Users/alpel/temp/db";
        final QuestDBTestNode node2 = newNode(2, root2);
        nodes.remove(node2);

        try (
                Path path = new Path();
                PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                TableReader reader = node2.getEngine().getReader("request_logs")
        ) {
            path.of(root2).concat("x.parquet").$();
            long start = System.nanoTime();
            PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
            PartitionEncoder.encode(partitionDescriptor, path);
            LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
        }
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10000000;
            ddl("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " rnd_byte() a_byte," +
                    " rnd_short() a_short," +
                    " rnd_char() a_char," +
                    " rnd_int() an_int," +
                    " rnd_long() a_long," +
                    " rnd_float() a_float," +
                    " rnd_double() a_double," +
                    " rnd_symbol('a','b','c') a_symbol," +
                    " rnd_geohash(4) a_geo_byte," +
                    " rnd_geohash(8) a_geo_short," +
                    " rnd_geohash(16) a_geo_int," +
                    " rnd_geohash(32) a_geo_long," +
                    " rnd_str('hello', 'world', '!') a_string," +
                    " rnd_bin() a_bin," +
                    " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                    " rnd_ipv4() a_ip," +
                    " rnd_uuid4() a_uuid," +
                    " rnd_long256() a_long256," +
                    " to_long128(rnd_long(), rnd_long()) a_long128," +
                    " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                    " timestamp_sequence(500000000000, 600) a_ts," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                long start = System.nanoTime();
                PartitionEncoder.encode(partitionDescriptor, path);
                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
            }
        });
    }

    @Test
    public void testUuid() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1;
            ddl("create table x as (select" +
                    " cast('7c0bd97b-0593-47d2-be17-b8f3f89ca555' as uuid), " +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                long start = System.nanoTime();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
            }
        });
    }
}
