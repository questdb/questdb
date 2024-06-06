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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionDecoderTest extends AbstractCairoTest {

    @Test
    public void testDecodeColumnChunk() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1001;
            ddl("create table x as (select" +
                    " x::int an_int," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            try (
                    Path path = new Path();
                    PartitionEncoder partitionEncoder = new PartitionEncoder();
                    PartitionDecoder partitionDecoder = new PartitionDecoder();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                partitionEncoder.encode(reader, 0, path);

                partitionDecoder.of(path);
                try (
                        PartitionDecoder.ColumnChunkBuffers buffers = partitionDecoder.decodeColumnChunk(
                                0,
                                0,
                                ColumnType.INT,
                                0,
                                0,
                                0,
                                0
                        )
                ) {
                    Assert.assertTrue(buffers.dataPtr() != 0);
                    Assert.assertEquals(rows * Integer.BYTES, buffers.dataPos());
                    Assert.assertTrue(buffers.dataSize() >= buffers.dataPos());
                    Assert.assertEquals(0, buffers.auxPtr());
                    Assert.assertEquals(0, buffers.auxPos());
                    Assert.assertTrue(buffers.auxSize() >= buffers.auxPos());

                    for (int i = 0; i < rows; i++) {
                        Assert.assertEquals(i + 1, Unsafe.getUnsafe().getInt(buffers.dataPtr() + (long) Integer.BYTES * i));
                    }

                    Unsafe.getUnsafe().freeMemory(buffers.dataPtr());
                }
            }
        });
    }

    @Test
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final long columns = 24;
            final long rows = 1001;
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
                    PartitionEncoder partitionEncoder = new PartitionEncoder();
                    PartitionDecoder partitionDecoder = new PartitionDecoder();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                partitionEncoder.encode(reader, 0, path);

                partitionDecoder.of(path);
                Assert.assertEquals(24, partitionDecoder.metadata().columnCount());
                Assert.assertEquals(rows, partitionDecoder.metadata().rowCount());
                Assert.assertEquals(1, partitionDecoder.metadata().rowGroupCount());

                final String[] expectedColumnNames = new String[]{
                        "id", "a_boolean", "a_byte", "a_short", "a_char", "an_int", "a_long", "a_float", "a_double", "a_symbol",
                        "a_geo_byte", "a_geo_short", "a_geo_int", "a_geo_long", "a_string", "a_bin", "a_varchar", "a_ip", "a_uuid", "a_long256",
                        "a_long128", "a_date", "a_ts", "designated_ts",
                };
                final long[] expectedPhysicalTypes = new long[]{
                        PartitionDecoder.INT64_PHYSICAL_TYPE, PartitionDecoder.BOOLEAN_PHYSICAL_TYPE,
                        PartitionDecoder.INT32_PHYSICAL_TYPE, PartitionDecoder.INT32_PHYSICAL_TYPE,
                        PartitionDecoder.INT32_PHYSICAL_TYPE, PartitionDecoder.INT32_PHYSICAL_TYPE,
                        PartitionDecoder.INT64_PHYSICAL_TYPE, PartitionDecoder.FLOAT_PHYSICAL_TYPE,
                        PartitionDecoder.DOUBLE_PHYSICAL_TYPE, PartitionDecoder.BYTE_ARRAY_PHYSICAL_TYPE,
                        PartitionDecoder.INT32_PHYSICAL_TYPE, PartitionDecoder.INT32_PHYSICAL_TYPE,
                        PartitionDecoder.INT32_PHYSICAL_TYPE, PartitionDecoder.INT64_PHYSICAL_TYPE,
                        PartitionDecoder.BYTE_ARRAY_PHYSICAL_TYPE, PartitionDecoder.BYTE_ARRAY_PHYSICAL_TYPE,
                        PartitionDecoder.BYTE_ARRAY_PHYSICAL_TYPE, PartitionDecoder.INT32_PHYSICAL_TYPE,
                        Numbers.encodeLowHighInts(PartitionDecoder.FIXED_LEN_BYTE_ARRAY_PHYSICAL_TYPE, 16), // uuid
                        Numbers.encodeLowHighInts(PartitionDecoder.FIXED_LEN_BYTE_ARRAY_PHYSICAL_TYPE, 32), // long256
                        Numbers.encodeLowHighInts(PartitionDecoder.FIXED_LEN_BYTE_ARRAY_PHYSICAL_TYPE, 16), // long128
                        PartitionDecoder.INT64_PHYSICAL_TYPE, PartitionDecoder.INT64_PHYSICAL_TYPE,
                        PartitionDecoder.INT64_PHYSICAL_TYPE,
                };

                for (int i = 0; i < columns; i++) {
                    TestUtils.assertEquals("column: " + i, expectedColumnNames[i], partitionDecoder.metadata().columnName(i));
                    Assert.assertEquals("column: " + i, i, partitionDecoder.metadata().columnId(i));
                    Assert.assertEquals("column: " + i, expectedPhysicalTypes[i], partitionDecoder.metadata().columnPhysicalType(i));
                }
            }
        });
    }
}
