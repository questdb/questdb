/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionEncoderTest extends AbstractCairoTest {
    private final static Log LOG = LogFactory.getLog(PartitionEncoderTest.class);
    private final TestTimestampType timestampType;

    public PartitionEncoderTest() {
        this.timestampType = TestUtils.getTimestampType();
    }

    @Test
    public void testBadCompression() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " from long_sequence(10)) timestamp(designated_ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                try {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encodeWithOptions(partitionDescriptor, path, 42, false, false, 0, 0, ParquetVersion.PARQUET_VERSION_V1, 0.0);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "unsupported compression codec id: 42");
                }
            }
        });
    }

    @Test
    public void testBadVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
                    " from long_sequence(10)) timestamp(designated_ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                try {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encodeWithOptions(partitionDescriptor, path, ParquetCompression.COMPRESSION_UNCOMPRESSED, false, false, 0, 0, 42, 0.0);
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "unsupported parquet version 42");
                }
            }
        });
    }

    @Test
    public void testAllNullsWithNonDefaultEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_varchar VARCHAR PARQUET ENCODING RLE_DICTIONARY," +
                    " a_long LONG PARQUET ENCODING RLE_DICTIONARY," +
                    " a_int INT PARQUET ENCODING RLE_DICTIONARY," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " NULL," +
                    " NULL," +
                    " NULL," +
                    " timestamp_sequence('2015-01-01', 1000000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT * FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testDeltaBinaryPackedEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_long LONG PARQUET ENCODING DELTA_BINARY_PACKED," +
                    " a_date DATE PARQUET ENCODING DELTA_BINARY_PACKED," +
                    " a_ts TIMESTAMP PARQUET ENCODING DELTA_BINARY_PACKED," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0) ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_timestamp('2015', '2016', 0) ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1000000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT * FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testDeltaLengthByteArrayEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_string STRING PARQUET ENCODING DELTA_LENGTH_BYTE_ARRAY," +
                    " a_varchar VARCHAR PARQUET ENCODING DELTA_LENGTH_BYTE_ARRAY," +
                    " a_bin BINARY PARQUET ENCODING DELTA_LENGTH_BYTE_ARRAY," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_str('hello', 'world', '!') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_bin(10, 20, 2) ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1000000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT * FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testMixedEncodingsAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_date DATE PARQUET ENCODING DELTA_BINARY_PACKED COMPRESSION ZSTD 3," +
                    " a_symbol SYMBOL," +
                    " a_string STRING PARQUET ENCODING DELTA_LENGTH_BYTE_ARRAY COMPRESSION GZIP 5," +
                    " a_long LONG PARQUET ENCODING DELTA_BINARY_PACKED COMPRESSION SNAPPY," +
                    " a_varchar VARCHAR PARQUET ENCODING RLE_DICTIONARY COMPRESSION ZSTD," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");
            execute("ALTER TABLE x ALTER COLUMN a_symbol SET PARQUET ENCODING RLE_DICTIONARY COMPRESSION LZ4_RAW");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0) ELSE NULL END," +
                    " rnd_symbol('a', 'b', 'c', NULL)," +
                    " CASE WHEN x % 2 = 0 THEN rnd_str('hello', 'world', '!') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1000000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT a_date, a_symbol::VARCHAR AS a_symbol, a_string, a_long, a_varchar, ts FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testRleDictionaryEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_symbol SYMBOL," +
                    " a_varchar VARCHAR PARQUET ENCODING RLE_DICTIONARY," +
                    " a_double DOUBLE PARQUET ENCODING RLE_DICTIONARY," +
                    " a_long LONG PARQUET ENCODING RLE_DICTIONARY," +
                    " an_int INT PARQUET ENCODING RLE_DICTIONARY," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");
            execute("ALTER TABLE x ALTER COLUMN a_symbol SET PARQUET ENCODING RLE_DICTIONARY");

            execute("INSERT INTO x SELECT" +
                    " rnd_symbol('a', 'b', 'c', NULL)," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_double() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_int() ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1000000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                assertSqlCursors(
                        "SELECT a_symbol::VARCHAR AS a_symbol, a_varchar, a_double, a_long, an_int, ts FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1000000;
            execute("create table x as (select" +
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
                    " timestamp_sequence(500000000000, 600)::" + timestampType.getTypeName() + " a_ts," +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
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
            execute("create table x as (select" +
                    " cast('7c0bd97b-0593-47d2-be17-b8f3f89ca555' as uuid), " +
                    " timestamp_sequence(400000000000, 500)::" + timestampType.getTypeName() + " designated_ts" +
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
