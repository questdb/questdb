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
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.ParquetTestUtils;
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
                    "a_varchar VARCHAR PARQUET(RLE_DICTIONARY)," +
                    " a_long LONG PARQUET(RLE_DICTIONARY)," +
                    " a_int INT PARQUET(RLE_DICTIONARY)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " NULL," +
                    " NULL," +
                    " NULL," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
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
                    "a_long LONG PARQUET(DELTA_BINARY_PACKED)," +
                    " a_date DATE PARQUET(DELTA_BINARY_PACKED)," +
                    " a_ts TIMESTAMP PARQUET(DELTA_BINARY_PACKED)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0) ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_timestamp('2015', '2016', 0) ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
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
                ParquetTestUtils.assertColumnsUseEncoding(
                        path.toString(),
                        configuration.getFilesFacade(),
                        ParquetEncoding.ENCODING_DELTA_BINARY_PACKED,
                        0, 1, 2 // a_long, a_date, a_ts
                );
            }
        });
    }

    @Test
    public void testDeltaLengthByteArrayEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_string STRING PARQUET(DELTA_LENGTH_BYTE_ARRAY)," +
                    " a_varchar VARCHAR PARQUET(DELTA_LENGTH_BYTE_ARRAY)," +
                    " a_bin BINARY PARQUET(DELTA_LENGTH_BYTE_ARRAY)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_str('hello', 'world', '!') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_bin(10, 20, 2) ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
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
                ParquetTestUtils.assertColumnsUseEncoding(
                        path.toString(),
                        configuration.getFilesFacade(),
                        ParquetEncoding.ENCODING_DELTA_LENGTH_BYTE_ARRAY,
                        0, 1, 2 // a_string, a_varchar, a_bin
                );
            }
        });
    }

    @Test
    public void testSymbolCompressionOnly() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            // Per-column ZSTD on symbol column with enough distinct symbols to trigger auto-scaling
            execute("CREATE TABLE x (" +
                    "a_symbol SYMBOL PARQUET(default, ZSTD)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            // Baseline: same data, no per-column compression
            execute("CREATE TABLE y (a_symbol SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " rnd_symbol(10_000, 10, 40, 0)," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(100_000)");

            execute("INSERT INTO y SELECT a_symbol, ts FROM x");

            // Verify parquetEncodingConfig survives symbol auto-scaling
            try (TableReader reader = engine.getReader("x")) {
                int config = reader.getMetadata().getColumnMetadata(0).getParquetEncodingConfig();
                Assert.assertNotEquals(
                        "parquetEncodingConfig should survive symbol auto-scaling (was 0x" + Integer.toHexString(config) + ")",
                        0, config);
            }

            // Encode both tables to parquet and compare file sizes
            long compressedSize;
            long uncompressedSize;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                compressedSize = Files.length(path.$());
            }

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("y")
            ) {
                path.of(root).concat("y.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                uncompressedSize = Files.length(path.$());
            }

            LOG.info().$("per-column ZSTD: ").$(compressedSize)
                    .$(", uncompressed: ").$(uncompressedSize).$();
            Assert.assertTrue(
                    "per-column ZSTD (" + compressedSize + ") should be smaller than uncompressed (" + uncompressedSize + ")",
                    compressedSize < uncompressedSize
            );
        });
    }

    @Test
    public void testMixedEncodingsAndCompression() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_date DATE PARQUET(DELTA_BINARY_PACKED, ZSTD(3))," +
                    " a_symbol SYMBOL," +
                    " a_string STRING PARQUET(DELTA_LENGTH_BYTE_ARRAY, GZIP(5))," +
                    " a_long LONG PARQUET(DELTA_BINARY_PACKED, SNAPPY)," +
                    " a_varchar VARCHAR PARQUET(RLE_DICTIONARY, ZSTD)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");
            execute("ALTER TABLE x ALTER COLUMN a_symbol SET PARQUET(RLE_DICTIONARY, LZ4_RAW)");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 0) ELSE NULL END," +
                    " rnd_symbol('a', 'b', 'c', NULL)," +
                    " CASE WHEN x % 2 = 0 THEN rnd_str('hello', 'world', '!') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
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
    public void testMinCompressionRatioDisabled() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_long LONG," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " rnd_long()," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(10_000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_SNAPPY,
                        true,
                        false,
                        0,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0 // disabled
                );
                assertSqlCursors(
                        "SELECT a_long, ts FROM x",
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testMinCompressionRatioForcesUncompressed() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_long LONG," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " rnd_long()," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(10_000)");

            long compressedSize;
            long fallbackSize;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                // File 1: Snappy + ratio=0.0 (disabled, stays compressed)
                path.of(root).concat("compressed.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_SNAPPY,
                        true,
                        false,
                        0,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0
                );
                compressedSize = Files.length(path.$());

                // File 2: Snappy + ratio=100.0 (forces fallback to uncompressed)
                path.of(root).concat("fallback.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_SNAPPY,
                        true,
                        false,
                        0,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        100.0
                );
                fallbackSize = Files.length(path.$());
            }

            LOG.info().$("compressed: ").$(compressedSize)
                    .$(", fallback: ").$(fallbackSize).$();
            Assert.assertTrue(
                    "fallback file (" + fallbackSize + ") should be >= compressed file (" + compressedSize + ")",
                    fallbackSize >= compressedSize
            );

            // Verify both files produce identical data
            inputRoot = root;
            assertSqlCursors(
                    "SELECT * FROM read_parquet('compressed.parquet')",
                    "SELECT * FROM read_parquet('fallback.parquet')"
            );
        });
    }

    @Test
    public void testRleDictionaryEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    "a_symbol SYMBOL," +
                    " a_varchar VARCHAR PARQUET(RLE_DICTIONARY)," +
                    " a_double DOUBLE PARQUET(RLE_DICTIONARY)," +
                    " a_long LONG PARQUET(RLE_DICTIONARY)," +
                    " an_int INT PARQUET(RLE_DICTIONARY)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");
            execute("ALTER TABLE x ALTER COLUMN a_symbol SET PARQUET(RLE_DICTIONARY)");

            execute("INSERT INTO x SELECT" +
                    " rnd_symbol('a', 'b', 'c', NULL)," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('ганьба', 'слава', 'добрий') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_double() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_long() ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_int() ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
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
                ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                        path.toString(),
                        configuration.getFilesFacade(),
                        0, 1, 2, 3, 4 // a_symbol, a_varchar, a_double, a_long, an_int
                );
            }
        });
    }

    @Test
    public void testPlainOverrideReplacesDefault() throws Exception {
        // STRING/BINARY columns default to DELTA_LENGTH_BYTE_ARRAY; a PARQUET(PLAIN)
        // override must replace that with PLAIN.
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE x (" +
                    " a_string STRING PARQUET(PLAIN)," +
                    " a_binary BINARY PARQUET(PLAIN)," +
                    " ts TIMESTAMP" +
                    ") TIMESTAMP(ts) PARTITION BY MONTH");

            execute("INSERT INTO x SELECT" +
                    " CASE WHEN x % 2 = 0 THEN rnd_str('alpha', 'bravo', 'charlie') ELSE NULL END," +
                    " CASE WHEN x % 2 = 0 THEN rnd_bin(10, 20, 2) ELSE NULL END," +
                    " timestamp_sequence('2015-01-01', 1_000_000)" +
                    " FROM long_sequence(1000)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                ParquetTestUtils.assertColumnsDoNotUseEncoding(
                        path.toString(),
                        configuration.getFilesFacade(),
                        ParquetEncoding.ENCODING_DELTA_LENGTH_BYTE_ARRAY,
                        0, 1 // a_string, a_binary
                );
                ParquetTestUtils.assertColumnsUseEncoding(
                        path.toString(),
                        configuration.getFilesFacade(),
                        ParquetEncoding.ENCODING_PLAIN,
                        0, 1 // a_string, a_binary
                );
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1_000_000;
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
