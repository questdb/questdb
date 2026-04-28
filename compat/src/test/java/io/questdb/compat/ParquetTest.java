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

package io.questdb.compat;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.DirectIntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static io.questdb.griffin.engine.table.parquet.ParquetCompression.packCompressionCodecLevel;

public class ParquetTest extends AbstractTest {
    private final static long DATA_PAGE_SIZE = 128; // bytes
    private final static Log LOG = LogFactory.getLog(ParquetTest.class);
    private final static int NUMERIC_MAX = 10;
    private final static int NUMERIC_MIN = -10;
    private final static long ROW_GROUP_SIZE = 64;
    private final static long INITIAL_ROWS = ROW_GROUP_SIZE * 2;
    private final static long UPDATE_ROWS = ROW_GROUP_SIZE * 4;

    @Test
    public void test1dArrayV1() throws Exception {
        test1dArray(ParquetVersion.PARQUET_VERSION_V1);
    }

    @Test
    public void test1dArrayV2() throws Exception {
        test1dArray(ParquetVersion.PARQUET_VERSION_V2);
    }

    @Test
    public void test2dArrayV1() throws Exception {
        test2dArray(ParquetVersion.PARQUET_VERSION_V1);
    }

    @Test
    public void test2dArrayV2() throws Exception {
        test2dArray(ParquetVersion.PARQUET_VERSION_V2);
    }

    @Test
    public void testAllTypesColTopMiddlePartition() throws Exception {
        final String tableName = "y";
        final int partitionBy = PartitionBy.MONTH;
        // column tops placed in the middle of the partition.
        testPartitionDataConsistency(tableName, partitionBy, "timestamp", false); // false
    }

    @Test
    public void testAllTypesColTopMiddlePartition_rawArrayEncoding() throws Exception {
        // column tops placed in the middle of the partition
        testPartitionDataConsistency("y", PartitionBy.MONTH, "timestamp", true);
    }

    @Test
    public void testAllTypesColTopNextPartition() throws Exception {
        final String tableName = "x";
        final int partitionBy = PartitionBy.DAY;
        // column tops added to the next partition.
        testPartitionDataConsistency(tableName, partitionBy, "timestamp_ns", false);
    }

    @Test
    public void testAllTypesColTopNextPartition_rawArrayEncoding() throws Exception {
        // column tops added to the next partition
        testPartitionDataConsistency("x", PartitionBy.DAY, "timestamp_ns", true);
    }

    @Test
    public void testBloomFilterAllTypes() throws Exception {
        int colCount = 55; // 28 original (SIMD path) + 27 column-top (non-SIMD path)
        String ddl = "CREATE TABLE bloom_test AS (SELECT" +
                " x::byte a_byte," +           // col 0
                " x::short a_short," +          // col 1
                " x::char a_char," +            // col 2
                " (x * 100)::int an_int," +     // col 3
                " x::long a_long," +            // col 4
                " cast(x * 1.5 AS FLOAT) a_float," + // col 5
                " x * 2.5 a_double," +          // col 6
                " 'hello_' || x a_string," +    // col 7
                " cast('sym_' || x AS SYMBOL) a_symbol," + // col 8
                " cast('вітаю_' || x AS VARCHAR) a_varchar," + // col 9
                " rnd_bin(1, 8, 0) a_bin," +    // col 10
                " cast(x * 1000000 AS TIMESTAMP) a_ts," + // col 11
                " cast(x * 86400000 AS DATE) a_date," +   // col 12
                " cast('192.168.1.' || (x % 256) AS IPv4) a_ip," + // col 13
                " rnd_geohash(4) a_geo_byte," +   // col 14
                " rnd_geohash(8) a_geo_short," +  // col 15
                " rnd_geohash(16) a_geo_int," +   // col 16
                " rnd_geohash(32) a_geo_long," +  // col 17
                " rnd_uuid4() a_uuid," +           // col 18
                " to_long128(x, x + 1000) a_long128," + // col 19
                " rnd_long256() a_long256," +      // col 20
                " rnd_decimal(2, 1, 0) a_decimal8," +   // col 21
                " rnd_decimal(4, 2, 0) a_decimal16," +  // col 22
                " rnd_decimal(9, 4, 0) a_decimal32," +  // col 23
                " rnd_decimal(18, 6, 0) a_decimal64," + // col 24
                " rnd_decimal(38, 10, 0) a_decimal128," + // col 25
                " rnd_decimal(76, 20, 0) a_decimal256," + // col 26
                " timestamp_sequence(400000000000, 1000000) designated_ts" + // col 27
                " FROM long_sequence(200)) TIMESTAMP(designated_ts) PARTITION BY DAY";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);

            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_byte_top BYTE");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_short_top SHORT");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_char_top CHAR");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN an_int_top INT");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_long_top LONG");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_float_top FLOAT");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_double_top DOUBLE");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_string_top STRING");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_symbol_top SYMBOL");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_varchar_top VARCHAR");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_bin_top BINARY");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_ts_top TIMESTAMP");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_date_top DATE");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_ip_top IPv4");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_geo_byte_top GEOHASH(4b)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_geo_short_top GEOHASH(8b)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_geo_int_top GEOHASH(16b)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_geo_long_top GEOHASH(32b)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_uuid_top UUID");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_long128_top LONG128");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_long256_top LONG256");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal8_top DECIMAL(2,1)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal16_top DECIMAL(4,2)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal32_top DECIMAL(9,4)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal64_top DECIMAL(18,6)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal128_top DECIMAL(38,10)");
            serverMain.getEngine().execute("ALTER TABLE bloom_test ADD COLUMN a_decimal256_top DECIMAL(76,20)");
            serverMain.getEngine().execute(
                    "INSERT INTO bloom_test(" +
                            "a_byte, a_short, a_char, an_int, a_long," +
                            " a_float, a_double," +
                            " a_string, a_symbol, a_varchar, a_bin," +
                            " a_ts, a_date, a_ip," +
                            " a_geo_byte, a_geo_short, a_geo_int, a_geo_long," +
                            " a_uuid, a_long128, a_long256," +
                            " a_decimal8, a_decimal16, a_decimal32," +
                            " a_decimal64, a_decimal128, a_decimal256," +
                            " a_byte_top, a_short_top, a_char_top, an_int_top, a_long_top," +
                            " a_float_top, a_double_top," +
                            " a_string_top, a_symbol_top, a_varchar_top, a_bin_top," +
                            " a_ts_top, a_date_top, a_ip_top," +
                            " a_geo_byte_top, a_geo_short_top, a_geo_int_top, a_geo_long_top," +
                            " a_uuid_top, a_long128_top, a_long256_top," +
                            " a_decimal8_top, a_decimal16_top, a_decimal32_top," +
                            " a_decimal64_top, a_decimal128_top, a_decimal256_top," +
                            " designated_ts" +
                            ") SELECT" +
                            " x::byte, x::short, x::char, (x * 100)::int, x::long," +
                            " cast(x * 1.5 AS FLOAT), x * 2.5," +
                            " 'hello_' || x, cast('sym_' || x AS SYMBOL)," +
                            " cast('вітаю_' || x AS VARCHAR), rnd_bin(1, 8, 0)," +
                            " cast(x * 1000000 AS TIMESTAMP), cast(x * 86400000 AS DATE)," +
                            " cast('192.168.1.' || (x % 256) AS IPv4)," +
                            " rnd_geohash(4), rnd_geohash(8), rnd_geohash(16), rnd_geohash(32)," +
                            " rnd_uuid4(), to_long128(x, x + 1000), rnd_long256()," +
                            " rnd_decimal(2, 1, 0), rnd_decimal(4, 2, 0), rnd_decimal(9, 4, 0)," +
                            " rnd_decimal(18, 6, 0), rnd_decimal(38, 10, 0), rnd_decimal(76, 20, 0)," +
                            " x::byte, x::short, x::char, (x * 100)::int, x::long," +
                            " cast(x * 1.5 AS FLOAT), x * 2.5," +
                            " 'hello_' || x, cast('sym_' || x AS SYMBOL)," +
                            " cast('вітаю_' || x AS VARCHAR), rnd_bin(1, 8, 0)," +
                            " cast(x * 1000000 AS TIMESTAMP), cast(x * 86400000 AS DATE)," +
                            " cast('192.168.1.' || (x % 256) AS IPv4)," +
                            " rnd_geohash(4), rnd_geohash(8), rnd_geohash(16), rnd_geohash(32)," +
                            " rnd_uuid4(), to_long128(x, x + 1000), rnd_long256()," +
                            " rnd_decimal(2, 1, 0), rnd_decimal(4, 2, 0), rnd_decimal(9, 4, 0)," +
                            " rnd_decimal(18, 6, 0), rnd_decimal(38, 10, 0), rnd_decimal(76, 20, 0)," +
                            " timestamp_sequence(400200000000, 1000000)" +
                            " FROM long_sequence(200)"
            );

            serverMain.awaitTable("bloom_test");
            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    DirectIntList bloomFilterIndexes = new DirectIntList(colCount, MemoryTag.NATIVE_DEFAULT);
                    TableReader reader = serverMain.getEngine().getReader("bloom_test")
            ) {
                path.of(root).concat("bloom_test.parquet").$();
                parquetPathStr = path.toString();
                for (int i = 0; i < colCount; i++) {
                    bloomFilterIndexes.add(i);
                }

                int partitionIndex = 0;
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        ROW_GROUP_SIZE * 64,
                        DATA_PAGE_SIZE * 64,
                        ParquetVersion.PARQUET_VERSION_V2,
                        bloomFilterIndexes.getAddress(),
                        (int) bloomFilterIndexes.size(),
                        0.01,
                        0.0,
                        -1L
                );
                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    List<BlockMetaData> rowGroups = metadata.getBlocks();
                    Assert.assertFalse("Expected at least 1 row group", rowGroups.isEmpty());
                    BlockMetaData firstBlock = rowGroups.get(0);
                    List<ColumnChunkMetaData> chunks = firstBlock.getColumns();
                    Assert.assertEquals(colCount, chunks.size());

                    // col 0: a_byte - INT32
                    BloomFilter bf = parquetFileReader.readBloomFilter(chunks.get(0));
                    Assert.assertNotNull("byte", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 1: a_short - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(1));
                    Assert.assertNotNull("short", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 2: a_char - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(2));
                    Assert.assertNotNull("char", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 3: an_int - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(3));
                    Assert.assertNotNull("int", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(100)));

                    // col 4: a_long - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(4));
                    Assert.assertNotNull("long", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1L)));

                    // col 5: a_float - FLOAT
                    bf = parquetFileReader.readBloomFilter(chunks.get(5));
                    Assert.assertNotNull("float", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1.5f)));

                    // col 6: a_double - DOUBLE
                    bf = parquetFileReader.readBloomFilter(chunks.get(6));
                    Assert.assertNotNull("double", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(2.5)));

                    // col 7: a_string - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(7));
                    Assert.assertNotNull("string", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("hello_1"))));

                    // col 8: a_symbol - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(8));
                    Assert.assertNotNull("symbol", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("sym_1"))));

                    // col 9: a_varchar - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(9));
                    Assert.assertNotNull("varchar", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("вітаю_1"))));

                    // col 10: a_bin - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(10));
                    Assert.assertNotNull("binary", bf);

                    // col 11: a_ts - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(11));
                    Assert.assertNotNull("timestamp", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1_000_000L)));

                    // col 12: a_date - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(12));
                    Assert.assertNotNull("date", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(86_400_000L)));

                    // col 13: a_ip - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(13));
                    Assert.assertNotNull("ipv4", bf);

                    // col 14-17: geohash columns
                    bf = parquetFileReader.readBloomFilter(chunks.get(14));
                    Assert.assertNotNull("geo_byte", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(15));
                    Assert.assertNotNull("geo_short", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(16));
                    Assert.assertNotNull("geo_int", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(17));
                    Assert.assertNotNull("geo_long", bf);

                    // col 18: a_uuid - FIXED_LEN_BYTE_ARRAY
                    bf = parquetFileReader.readBloomFilter(chunks.get(18));
                    Assert.assertNotNull("uuid", bf);

                    // col 19: a_long128 - FIXED_LEN_BYTE_ARRAY (little-endian)
                    bf = parquetFileReader.readBloomFilter(chunks.get(19));
                    Assert.assertNotNull("long128", bf);
                    ByteBuffer l128Buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                    l128Buf.putLong(1L).putLong(1001L); // x=1: lo=1, hi=1+1000
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromConstantByteArray(l128Buf.array()))));

                    // col 20: a_long256
                    bf = parquetFileReader.readBloomFilter(chunks.get(20));
                    Assert.assertNotNull("long256", bf);

                    // col 21-26: decimal columns - FIXED_LEN_BYTE_ARRAY (big-endian)
                    for (int colIdx = 21; colIdx <= 26; colIdx++) {
                        bf = parquetFileReader.readBloomFilter(chunks.get(colIdx));
                        Assert.assertNotNull("decimal col " + colIdx, bf);
                    }

                    // col 27: designated_ts - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(27));
                    Assert.assertNotNull("designated_ts", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(400_000_000_000L)));

                    // --- Column-top columns (non-SIMD bitpacked path, column_top = 200) ---

                    // col 28: a_byte_top - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(28));
                    Assert.assertNotNull("byte_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 29: a_short_top - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(29));
                    Assert.assertNotNull("short_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 30: a_char_top - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(30));
                    Assert.assertNotNull("char_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1)));

                    // col 31: an_int_top - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(31));
                    Assert.assertNotNull("int_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(100)));

                    // col 32: a_long_top - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(32));
                    Assert.assertNotNull("long_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1L)));

                    // col 33: a_float_top - FLOAT
                    bf = parquetFileReader.readBloomFilter(chunks.get(33));
                    Assert.assertNotNull("float_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1.5f)));

                    // col 34: a_double_top - DOUBLE
                    bf = parquetFileReader.readBloomFilter(chunks.get(34));
                    Assert.assertNotNull("double_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(2.5)));

                    // col 35: a_string_top - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(35));
                    Assert.assertNotNull("string_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("hello_1"))));

                    // col 36: a_symbol_top - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(36));
                    Assert.assertNotNull("symbol_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("sym_1"))));

                    // col 37: a_varchar_top - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(37));
                    Assert.assertNotNull("varchar_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromString("вітаю_1"))));

                    // col 38: a_bin_top - BINARY
                    bf = parquetFileReader.readBloomFilter(chunks.get(38));
                    Assert.assertNotNull("binary_top", bf);

                    // col 39: a_ts_top - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(39));
                    Assert.assertNotNull("timestamp_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(1_000_000L)));

                    // col 40: a_date_top - INT64
                    bf = parquetFileReader.readBloomFilter(chunks.get(40));
                    Assert.assertNotNull("date_top", bf);
                    Assert.assertTrue(bf.findHash(bf.hash(86_400_000L)));

                    // col 41: a_ip_top - INT32
                    bf = parquetFileReader.readBloomFilter(chunks.get(41));
                    Assert.assertNotNull("ipv4_top", bf);

                    // col 42-45: geohash_top columns
                    bf = parquetFileReader.readBloomFilter(chunks.get(42));
                    Assert.assertNotNull("geo_byte_top", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(43));
                    Assert.assertNotNull("geo_short_top", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(44));
                    Assert.assertNotNull("geo_int_top", bf);
                    bf = parquetFileReader.readBloomFilter(chunks.get(45));
                    Assert.assertNotNull("geo_long_top", bf);

                    // col 46: a_uuid_top
                    bf = parquetFileReader.readBloomFilter(chunks.get(46));
                    Assert.assertNotNull("uuid_top", bf);

                    // col 47: a_long128_top - FIXED_LEN_BYTE_ARRAY (little-endian)
                    bf = parquetFileReader.readBloomFilter(chunks.get(47));
                    Assert.assertNotNull("long128_top", bf);
                    l128Buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                    l128Buf.putLong(1L).putLong(1001L);
                    Assert.assertTrue(bf.findHash(bf.hash(Binary.fromConstantByteArray(l128Buf.array()))));

                    // col 48: a_long256_top
                    bf = parquetFileReader.readBloomFilter(chunks.get(48));
                    Assert.assertNotNull("long256_top", bf);

                    // col 49-54: decimal_top columns - FIXED_LEN_BYTE_ARRAY (big-endian)
                    for (int colIdx = 49; colIdx <= 54; colIdx++) {
                        bf = parquetFileReader.readBloomFilter(chunks.get(colIdx));
                        Assert.assertNotNull("decimal_top col " + colIdx, bf);
                    }
                }

                try (ParquetReader<GenericRecord> avroReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                    long rowCount = 0;
                    while (avroReader.read() != null) {
                        rowCount++;
                    }
                    Assert.assertEquals(400, rowCount);
                }
            }
        }
    }

    @Test
    public void testBloomFilterDecimalValues() throws Exception {
        String ddl = "CREATE TABLE bloom_decimal_test AS (SELECT" +
                " 1.5::decimal(2,1) a_decimal8," +
                " 12.34::decimal(4,2) a_decimal16," +
                " 12345.6789::decimal(9,4) a_decimal32," +
                " 123456789.012345::decimal(18,6) a_decimal64," +
                " 1234567890.1234567890::decimal(38,10) a_decimal128," +
                " 1234567890.12345678901234567890::decimal(76,20) a_decimal256," +
                " timestamp_sequence(400000000000, 1000000) designated_ts" +
                " FROM long_sequence(1)) TIMESTAMP(designated_ts) PARTITION BY DAY";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);
            serverMain.awaitTable("bloom_decimal_test");

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    DirectIntList bloomFilterIndexes = new DirectIntList(6, MemoryTag.NATIVE_DEFAULT);
                    TableReader reader = serverMain.getEngine().getReader("bloom_decimal_test")
            ) {
                path.of(root).concat("bloom_decimal_test.parquet").$();
                parquetPathStr = path.toString();

                for (int i = 0; i < 6; i++) {
                    bloomFilterIndexes.add(i);
                }

                int partitionIndex = 0;
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        ParquetVersion.PARQUET_VERSION_V2,
                        bloomFilterIndexes.getAddress(),
                        (int) bloomFilterIndexes.size(),
                        0.01,
                        0.0,
                        -1L
                );

                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    BlockMetaData firstBlock = metadata.getBlocks().get(0);
                    List<ColumnChunkMetaData> chunks = firstBlock.getColumns();
                    // decimal8(2,1): 1.5 -> unscaled 15, stored as 1 byte big-endian
                    BloomFilter d8Filter = parquetFileReader.readBloomFilter(chunks.get(0));
                    Assert.assertNotNull(d8Filter);
                    Assert.assertTrue(d8Filter.findHash(d8Filter.hash(Binary.fromConstantByteArray(new byte[]{15}))));

                    // decimal16(4,2): 12.34 -> unscaled 1234, stored as 2 bytes big-endian
                    BloomFilter d16Filter = parquetFileReader.readBloomFilter(chunks.get(1));
                    Assert.assertNotNull(d16Filter);
                    ByteBuffer d16Buf = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
                    d16Buf.putShort((short) 1234);
                    Assert.assertTrue(d16Filter.findHash(d16Filter.hash(Binary.fromConstantByteArray(d16Buf.array()))));

                    // decimal32(9,4): 12345.6789 -> unscaled 123456789, stored as 4 bytes big-endian
                    BloomFilter d32Filter = parquetFileReader.readBloomFilter(chunks.get(2));
                    Assert.assertNotNull(d32Filter);
                    ByteBuffer d32Buf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
                    d32Buf.putInt(123_456_789);
                    Assert.assertTrue(d32Filter.findHash(d32Filter.hash(Binary.fromConstantByteArray(d32Buf.array()))));

                    // decimal64(18,6): 123456789.012345 -> unscaled 123456789012345, stored as 8 bytes big-endian
                    BloomFilter d64Filter = parquetFileReader.readBloomFilter(chunks.get(3));
                    Assert.assertNotNull(d64Filter);
                    ByteBuffer d64Buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                    d64Buf.putLong(123_456_789_012_345L);
                    Assert.assertTrue(d64Filter.findHash(d64Filter.hash(Binary.fromConstantByteArray(d64Buf.array()))));

                    // decimal128 and decimal256: verify bloom filters exist
                    BloomFilter d128Filter = parquetFileReader.readBloomFilter(chunks.get(4));
                    Assert.assertNotNull("Bloom filter should exist for decimal128", d128Filter);
                    BloomFilter d256Filter = parquetFileReader.readBloomFilter(chunks.get(5));
                    Assert.assertNotNull("Bloom filter should exist for decimal256", d256Filter);
                }

                try (ParquetReader<GenericRecord> avroReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                    GenericRecord record = avroReader.read();
                    Assert.assertNotNull(record);
                    Assert.assertNull("Should have exactly 1 row", avroReader.read());
                }
            }
        }
    }

    @Test
    public void testBloomFilterNoFalseNegatives() throws Exception {
        String ddl = "CREATE TABLE bloom_fn_test AS (SELECT" +
                " x::int an_int," +
                " x::long a_long," +
                " x * 1.5 a_double," +
                " 'val_' || x a_string," +
                " timestamp_sequence(400000000000, 1000000) designated_ts" +
                " FROM long_sequence(500)) TIMESTAMP(designated_ts) PARTITION BY DAY";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);
            serverMain.awaitTable("bloom_fn_test");

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    DirectIntList bloomFilterIndexes = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT);
                    TableReader reader = serverMain.getEngine().getReader("bloom_fn_test")
            ) {
                path.of(root).concat("bloom_fn_test.parquet").$();
                parquetPathStr = path.toString();

                for (int i = 0; i < 4; i++) {
                    bloomFilterIndexes.add(i);
                }

                int partitionIndex = 0;
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        ParquetVersion.PARQUET_VERSION_V1,
                        bloomFilterIndexes.getAddress(),
                        (int) bloomFilterIndexes.size(),
                        0.01,
                        0.0,
                        -1L
                );

                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    List<BlockMetaData> rowGroups = metadata.getBlocks();
                    Assert.assertTrue("Expected multiple row groups", rowGroups.size() > 1);

                    long rowOffset = 1;
                    for (int rg = 0; rg < rowGroups.size(); rg++) {
                        BlockMetaData block = rowGroups.get(rg);
                        long blockRowCount = block.getRowCount();
                        List<ColumnChunkMetaData> chunks = block.getColumns();

                        BloomFilter intFilter = parquetFileReader.readBloomFilter(chunks.get(0));
                        BloomFilter longFilter = parquetFileReader.readBloomFilter(chunks.get(1));
                        BloomFilter doubleFilter = parquetFileReader.readBloomFilter(chunks.get(2));
                        BloomFilter stringFilter = parquetFileReader.readBloomFilter(chunks.get(3));

                        Assert.assertNotNull(intFilter);
                        Assert.assertNotNull(longFilter);
                        Assert.assertNotNull(doubleFilter);
                        Assert.assertNotNull(stringFilter);

                        for (long x = rowOffset; x < rowOffset + blockRowCount; x++) {
                            int intVal = (int) x;
                            Assert.assertTrue("int bloom filter false negative: " + intVal + " in row group " + rg,
                                    intFilter.findHash(intFilter.hash(intVal)));
                            Assert.assertTrue("long bloom filter false negative: " + x + " in row group " + rg,
                                    longFilter.findHash(longFilter.hash(x)));
                            Assert.assertTrue("double bloom filter false negative: " + (x * 1.5) + " in row group " + rg,
                                    doubleFilter.findHash(doubleFilter.hash(x * 1.5)));
                            Assert.assertTrue("string bloom filter false negative: val_" + x + " in row group " + rg,
                                    stringFilter.findHash(stringFilter.hash(Binary.fromString("val_" + x))));
                        }
                        rowOffset += blockRowCount;
                    }
                    Assert.assertEquals(501, rowOffset);
                }
            }
        }
    }

    @Test
    public void testBloomFilterSelectiveColumns() throws Exception {
        String ddl = "CREATE TABLE bloom_selective_test AS (SELECT" +
                " x::int col_with_bloom," +
                " x::long col_without_bloom," +
                " 'text_' || x col_str_with_bloom," +
                " timestamp_sequence(400000000000, 1000000) designated_ts" +
                " FROM long_sequence(100)) TIMESTAMP(designated_ts) PARTITION BY DAY";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);
            serverMain.awaitTable("bloom_selective_test");

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    DirectIntList bloomFilterIndexes = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT);
                    TableReader reader = serverMain.getEngine().getReader("bloom_selective_test")
            ) {
                path.of(root).concat("bloom_selective_test.parquet").$();
                parquetPathStr = path.toString();
                bloomFilterIndexes.add(0);
                bloomFilterIndexes.add(2);

                int partitionIndex = 0;
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        ParquetVersion.PARQUET_VERSION_V1,
                        bloomFilterIndexes.getAddress(),
                        (int) bloomFilterIndexes.size(),
                        0.01,
                        0.0,
                        -1L
                );

                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    BlockMetaData firstBlock = metadata.getBlocks().get(0);
                    List<ColumnChunkMetaData> chunks = firstBlock.getColumns();

                    // col_with_bloom (col 0) should have bloom filter
                    BloomFilter bf0 = parquetFileReader.readBloomFilter(chunks.get(0));
                    Assert.assertNotNull("Bloom filter should exist for col_with_bloom", bf0);
                    Assert.assertTrue(bf0.findHash(bf0.hash(1)));

                    // col_without_bloom (col 1) should NOT have bloom filter
                    BloomFilter bf1 = parquetFileReader.readBloomFilter(chunks.get(1));
                    Assert.assertNull("Bloom filter should NOT exist for col_without_bloom", bf1);

                    // col_str_with_bloom (col 2) should have bloom filter
                    BloomFilter bf2 = parquetFileReader.readBloomFilter(chunks.get(2));
                    Assert.assertNotNull("Bloom filter should exist for col_str_with_bloom", bf2);
                    Assert.assertTrue(bf2.findHash(bf2.hash(Binary.fromString("text_1"))));

                    // designated_ts (col 3) should NOT have bloom filter
                    BloomFilter bf3 = parquetFileReader.readBloomFilter(chunks.get(3));
                    Assert.assertNull("Bloom filter should NOT exist for designated_ts", bf3);
                }
            }
        }
    }

    private static void assertArray(ArrayView expected, Object actual) {
        if (expected.isNull()) {
            Assert.assertNull(actual);
            return;
        }
        ArrayList<?> actualList = (ArrayList<?>) actual;
        Assert.assertEquals(expected.getFlatViewLength(), actualList.size());
        for (int i = 0, n = actualList.size(); i < n; i++) {
            GenericRecord record = (GenericRecord) actualList.get(i);
            Assert.assertEquals(expected.getDouble(i), (Double) record.get("element"), 0.0000001);
        }
    }

    private static void assertBinary(BinarySequence expected, Object actual) {
        if (expected == null) {
            Assert.assertNull(actual);
            return;
        }
        ByteBuffer buffer = (ByteBuffer) actual;
        Assert.assertEquals(expected.length(), buffer.remaining());
        for (int i = 0; i < expected.length(); i++) {
            Assert.assertEquals(expected.byteAt(i), buffer.get());
        }
    }

    private static void assertDecimal128(Decimal128 expected, Object actual) {
        if (expected.isNull()) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        buf.order(ByteOrder.BIG_ENDIAN);
        // Parquet stores in big-endian, so high comes first
        long actualHigh = buf.getLong();
        long actualLow = buf.getLong();
        Assert.assertEquals(expected.getHigh(), actualHigh);
        Assert.assertEquals(expected.getLow(), actualLow);
    }

    private static void assertDecimal16(short expected, Object actual) {
        if (expected == Decimals.DECIMAL16_NULL) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        buf.order(ByteOrder.BIG_ENDIAN);
        Assert.assertEquals(expected, buf.getShort());
    }

    private static void assertDecimal256(Decimal256 expected, Object actual) {
        if (expected.isNull()) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        buf.order(ByteOrder.BIG_ENDIAN);
        // Parquet stores in big-endian, so HH comes first
        long actualHH = buf.getLong();
        long actualHL = buf.getLong();
        long actualLH = buf.getLong();
        long actualLL = buf.getLong();
        Assert.assertEquals(expected.getHh(), actualHH);
        Assert.assertEquals(expected.getHl(), actualHL);
        Assert.assertEquals(expected.getLh(), actualLH);
        Assert.assertEquals(expected.getLl(), actualLL);
    }

    private static void assertDecimal32(int expected, Object actual) {
        if (expected == Decimals.DECIMAL32_NULL) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        buf.order(ByteOrder.BIG_ENDIAN);
        Assert.assertEquals(expected, buf.getInt());
    }

    private static void assertDecimal64(long expected, Object actual) {
        if (expected == Decimals.DECIMAL64_NULL) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        buf.order(ByteOrder.BIG_ENDIAN);
        Assert.assertEquals(expected, buf.getLong());
    }

    private static void assertDecimal8(byte expected, Object actual) {
        if (expected == Decimals.DECIMAL8_NULL) {
            Assert.assertNull(actual);
            return;
        }
        GenericData.Fixed fixed = (GenericData.Fixed) actual;
        ByteBuffer buf = ByteBuffer.wrap(fixed.bytes());
        // Parquet stores decimals in big-endian
        Assert.assertEquals(expected, buf.get());
    }

    private static void assertGeoHash(long expected, Object value) {
        if (value == null) {
            Assert.assertEquals(expected, -1L);
            return;
        }
        if (value instanceof Integer) {
            Assert.assertEquals(expected, ((Integer) value).longValue());
        } else if (value instanceof Long) {
            Assert.assertEquals(expected, (long) value);
        } else {
            Assert.fail("Unexpected type: " + value.getClass());
        }
    }

    private static void assertLong128(long expectedLo, long expectedHi, Object value) {
        if (value == null) {
            Assert.assertEquals(Long.MIN_VALUE, expectedLo);
            Assert.assertEquals(Long.MIN_VALUE, expectedHi);
            return;
        }
        GenericData.Fixed long128 = (GenericData.Fixed) value;
        ByteBuffer long128Buf = ByteBuffer.wrap(long128.bytes());
        long128Buf.order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(expectedLo, long128Buf.getLong());
        Assert.assertEquals(expectedHi, long128Buf.getLong());
    }

    private static void assertLong256(Long256 expectedLong256, Object value) {
        if (value == null) {
            Assert.assertTrue(Long256Impl.isNull(expectedLong256));
            return;
        }
        GenericData.Fixed long256 = (GenericData.Fixed) value;
        ByteBuffer long256Buf = ByteBuffer.wrap(long256.bytes());
        long256Buf.order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(expectedLong256.getLong0(), long256Buf.getLong());
        Assert.assertEquals(expectedLong256.getLong1(), long256Buf.getLong());
        Assert.assertEquals(expectedLong256.getLong2(), long256Buf.getLong());
        Assert.assertEquals(expectedLong256.getLong3(), long256Buf.getLong());
    }

    private static <T extends Comparable<T>> void assertMinMaxRange(List<ColumnChunkMetaData> chunks, int index, T min, T max) {
        Statistics<T> statistics = chunks.get(index).getStatistics();
        Assert.assertTrue(statistics.compareMinToValue(min) >= 0);
        Assert.assertTrue(statistics.compareMaxToValue(max) <= 0);
    }

    private static <T extends Comparable<T>> void assertNullCount(List<ColumnChunkMetaData> chunks, int index, long nullCount) {
        Statistics<T> statistics = chunks.get(index).getStatistics();
        Assert.assertEquals(nullCount, statistics.getNumNulls());
    }

    private static void assertNullableString(Object expected, Object actual) {
        if (expected == null) {
            Assert.assertNull(actual);
        } else {
            Assert.assertEquals(expected.toString(), actual.toString());
        }
    }

    private static void assertPrimitiveValue(Object expected, Object value, Object nullValue) {
        if (value == null) {
            Assert.assertEquals(expected, nullValue);
        } else {
            Assert.assertEquals(expected, value);
        }
    }

    private static void assertRawArray(ArrayView expected, Object actual) {
        if (expected.isNull()) {
            Assert.assertNull(actual);
            return;
        }

        ByteBuffer buffer = (ByteBuffer) actual;
        byte[] arr = buffer.array();

        long ptr = Unsafe.malloc(arr.length, MemoryTag.NATIVE_DEFAULT);
        try (BorrowedArray borrowedArray = new BorrowedArray()) {
            for (int i = 0; i < arr.length; i++) {
                Unsafe.putByte(ptr + i, arr[i]);
            }

            // the shape is padded to 8 bytes, hence Long.BYTES
            borrowedArray.of(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), ptr, ptr + Long.BYTES, arr.length - Long.BYTES);

            Assert.assertEquals(1, expected.getDimCount());
            Assert.assertEquals(1, borrowedArray.getDimCount());
            Assert.assertEquals(expected.getDimLen(0), borrowedArray.getDimLen(0));
            for (int i = 0, n = borrowedArray.getDimLen(0); i < n; i++) {
                Assert.assertEquals(expected.getDouble(i), borrowedArray.getDouble(i), 0.0000001);
            }
        } finally {
            Unsafe.free(ptr, arr.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertSchema(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType, int maxDefinitionLevel) {
        Assert.assertEquals(expectedName, descriptor.getPath()[0]);
        Assert.assertEquals(expectedType, descriptor.getPrimitiveType().getPrimitiveTypeName());
        Assert.assertEquals(0, descriptor.getMaxRepetitionLevel());
        Assert.assertEquals(maxDefinitionLevel, descriptor.getMaxDefinitionLevel());
    }

    private static void assertSchemaArray(ColumnDescriptor descriptor, String expectedName, int maxRepLevel, int maxDefLevel) {
        Assert.assertEquals(expectedName, descriptor.getPath()[0]);
        Assert.assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE, descriptor.getPrimitiveType().getPrimitiveTypeName());
        Assert.assertEquals(maxRepLevel, descriptor.getMaxRepetitionLevel());
        Assert.assertEquals(maxDefLevel, descriptor.getMaxDefinitionLevel());
    }

    private static void assertSchemaNonNullable(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType) {
        assertSchema(descriptor, expectedName, expectedType, 0);
    }

    private static void assertSchemaNullable(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType) {
        assertSchema(descriptor, expectedName, expectedType, 1);
    }

    private static void assertSchemaNullable(ColumnDescriptor descriptor, String expectedName, String expectedLogicTypeAnnotation, PrimitiveType.PrimitiveTypeName expectedType) {
        assertSchema(descriptor, expectedName, expectedType, 1);
        Assert.assertEquals(expectedLogicTypeAnnotation, descriptor.getPrimitiveType().getLogicalTypeAnnotation().toString());
    }

    private static void assertUuid(StringSink sink, long expectedLo, long expectedHi, Object actual) {
        if (actual == null) {
            Assert.assertEquals(Long.MIN_VALUE, expectedLo);
            Assert.assertEquals(Long.MIN_VALUE, expectedHi);
            return;
        }
        Uuid uuid = new Uuid(expectedLo, expectedHi);
        sink.clear();
        uuid.toSink(sink);
        Assert.assertEquals(sink.toString(), actual.toString());
    }

    private static void assertVarchar(Utf8Sequence expected, Object actual) {
        if (expected == null) {
            Assert.assertNull(actual);
        } else {
            Assert.assertEquals(Utf8s.toString(expected), actual.toString());
        }
    }

    private void test1dArray(int parquetVersion) throws Exception {
        final String ddl = "create table x as (select " +
                " array[1, 2, 3] arr, " +
                " timestamp_sequence(400000000000, 1000000000) ts" +
                " from long_sequence(3)) timestamp(ts) partition by day";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);

            // create new active partition
            final String insert = "insert into x values (null, '1970-02-02T02:02:02.020202Z')";
            serverMain.getEngine().execute(insert); // txn 2

            serverMain.awaitTxn("x", 2);

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = serverMain.getEngine().getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                parquetPathStr = path.toString();
                long start = System.nanoTime();

                int partitionIndex = 0;
                StringSink partitionName = new StringSink();
                long timestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                PartitionBy.setSinkForPartition(partitionName, ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, timestamp);

                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        10,
                        DATA_PAGE_SIZE,
                        parquetVersion,
                        0.0
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (
                        ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
                        ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()
                ) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    FileMetaData fileMetaData = metadata.getFileMetaData();
                    Assert.assertEquals("QuestDB version 9.0", fileMetaData.getCreatedBy());

                    MessageType schema = fileMetaData.getSchema();
                    List<ColumnDescriptor> columns = schema.getColumns();
                    Assert.assertEquals(2, schema.getColumns().size());

                    assertSchemaArray(columns.get(0), "arr", 1, 3);
                    // designated ts is non-nullable
                    assertSchemaNonNullable(columns.get(1), "ts", PrimitiveType.PrimitiveTypeName.INT64);

                    long rowCount = 0;
                    List<BlockMetaData> rowGroups = metadata.getBlocks();
                    for (int i = 0; i < rowGroups.size(); i++) {
                        BlockMetaData blockMetaData = rowGroups.get(i);
                        long blockRowCount = blockMetaData.getRowCount();
                        if (i == rowGroups.size() - 1) {
                            Assert.assertTrue(blockRowCount <= ROW_GROUP_SIZE);
                        } else {
                            Assert.assertEquals(ROW_GROUP_SIZE, blockRowCount);
                        }
                        rowCount += blockRowCount;
                        List<ColumnChunkMetaData> chunks = blockMetaData.getColumns();
                        // arr
                        assertNullCount(chunks, 0, 0);
                    }
                    Assert.assertEquals(3, rowCount);

                    long actualRows = 0;
                    GenericRecord nextParquetRecord;
                    while ((nextParquetRecord = parquetReader.read()) != null) {
                        final Object arr = nextParquetRecord.get("arr");
                        Assert.assertNotNull(arr);
                        Assert.assertEquals(
                                "[{\"element\": 1.0}, {\"element\": 2.0}, {\"element\": 3.0}]",
                                arr.toString()
                        );
                        actualRows++;
                    }
                    Assert.assertEquals(3, actualRows);
                }
            }
        }
    }

    private void test2dArray(int parquetVersion) throws Exception {
        final String ddl = "create table x as (select " +
                " array[[1, 2, 3], [4, 5, 6], [7, 8, 9]] arr, " +
                " timestamp_sequence(400000000000, 1000000000) ts" +
                " from long_sequence(3)) timestamp(ts) partition by day";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl);

            // create new active partition
            final String insert = "insert into x values (null, '1970-02-02T02:02:02.020202Z')";
            serverMain.getEngine().execute(insert); // txn 2

            serverMain.awaitTxn("x", 2);

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = serverMain.getEngine().getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                parquetPathStr = path.toString();
                long start = System.nanoTime();

                int partitionIndex = 0;
                StringSink partitionName = new StringSink();
                long timestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                PartitionBy.setSinkForPartition(partitionName, ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY, timestamp);

                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        10,
                        DATA_PAGE_SIZE,
                        parquetVersion,
                        0.0
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));

                try (
                        ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
                        ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()
                ) {
                    ParquetMetadata metadata = parquetFileReader.getFooter();
                    FileMetaData fileMetaData = metadata.getFileMetaData();
                    Assert.assertEquals("QuestDB version 9.0", fileMetaData.getCreatedBy());

                    MessageType schema = fileMetaData.getSchema();
                    List<ColumnDescriptor> columns = schema.getColumns();
                    Assert.assertEquals(2, schema.getColumns().size());

                    assertSchemaArray(columns.get(0), "arr", 2, 4);
                    // designated ts is non-nullable
                    assertSchemaNonNullable(columns.get(1), "ts", PrimitiveType.PrimitiveTypeName.INT64);

                    long rowCount = 0;
                    List<BlockMetaData> rowGroups = metadata.getBlocks();
                    for (int i = 0; i < rowGroups.size(); i++) {
                        BlockMetaData blockMetaData = rowGroups.get(i);
                        long blockRowCount = blockMetaData.getRowCount();
                        if (i == rowGroups.size() - 1) {
                            Assert.assertTrue(blockRowCount <= ROW_GROUP_SIZE);
                        } else {
                            Assert.assertEquals(ROW_GROUP_SIZE, blockRowCount);
                        }
                        rowCount += blockRowCount;
                        List<ColumnChunkMetaData> chunks = blockMetaData.getColumns();
                        // arr
                        assertNullCount(chunks, 0, 0);
                    }
                    Assert.assertEquals(3, rowCount);

                    long actualRows = 0;
                    GenericRecord nextParquetRecord;
                    while ((nextParquetRecord = parquetReader.read()) != null) {
                        final Object arr = nextParquetRecord.get("arr");
                        Assert.assertNotNull(arr);
                        Assert.assertEquals(
                                "[{\"list\": [{\"element\": 1.0}, {\"element\": 2.0}, {\"element\": 3.0}]}, {\"list\": [{\"element\": 4.0}, {\"element\": 5.0}, {\"element\": 6.0}]}, {\"list\": [{\"element\": 7.0}, {\"element\": 8.0}, {\"element\": 9.0}]}]",
                                arr.toString()
                        );
                        actualRows++;
                    }
                    Assert.assertEquals(3, actualRows);
                }
            }
        }
    }


    private void testPartitionDataConsistency(String tableName, int partitionBy, String designedTimestampType, boolean rawArrayEncoding) throws Exception {
        String ddl = "create table " + tableName + " as (select" +
                " x id," +
                " rnd_boolean() a_boolean," +
                " rnd_byte() a_byte," +
                " rnd_short() a_short," +
                " rnd_char() a_char," +
                " rnd_int(" + NUMERIC_MIN + ", " + NUMERIC_MAX + ", 0) an_int," +
                " rnd_long(" + NUMERIC_MIN + ", " + NUMERIC_MAX + ", 0) a_long," +
                " rnd_float() a_float," +
                " rnd_double() a_double," +
                " rnd_symbol('a','b','c') a_symbol," +
                " rnd_geohash(4) a_geo_byte," +
                " rnd_geohash(8) a_geo_short," +
                " rnd_geohash(16) a_geo_int," +
                " rnd_geohash(32) a_geo_long," +
                " rnd_str('hello', 'world', '!') a_string," +
                " rnd_bin(1, 8, 0) a_bin," +
                " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                " rnd_double_array(1) an_array," +
                " rnd_ipv4() a_ip," +
                " rnd_uuid4() a_uuid," +
                " rnd_long256() a_long256," +
                " to_long128(rnd_long(), rnd_long()) a_long128," +
                " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                " timestamp_sequence(500000000000, 600) a_ts," +
                " timestamp_sequence_ns(500000000000, 600000) a_ns," +
                " rnd_decimal(2, 1, 0) a_decimal8," +
                " rnd_decimal(4, 2, 0) a_decimal16," +
                " rnd_decimal(9, 4, 0) a_decimal32," +
                " rnd_decimal(18, 6, 0) a_decimal64," +
                " rnd_decimal(38, 10, 0) a_decimal128," +
                " rnd_decimal(76, 20, 0) a_decimal256," +
                " timestamp_sequence(400000000000, 500)::" + designedTimestampType + " designated_ts" +
                " from long_sequence(" + INITIAL_ROWS + ")) timestamp(designated_ts) partition by " + PartitionBy.toString(partitionBy) + ";";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().execute(ddl); // txn 1

            serverMain.getEngine().execute("alter table " + tableName + " add column a_boolean_top boolean"); // txn 2
            serverMain.getEngine().execute("alter table " + tableName + " add column a_byte_top byte"); // txn 3
            serverMain.getEngine().execute("alter table " + tableName + " add column a_short_top short"); // txn 4
            serverMain.getEngine().execute("alter table " + tableName + " add column a_char_top char"); // txn 5
            serverMain.getEngine().execute("alter table " + tableName + " add column an_int_top int"); // txn 6
            serverMain.getEngine().execute("alter table " + tableName + " add column a_long_top long"); // txn 7
            serverMain.getEngine().execute("alter table " + tableName + " add column a_float_top float"); // txn 8
            serverMain.getEngine().execute("alter table " + tableName + " add column a_double_top double"); // txn 9
            serverMain.getEngine().execute("alter table " + tableName + " add column a_symbol_top symbol"); // txn 10
            serverMain.getEngine().execute("alter table " + tableName + " add column a_geo_byte_top geohash(4b)"); // txn 11
            serverMain.getEngine().execute("alter table " + tableName + " add column a_geo_short_top geohash(8b)"); // txn 12
            serverMain.getEngine().execute("alter table " + tableName + " add column a_geo_int_top geohash(16b)"); // txn 13
            serverMain.getEngine().execute("alter table " + tableName + " add column a_geo_long_top geohash(32b)"); // txn 14
            serverMain.getEngine().execute("alter table " + tableName + " add column a_string_top string"); // txn 15
            serverMain.getEngine().execute("alter table " + tableName + " add column a_bin_top binary"); // txn 16
            serverMain.getEngine().execute("alter table " + tableName + " add column a_varchar_top varchar"); // txn 17
            serverMain.getEngine().execute("alter table " + tableName + " add column an_array_top double[]"); // txn 18
            serverMain.getEngine().execute("alter table " + tableName + " add column a_ip_top ipv4"); // txn 19
            serverMain.getEngine().execute("alter table " + tableName + " add column a_uuid_top uuid"); // txn 20
            serverMain.getEngine().execute("alter table " + tableName + " add column a_long128_top long128"); // txn 21
            serverMain.getEngine().execute("alter table " + tableName + " add column a_long256_top long256"); // txn 22
            serverMain.getEngine().execute("alter table " + tableName + " add column a_date_top date"); //  txn 23
            serverMain.getEngine().execute("alter table " + tableName + " add column a_ts_top timestamp"); // txn 24
            serverMain.getEngine().execute("alter table " + tableName + " add column a_ns_top timestamp_ns"); // txn 25
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal8_top decimal(2,1)"); // txn 26
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal16_top decimal(4,2)"); // txn 27
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal32_top decimal(9,4)"); // txn 28
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal64_top decimal(18,6)"); // txn 29
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal128_top decimal(38,10)"); // txn 30
            serverMain.getEngine().execute("alter table " + tableName + " add column a_decimal256_top decimal(76,20)"); // txn 31

            String insert = "insert into " + tableName + "(id, a_boolean_top, a_byte_top, a_short_top, a_char_top," +
                    " an_int_top, a_long_top, a_float_top, a_double_top,\n" +
                    " a_symbol_top, a_geo_byte_top, a_geo_short_top, a_geo_int_top, a_geo_long_top,\n" +
                    " a_string_top, a_bin_top, a_varchar_top, an_array_top, a_ip_top, a_uuid_top, a_long128_top, a_long256_top,\n" +
                    " a_date_top, a_ts_top, a_ns_top," +
                    " a_decimal8_top, a_decimal16_top, a_decimal32_top, a_decimal64_top, a_decimal128_top, a_decimal256_top," +
                    " designated_ts) select\n" +
                    " " + INITIAL_ROWS + " + x," +
                    " rnd_boolean()," +
                    " rnd_byte()," +
                    " rnd_short()," +
                    " rnd_char()," +
                    " rnd_int(" + NUMERIC_MIN + ", " + NUMERIC_MAX + ", 2)," +
                    " rnd_long(" + NUMERIC_MIN + ", " + NUMERIC_MAX + ", 2)," +
                    " rnd_float(2)," +
                    " rnd_double(2)," +
                    " rnd_symbol('a','b','c', null)," +
                    " rnd_geohash(4)," +
                    " rnd_geohash(8)," +
                    " rnd_geohash(16)," +
                    " rnd_geohash(32)," +
                    " rnd_str('hello', 'world', '!', null)," +
                    " rnd_bin(1, 8, 2)," +
                    " rnd_varchar('ганьба','слава','добрий','вечір', null)," +
                    " rnd_double_array(1)," +
                    " rnd_ipv4('192.168.88.0/24', 2)," +
                    " rnd_uuid4()," +
                    " to_long128(rnd_long(0,10, 2), null)," +
                    " to_long256(rnd_long(0,10, 2), null, null, null)," +
                    " rnd_date(" +
                    "    to_date('2022', 'yyyy')," +
                    "    to_date('2027', 'yyyy')," +
                    "    2) a_date_top," +
                    " rnd_timestamp(" +
                    "    to_timestamp('2022', 'yyyy')," +
                    "    to_timestamp('2027', 'yyyy')," +
                    "    2) a_ts_top," +
                    " rnd_timestamp_ns(" +
                    "    to_timestamp_ns('2022', 'yyyy')," +
                    "    to_timestamp_ns('2027', 'yyyy')," +
                    "    2) a_ns_top," +
                    " rnd_decimal(2, 1, 2) a_decimal8_top," +
                    " rnd_decimal(4, 2, 2) a_decimal16_top," +
                    " rnd_decimal(9, 4, 2) a_decimal32_top," +
                    " rnd_decimal(18, 6, 2) a_decimal64_top," +
                    " rnd_decimal(38, 10, 2) a_decimal128_top," +
                    " rnd_decimal(76, 20, 2) a_decimal256_top," +
                    " timestamp_sequence(1600000000000, 500)::" + designedTimestampType +
                    " from long_sequence(" + UPDATE_ROWS + ");";

            serverMain.getEngine().execute(insert); // txn 26

            serverMain.awaitTable(tableName);

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = serverMain.getEngine().getReader(tableName)
            ) {
                path.of(root).concat(tableName + ".parquet").$();
                parquetPathStr = path.toString();
                long start = System.nanoTime();

                int partitionIndex = 0;
                StringSink partitionName = new StringSink();
                long timestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                PartitionBy.setSinkForPartition(partitionName, reader.getMetadata().getTimestampType(), partitionBy, timestamp);

                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        packCompressionCodecLevel(5, ParquetCompression.COMPRESSION_ZSTD),
                        true,
                        rawArrayEncoding,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                long partitionRowCount = reader.getPartitionRowCount(partitionIndex);
                final InputFile inputFile = new LocalInputFile(java.nio.file.Path.of(parquetPathStr));
                validateParquetData(inputFile, serverMain.getEngine(), reader.getTableToken(), partitionRowCount, partitionName.toString(), rawArrayEncoding);
                validateParquetMetadata(inputFile, partitionRowCount, rawArrayEncoding);
            }
        }
    }

    private void validateParquetData(InputFile inputFile, CairoEngine engine, TableToken tableToken, long rows, String partition, boolean rawArrayEncoding) throws Exception {
        final SqlExecutionContext executionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null
                );

        try (
                final ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
                final RecordCursorFactory factory = engine.select(
                        "select * from " +
                                tableToken.getTableName() +
                                " where designated_ts in '" + partition + "'",
                        executionContext
                );
                final RecordCursor cursor = factory.getCursor(executionContext)
        ) {
            StringSink sink = new StringSink();
            long actualRows = 0;

            final Record tableReaderRecord = cursor.getRecord();

            GenericRecord nextParquetRecord;
            while (cursor.hasNext()) {
                nextParquetRecord = parquetReader.read();
                Assert.assertNotNull("Missing parquet record [currentRow=" + actualRows + ", totalRows=" + rows + "]", nextParquetRecord);

                Assert.assertEquals(tableReaderRecord.getLong(0), nextParquetRecord.get("id"));
                Assert.assertEquals(++actualRows, nextParquetRecord.get("id"));
                Assert.assertEquals(tableReaderRecord.getBool(1), nextParquetRecord.get("a_boolean"));
                Assert.assertEquals((int) tableReaderRecord.getByte(2), nextParquetRecord.get("a_byte"));
                Assert.assertEquals((int) tableReaderRecord.getShort(3), nextParquetRecord.get("a_short"));
                Assert.assertEquals((int) tableReaderRecord.getChar(4), nextParquetRecord.get("a_char"));

                assertPrimitiveValue(tableReaderRecord.getInt(5), nextParquetRecord.get("an_int"), Integer.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getLong(6), nextParquetRecord.get("a_long"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getFloat(7), nextParquetRecord.get("a_float"), Float.NaN);
                assertPrimitiveValue(tableReaderRecord.getDouble(8), nextParquetRecord.get("a_double"), Double.NaN);

                assertNullableString(tableReaderRecord.getSymA(9), nextParquetRecord.get("a_symbol"));

                assertGeoHash(tableReaderRecord.getGeoByte(10), nextParquetRecord.get("a_geo_byte"));
                assertGeoHash(tableReaderRecord.getGeoShort(11), nextParquetRecord.get("a_geo_short"));
                assertGeoHash(tableReaderRecord.getGeoInt(12), nextParquetRecord.get("a_geo_int"));
                assertGeoHash(tableReaderRecord.getGeoLong(13), nextParquetRecord.get("a_geo_long"));

                assertNullableString(tableReaderRecord.getStrA(14), nextParquetRecord.get("a_string"));
                assertBinary(tableReaderRecord.getBin(15), nextParquetRecord.get("a_bin"));
                assertVarchar(tableReaderRecord.getVarcharA(16), nextParquetRecord.get("a_varchar"));
                if (rawArrayEncoding) {
                    assertRawArray(tableReaderRecord.getArray(17, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array"));
                } else {
                    assertArray(tableReaderRecord.getArray(17, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array"));
                }
                assertPrimitiveValue(tableReaderRecord.getIPv4(18), nextParquetRecord.get("a_ip"), Numbers.IPv4_NULL);

                long uuidLo = tableReaderRecord.getLong128Lo(19);
                long uuidHi = tableReaderRecord.getLong128Hi(19);
                assertUuid(sink, uuidLo, uuidHi, nextParquetRecord.get("a_uuid"));

                assertLong256(tableReaderRecord.getLong256A(20), nextParquetRecord.get("a_long256"));

                assertLong128(tableReaderRecord.getLong128Lo(21), tableReaderRecord.getLong128Hi(21), nextParquetRecord.get("a_long128"));

                assertPrimitiveValue(tableReaderRecord.getDate(22), nextParquetRecord.get("a_date"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(23), nextParquetRecord.get("a_ts"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(24), nextParquetRecord.get("a_ns"), Long.MIN_VALUE);

                // decimal columns
                assertDecimal8(tableReaderRecord.getDecimal8(25), nextParquetRecord.get("a_decimal8"));
                assertDecimal16(tableReaderRecord.getDecimal16(26), nextParquetRecord.get("a_decimal16"));
                assertDecimal32(tableReaderRecord.getDecimal32(27), nextParquetRecord.get("a_decimal32"));
                assertDecimal64(tableReaderRecord.getDecimal64(28), nextParquetRecord.get("a_decimal64"));
                Decimal128 decimal128 = new Decimal128();
                tableReaderRecord.getDecimal128(29, decimal128);
                assertDecimal128(decimal128, nextParquetRecord.get("a_decimal128"));
                Decimal256 decimal256 = new Decimal256();
                tableReaderRecord.getDecimal256(30, decimal256);
                assertDecimal256(decimal256, nextParquetRecord.get("a_decimal256"));

                assertPrimitiveValue(tableReaderRecord.getTimestamp(31), nextParquetRecord.get("designated_ts"), Long.MIN_VALUE);

                // column tops

                Assert.assertEquals(tableReaderRecord.getBool(32), nextParquetRecord.get("a_boolean_top"));
                Assert.assertEquals((int) tableReaderRecord.getByte(33), nextParquetRecord.get("a_byte_top"));
                Assert.assertEquals((int) tableReaderRecord.getShort(34), nextParquetRecord.get("a_short_top"));
                Assert.assertEquals((int) tableReaderRecord.getChar(35), nextParquetRecord.get("a_char_top"));

                assertPrimitiveValue(tableReaderRecord.getInt(36), nextParquetRecord.get("an_int_top"), Integer.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getLong(37), nextParquetRecord.get("a_long_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getFloat(38), nextParquetRecord.get("a_float_top"), Float.NaN);
                assertPrimitiveValue(tableReaderRecord.getDouble(39), nextParquetRecord.get("a_double_top"), Double.NaN);
                assertNullableString(tableReaderRecord.getSymA(40), nextParquetRecord.get("a_symbol_top"));
                assertGeoHash(tableReaderRecord.getGeoByte(41), nextParquetRecord.get("a_geo_byte_top"));
                assertGeoHash(tableReaderRecord.getGeoShort(42), nextParquetRecord.get("a_geo_short_top"));
                assertGeoHash(tableReaderRecord.getGeoInt(43), nextParquetRecord.get("a_geo_int_top"));
                assertGeoHash(tableReaderRecord.getGeoLong(44), nextParquetRecord.get("a_geo_long_top"));
                assertNullableString(tableReaderRecord.getStrA(45), nextParquetRecord.get("a_string_top"));
                assertBinary(tableReaderRecord.getBin(46), nextParquetRecord.get("a_bin_top"));
                assertVarchar(tableReaderRecord.getVarcharA(47), nextParquetRecord.get("a_varchar_top"));
                if (rawArrayEncoding) {
                    assertRawArray(tableReaderRecord.getArray(48, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array_top"));
                } else {
                    assertArray(tableReaderRecord.getArray(48, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array_top"));
                }
                assertPrimitiveValue(tableReaderRecord.getIPv4(49), nextParquetRecord.get("a_ip_top"), Numbers.IPv4_NULL);
                assertUuid(sink, tableReaderRecord.getLong128Lo(50), tableReaderRecord.getLong128Hi(50), nextParquetRecord.get("a_uuid_top"));
                assertLong128(tableReaderRecord.getLong128Lo(51), tableReaderRecord.getLong128Hi(51), nextParquetRecord.get("a_long128_top"));
                assertLong256(tableReaderRecord.getLong256A(52), nextParquetRecord.get("a_long256_top"));
                assertPrimitiveValue(tableReaderRecord.getDate(53), nextParquetRecord.get("a_date_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(54), nextParquetRecord.get("a_ts_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(55), nextParquetRecord.get("a_ns_top"), Long.MIN_VALUE);

                // decimal column tops
                assertDecimal8(tableReaderRecord.getDecimal8(56), nextParquetRecord.get("a_decimal8_top"));
                assertDecimal16(tableReaderRecord.getDecimal16(57), nextParquetRecord.get("a_decimal16_top"));
                assertDecimal32(tableReaderRecord.getDecimal32(58), nextParquetRecord.get("a_decimal32_top"));
                assertDecimal64(tableReaderRecord.getDecimal64(59), nextParquetRecord.get("a_decimal64_top"));
                tableReaderRecord.getDecimal128(60, decimal128);
                assertDecimal128(decimal128, nextParquetRecord.get("a_decimal128_top"));
                tableReaderRecord.getDecimal256(61, decimal256);
                assertDecimal256(decimal256, nextParquetRecord.get("a_decimal256_top"));
            }
            Assert.assertEquals(rows, actualRows);
        }
    }

    private void validateParquetMetadata(InputFile inputFile, long rows, boolean rawArrayEncoding) throws IOException {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = parquetFileReader.getFooter();
            FileMetaData fileMetaData = metadata.getFileMetaData();
            Assert.assertEquals("QuestDB version 9.0", fileMetaData.getCreatedBy());

            MessageType schema = fileMetaData.getSchema();
            List<ColumnDescriptor> columns = schema.getColumns();
            Assert.assertEquals(62, schema.getColumns().size());

            assertSchemaNullable(columns.get(0), "id", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNonNullable(columns.get(1), "a_boolean", PrimitiveType.PrimitiveTypeName.BOOLEAN);
            assertSchemaNonNullable(columns.get(2), "a_byte", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(3), "a_short", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(4), "a_char", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(5), "an_int", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(6), "a_long", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(7), "a_float", PrimitiveType.PrimitiveTypeName.FLOAT);
            assertSchemaNullable(columns.get(8), "a_double", PrimitiveType.PrimitiveTypeName.DOUBLE);
            assertSchemaNullable(columns.get(9), "a_symbol", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(10), "a_geo_byte", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(11), "a_geo_short", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(12), "a_geo_int", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(13), "a_geo_long", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(14), "a_string", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(15), "a_bin", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(16), "a_varchar", PrimitiveType.PrimitiveTypeName.BINARY);
            if (rawArrayEncoding) {
                assertSchemaNullable(columns.get(17), "an_array", PrimitiveType.PrimitiveTypeName.BINARY);
            }
            assertSchemaNullable(columns.get(18), "a_ip", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(19), "a_uuid", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(20), "a_long256", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(21), "a_long128", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(22), "a_date", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(23), "a_ts", "TIMESTAMP(MICROS,true)", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(24), "a_ns", "TIMESTAMP(NANOS,true)", PrimitiveType.PrimitiveTypeName.INT64);
            // decimal columns
            assertSchemaNullable(columns.get(25), "a_decimal8", "DECIMAL(2,1)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(26), "a_decimal16", "DECIMAL(4,2)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(27), "a_decimal32", "DECIMAL(9,4)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(28), "a_decimal64", "DECIMAL(18,6)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(29), "a_decimal128", "DECIMAL(38,10)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(30), "a_decimal256", "DECIMAL(76,20)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            // designated ts is non-nullable
            assertSchemaNonNullable(columns.get(31), "designated_ts", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNonNullable(columns.get(32), "a_boolean_top", PrimitiveType.PrimitiveTypeName.BOOLEAN);
            assertSchemaNonNullable(columns.get(33), "a_byte_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(34), "a_short_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(35), "a_char_top", PrimitiveType.PrimitiveTypeName.INT32);

            assertSchemaNullable(columns.get(36), "an_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(37), "a_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(38), "a_float_top", PrimitiveType.PrimitiveTypeName.FLOAT);
            assertSchemaNullable(columns.get(39), "a_double_top", PrimitiveType.PrimitiveTypeName.DOUBLE);
            assertSchemaNullable(columns.get(40), "a_symbol_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(41), "a_geo_byte_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(42), "a_geo_short_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(43), "a_geo_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(44), "a_geo_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(45), "a_string_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(46), "a_bin_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(47), "a_varchar_top", PrimitiveType.PrimitiveTypeName.BINARY);
            if (rawArrayEncoding) {
                assertSchemaNullable(columns.get(48), "an_array_top", PrimitiveType.PrimitiveTypeName.BINARY);
            } else {
                assertSchemaArray(columns.get(48), "an_array_top", 1, 3);
            }
            assertSchemaNullable(columns.get(49), "a_ip_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(50), "a_uuid_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(51), "a_long128_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(52), "a_long256_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(53), "a_date_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(54), "a_ts_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(55), "a_ns_top", PrimitiveType.PrimitiveTypeName.INT64);
            // decimal column tops
            assertSchemaNullable(columns.get(56), "a_decimal8_top", "DECIMAL(2,1)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(57), "a_decimal16_top", "DECIMAL(4,2)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(58), "a_decimal32_top", "DECIMAL(9,4)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(59), "a_decimal64_top", "DECIMAL(18,6)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(60), "a_decimal128_top", "DECIMAL(38,10)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(61), "a_decimal256_top", "DECIMAL(76,20)", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);

            long rowCount = 0;
            List<BlockMetaData> rowGroups = metadata.getBlocks();
            for (int i = 0; i < rowGroups.size(); i++) {
                BlockMetaData blockMetaData = rowGroups.get(i);
                long blockRowCount = blockMetaData.getRowCount();
                if (i == rowGroups.size() - 1) {
                    Assert.assertTrue(blockRowCount <= ROW_GROUP_SIZE);
                } else {
                    Assert.assertEquals(ROW_GROUP_SIZE, blockRowCount);
                }
                rowCount += blockRowCount;
                List<ColumnChunkMetaData> chunks = blockMetaData.getColumns();
                // an_int
                assertMinMaxRange(chunks, 5, NUMERIC_MIN, NUMERIC_MAX);
                // a_long
                assertMinMaxRange(chunks, 6, (long) NUMERIC_MIN, (long) NUMERIC_MAX);
                // a_float
                assertMinMaxRange(chunks, 7, 0.0f, 1.0f);
                // a_double
                assertMinMaxRange(chunks, 8, 0.0d, 1.0d);
                // an_int_top (index 36 after adding 6 decimal columns)
                assertMinMaxRange(chunks, 36, NUMERIC_MIN, NUMERIC_MAX);
                // a_long_top (index 37)
                assertMinMaxRange(chunks, 37, (long) NUMERIC_MIN, (long) NUMERIC_MAX);
                // a_float_top (index 38)
                assertMinMaxRange(chunks, 38, 0.0f, 1.0f);
                // a_double_top (index 39)
                assertMinMaxRange(chunks, 39, 0.0d, 1.0d);
            }
            Assert.assertEquals(rowCount, rows);
        }
    }
}
