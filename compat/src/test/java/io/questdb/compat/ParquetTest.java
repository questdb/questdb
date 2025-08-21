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
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

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
                Unsafe.getUnsafe().putByte(ptr + i, arr[i]);
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
                        parquetVersion
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                Configuration configuration = new Configuration();
                final org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(parquetPathStr);
                final InputFile inputFile = HadoopInputFile.fromPath(parquetPath, configuration);

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
                        parquetVersion
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                Configuration configuration = new Configuration();
                final org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(parquetPathStr);
                final InputFile inputFile = HadoopInputFile.fromPath(parquetPath, configuration);

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
                " timestamp_sequence(500000000000::timestamp_ns, 600000) a_ns," +
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

            String insert = "insert into " + tableName + "(id, a_boolean_top, a_byte_top, a_short_top, a_char_top," +
                    " an_int_top, a_long_top, a_float_top, a_double_top,\n" +
                    " a_symbol_top, a_geo_byte_top, a_geo_short_top, a_geo_int_top, a_geo_long_top,\n" +
                    " a_string_top, a_bin_top, a_varchar_top, an_array_top, a_ip_top, a_uuid_top, a_long128_top, a_long256_top,\n" +
                    " a_date_top, a_ts_top, a_ns_top, designated_ts) select\n" +
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
                    " rnd_timestamp(" +
                    "    to_timestamp_ns('2022', 'yyyy')," +
                    "    to_timestamp_ns('2027', 'yyyy')," +
                    "    2) a_ns_top," +
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
                        (5L << 32) | ParquetCompression.COMPRESSION_ZSTD,
                        true,
                        rawArrayEncoding,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        ParquetVersion.PARQUET_VERSION_V1
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                long partitionRowCount = reader.getPartitionRowCount(partitionIndex);
                Configuration configuration = new Configuration();
                final org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(parquetPathStr);
                final InputFile inputFile = HadoopInputFile.fromPath(parquetPath, configuration);
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
                assertPrimitiveValue(tableReaderRecord.getTimestamp(25), nextParquetRecord.get("designated_ts"), Long.MIN_VALUE);

                // column tops

                Assert.assertEquals(tableReaderRecord.getBool(26), nextParquetRecord.get("a_boolean_top"));
                Assert.assertEquals((int) tableReaderRecord.getByte(27), nextParquetRecord.get("a_byte_top"));
                Assert.assertEquals((int) tableReaderRecord.getShort(28), nextParquetRecord.get("a_short_top"));
                Assert.assertEquals((int) tableReaderRecord.getChar(29), nextParquetRecord.get("a_char_top"));

                assertPrimitiveValue(tableReaderRecord.getInt(30), nextParquetRecord.get("an_int_top"), Integer.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getLong(31), nextParquetRecord.get("a_long_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getFloat(32), nextParquetRecord.get("a_float_top"), Float.NaN);
                assertPrimitiveValue(tableReaderRecord.getDouble(33), nextParquetRecord.get("a_double_top"), Double.NaN);
                assertNullableString(tableReaderRecord.getSymA(34), nextParquetRecord.get("a_symbol_top"));
                assertGeoHash(tableReaderRecord.getGeoByte(35), nextParquetRecord.get("a_geo_byte_top"));
                assertGeoHash(tableReaderRecord.getGeoShort(36), nextParquetRecord.get("a_geo_short_top"));
                assertGeoHash(tableReaderRecord.getGeoInt(37), nextParquetRecord.get("a_geo_int_top"));
                assertGeoHash(tableReaderRecord.getGeoLong(38), nextParquetRecord.get("a_geo_long_top"));
                assertNullableString(tableReaderRecord.getStrA(39), nextParquetRecord.get("a_string_top"));
                assertBinary(tableReaderRecord.getBin(40), nextParquetRecord.get("a_bin_top"));
                assertVarchar(tableReaderRecord.getVarcharA(41), nextParquetRecord.get("a_varchar_top"));
                if (rawArrayEncoding) {
                    assertRawArray(tableReaderRecord.getArray(42, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array_top"));
                } else {
                    assertArray(tableReaderRecord.getArray(42, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1)), nextParquetRecord.get("an_array_top"));
                }
                assertPrimitiveValue(tableReaderRecord.getIPv4(43), nextParquetRecord.get("a_ip_top"), Numbers.IPv4_NULL);
                assertUuid(sink, tableReaderRecord.getLong128Lo(44), tableReaderRecord.getLong128Hi(44), nextParquetRecord.get("a_uuid_top"));
                assertLong128(tableReaderRecord.getLong128Lo(45), tableReaderRecord.getLong128Hi(45), nextParquetRecord.get("a_long128_top"));
                assertLong256(tableReaderRecord.getLong256A(46), nextParquetRecord.get("a_long256_top"));
                assertPrimitiveValue(tableReaderRecord.getDate(47), nextParquetRecord.get("a_date_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(48), nextParquetRecord.get("a_ts_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(49), nextParquetRecord.get("a_ns_top"), Long.MIN_VALUE);
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
            Assert.assertEquals(50, schema.getColumns().size());

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
            assertSchemaNullable(columns.get(23), "a_ts", PrimitiveType.PrimitiveTypeName.INT64);
            // designated ts is non-nullable
            assertSchemaNullable(columns.get(24), "a_ns", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNonNullable(columns.get(25), "designated_ts", PrimitiveType.PrimitiveTypeName.INT64);

            assertSchemaNonNullable(columns.get(26), "a_boolean_top", PrimitiveType.PrimitiveTypeName.BOOLEAN);
            assertSchemaNonNullable(columns.get(27), "a_byte_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(28), "a_short_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNonNullable(columns.get(29), "a_char_top", PrimitiveType.PrimitiveTypeName.INT32);

            assertSchemaNullable(columns.get(30), "an_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(31), "a_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(32), "a_float_top", PrimitiveType.PrimitiveTypeName.FLOAT);
            assertSchemaNullable(columns.get(33), "a_double_top", PrimitiveType.PrimitiveTypeName.DOUBLE);
            assertSchemaNullable(columns.get(34), "a_symbol_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(35), "a_geo_byte_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(36), "a_geo_short_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(37), "a_geo_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(38), "a_geo_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(39), "a_string_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(40), "a_bin_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(41), "a_varchar_top", PrimitiveType.PrimitiveTypeName.BINARY);
            if (rawArrayEncoding) {
                assertSchemaNullable(columns.get(42), "an_array_top", PrimitiveType.PrimitiveTypeName.BINARY);
            } else {
                assertSchemaArray(columns.get(42), "an_array_top", 1, 3);
            }
            assertSchemaNullable(columns.get(43), "a_ip_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(44), "a_uuid_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(45), "a_long128_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(46), "a_long256_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(47), "a_date_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(48), "a_ts_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(49), "a_ns_top", PrimitiveType.PrimitiveTypeName.INT64);

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
                // an_int_top
                assertMinMaxRange(chunks, 30, NUMERIC_MIN, NUMERIC_MAX);
                // a_long_top
                assertMinMaxRange(chunks, 31, (long) NUMERIC_MIN, (long) NUMERIC_MAX);
                // a_float_top
                assertMinMaxRange(chunks, 32, 0.0f, 1.0f);
                // a_double_top
                assertMinMaxRange(chunks, 33, 0.0d, 1.0d);
            }
            Assert.assertEquals(rowCount, rows);
        }
    }
}
