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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
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
    public void testAllTypes() throws Exception {
        final String tableName = "x";
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
                " rnd_ipv4() a_ip," +
                " rnd_uuid4() a_uuid," +
                " rnd_long256() a_long256," +
                " to_long128(rnd_long(), rnd_long()) a_long128," +
                " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                " timestamp_sequence(500000000000, 600) a_ts," +
                " timestamp_sequence(400000000000, 500) designated_ts" +
                " from long_sequence(" + INITIAL_ROWS + ")) timestamp(designated_ts) partition by month";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().compile(ddl); // txn 1

            serverMain.getEngine().compile("alter table " + tableName + " add column an_int_top int");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_long_top long");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_float_top float");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_double_top double");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_symbol_top symbol");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_geo_byte_top geohash(4b)");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_geo_short_top geohash(8b)");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_geo_int_top geohash(16b)");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_geo_long_top geohash(32b)");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_string_top string");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_bin_top binary");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_varchar_top varchar");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_ip_top ipv4");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_uuid_top uuid");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_long128_top long128");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_long256_top long256");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_date_top date");
            serverMain.getEngine().compile("alter table " + tableName + " add column a_ts_top timestamp");

            String insert = "insert into " + tableName + "(id, an_int_top, a_long_top, a_float_top, a_double_top,\n" +
                    " a_symbol_top, a_geo_byte_top, a_geo_short_top, a_geo_int_top, a_geo_long_top,\n" +
                    " a_string_top, a_bin_top, a_varchar_top, a_ip_top, a_uuid_top, a_long128_top, a_long256_top,\n" +
                    " a_date_top, a_ts_top, designated_ts) select\n" +
                    " " + INITIAL_ROWS + " + x," +
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
                    " timestamp_sequence(1600000000000, 500)" +
                    " from long_sequence(" + UPDATE_ROWS + ");";

            serverMain.getEngine().compile(insert); // txn 20

            serverMain.awaitTxn("x", 20);

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
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, partitionIndex);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        (5L << 32) | PartitionEncoder.COMPRESSION_ZSTD,
                        true,
                        ROW_GROUP_SIZE,
                        DATA_PAGE_SIZE,
                        PartitionEncoder.PARQUET_VERSION_V1
                );

                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();
                long partitionRowCount = reader.getPartitionRowCount(partitionIndex);
                Configuration configuration = new Configuration();
                final org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(parquetPathStr);
                final InputFile inputFile = HadoopInputFile.fromPath(parquetPath, configuration);
                validateParquetData(inputFile, reader, partitionRowCount);
                validateParquetMetadata(inputFile, partitionRowCount);
            }
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
            Assert.assertEquals(expectedLo, Long.MIN_VALUE);
            Assert.assertEquals(expectedHi, Long.MIN_VALUE);
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

    private static void assertSchema(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType, int maxDefinitionLevel) {
        Assert.assertEquals(descriptor.getPath()[0], expectedName);
        Assert.assertEquals(descriptor.getPrimitiveType().getPrimitiveTypeName(), expectedType);
        Assert.assertEquals(descriptor.getMaxRepetitionLevel(), 0);
        Assert.assertEquals(descriptor.getMaxDefinitionLevel(), maxDefinitionLevel);
    }

    private static void assertSchemaNonNullable(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType) {
        assertSchema(descriptor, expectedName, expectedType, 0);
    }

    private static void assertSchemaNullable(ColumnDescriptor descriptor, String expectedName, PrimitiveType.PrimitiveTypeName expectedType) {
        assertSchema(descriptor, expectedName, expectedType, 1);
    }

    private static void assertUuid(StringSink sink, long expectedLo, long expectedHi, Object actual) {
        if (actual == null) {
            Assert.assertEquals(expectedLo, Long.MIN_VALUE);
            Assert.assertEquals(expectedHi, Long.MIN_VALUE);
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

    private void validateParquetData(InputFile inputFile, TableReader tableReader, long rows) throws IOException {
        try (final ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            StringSink sink = new StringSink();
            long actualRows = 0;

            final RecordCursor cursor = tableReader.getCursor();
            final Record tableReaderRecord = cursor.getRecord();

            GenericRecord nextParquetRecord;
            while (cursor.hasNext()) {
                nextParquetRecord = parquetReader.read();
                Assert.assertNotNull(nextParquetRecord);

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
                assertPrimitiveValue(tableReaderRecord.getIPv4(17), nextParquetRecord.get("a_ip"), Numbers.IPv4_NULL);

                long uuidLo = tableReaderRecord.getLong128Lo(18);
                long uuidHi = tableReaderRecord.getLong128Hi(18);
                assertUuid(sink, uuidLo, uuidHi, nextParquetRecord.get("a_uuid"));

                assertLong256(tableReaderRecord.getLong256A(19), nextParquetRecord.get("a_long256"));

                assertLong128(tableReaderRecord.getLong128Lo(20), tableReaderRecord.getLong128Hi(20), nextParquetRecord.get("a_long128"));

                assertPrimitiveValue(tableReaderRecord.getDate(21), nextParquetRecord.get("a_date"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(22), nextParquetRecord.get("a_ts"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(23), nextParquetRecord.get("designated_ts"), Long.MIN_VALUE);

                // column tops
                assertPrimitiveValue(tableReaderRecord.getInt(24), nextParquetRecord.get("an_int_top"), Integer.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getLong(25), nextParquetRecord.get("a_long_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getFloat(26), nextParquetRecord.get("a_float_top"), Float.NaN);
                assertPrimitiveValue(tableReaderRecord.getDouble(27), nextParquetRecord.get("a_double_top"), Double.NaN);
                assertNullableString(tableReaderRecord.getSymA(28), nextParquetRecord.get("a_symbol_top"));
                assertGeoHash(tableReaderRecord.getGeoByte(29), nextParquetRecord.get("a_geo_byte_top"));
                assertGeoHash(tableReaderRecord.getGeoShort(30), nextParquetRecord.get("a_geo_short_top"));
                assertGeoHash(tableReaderRecord.getGeoInt(31), nextParquetRecord.get("a_geo_int_top"));
                assertGeoHash(tableReaderRecord.getGeoLong(32), nextParquetRecord.get("a_geo_long_top"));
                assertNullableString(tableReaderRecord.getStrA(33), nextParquetRecord.get("a_string_top"));
                assertBinary(tableReaderRecord.getBin(34), nextParquetRecord.get("a_bin_top"));
                assertVarchar(tableReaderRecord.getVarcharA(35), nextParquetRecord.get("a_varchar_top"));
                assertPrimitiveValue(tableReaderRecord.getIPv4(36), nextParquetRecord.get("a_ip_top"), Numbers.IPv4_NULL);
                assertUuid(sink, tableReaderRecord.getLong128Lo(37), tableReaderRecord.getLong128Hi(37), nextParquetRecord.get("a_uuid_top"));
                assertLong128(tableReaderRecord.getLong128Lo(38), tableReaderRecord.getLong128Hi(38), nextParquetRecord.get("a_long128_top"));
                assertLong256(tableReaderRecord.getLong256A(39), nextParquetRecord.get("a_long256_top"));
                assertPrimitiveValue(tableReaderRecord.getDate(40), nextParquetRecord.get("a_date_top"), Long.MIN_VALUE);
                assertPrimitiveValue(tableReaderRecord.getTimestamp(41), nextParquetRecord.get("a_ts_top"), Long.MIN_VALUE);
            }
            Assert.assertEquals(rows, actualRows);
        }
    }

    private void validateParquetMetadata(InputFile inputFile, long rows) throws IOException {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = parquetFileReader.getFooter();
            FileMetaData fileMetaData = metadata.getFileMetaData();
            Assert.assertEquals(fileMetaData.getCreatedBy(), "QuestDB version 8.0");

            MessageType schema = fileMetaData.getSchema();
            List<ColumnDescriptor> columns = schema.getColumns();
            Assert.assertEquals(schema.getColumns().size(), 42);

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
            assertSchemaNullable(columns.get(17), "a_ip", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(18), "a_uuid", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(19), "a_long256", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(20), "a_long128", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(21), "a_date", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(22), "a_ts", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(23), "designated_ts", PrimitiveType.PrimitiveTypeName.INT64);

            assertSchemaNullable(columns.get(24), "an_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(25), "a_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(26), "a_float_top", PrimitiveType.PrimitiveTypeName.FLOAT);
            assertSchemaNullable(columns.get(27), "a_double_top", PrimitiveType.PrimitiveTypeName.DOUBLE);
            assertSchemaNullable(columns.get(28), "a_symbol_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(29), "a_geo_byte_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(30), "a_geo_short_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(31), "a_geo_int_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(32), "a_geo_long_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(33), "a_string_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(34), "a_bin_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(35), "a_varchar_top", PrimitiveType.PrimitiveTypeName.BINARY);
            assertSchemaNullable(columns.get(36), "a_ip_top", PrimitiveType.PrimitiveTypeName.INT32);
            assertSchemaNullable(columns.get(37), "a_uuid_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(38), "a_long128_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(39), "a_long256_top", PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
            assertSchemaNullable(columns.get(40), "a_date_top", PrimitiveType.PrimitiveTypeName.INT64);
            assertSchemaNullable(columns.get(41), "a_ts_top", PrimitiveType.PrimitiveTypeName.INT64);

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
                assertMinMaxRange(chunks, 24, NUMERIC_MIN, NUMERIC_MAX);
                // a_long_top
                assertMinMaxRange(chunks, 25, (long) NUMERIC_MIN, (long) NUMERIC_MAX);
                // a_float_top
                assertMinMaxRange(chunks, 26, 0.0f, 1.0f);
                // a_double_top
                assertMinMaxRange(chunks, 27, 0.0d, 1.0d);
            }
            Assert.assertEquals(rowCount, rows);
        }
    }
}
