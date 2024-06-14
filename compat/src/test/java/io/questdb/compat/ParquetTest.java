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
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Uuid;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ParquetTest extends AbstractTest {
    private final static Log LOG = LogFactory.getLog(ParquetTest.class);

    @Test
    public void testAllTypes() throws Exception {

        final long rows = 1000;

        String ddl = "create table x as (select" +
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
                " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month";

        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            serverMain.getEngine().compile(ddl);
            serverMain.awaitTxn("x", 1);

            final String parquetPathStr;
            try (
                    Path path = new Path();
                    PartitionEncoder partitionEncoder = new PartitionEncoder();
                    TableReader reader = serverMain.getEngine().getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                parquetPathStr = path.toString();
                long start = System.nanoTime();
                partitionEncoder.encode(reader, 0, path);
                LOG.info().$("Took: ").$((System.nanoTime() - start) / 1_000_000).$("ms").$();

                Configuration configuration = new Configuration();
                final org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(parquetPathStr);
                final InputFile inputFile = HadoopInputFile.fromPath(parquetPath, configuration);
                validateParquetData(inputFile, reader, rows);
                validateParquetMetadata(inputFile, rows);
            }
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
                Assert.assertEquals((int)tableReaderRecord.getByte(2), nextParquetRecord.get("a_byte"));
                Assert.assertEquals((int)tableReaderRecord.getShort(3), nextParquetRecord.get("a_short"));
                Assert.assertEquals((int)tableReaderRecord.getChar(4), nextParquetRecord.get("a_char"));
                Assert.assertEquals(tableReaderRecord.getInt(5), nextParquetRecord.get("an_int"));
                Assert.assertEquals(tableReaderRecord.getLong(6), nextParquetRecord.get("a_long"));
                Assert.assertEquals(tableReaderRecord.getFloat(7), nextParquetRecord.get("a_float"));
                Assert.assertEquals(tableReaderRecord.getDouble(8), nextParquetRecord.get("a_double"));
                Assert.assertEquals(tableReaderRecord.getSymA(9), nextParquetRecord.get("a_symbol").toString());
                Assert.assertEquals((int)tableReaderRecord.getGeoByte(10), nextParquetRecord.get("a_geo_byte"));
                Assert.assertEquals((int)tableReaderRecord.getGeoShort(11), nextParquetRecord.get("a_geo_short"));
                Assert.assertEquals(tableReaderRecord.getGeoInt(12), nextParquetRecord.get("a_geo_int"));
                Assert.assertEquals(tableReaderRecord.getGeoLong(13), nextParquetRecord.get("a_geo_long"));
                Assert.assertEquals(tableReaderRecord.getStrA(14), nextParquetRecord.get("a_string").toString());

                ByteBuffer buffer = (ByteBuffer) nextParquetRecord.get("a_bin");
                BinarySequence binarySequence = tableReaderRecord.getBin(15);
                Assert.assertEquals(binarySequence.length(), buffer.remaining());
                for (int i = 0; i < binarySequence.length(); i++) {
                    Assert.assertEquals(binarySequence.byteAt(i), buffer.get());
                }

                Assert.assertEquals(Utf8s.toString(tableReaderRecord.getVarcharA(16)), nextParquetRecord.get("a_varchar").toString());
                Assert.assertEquals(tableReaderRecord.getIPv4(17), nextParquetRecord.get("a_ip"));

                long uuidLo = tableReaderRecord.getLong128Lo(18);
                long uuidHi = tableReaderRecord.getLong128Hi(18);
                Uuid uuid = new Uuid(uuidLo, uuidHi);
                sink.clear();
                uuid.toSink(sink);
                Assert.assertEquals(sink.toString(), nextParquetRecord.get("a_uuid").toString());

                Long256 expectedLong256 = tableReaderRecord.getLong256A(19);
                GenericData.Fixed long256 = (GenericData.Fixed) nextParquetRecord.get("a_long256");
                ByteBuffer long256Buf = ByteBuffer.wrap(long256.bytes());
                 long256Buf.order(ByteOrder.LITTLE_ENDIAN);
                Assert.assertEquals(expectedLong256.getLong0(),  long256Buf.getLong());
                Assert.assertEquals(expectedLong256.getLong1(),  long256Buf.getLong());
                Assert.assertEquals(expectedLong256.getLong2(),  long256Buf.getLong());
                Assert.assertEquals(expectedLong256.getLong3(),  long256Buf.getLong());


                GenericData.Fixed long128 = (GenericData.Fixed) nextParquetRecord.get("a_long128");
                ByteBuffer long128Buf = ByteBuffer.wrap(long128.bytes());
                long128Buf.order(ByteOrder.LITTLE_ENDIAN);
                Assert.assertEquals(tableReaderRecord.getLong128Lo(20),  long128Buf.getLong());
                Assert.assertEquals(tableReaderRecord.getLong128Hi(20),  long128Buf.getLong());

                Assert.assertEquals(tableReaderRecord.getDate(21), nextParquetRecord.get("a_date"));
                Assert.assertEquals(tableReaderRecord.getTimestamp(22), nextParquetRecord.get("a_ts"));
                Assert.assertEquals(tableReaderRecord.getTimestamp(23), nextParquetRecord.get("designated_ts"));
            }

            Assert.assertEquals(rows, actualRows);
        }
    }

    // TODO: validate metadata
    private void validateParquetMetadata(InputFile inputFile, long rows) throws IOException {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata metadata = parquetFileReader.getFooter();
            FileMetaData fileMetaData = metadata.getFileMetaData();
            Assert.assertEquals(fileMetaData.getCreatedBy(), "QuestDB");

            MessageType schema = fileMetaData.getSchema();
            schema.getColumns().forEach(columnDescriptor -> {
                columnDescriptor.getPath();
                columnDescriptor.getPrimitiveType();
                columnDescriptor.getMaxRepetitionLevel();
                columnDescriptor.getMaxDefinitionLevel();
            });

            metadata.getBlocks().forEach(blockMetaData -> {
                blockMetaData.getRowCount();
                blockMetaData.getPath();
                blockMetaData.getStartingPos();
                blockMetaData.getCompressedSize();
                blockMetaData.getTotalByteSize();
                blockMetaData.getOrdinal();
                blockMetaData.getRowIndexOffset();

                blockMetaData.getColumns().forEach(columnChunkMetaData -> {
                    columnChunkMetaData.getCodec();
                    columnChunkMetaData.getEncodingStats();
                    columnChunkMetaData.getStatistics();
                    columnChunkMetaData.getValueCount();
                    columnChunkMetaData.hasDictionaryPage();
                    columnChunkMetaData.getTotalSize();
                    columnChunkMetaData.getEncodings();
                });
                Assert.assertEquals(blockMetaData.getRowCount(), rows);
            });
        }
    }
}
