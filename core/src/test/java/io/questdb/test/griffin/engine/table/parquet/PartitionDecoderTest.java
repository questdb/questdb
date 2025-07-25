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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class PartitionDecoderTest extends AbstractCairoTest {

    @Test
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            final long columns = 24;
            final long rows = 1001;
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
                    " timestamp_sequence(500000000000, 600) a_ts," +
                    " timestamp_sequence(500000000000::timestamp_ns, 600000) a_ns," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            long fd = -1;
            long addr = 0;
            long fileSize = 0;
            try (
                    Path path = new Path();
                    PartitionDecoder partitionDecoder = new PartitionDecoder();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                fd = TableUtils.openRO(ff, path.$(), LOG);
                fileSize = ff.length(fd);
                addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                partitionDecoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                Assert.assertEquals(reader.getMetadata().getColumnCount(), partitionDecoder.metadata().columnCount());
                Assert.assertEquals(rows, partitionDecoder.metadata().rowCount());
                Assert.assertEquals(1, partitionDecoder.metadata().rowGroupCount());

                TableReaderMetadata readerMeta = reader.getMetadata();
                Assert.assertEquals(readerMeta.getColumnCount(), partitionDecoder.metadata().columnCount());

                for (int i = 0; i < columns; i++) {
                    TestUtils.assertEquals("column: " + i, readerMeta.getColumnName(i), partitionDecoder.metadata().columnName(i));
                    Assert.assertEquals("column: " + i, i, partitionDecoder.metadata().columnId(i));
                    Assert.assertEquals("column: " + i, readerMeta.getColumnType(i), partitionDecoder.metadata().getColumnType(i));
                }
            } finally {
                ff.close(fd);
                ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
        });
    }

    @Test
    public void testOutOfMemory() throws Exception {
        final long rows = 10;
        final FilesFacade ff = configuration.getFilesFacade();

        // We first set up the table without memory limits.
        assertMemoryLeak(() -> execute("create table x as (select" +
                " x id," +
                " timestamp_sequence(400000000000, 500) designated_ts" +
                " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by day"));

        assertMemoryLeak(() -> {
            final long memInit = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            long fd = -1;
            long addr = 0;
            long fileSize = 0;
            try (
                    Path path = new Path();
                    RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    PartitionDecoder partitionDecoder = new PartitionDecoder()
            ) {
                path.of(root).concat("x.parquet").$();

                // Encode
                try (
                        TableReader reader = engine.getReader("x");
                        PartitionDescriptor partitionDescriptor = new PartitionDescriptor()
                ) {
                    PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                    PartitionEncoder.encode(partitionDescriptor, path);
                }

                fd = TableUtils.openRO(configuration.getFilesFacade(), path.$(), LOG);
                fileSize = ff.length(fd);
                addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                partitionDecoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                columns.add(0);
                columns.add(ColumnType.LONG);

                try {
                    // prevent more allocs
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                    partitionDecoder.decodeRowGroup(rowGroupBuffers, columns, 0, 0, 1);
                    Assert.fail("Expected CairoException for out of memory");
                } catch (CairoException e) {
                    final String msg = e.getMessage();
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(msg, "could not decode row group 0");
                    TestUtils.assertContains(msg, "memory limit exceeded when allocating");
                } finally {
                    // Reset to allow allocs again
                    Unsafe.setRssMemLimit(0);
                }

                final long memBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                partitionDecoder.decodeRowGroup(rowGroupBuffers, columns, 0, 0, 1);
                final long memAfter = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

                // Allocation happened in Rust code, associated to the `RowGroupBuffers` object.
                Assert.assertTrue(memAfter > memBefore);
            } finally {
                ff.close(fd);
                ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }

            // Freed memory is tracked.
            final long memFinal = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            Assert.assertEquals(memFinal, memInit);
        });
    }

    /**
     * Test error handling when we update a row group that does not exist.
     */
    @Test
    public void testUpdateInvalidRowGroup() throws Exception {
        final FilesFacade ff = configuration.getFilesFacade();

        // We first set up the table without memory limits.
        assertMemoryLeak(() -> {
            TableModel src = new TableModel(configuration, "x", PartitionBy.DAY);
            createPopulateTable(
                    1,
                    src.timestamp("designated_ts")
                            .col("id", ColumnType.LONG),
                    100,
                    "1970-01-05",
                    2
            ); // generate 2 partitions

            // Convert the partition to parquet via SQL.
            execute("alter table x convert partition to parquet where designated_ts >= 0");

            long fd = -1;
            long addr = 0;
            long fileSize = 0;
            try (
                    Path path = new Path();
                    RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    RowGroupStatBuffers rowGroupStatBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    PartitionDecoder partitionDecoder = new PartitionDecoder()
            ) {
                // Check that the partition directory and data.parquet file now exists on disk.
                path.of(root).concat("x~").concat("1970-01-05.1").slash$();
                Assert.assertTrue(ff.exists(path.$()));
                // Assert.assertTrue(ff.isDirOrSoftLinkDir(path.$())); -- see https://github.com/questdb/questdb/issues/5054
                path.concat("data.parquet").$();
                Assert.assertTrue(ff.exists(path.$()));
                Assert.assertFalse(ff.isDirOrSoftLinkDir(path.$()));

                // Open it up.
                fd = TableUtils.openRO(configuration.getFilesFacade(), path.$(), LOG);
                fileSize = ff.length(fd);
                addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                partitionDecoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                columns.add(0);
                columns.add(ColumnType.LONG);

                final CairoException badReadRowGroupStats = Assert.assertThrows(
                        CairoException.class,
                        () -> partitionDecoder.readRowGroupStats(rowGroupStatBuffers, columns, 1000)
                );
                TestUtils.assertContains(
                        badReadRowGroupStats.getMessage(),
                        "row group index 1000 out of range [0,1)");

                final CairoException badDecodeRowGroup = Assert.assertThrows(
                        CairoException.class,
                        () -> partitionDecoder.decodeRowGroup(rowGroupBuffers, columns, 1000, 0, 1)
                );
                TestUtils.assertContains(
                        badDecodeRowGroup.getMessage(),
                        "row group index 1000 out of range [0,1)");
            } finally {
                ff.close(fd);
                ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
        });
    }
}
