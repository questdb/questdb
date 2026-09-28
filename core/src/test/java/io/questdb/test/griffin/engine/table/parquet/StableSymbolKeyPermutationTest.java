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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.engine.table.parquet.OwnedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.StableSymbolKeyPermutation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class StableSymbolKeyPermutationTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(StableSymbolKeyPermutationTest.class);

    @Test
    public void testGatherAllParquetTypesAndUnevenTops() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
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
                    " case when x % 4 = 0 then null when x % 4 = 1 then 'a' when x % 4 = 2 then 'b' else 'c' end::symbol k," +
                    " rnd_geohash(4) a_geo_byte," +
                    " rnd_geohash(8) a_geo_short," +
                    " rnd_geohash(16) a_geo_int," +
                    " rnd_geohash(32) a_geo_long," +
                    " rnd_str('hello', 'world', null) a_string," +
                    " rnd_bin(2, 12, 1) a_bin," +
                    " rnd_varchar('ганьба','ascii',null) a_varchar," +
                    " rnd_ipv4() a_ip," +
                    " rnd_uuid4() a_uuid," +
                    " rnd_long256() a_long256," +
                    " to_long128(rnd_long(), rnd_long()) a_long128," +
                    " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                    " timestamp_sequence(500000000000, 600) a_ts," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(12)) timestamp(designated_ts) partition by month");
            execute("alter table x add column top_int int");
            execute("alter table x add column top_string string");
            execute("alter table x add column top_varchar varchar");
            execute("alter table x add column top_bin binary");
            execute("insert into x select" +
                    " x + 12," +
                    " rnd_boolean(), rnd_byte(), rnd_short(), rnd_char(), rnd_int(), rnd_long(), rnd_float(), rnd_double()," +
                    " (case when x % 4 = 0 then null when x % 4 = 1 then 'a' when x % 4 = 2 then 'b' else 'c' end)::symbol," +
                    " rnd_geohash(4), rnd_geohash(8), rnd_geohash(16), rnd_geohash(32)," +
                    " rnd_str('hello', 'world', null), rnd_bin(2, 12, 1), rnd_varchar('ганьба','ascii',null)," +
                    " rnd_ipv4(), rnd_uuid4(), rnd_long256(), to_long128(rnd_long(), rnd_long())," +
                    " cast(timestamp_sequence(600000008400, 700) as date)," +
                    " timestamp_sequence(500000007200, 600), timestamp_sequence(400000006000, 500)," +
                    " case when x % 3 = 0 then null else x::int end," +
                    " case when x % 3 = 0 then null else 'top-' || x end," +
                    " case when x % 3 = 0 then null else 'верх-' || x end," +
                    " rnd_bin(2, 12, 1)" +
                    " from long_sequence(12)");

            try (
                    Path path = new Path();
                    PartitionDescriptor source = new PartitionDescriptor();
                    OwnedMemoryPartitionDescriptor gathered = new OwnedMemoryPartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                PartitionEncoder.populateFromTableReader(reader, source, 0);
                final int keyColumn = reader.getMetadata().getColumnIndexQuiet("k");
                final int keySpaceSize = reader.getSymbolMapReader(keyColumn).getSymbolCount() + 1;
                try (StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(
                        source.getColumnAddress(keyColumn),
                        source.getColumnTop(keyColumn),
                        source.getPartitionRowCount(),
                        keySpaceSize,
                        5
                )) {
                    permutation.gather(source, gathered);
                    final long gatheredKeys = gathered.getColumnAddress(keyColumn);
                    for (long row = 0; row < source.getPartitionRowCount(); row++) {
                        final long sourceRow = permutation.getSourceRow(row);
                        Assert.assertEquals(
                                Unsafe.getInt(source.getColumnAddress(keyColumn) + sourceRow * Integer.BYTES),
                                Unsafe.getInt(gatheredKeys + row * Integer.BYTES)
                        );
                    }
                    path.of(root).concat("clustered-gather.parquet").$();
                    PartitionEncoder.encode(gathered, path);
                    path.of(root).concat("clustered.parquet").$();
                    PartitionEncoder.encode(source, path, permutation);
                }
            }

            final String columns = "id,a_boolean,a_byte,a_short,a_char,an_int,a_long,a_float,a_double," +
                    "a_geo_byte,a_geo_short,a_geo_int,a_geo_long,a_string,a_bin,a_varchar,a_ip,a_uuid," +
                    "a_long256,a_long128,a_date,a_ts,designated_ts,top_int,top_string,top_varchar,top_bin";
            final String expected = "select " + columns + " from x order by id % 4, designated_ts";
            assertSqlCursors(expected, "select " + columns + " from read_parquet('clustered-gather.parquet')");
            assertSqlCursors(expected, "select " + columns + " from read_parquet('clustered.parquet')");
            assertSqlCursors(
                    "select cast(coalesce(k, 'NULL') as varchar) k from x order by id % 4, designated_ts",
                    "select cast(coalesce(k, 'NULL') as varchar) k from read_parquet('clustered.parquet')"
            );
            assertRowGroupSizes("clustered.parquet", 5, 1, 5, 1, 5, 1, 5, 1);
        });
    }

    @Test
    public void testGatherFailureReleasesPartiallyBuiltDescriptor() {
        final long keyAddress = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        final long valueAddress = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putInt(keyAddress, 0);
            Unsafe.putLong(valueAddress, 42);
            try (
                    StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(keyAddress, 0, 1, 2, 10);
                    PartitionDescriptor source = new PartitionDescriptor();
                    OwnedMemoryPartitionDescriptor destination = new OwnedMemoryPartitionDescriptor()
            ) {
                source.of("x", 1, -1);
                source.addColumn("value", ColumnType.LONG, 0, 0, valueAddress, Long.BYTES, 0, 0, 0, 0, 0);
                source.addColumn("unsupported", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1, true), 1, 0, 0, 0, 0, 0, 0, 0, 0);
                final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3);
                try {
                    permutation.gather(source, destination);
                    Assert.fail();
                } catch (Throwable th) {
                    TestUtils.assertContains(th.getMessage(), "unsupported clustered gather type");
                }
                Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));
            }
        } finally {
            Unsafe.free(valueAddress, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(keyAddress, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testEmptyPermutationEncoding() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("create table x (k symbol, v int, ts timestamp) timestamp(ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x");
                    StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(0, 0, 0, 1, 10)
            ) {
                path.of(root).concat("empty-clustered.parquet").$();
                PartitionEncoder.populateEmptyPartition(reader, descriptor);
                PartitionEncoder.encode(descriptor, path, permutation);
            }
            assertQuery("select count() count from read_parquet('empty-clustered.parquet')")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n0\n");
        });
    }

    @Test
    public void testEncodingWithoutPermutationIsByteStable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x::int v, timestamp_sequence(0, 1000) ts from long_sequence(20)) " +
                    "timestamp(ts) partition by month");
            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("plain-a.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);
                path.of(root).concat("plain-b.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        descriptor,
                        path,
                        0,
                        true,
                        false,
                        0,
                        0,
                        1,
                        0.0
                );
            }
            Assert.assertArrayEquals(
                    java.nio.file.Files.readAllBytes(java.nio.file.Path.of(root.toString(), "plain-a.parquet")),
                    java.nio.file.Files.readAllBytes(java.nio.file.Path.of(root.toString(), "plain-b.parquet"))
            );
        });
    }

    @Test
    public void testNativeStreamingPermutation() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            final long keyAddress = Unsafe.malloc(4L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            final long valueAddress = Unsafe.malloc(4L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            long writer = 0;
            try (
                    DirectUtf8Sink names = new DirectUtf8Sink(8);
                    DirectLongList metadata = new DirectLongList(3, MemoryTag.NATIVE_DEFAULT);
                    DirectLongList columnData = new DirectLongList(7, MemoryTag.NATIVE_DEFAULT)
            ) {
                Unsafe.putInt(keyAddress, 1);
                Unsafe.putInt(keyAddress + Integer.BYTES, 0);
                Unsafe.putInt(keyAddress + 2L * Integer.BYTES, 1);
                Unsafe.putInt(keyAddress + 3L * Integer.BYTES, 0);
                Unsafe.putInt(valueAddress, 10);
                Unsafe.putInt(valueAddress + Integer.BYTES, 20);
                Unsafe.putInt(valueAddress + 2L * Integer.BYTES, 30);
                Unsafe.putInt(valueAddress + 3L * Integer.BYTES, 40);

                names.put("v");
                metadata.add(1);
                metadata.add(ColumnType.INT & 0xFFFFFFFFL);
                metadata.add(0);
                columnData.add(0);
                columnData.add(valueAddress);
                columnData.add(4L * Integer.BYTES);
                columnData.add(0);
                columnData.add(0);
                columnData.add(0);
                columnData.add(0);

                writer = PartitionEncoder.createUnorderedStreamingParquetWriter(
                        Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER),
                        1,
                        names.ptr(),
                        names.size(),
                        metadata.getAddress(),
                        -1,
                        false,
                        0,
                        true,
                        false,
                        0,
                        0,
                        1,
                        0,
                        0,
                        0.01,
                        0.0
                );
                try (StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(
                        keyAddress, 0, 4, 3, 10
                )) {
                    Assert.assertEquals(
                            0,
                            PartitionEncoder.writeStreamingParquetChunkWithPermutation(
                                    writer,
                                    columnData.getAddress(),
                                    4,
                                    permutation
                            )
                    );
                }

                final long buffer = PartitionEncoder.finishStreamingParquetWrite(writer);
                final int size = Math.toIntExact(Unsafe.getLong(buffer));
                final byte[] bytes = new byte[size];
                for (int i = 0; i < size; i++) {
                    bytes[i] = Unsafe.getByte(buffer + 16L + i);
                }
                java.nio.file.Files.write(java.nio.file.Path.of(root.toString(), "stream-permuted.parquet"), bytes);
            } finally {
                if (writer != 0) {
                    PartitionEncoder.closeStreamingParquetWriter(writer);
                }
                Unsafe.free(valueAddress, 4L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(keyAddress, 4L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            assertQuery("select v from read_parquet('stream-permuted.parquet')")
                    .expectSize()
                    .returns("v\n20\n40\n10\n30\n");
        });
    }

    @Test
    public void testNullTopSparseKeysStableOrderAndPacking() {
        final long address = Unsafe.malloc(5L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            // Row zero is below the top and therefore null. The remaining raw
            // keys normalize to [5, 0, 1, 5, 1]. Keys 2 through 4 stay absent.
            Unsafe.putInt(address, 4);
            Unsafe.putInt(address + Integer.BYTES, Integer.MIN_VALUE);
            Unsafe.putInt(address + 2L * Integer.BYTES, 0);
            Unsafe.putInt(address + 3L * Integer.BYTES, 4);
            Unsafe.putInt(address + 4L * Integer.BYTES, 0);
            try (StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(address, 1, 6, 8, 2)) {
                Assert.assertArrayEquals(new long[]{0, 2, 3, 5, 1, 4}, sourceRows(permutation, 6));
                Assert.assertArrayEquals(new long[]{0, 2, 4, 4, 4, 4, 6, 6, 6}, keyOffsets(permutation));
                Assert.assertEquals(3, permutation.getRowGroupCount());
                Assert.assertArrayEquals(new long[]{0, 2, 4, 6}, rowGroupBoundaries(permutation));
                Assert.assertArrayEquals(new long[]{0, 1, 5, 8}, rowGroupFirstKeys(permutation));
                Assert.assertEquals(1, permutation.getRowGroupKeyCount(0));
                Assert.assertEquals(0, permutation.getRowGroupKeyOffset(0, 0));
                Assert.assertEquals(2, permutation.getRowGroupKeyOffset(0, 1));
            }
            try (StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(address, 1, 6, 8, 10)) {
                // One shared group spans sparse keys 0 through 5. Empty keys
                // produce repeated directory offsets.
                Assert.assertEquals(1, permutation.getRowGroupCount());
                Assert.assertEquals(6, permutation.getRowGroupKeyCount(0));
                Assert.assertArrayEquals(new long[]{0, 2, 4, 4, 4, 4, 6}, rowGroupKeyOffsets(permutation, 0));
            }
        } finally {
            Unsafe.free(address, 5L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPackingKeepsSplitHotKeyInDedicatedGroups() {
        final long address = Unsafe.malloc(6L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 5; i++) {
                Unsafe.putInt(address + (long) i * Integer.BYTES, 0);
            }
            Unsafe.putInt(address + 5L * Integer.BYTES, 1);
            try (StableSymbolKeyPermutation permutation = StableSymbolKeyPermutation.build(address, 0, 6, 3, 2)) {
                Assert.assertArrayEquals(new long[]{0, 2, 4, 5, 6}, rowGroupBoundaries(permutation));
                Assert.assertArrayEquals(new long[]{1, 1, 1, 2, 3}, rowGroupFirstKeys(permutation));
                for (int group = 0; group < permutation.getRowGroupCount(); group++) {
                    Assert.assertEquals(1, permutation.getRowGroupKeyCount(group));
                }
            }
        } finally {
            Unsafe.free(address, 6L * Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRejectsOutOfRangeKeyWithoutLeakingNativeMemory() {
        final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3);
        final long address = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.putInt(address, 99);
            try {
                StableSymbolKeyPermutation.build(address, 0, 1, 3, 10);
                Assert.fail();
            } catch (Throwable th) {
                TestUtils.assertContains(th.getMessage(), "outside clustered key space");
            }
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));
            try {
                StableSymbolKeyPermutation.build(0, Long.MAX_VALUE, Long.MAX_VALUE, 1, 1);
                Assert.fail();
            } catch (Throwable th) {
                TestUtils.assertContains(th.getMessage(), "size overflow");
            }
            Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_O3));
        } finally {
            Unsafe.free(address, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertRowGroupSizes(String fileName, long... expectedSizes) {
        final FilesFacade ff = configuration.getFilesFacade();
        long fd = -1;
        long address = 0;
        long fileSize = 0;
        try (
                Path path = new Path();
                ParquetFileDecoder decoder = new ParquetFileDecoder()
        ) {
            path.of(root).concat(fileName).$();
            fd = TableUtils.openRO(ff, path.$(), LOG);
            fileSize = ff.length(fd);
            address = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            decoder.of(address, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            Assert.assertEquals(expectedSizes.length, decoder.metadata().getRowGroupCount());
            for (int i = 0; i < expectedSizes.length; i++) {
                Assert.assertEquals(expectedSizes[i], decoder.metadata().getRowGroupSize(i));
            }
        } finally {
            if (address != 0) {
                ff.munmap(address, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                ff.close(fd);
            }
        }
    }

    private static long[] keyOffsets(StableSymbolKeyPermutation permutation) {
        final long[] values = new long[permutation.getKeySpaceSize() + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = permutation.getKeyOffset(i);
        }
        return values;
    }

    private static long[] rowGroupBoundaries(StableSymbolKeyPermutation permutation) {
        final long[] values = new long[permutation.getRowGroupCount() + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = permutation.getRowGroupBoundary(i);
        }
        return values;
    }

    private static long[] rowGroupFirstKeys(StableSymbolKeyPermutation permutation) {
        final long[] values = new long[permutation.getRowGroupCount() + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = permutation.getRowGroupFirstKey(i);
        }
        return values;
    }

    private static long[] rowGroupKeyOffsets(StableSymbolKeyPermutation permutation, int rowGroup) {
        final long[] values = new long[permutation.getRowGroupKeyCount(rowGroup) + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = permutation.getRowGroupKeyOffset(rowGroup, i);
        }
        return values;
    }

    private static long[] sourceRows(StableSymbolKeyPermutation permutation, int rowCount) {
        final long[] values = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            values[i] = permutation.getSourceRow(i);
        }
        return values;
    }
}
