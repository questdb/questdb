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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionDeltaStats;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameSource;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.PartitionFrameStateFactory;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** Tests TableReader's consumer contract; Rust catalog tests cover actual snapshot selection and pins. */
public class TableReaderSourceTest extends AbstractCairoTest {
    @Test
    public void testNativeSourceBinding() throws Exception {
        assertSourceBinding(false);
    }

    @Test
    public void testParquetSourceBinding() throws Exception {
        assertSourceBinding(true);
    }

    private static void assertValues(TableReader reader, long... expected) {
        Assert.assertEquals(expected.length, reader.openPartition(0));
        Assert.assertEquals(expected.length, reader.getPartitionRowCountFromMetadata(0));
        Assert.assertEquals(expected.length, reader.getLogicalRowCount());
        if (reader.getPartitionFormat(0) == PartitionFormat.NATIVE) {
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals(expected[i], reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(0), 0)).getLong(i * Long.BYTES));
            }
        } else {
            ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(0);
            try (RowGroupBuffers buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                 DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT)) {
                columns.add(0);
                columns.add(ColumnType.LONG);
                int offset = 0;
                for (int group = 0; group < decoder.metadata().getRowGroupCount(); group++) {
                    int count = Math.toIntExact(decoder.metadata().getRowGroupSize(group));
                    Assert.assertEquals(count, decoder.decodeRowGroup(buffers, columns, group, 0, count));
                    for (int i = 0; i < count; i++) {
                        Assert.assertEquals(expected[offset++], Unsafe.getLong(buffers.getChunkDataPtr(0) + (long) i * Long.BYTES));
                    }
                }
                Assert.assertEquals(expected.length, offset);
            }
        }
    }

    private void assertSourceBinding(boolean isParquet) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE source_a (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO source_a VALUES (1, '1970-01-01T00:00:00')");
            TableToken token = engine.verifyTableName("source_a");
            Source a;
            try (TableReader reader = engine.getReader(token)) {
                a = new Source(PartitionFrameSource.NATIVE, reader.getTxFile().getPartitionNameTxn(0),
                        reader.getColumnVersionReader().getVersion(), 0, -1, 1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            try (TableWriter writer = newOffPoolWriter(configuration, "source_a")) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(0);
                writer.getTxWriter().setPartitionHasDelta(0, true);
                writer.bumpPartitionTableVersion();
            }
            // Create candidates after closing the writer, which purges unreferenced partition directories on open.
            Source b = createSource(token, 100, a.columnVersion, isParquet, 11);
            Source c = createSource(token, 101, a.columnVersion, !isParquet, 21);
            AtomicReference<Source> selected = new AtomicReference<>(a);
            CairoConfiguration bindingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public PartitionFrameStateFactory newPartitionFrameStateFactory(TableToken ignored) {
                    return new SourceFactory(selected);
                }
            };
            try (TableReader old = newOffPoolReader(bindingConfiguration, "source_a")) {
                assertValues(old, 1);
                selected.set(b);
                try (TableReader replacement = newOffPoolReader(bindingConfiguration, "source_a")) {
                    Assert.assertEquals(isParquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE,
                            replacement.getPartitionFormatFromMetadata(0));
                    Assert.assertEquals(3, replacement.getPartitionRowCountFromMetadata(0));
                    java.nio.file.Path directory = java.nio.file.Path.of(configuration.getDbRoot(), token.getDirName(), "1970-01-01.100");
                    java.nio.file.Path payload = isParquet ? directory.resolve("data.parquet") : directory;
                    java.nio.file.Path hidden = payload.resolveSibling("hidden");
                    Files.move(payload, hidden);
                    try {
                        CairoException error = Assert.assertThrows(CairoException.class, () -> replacement.openPartition(0));
                        TestUtils.assertContains(error.getFlyweightMessage(), payload.toString());
                        Assert.assertEquals(-1, replacement.getPartitionRowCount(0));
                    } finally {
                        Files.move(hidden, payload);
                    }
                    assertValues(replacement, 11, 12, 13);
                    selected.set(c);
                    try (TableReader current = newOffPoolReader(bindingConfiguration, "source_a")) {
                        assertValues(current, 21, 22, 23);
                        assertValues(old, 1);
                        assertValues(replacement, 11, 12, 13);
                        old.goPassive();
                        Assert.assertEquals(-1, old.getPartitionRowCount(0));
                        old.goActive();
                        assertValues(old, 21, 22, 23);
                    }
                }
            }
        });
    }

    private Source createSource(TableToken target, long nameTxn, long columnVersion, boolean isParquet, long first) throws Exception {
        String name = "source_" + nameTxn;
        execute("CREATE TABLE " + name + " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO " + name + " VALUES (" + first + ", '1970-01-01T00:00:00'), ("
                + (first + 1) + ", '1970-01-01T01:00:00'), (" + (first + 2) + ", '1970-01-01T02:00:00'), (0, '1970-01-02T00:00:00')");
        if (isParquet) {
            execute("ALTER TABLE " + name + " CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
        }
        TableToken fixture = engine.verifyTableName(name);
        try (TableReader reader = engine.getReader(fixture);
             Path source = new Path().of(configuration.getDbRoot()).concat(fixture);
             Path destination = new Path().of(configuration.getDbRoot()).concat(target)) {
            Assert.assertEquals(3, reader.openPartition(0));
            Assert.assertEquals(isParquet, reader.getTxFile().isPartitionParquet(0));
            TableUtils.setPathForNativePartition(source, ColumnType.TIMESTAMP, PartitionBy.DAY, 0, reader.getTxFile().getPartitionNameTxn(0));
            TableUtils.setPathForNativePartition(destination, ColumnType.TIMESTAMP, PartitionBy.DAY, 0, nameTxn);
            TestUtils.copyDirectory(source, destination, configuration.getMkDirMode());
            if (isParquet) {
                java.nio.file.Path metadata = java.nio.file.Path.of(destination.toString(), "_pm");
                byte[] bytes = Files.readAllBytes(metadata);
                int committed = Math.toIntExact(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong());
                byte[] prefix = Arrays.copyOf(bytes, committed);
                // A selected prefix ignores the mutable header's newer length and does not rescan its CRC.
                prefix[committed - 8] ^= 1;
                ByteBuffer.wrap(prefix).order(ByteOrder.LITTLE_ENDIAN).putLong(committed + 128);
                Files.delete(metadata);
                metadata = metadata.resolveSibling("_pm.b");
                Files.write(metadata, prefix);
                Files.write(metadata, new byte[128], StandardOpenOption.APPEND);
                return new Source(PartitionFrameSource.PARQUET, nameTxn, -1, committed, 1, 3);
            }
            return new Source(PartitionFrameSource.NATIVE, nameTxn, columnVersion, 0, -1, 3);
        }
    }

    private record Captured(Source source, TableReader reader, int partitionIndex) {
    }

    private record Source(int kind, long nameTxn, long columnVersion, long metadataBytes, int metadataSlot, long rows) {
    }

    private static class SourceFactory implements PartitionFrameStateFactory {
        // The existing PartitionFrameState ABI has six longs; these tests use its row-count fields only.
        private static final long HEADER_BYTES = 6L * Long.BYTES;
        private final AtomicReference<Source> selected;
        private final Map<Long, Captured> states = new HashMap<>();

        private SourceFactory(AtomicReference<Source> selected) {
            this.selected = selected;
        }

        @Override
        public void bind(long state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            Assert.assertTrue(states.isEmpty());
        }

        @Override
        public void destroy(long state) {
            Captured captured = states.remove(state);
            try {
                Assert.assertNotNull(captured);
                Assert.assertEquals("mappings must close before the source pin is released", -1,
                        captured.reader.getPartitionRowCount(captured.partitionIndex));
            } finally {
                Unsafe.free(state, HEADER_BYTES, MemoryTag.NATIVE_DEFAULT);
            }
        }

        @Override
        public long getLogicalRowCount(long state) {
            return PartitionFrameState.getBasePartitionRowCount(state);
        }

        @Override
        public long open(TableReader reader, int partitionIndex, long readerSeqTxn) {
            Source source = selected.get();
            long state = Unsafe.calloc(HEADER_BYTES, MemoryTag.NATIVE_DEFAULT);
            Unsafe.putLong(state + 2L * Long.BYTES, source.rows);
            Unsafe.putLong(state + 5L * Long.BYTES, source.rows);
            states.put(state, new Captured(source, reader, partitionIndex));
            return state;
        }

        @Override
        public long openDetached(Path partitionPath, long readerSeqTxn) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readSource(long state, PartitionFrameSource target) {
            Source source = states.get(state).source;
            target.of(source.kind, source.nameTxn, source.columnVersion, source.metadataBytes, source.metadataSlot);
        }

        @Override
        public void readStats(long state, PartitionDeltaStats target) {
            target.of(getLogicalRowCount(state), Long.MAX_VALUE, Long.MIN_VALUE);
        }
    }
}
