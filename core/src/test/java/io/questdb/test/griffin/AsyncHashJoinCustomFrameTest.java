/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _  _  ___  ___| |_|  _ \| __ )
 *   | | | | || |/ _ \/ __| __| | | |  _ \
 *   | |_| | __ |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionDeltaStats;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFrameDecoder;
import io.questdb.cairo.sql.PartitionFrameSource;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.PartitionFrameStateFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.jit.JitUtil;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.executeOnWorkers;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.find;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.ordinaryRows;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.plan;

public class AsyncHashJoinCustomFrameTest extends AbstractCairoTest {
    private static final int ROW_GROUP_ROWS = 12;
    private static final int SUBFRAME_ROWS = 6;

    @Test
    public void testCustomBuildFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 8);
        // With this ratio only a build made entirely of decoded frames is worth copying.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000000");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "1000000");
        assertMemoryLeak(() -> executeOnWorkers(WorkerPoolMode.FIBER_HOST, 2, null, withCustomFrames(new AtomicInteger()), LOG, (db, context) -> {
            createTables(db, context);
            final String sql = "SELECT p.ts, p.v, d.v FROM p LEFT JOIN d ON p.k = d.k ORDER BY p.ts";
            final String expected = ordinaryRows(sql, context);
            markCustom(db, "d", 2);
            for (boolean inRounds : new boolean[]{false, true}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, inRounds ? 0 : Long.MAX_VALUE);
                for (boolean copy : new boolean[]{false, true}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, copy ? 1024 * 1024 : 0);
                    try (RecordCursorFactory factory = db.select(sql, context);
                         RecordCursor cursor = factory.getCursor(context)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        Assert.assertEquals(expected, readRows(cursor, factory));
                        final HashJoinBuildStage stage = join.getAtom().getStage(0);
                        Assert.assertEquals(4, stage.getBuildFrameCount());
                        Assert.assertEquals(inRounds, stage.isBuiltInRounds());
                        Assert.assertEquals(copy, stage.isPayloadCopied());
                    }
                }
            }
        }));
    }

    @Test
    public void testCustomProbeFrames() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_PAGE_FRAME_MAX_ROWS, 2);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, ROW_GROUP_ROWS);
        final AtomicInteger remainingDecodes = new AtomicInteger();
        assertMemoryLeak(() -> executeOnWorkers(WorkerPoolMode.FIBER_HOST, 2, null, withCustomFrames(remainingDecodes), LOG, (db, context) -> {
            createTables(db, context);
            db.execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '1970-01-01'", context);
            final String sql = "SELECT p.ts, p.v, d.v FROM p LEFT JOIN d ON p.k = d.k WHERE p.v <> 70 ORDER BY p.ts";
            final String limited = sql + " LIMIT 7";
            final String expected = ordinaryRows(sql, context);
            final String expectedLimited = ordinaryRows(limited, context);
            markCustom(db, "p", 2);
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                context.setJitMode(jitMode);
                remainingDecodes.set(0);
                try (RecordCursorFactory factory = db.select(sql, context)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    Assert.assertEquals(jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported(),
                            plan(factory, context).contains("Async JIT Hash Join"));
                    AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting();
                    try (RecordCursor cursor = openWithFirstReadRows(factory, context)) {
                        Assert.assertEquals(2, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(1, join.getDispatchedFrameCount());
                        Assert.assertEquals(2, join.getAtom().getJoinedProbeRowCount());
                        final StringSink actual = new StringSink();
                        CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), actual, true);
                        Assert.assertEquals(expected, actual.put(readRows(cursor, factory)).toString());
                        Assert.assertEquals(8, join.getProbeFrameCount());
                        for (int i = 0; i < join.getProbeFrameCount(); i++) {
                            Assert.assertEquals(SUBFRAME_ROWS, join.getProbeFrameRowCount(i));
                        }
                        Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
                        Assert.assertTrue(remainingDecodes.get() > 0);
                    }
                }
                try (RecordCursorFactory factory = db.select(limited, context);
                     RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertNotNull(find(factory));
                    Assert.assertEquals(7, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
                    Assert.assertTrue(cursor.hasNext());
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(context.getCircuitBreaker(), counter);
                    Assert.assertEquals(6, counter.get());
                    Assert.assertFalse(cursor.hasNext());
                    cursor.toTop();
                    Assert.assertEquals(expectedLimited, readRows(cursor, factory));
                }
            }
        }));
    }

    private static void createTables(CairoEngine db, SqlExecutionContext context) throws Exception {
        context.setParallelHashJoinEnabled(true);
        db.execute("""
                CREATE TABLE p AS (
                    SELECT x k, x * 10 v, timestamp_sequence(0, 3600000000) ts FROM long_sequence(48)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, context);
        db.execute("""
                CREATE TABLE d AS (
                    SELECT x k, x * 100 v, timestamp_sequence(0, 7200000000) ts FROM long_sequence(24)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, context);
    }

    private static void markCustom(CairoEngine db, String table, int partitionCount) {
        db.releaseAllReaders();
        db.releaseAllWriters();
        try (TableWriter writer = TestUtils.newOffPoolWriter(db.getConfiguration(), db.verifyTableName(table), db)) {
            for (int i = 0; i < partitionCount; i++) {
                writer.getTxWriter().setPartitionDeltaActiveByTimestamp(writer.getTxWriter().getPartitionTimestampByIndex(i));
                writer.getTxWriter().setPartitionHasDelta(i, true);
            }
            writer.bumpPartitionTableVersion();
        }
    }

    private static RecordCursor openWithFirstReadRows(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        context.setFirstReadRows(2);
        try {
            return factory.getCursor(context);
        } finally {
            context.setFirstReadRows(Long.MAX_VALUE);
        }
    }

    private static String readRows(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        while (cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink, true);
        }
        return sink.toString();
    }

    private static CairoConfiguration withCustomFrames(AtomicInteger remainingDecodes) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public PartitionFrameDecoder newPartitionFrameDecoder() {
                return new FixedWidthDecoder(remainingDecodes);
            }

            @Override
            public PartitionFrameStateFactory newPartitionFrameStateFactory(TableToken token) {
                return new FixtureStateFactory(this);
            }
        };
    }

    // Exposes each base window as custom subframes without changing its logical rows. Native
    // partitions have one window. The Parquet fixture has two row groups per partition.
    private static class FixtureStateFactory implements PartitionFrameStateFactory {
        private final CairoConfiguration configuration;
        private final Map<Long, PartitionFrameSource> sources = new HashMap<>();

        private FixtureStateFactory(CairoConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public void bind(long state) {
            Assert.assertTrue(PartitionFrameState.isBound(state));
        }

        @Override
        public void close() {
            Assert.assertTrue(sources.isEmpty());
        }

        @Override
        public void destroy(long state) {
            sources.remove(state);
            final long metadata = PartitionFrameState.getParquetMetadataAddr(state);
            if (metadata != 0) {
                configuration.getFilesFacade().munmap(metadata, PartitionFrameState.getParquetMetadataSize(state), MemoryTag.MMAP_DEFAULT);
            }
            final long parquet = PartitionFrameState.getParquetAddr(state);
            if (parquet != 0) {
                configuration.getFilesFacade().munmap(parquet, PartitionFrameState.getParquetFileSize(state), MemoryTag.MMAP_DEFAULT);
            }
            Unsafe.free(state, stateSize(PartitionFrameState.getWindowCount(state)), MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public long open(TableReader reader, int partitionIndex, long readerSeqTxn) {
            final boolean parquet = reader.getTxFile().isPartitionParquet(partitionIndex);
            final long rows = reader.getTxFile().getPartitionSize(partitionIndex);
            final int windows = parquet ? Math.toIntExact(rows / ROW_GROUP_ROWS) : 1;
            final long windowRows = parquet ? ROW_GROUP_ROWS : rows;
            final long state = Unsafe.calloc(stateSize(windows), MemoryTag.NATIVE_DEFAULT);
            final long summaries = state + PartitionFrameState.HEADER_BYTES;
            // The fixed state header and window summaries read by PartitionFrameState.
            Unsafe.putLong(state, summaries);
            Unsafe.putLong(state + Long.BYTES, windows);
            Unsafe.putLong(state + 2L * Long.BYTES, rows);
            Unsafe.putLong(state + 3L * Long.BYTES, 1);
            Unsafe.putLong(state + 4L * Long.BYTES, SUBFRAME_ROWS);
            Unsafe.putLong(state + 5L * Long.BYTES, rows);
            for (int w = 0; w < windows; w++) {
                final long summary = summaries + (long) w * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
                Unsafe.putLong(summary + PartitionFrameState.SUMMARY_BASE_ROWS * Long.BYTES, windowRows);
                Unsafe.putLong(summary + PartitionFrameState.SUMMARY_LOGICAL_ROWS * Long.BYTES, windowRows);
                Unsafe.putLong(summary + PartitionFrameState.SUMMARY_FLAGS * Long.BYTES, PartitionFrameState.WINDOW_HAS_DELTA);
            }
            final long nameTxn = reader.getTxFile().getPartitionNameTxn(partitionIndex);
            final PartitionFrameSource source = new PartitionFrameSource();
            try {
                if (parquet) {
                    final long timestamp = reader.getPartitionTimestampByIndex(partitionIndex);
                    try (Path path = new Path().of(configuration.getDbRoot()).concat(reader.getTableToken())) {
                        final int len = path.size();
                        TableUtils.setPathForParquetPartitionMetadata(path, ColumnType.TIMESTAMP, PartitionBy.DAY, timestamp, nameTxn);
                        mapFile(path, state + 7L * Long.BYTES);
                        TableUtils.setPathForParquetPartition(path.trimTo(len), ColumnType.TIMESTAMP, PartitionBy.DAY, timestamp, nameTxn);
                        mapFile(path, state + 9L * Long.BYTES);
                    }
                    source.of(PartitionFrameSource.PARQUET, nameTxn, -1, PartitionFrameState.getParquetMetadataSize(state), 0);
                } else {
                    source.of(PartitionFrameSource.NATIVE, nameTxn, reader.getColumnVersionReader().getVersion(), 0, -1);
                }
                sources.put(state, source);
                return state;
            } catch (Throwable th) {
                destroy(state);
                throw th;
            }
        }

        @Override
        public void readSource(long state, PartitionFrameSource target) {
            final PartitionFrameSource source = sources.get(state);
            target.of(source.getKind(), source.getPartitionNameTxn(), source.getColumnVersion(), source.getMetadataCommittedBytes(), source.getMetadataSlot());
        }

        @Override
        public void readStats(long state, PartitionDeltaStats target) {
            target.of(getLogicalRowCount(state), Long.MAX_VALUE, Long.MIN_VALUE);
        }

        private void mapFile(Path file, long descriptor) {
            final FilesFacade ff = configuration.getFilesFacade();
            final long fd = ff.openRO(file.$());
            Assert.assertTrue("cannot open " + file, fd > -1);
            try {
                final long size = ff.length(fd);
                Unsafe.putLong(descriptor, TableUtils.mapRO(ff, fd, size, 0, MemoryTag.MMAP_DEFAULT));
                Unsafe.putLong(descriptor + Long.BYTES, size);
            } finally {
                ff.close(fd);
            }
        }

        private static long stateSize(int windows) {
            return PartitionFrameState.HEADER_BYTES + (long) windows * PartitionFrameState.SUMMARY_LONGS * Long.BYTES;
        }
    }

    // Copies the LONG and TIMESTAMP base views into task-owned buffers, which the consumer can
    // still read after the decoder has prepared another window.
    private static class FixedWidthDecoder implements PartitionFrameDecoder {
        private final DirectIntList projection = new DirectIntList(8, MemoryTag.NATIVE_DEFAULT);
        private final AtomicInteger remainingDecodes;
        private long baseViews;
        private int baseViewCount;

        private FixedWidthDecoder(AtomicInteger remainingDecodes) {
            this.remainingDecodes = remainingDecodes;
        }

        @Override
        public void bind(long state) {
        }

        @Override
        public void close() {
            projection.close();
        }

        @Override
        public long decodeFilteredSubframe(RowGroupBuffers buffers, int columnOffset, int rowLo, int rowHi,
                                           long survivorRowsAddress, long survivorRowCount, boolean fillNulls) {
            return decode(buffers, columnOffset, projection, rowLo, rowHi, survivorRowsAddress, survivorRowCount, fillNulls);
        }

        @Override
        public long decodeSubframe(RowGroupBuffers buffers, int rowLo, int rowHi) {
            return decode(buffers, 0, projection, rowLo, rowHi, 0, -1, false);
        }

        @Override
        public long materializeRemaining(RowGroupBuffers buffers, int columnOffset, DirectIntList columns, int rowLo, int rowHi,
                                         long survivorRowsAddress, long survivorRowCount, boolean fillNulls) {
            remainingDecodes.incrementAndGet();
            return decode(buffers, columnOffset, columns, rowLo, rowHi, survivorRowsAddress, survivorRowCount, fillNulls);
        }

        @Override
        public void prepareWindow(int windowIndex, DirectIntList projectionPairs, int primaryColumnCount,
                                  long baseViewsAddress, int baseViewCount, long memoryTrackerAddress) {
            projection.clear();
            for (int i = 0; i < primaryColumnCount * 2; i++) {
                projection.add(projectionPairs.get(i));
            }
            this.baseViews = baseViewsAddress;
            this.baseViewCount = baseViewCount;
        }

        @Override
        public void releaseDecodeResource(long resource) {
            Unsafe.free(resource, Unsafe.getLong(resource), MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void releaseWindow() {
            baseViews = 0;
            baseViewCount = 0;
        }

        private long decode(RowGroupBuffers buffers, int columnOffset, DirectIntList columns, int rowLo, int rowHi,
                            long survivorRows, long survivorCount, boolean fillNulls) {
            final int columnCount = (int) (columns.size() / 2);
            final long rows = survivorCount < 0 || fillNulls ? rowHi - rowLo : survivorCount;
            final long columnBytes = rows * Long.BYTES;
            final long resourceSize = Long.BYTES + columnCount * columnBytes;
            final long resource = Unsafe.malloc(resourceSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.putLong(resource, resourceSize);
            try (DirectLongList views = new DirectLongList(Math.max(4, columnCount * 4L), MemoryTag.NATIVE_DEFAULT)) {
                for (int i = 0; i < columnCount; i++) {
                    final int type = columns.get(2L * i + 1);
                    Assert.assertTrue(ColumnType.isTimestamp(type) || type == ColumnType.LONG);
                    final long base = findDataAddress(columns.get(2L * i), rowHi);
                    final long result = resource + Long.BYTES + i * columnBytes;
                    if (survivorCount < 0) {
                        Unsafe.copyMemory(base + (long) rowLo * Long.BYTES, result, columnBytes);
                    } else {
                        if (fillNulls) {
                            for (long r = 0; r < rows; r++) {
                                Unsafe.putLong(result + r * Long.BYTES, Numbers.LONG_NULL);
                            }
                        }
                        for (long r = 0; r < survivorCount; r++) {
                            final long row = Unsafe.getLong(survivorRows + r * Long.BYTES);
                            Assert.assertTrue(row >= 0 && row < rowHi - rowLo);
                            Unsafe.putLong(result + (fillNulls ? row : r) * Long.BYTES, Unsafe.getLong(base + (rowLo + row) * Long.BYTES));
                        }
                    }
                    views.add(result);
                    views.add(columnBytes);
                    views.add(0);
                    views.add(0);
                }
                buffers.installExternalColumnViews(columnOffset, views.getAddress(), columnCount);
                return resource;
            } catch (Throwable th) {
                releaseDecodeResource(resource);
                throw th;
            }
        }

        private long findDataAddress(int columnId, int rowHi) {
            for (int i = 0; i < baseViewCount; i++) {
                // Base descriptors are [columnId, type, dataPtr, dataSize, auxPtr, auxSize, top].
                final long view = baseViews + 7L * i * Long.BYTES;
                if (Unsafe.getLong(view) == columnId) {
                    Assert.assertEquals(0, Unsafe.getLong(view + 6L * Long.BYTES));
                    Assert.assertTrue((long) rowHi * Long.BYTES <= Unsafe.getLong(view + 3L * Long.BYTES));
                    return Unsafe.getLong(view + 2L * Long.BYTES);
                }
            }
            throw new AssertionError("missing base column " + columnId);
        }
    }
}
