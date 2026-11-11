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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameState;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.BwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.FwdTableReaderPageFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Page frames carry window-relative row positions as ints. A partition window whose logical row
 * count exceeds {@link Integer#MAX_VALUE} must fail the query with a clear error, never hand out
 * an overflowed or wrapped frame. The window is synthetic: a partition frame state that declares
 * one window of {@link #WINDOW_LOGICAL_ROWS} rows over a small real partition.
 */
public class TableReaderPageFrameWindowLimitTest extends AbstractCairoTest {
    private static final int HEADER_LONGS = 6;
    private static final long PAST_INT_MAX = (1L << 31) + 1;
    private static final long PAST_UINT_MAX = (1L << 32) + 1;
    private static final long STATE_SIZE = (HEADER_LONGS + PartitionFrameState.SUMMARY_LONGS) * Long.BYTES;
    private static final long SUBFRAME_SIZE = 1024;
    private static final long WINDOW_LOGICAL_ROWS = 5L << 30;

    @Test
    public void testBwdNativeWindowAtIntMax() throws Exception {
        assertWindowFrame(false, false, Integer.MAX_VALUE - 3, Integer.MAX_VALUE);
    }

    @Test
    public void testBwdNativeWindowPastIntMax() throws Exception {
        assertWindowTooLarge(false, false, PAST_INT_MAX);
    }

    @Test
    public void testBwdNativeWindowPastUintMax() throws Exception {
        assertWindowTooLarge(false, false, PAST_UINT_MAX);
    }

    @Test
    public void testBwdParquetWindowAtIntMax() throws Exception {
        assertWindowFrame(true, false, Integer.MAX_VALUE - 3, Integer.MAX_VALUE);
    }

    @Test
    public void testBwdParquetWindowPastIntMax() throws Exception {
        assertWindowTooLarge(true, false, PAST_INT_MAX);
    }

    @Test
    public void testBwdParquetWindowPastUintMax() throws Exception {
        assertWindowTooLarge(true, false, PAST_UINT_MAX);
    }

    @Test
    public void testFwdNativeWindowAtIntMax() throws Exception {
        assertWindowFrame(false, true, Integer.MAX_VALUE - 3, Integer.MAX_VALUE);
    }

    @Test
    public void testFwdNativeWindowPastIntMax() throws Exception {
        assertWindowTooLarge(false, true, PAST_INT_MAX);
    }

    @Test
    public void testFwdNativeWindowPastUintMax() throws Exception {
        assertWindowTooLarge(false, true, PAST_UINT_MAX);
    }

    @Test
    public void testFwdParquetWindowAtIntMax() throws Exception {
        assertWindowFrame(true, true, Integer.MAX_VALUE - 3, Integer.MAX_VALUE);
    }

    @Test
    public void testFwdParquetWindowPastIntMax() throws Exception {
        assertWindowTooLarge(true, true, PAST_INT_MAX);
    }

    @Test
    public void testFwdParquetWindowPastUintMax() throws Exception {
        assertWindowTooLarge(true, true, PAST_UINT_MAX);
    }

    private void assertWindowFrame(boolean parquet, boolean forward, long rowLo, long rowHi) throws Exception {
        withWindowCursor(parquet, forward, rowLo, rowHi, cursor -> {
            final PageFrame frame = cursor.next();
            Assert.assertNotNull(frame);
            Assert.assertEquals(parquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE, frame.getFormat());
            Assert.assertEquals(0, frame.getParquetRowGroup());
            Assert.assertEquals(rowLo, frame.getParquetRowGroupLo());
            Assert.assertEquals(rowHi, frame.getParquetRowGroupHi());
            Assert.assertEquals(rowLo, frame.getPartitionLo());
            Assert.assertEquals(rowHi, frame.getPartitionHi());
        });
    }

    private void assertWindowTooLarge(boolean parquet, boolean forward, long rowLo) throws Exception {
        withWindowCursor(parquet, forward, rowLo, rowLo + 3, cursor -> {
            try {
                final PageFrame frame = cursor.next();
                Assert.fail("expected a window-limit error, got a frame [rowGroupLo="
                        + (frame == null ? "null" : frame.getParquetRowGroupLo())
                        + ", rowGroupHi=" + (frame == null ? "null" : frame.getParquetRowGroupHi()) + ']');
            } catch (CairoException e) {
                Assert.assertFalse(e.isCritical());
                TestUtils.assertContains(
                        e.getFlyweightMessage(),
                        "partition window is too large to query, a window holds at most 2147483647 rows "
                                + "[table=x, partition=2020-01-01T00:00:00.000000Z, window=0, row=" + rowLo + ']'
                );
            }
        });
    }

    private void withWindowCursor(boolean parquet, boolean forward, long rowLo, long rowHi, CursorAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (v long, ts timestamp) timestamp(ts) partition by day");
            execute("insert into x values (1, '2020-01-01T00:00:00'), (2, '2020-01-01T00:00:01'), "
                    + "(3, '2020-01-01T00:00:02'), (4, '2020-01-01T00:00:03'), (5, '2020-01-02T00:00:00')");
            if (parquet) {
                execute("alter table x convert partition to parquet list '2020-01-01'");
            }
            final long state = Unsafe.malloc(STATE_SIZE, MemoryTag.NATIVE_DEFAULT);
            try (
                    TableReader reader = engine.getReader("x");
                    TablePageFrameCursor cursor = newPageFrameCursor(reader, forward)
            ) {
                reader.openPartition(0);
                Assert.assertEquals(parquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE, reader.getPartitionFormat(0));
                final ParquetPartitionDecoder decoder = parquet ? reader.getAndInitParquetPartitionDecoder(0) : null;
                final long baseRows = parquet ? decoder.metadata().getRowGroupSize(0) : reader.getPartitionRowCount(0);
                Assert.assertEquals(4, baseRows);
                writeOneWindowState(state, baseRows);
                cursor.of(sqlExecutionContext, new WindowPartitionFrameCursor(reader, state, decoder, rowLo, rowHi));
                assertion.run(cursor);
            } finally {
                Unsafe.free(state, STATE_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static TablePageFrameCursor newPageFrameCursor(TableReader reader, boolean forward) {
        final IntList columnIndexes = new IntList();
        final IntList columnSizeShifts = new IntList();
        for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizeShifts.add(ColumnType.pow2SizeOf(reader.getMetadata().getColumnType(i)));
        }
        return forward
                ? new FwdTableReaderPageFrameCursor(columnIndexes, columnSizeShifts, null, 1)
                : new BwdTableReaderPageFrameCursor(columnIndexes, columnSizeShifts, null, 1);
    }

    // Header and window-summary layout read by PartitionFrameState.
    private static void writeOneWindowState(long state, long baseRows) {
        final long summaries = state + HEADER_LONGS * Long.BYTES;
        Unsafe.putLong(state, summaries);
        Unsafe.putLong(state + Long.BYTES, 1);
        Unsafe.putLong(state + 2 * Long.BYTES, WINDOW_LOGICAL_ROWS);
        Unsafe.putLong(state + 3 * Long.BYTES, 1);
        Unsafe.putLong(state + 4 * Long.BYTES, SUBFRAME_SIZE);
        Unsafe.putLong(state + 5 * Long.BYTES, baseRows);
        Unsafe.putLong(summaries + PartitionFrameState.SUMMARY_BASE_ROWS * Long.BYTES, baseRows);
        Unsafe.putLong(summaries + Long.BYTES, 0);
        Unsafe.putLong(summaries + PartitionFrameState.SUMMARY_LOGICAL_ROWS * Long.BYTES, WINDOW_LOGICAL_ROWS);
        Unsafe.putLong(summaries + PartitionFrameState.SUMMARY_FLAGS * Long.BYTES, PartitionFrameState.WINDOW_HAS_DELTA);
    }

    @FunctionalInterface
    private interface CursorAssertion {
        void run(TablePageFrameCursor cursor);
    }

    // Hands out one frame of partition 0, [rowLo, rowHi), bound to the synthetic state.
    private static class WindowPartitionFrameCursor implements PartitionFrameCursor, PartitionFrame {
        private final ParquetPartitionDecoder decoder;
        private final TableReader reader;
        private final long rowHi;
        private final long rowLo;
        private final long state;
        private boolean consumed;

        private WindowPartitionFrameCursor(TableReader reader, long state, @Nullable ParquetPartitionDecoder decoder, long rowLo, long rowHi) {
            this.reader = reader;
            this.state = state;
            this.decoder = decoder;
            this.rowLo = rowLo;
            this.rowHi = rowHi;
        }

        @Override
        public void close() {
        }

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return decoder;
        }

        @Override
        public byte getPartitionFormat() {
            return decoder != null ? PartitionFormat.PARQUET : PartitionFormat.NATIVE;
        }

        @Override
        public long getPartitionFrameState() {
            return state;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return reader.getSymbolTable(columnIndex);
        }

        @Override
        public TableReader getTableReader() {
            return reader;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return reader.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PartitionFrame next(long skipTarget) {
            if (consumed) {
                return null;
            }
            consumed = true;
            return this;
        }

        @Override
        public boolean reload() {
            return false;
        }

        @Override
        public long size() {
            return rowHi - rowLo;
        }

        @Override
        public void toTop() {
            consumed = false;
        }
    }
}
