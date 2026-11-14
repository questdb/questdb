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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState.PAGE_FRAME_PREFETCH_ROWS;

/**
 * The read-ahead of a descending page-frame stream,
 * {@link QwpEgressProcessorState#appendPageFrameSlice}: before it copies a
 * NATIVE frame's rows last to first, it reads their column pages first to
 * last, because kernel readahead only runs forwards and copying column data
 * that is not in the page cache downwards would fault it in a page at a time.
 * The tests pin what the stream reads ahead: every row of a native frame once,
 * in chunks that walk down the frame ahead of the slices, and every OS page of
 * each column once; nothing for column tops, parquet frames and ascending
 * streams. Whether the rows come out in the right order is the business of
 * {@link QwpEgressPageFrameTest}.
 */
public class QwpEgressPageFramePrefetchTest extends AbstractCairoTest {

    @Test
    public void testAscendingStreamReadsNothingAhead() throws Exception {
        assertMemoryLeak(() -> {
            createTable(3, 140_000);
            final Stream stream = stream("SELECT * FROM t", PartitionFrameCursorFactory.ORDER_ASC, 140_000);
            Assert.assertEquals(3 * 140_000, stream.rows);
            Assert.assertEquals(0, stream.prefetchedRows);
            Assert.assertEquals(0, stream.prefetchedPages);
        });
    }

    @Test
    public void testDescendingStreamReadsAheadEveryNativeRowOnce() throws Exception {
        assertMemoryLeak(() -> {
            // Three partitions, one frame each, of more rows than a read-ahead chunk;
            // the oldest one in parquet, which the memory pool decodes into memory.
            createTable(3, 140_000);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final Stream stream = stream("SELECT * FROM t ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC, 140_000);
            Assert.assertEquals(3 * 140_000, stream.rows);
            Assert.assertEquals("the rows of both native frames, each once", 2 * 140_000, stream.prefetchedRows);
            Assert.assertTrue(stream.prefetchedPages > 0);
        });
    }

    @Test
    public void testPrefetchChunksWalkDownTheFrame() {
        final long frameRows = 150_000;
        // the first slice of a frame reads ahead a chunk from the frame's end down
        long prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 1_024);
        Assert.assertEquals(frameRows - PAGE_FRAME_PREFETCH_ROWS, prefetchedLo);
        // the slices inside the chunk read nothing ahead: they are at or above it
        Assert.assertTrue(frameRows - 2 * 1_024 >= prefetchedLo);
        // the first slice below the chunk reads ahead the next chunk down
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1);
        Assert.assertEquals(frameRows - 2 * PAGE_FRAME_PREFETCH_ROWS, prefetchedLo);
        // the last chunk stops at the frame's first row
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1);
        Assert.assertEquals(0, prefetchedLo);
        // a frame smaller than a chunk is read ahead whole
        Assert.assertEquals(0, QwpEgressProcessorState.pageFramePrefetchLo(1_500, 476));
        // a slice taller than a chunk is read ahead whole
        Assert.assertEquals(10, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, 10));
    }

    @Test
    public void testTouchReadsEachPageOfTheRowsOnce() throws Exception {
        assertMemoryLeak(() -> {
            final int rows = 20_000;
            execute("""
                    CREATE TABLE t (
                        ts TIMESTAMP, l LONG, i INT, b BYTE, bo BOOLEAN, sym SYMBOL,
                        v VARCHAR, vi VARCHAR, vm VARCHAR, s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""");
            // v: 40 bytes, stored in the data vector; vi: at most 5 bytes, inlined in
            // the aux vector, so there is no data vector; vm: 40 bytes in the first
            // row, inlined after it; s: 10 chars, 4 + 2 * 10 bytes
            execute("INSERT INTO t SELECT x::TIMESTAMP, x, x::INT, x::BYTE, x % 2 = 0, 's' || (x % 7), "
                    + "lpad(x::STRING, 40, '0')::VARCHAR, x::VARCHAR, "
                    + "CASE WHEN x = 1 THEN lpad(x::STRING, 40, '0')::VARCHAR ELSE x::VARCHAR END, "
                    + "lpad(x::STRING, 10, '0') FROM long_sequence(" + rows + ")");
            // columns added after the rows are column tops: nothing to read
            execute("ALTER TABLE t ADD COLUMN top LONG");
            execute("ALTER TABLE t ADD COLUMN vtop VARCHAR");
            sqlExecutionContext.changePageFrameSizes(rows, rows);
            RecordCursorFactory factory = null;
            PageFrameCursor cursor = null;
            try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
                factory = select("SELECT * FROM t ORDER BY ts DESC");
                cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_DESC);
                final RecordMetadata metadata = factory.getMetadata();
                state.beginStreamingPageFrame(1, factory, cursor, metadata.getColumnCount(), 0, "t", CompiledQuery.SELECT, false, true);
                factory = null;
                cursor = null;
                final QwpResultBatchBuffer buffer = beginBatch(state, metadata);
                final PageFrame frame = state.advanceToPageFrame();
                Assert.assertNotNull(frame);
                Assert.assertEquals(PartitionFormat.NATIVE, frame.getFormat());
                Assert.assertEquals("one frame holds the partition", rows, state.getStreamingPageFrameRowHi());

                for (long[] range : new long[][]{{0, rows}, {rows / 3, 2 * rows / 3}, {7, 8}, {5_000, 5_000}}) {
                    final long lo = range[0];
                    final long hi = range[1];
                    final long expected = pages(lo * 8, hi * 8) // ts
                            + pages(lo * 8, hi * 8) // l
                            + pages(lo * 4, hi * 4) // i
                            + pages(lo, hi) // b
                            + pages(lo, hi) // bo
                            + pages(lo * 4, hi * 4) // sym keys
                            + pages(lo * 16, hi * 16) + pages(lo * 40, hi * 40) // v aux, data
                            + pages(lo * 16, hi * 16) // vi aux
                            + pages(lo * 16, hi * 16) + (lo == 0 ? pages(0, 40) : 0) // vm aux, data
                            + pages(lo * 8, hi * 8) + pages(lo * 24, hi * 24); // s aux, data
                    Assert.assertEquals("rows [" + lo + ", " + hi + ")", expected, buffer.touchPageFrameRows(frame, lo, hi));
                }
                state.endStreaming();
            } finally {
                Misc.free(cursor);
                Misc.free(factory);
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    private static QwpResultBatchBuffer beginBatch(QwpEgressProcessorState state, RecordMetadata metadata) {
        final int columnCount = metadata.getColumnCount();
        final ObjList<QwpEgressColumnDef> defs = state.borrowColumnDefs(columnCount);
        for (int i = 0; i < columnCount; i++) {
            defs.getQuick(i).of(metadata.getColumnName(i), metadata.getColumnType(i));
        }
        final QwpResultBatchBuffer buffer = state.getBatchBuffer();
        buffer.beginBatch(defs, state.getStreamingSymbolTableSource(), state.getConnSymbolDict());
        return buffer;
    }

    private static void createTable(int partitions, int rowsPerPartition) throws Exception {
        execute("""
                CREATE TABLE t (
                    ts TIMESTAMP, l LONG, sym SYMBOL, v VARCHAR, a DOUBLE[]
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""");
        // partition k holds the rows k * rowsPerPartition + 1 .. (k + 1) * rowsPerPartition
        execute("INSERT INTO t SELECT (((x - 1) / " + rowsPerPartition + ") * 86_400_000_000 + (x - 1) % " + rowsPerPartition + ")::TIMESTAMP, "
                + "x, 's' || (x % 7), lpad(x::STRING, 20, '0')::VARCHAR, ARRAY[x::DOUBLE, 2.0] "
                + "FROM long_sequence(" + (long) partitions * rowsPerPartition + ")");
        execute("ALTER TABLE t ADD COLUMN top VARCHAR");
    }

    // OS pages that hold the bytes [lo, hi) of a page-aligned vector
    private static long pages(long lo, long hi) {
        return hi > lo ? (hi - 1) / Files.PAGE_SIZE - lo / Files.PAGE_SIZE + 1 : 0;
    }

    /**
     * Streams {@code sql}'s page frames in {@code order} through the state, as
     * {@code streamResults} does, starting a fresh batch every
     * {@link QwpEgressUpgradeProcessor#MAX_ROWS_PER_BATCH} rows.
     */
    private Stream stream(String sql, int order, int frameRows) throws Exception {
        sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
        RecordCursorFactory factory = null;
        PageFrameCursor cursor = null;
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
            factory = select(sql);
            Assert.assertEquals(order, QwpEgressUpgradeProcessor.pageFrameOrder(factory, false));
            cursor = factory.getPageFrameCursor(sqlExecutionContext, order);
            final RecordMetadata metadata = factory.getMetadata();
            state.beginStreamingPageFrame(1, factory, cursor, metadata.getColumnCount(), 0, sql, CompiledQuery.SELECT, false,
                    order == PartitionFrameCursorFactory.ORDER_DESC);
            factory = null;
            cursor = null;
            final QwpResultBatchBuffer buffer = beginBatch(state, metadata);
            final Stream stream = new Stream();
            while (state.advanceToPageFrame() != null) {
                stream.rows += state.appendPageFrameSlice(1_024);
                if (buffer.getRowCount() >= QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH) {
                    beginBatch(state, metadata);
                }
            }
            stream.prefetchedRows = state.getStreamingPageFramePrefetchedRows();
            stream.prefetchedPages = state.getStreamingPageFramePrefetchedPages();
            state.endStreaming();
            return stream;
        } finally {
            Misc.free(cursor);
            Misc.free(factory);
            sqlExecutionContext.restoreToDefaultPageFrameSizes();
        }
    }

    private static class Stream {
        long prefetchedPages;
        long prefetchedRows;
        long rows;
    }
}
