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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState.PAGE_FRAME_PREFETCH_ROWS;

/**
 * The read-ahead of a descending page-frame stream,
 * {@link QwpEgressProcessorState#appendPageFrameSlice}: before it copies a
 * NATIVE frame's rows last to first, it reads their column pages first to
 * last, because kernel readahead only runs forwards and copying column data
 * that is not in the page cache downwards would fault it in a page at a time.
 * The tests pin what the stream reads ahead: every row of a native frame once,
 * in chunks that walk down the frame ahead of the slices, start with the first
 * batch's rows and double up to {@link QwpEgressProcessorState#PAGE_FRAME_PREFETCH_ROWS},
 * so the first batch reads ahead only its own rows, however wide they are;
 * every OS page of each column once; nothing for column tops, parquet frames
 * and ascending streams. Whether the rows come out in the right order is the
 * business of {@link QwpEgressPageFrameTest}.
 */
public class QwpEgressPageFramePrefetchTest extends AbstractCairoTest {

    @Test
    public void testAscendingStreamReadsNothingAhead() throws Exception {
        assertMemoryLeak(() -> {
            createTable(3, 147_456);
            final Stream stream = stream("SELECT * FROM t", PartitionFrameCursorFactory.ORDER_ASC, 147_456, QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH);
            Assert.assertEquals(3 * 147_456, stream.rows);
            Assert.assertEquals(0, stream.prefetchedRows);
            Assert.assertEquals(0, stream.prefetchedPages);
        });
    }

    @Test
    public void testDescendingStreamReadsAheadEveryNativeRowOnce() throws Exception {
        assertMemoryLeak(() -> {
            // Three partitions, one frame each, of nine full batches and more rows than
            // a read-ahead chunk; the oldest one in parquet, which the memory pool
            // decodes into memory.
            createTable(3, 147_456);
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
            final Stream stream = stream("SELECT * FROM t ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC, 147_456, QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH);
            Assert.assertEquals(3 * 147_456, stream.rows);
            Assert.assertEquals("the rows of both native frames, each once", 2 * 147_456, stream.prefetchedRows);
            // Each native frame streams in 1_024-row slices from its top down, 16 to a
            // batch. A slice that reaches below the rows read ahead so far reads ahead
            // the next chunk: the rows its batch still takes, or as many rows as the
            // stream has read ahead before it, whichever is more, and at most 65_536
            // rows; the slices in between read nothing ahead. The first frame's first
            // chunk is thus the first batch, 16_384 rows, and its chunks double from
            // there, 16_384, 32_768 and 65_536 rows, which the first slices of the 2nd,
            // 3rd and 5th batch read ahead, and the 9th batch reads the frame's last
            // 16_384. The second frame comes after 147_456 rows read ahead, so it takes
            // 65_536-row chunks from its top down. The parquet frame, last, reads
            // nothing ahead.
            final ObjList<String> expected = new ObjList<>();
            expected.add(readAhead(146_432, 147_456, 131_072, 147_456));
            expected.add(readAhead(130_048, 131_072, 114_688, 131_072));
            expected.add(readAhead(113_664, 114_688, 81_920, 114_688));
            expected.add(readAhead(80_896, 81_920, 16_384, 81_920));
            expected.add(readAhead(15_360, 16_384, 0, 16_384));
            expected.add(readAhead(146_432, 147_456, 81_920, 147_456));
            expected.add(readAhead(80_896, 81_920, 16_384, 81_920));
            expected.add(readAhead(15_360, 16_384, 0, 16_384));
            Assert.assertEquals(expected, stream.readAheads());
            Assert.assertEquals("the first batch reads ahead only its own rows", 16_384, stream.batchReadAheadRows.getQuick(0));
            Assert.assertEquals(
                    tablePages(131_072, 147_456) + tablePages(114_688, 131_072) + tablePages(81_920, 114_688)
                            + tablePages(16_384, 81_920) + tablePages(0, 16_384)
                            + tablePages(81_920, 147_456) + tablePages(16_384, 81_920) + tablePages(0, 16_384),
                    stream.prefetchedPages
            );
        });
    }

    @Test
    public void testFirstBatchOfWideRowsReadsAheadOnlyItsOwnRows() throws Exception {
        assertMemoryLeak(() -> {
            // 50 DOUBLE columns and the timestamp, 40_960 rows in one frame
            final int columns = 50;
            final int rows = 40_960;
            final StringBuilder sql = new StringBuilder("CREATE TABLE w AS (SELECT x::TIMESTAMP ts");
            for (int i = 0; i < columns; i++) {
                sql.append(", x::DOUBLE d").append(i);
            }
            execute(sql.append(" FROM long_sequence(").append(rows).append(")) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL"));

            // A full batch reads ahead its 16_384 rows of every column, not 65_536 rows,
            // in one chunk, which its first slice reads.
            final Stream fullBatches = stream("SELECT * FROM w ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC, rows, QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH);
            Assert.assertEquals(16_384, fullBatches.batchReadAheadRows.getQuick(0));
            Assert.assertEquals((columns + 1) * pages(24_576 * 8, 40_960 * 8), fullBatches.batchReadAheadPages.getQuick(0));
            Assert.assertEquals(16_384, fullBatches.slices.getQuick(0).rowsRead);
            Assert.assertEquals("every row once", rows, fullBatches.prefetchedRows);

            // A client that asks for batches of 100 rows gets slices of 100 rows; the
            // first batch reads ahead its 100 rows.
            final Stream smallBatches = stream("SELECT * FROM w ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC, rows, 100);
            Assert.assertEquals(100, smallBatches.batchReadAheadRows.getQuick(0));
            Assert.assertEquals((columns + 1) * pages((rows - 100) * 8L, rows * 8L), smallBatches.batchReadAheadPages.getQuick(0));
            Assert.assertEquals("every row once", rows, smallBatches.prefetchedRows);
        });
    }

    @Test
    public void testPrefetchChunksGrowWithTheStream() {
        final long frameRows = 150_000;
        // the stream's first chunk is the rows its first batch takes
        long prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 1_024, 0, 16_384);
        Assert.assertEquals(frameRows - 16_384, prefetchedLo);
        Assert.assertEquals(frameRows - 100, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 100, 0, 100));
        // later chunks reach as many rows down as the stream has read ahead so far
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1_024, 16_384, 16_384);
        Assert.assertEquals(frameRows - 32_768, prefetchedLo);
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1_024, 32_768, 16_384);
        Assert.assertEquals(frameRows - 65_536, prefetchedLo);
        // up to PAGE_FRAME_PREFETCH_ROWS
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1_024, 65_536, 16_384);
        Assert.assertEquals(frameRows - 65_536 - PAGE_FRAME_PREFETCH_ROWS, prefetchedLo);
        // the last chunk stops at the frame's first row
        prefetchedLo = QwpEgressProcessorState.pageFramePrefetchLo(prefetchedLo, prefetchedLo - 1_024, 131_072, 16_384);
        Assert.assertEquals(0, prefetchedLo);
        // a new frame after a long stream is read ahead in chunks of no more than
        // PAGE_FRAME_PREFETCH_ROWS, or whole
        Assert.assertEquals(frameRows - PAGE_FRAME_PREFETCH_ROWS, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 1_024, 1_000_000, 16_384));
        Assert.assertEquals(0, QwpEgressProcessorState.pageFramePrefetchLo(1_500, 476, 1_000_000, 16_384));
        // the rows the batch still takes, when more than the stream has read ahead,
        // as after a small first frame
        Assert.assertEquals(frameRows - 16_084, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 1_024, 300, 16_084));
        // and no more than PAGE_FRAME_PREFETCH_ROWS of them either
        Assert.assertEquals(frameRows - PAGE_FRAME_PREFETCH_ROWS, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, frameRows - 1_024, 0, 100_000));
        // a slice that reaches further down than the chunk is read ahead whole
        Assert.assertEquals(10, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, 10, 0, 1));
        Assert.assertEquals(10, QwpEgressProcessorState.pageFramePrefetchLo(frameRows, 10, 100_000, 16_384));
    }

    @Test
    public void testReadAheadKeepsPaceWithTheStream() throws Exception {
        // Random partition and frame sizes, batch sizes and a parquet partition: every
        // native row is read ahead once, before its slice copies it, in chunks of the
        // rows the batch still takes or as many rows as the stream has read ahead
        // before, whichever is more, at least what the slice still needs and at most
        // PAGE_FRAME_PREFETCH_ROWS; parquet rows never.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int[] frameRowChoices = {700, 1_024, 5_000, 70_000, 200_000};
        final int[] batchRowChoices = {1, 7, 100, 1_000, QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH};
        assertMemoryLeak(() -> {
            for (int iteration = 0; iteration < 4; iteration++) {
                execute("DROP TABLE IF EXISTS t");
                final int partitions = 1 + rnd.nextInt(3);
                final long[] partitionRows = new long[partitions];
                for (int i = 0; i < partitions; i++) {
                    partitionRows[i] = 1 + rnd.nextInt(150_000);
                }
                createTable(partitionRows);
                final boolean hasParquet = partitions > 1 && rnd.nextBoolean();
                if (hasParquet) {
                    execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
                }
                final int frameRows = frameRowChoices[rnd.nextInt(frameRowChoices.length)];
                final int batchRows = batchRowChoices[rnd.nextInt(batchRowChoices.length)];
                final String context = "partitions=" + Arrays.toString(partitionRows)
                        + ", hasParquet=" + hasParquet + ", frameRows=" + frameRows + ", batchRows=" + batchRows;

                final Stream stream = stream("SELECT * FROM t ORDER BY ts DESC", PartitionFrameCursorFactory.ORDER_DESC, frameRows, batchRows);
                long totalRows = 0;
                long nativeRows = 0;
                for (int i = 0; i < partitions; i++) {
                    totalRows += partitionRows[i];
                    if (!hasParquet || i > 0) {
                        nativeRows += partitionRows[i];
                    }
                }
                Assert.assertEquals(context, totalRows, stream.rows);
                Assert.assertEquals(context, nativeRows, stream.prefetchedRows);
                // the lowest row of the current frame read ahead so far
                long readAheadLo = 0;
                for (int i = 0, n = stream.slices.size(); i < n; i++) {
                    final Slice slice = stream.slices.getQuick(i);
                    if (!slice.isNative) {
                        Assert.assertEquals(context + ", slice " + i, 0, slice.rowsRead);
                        continue;
                    }
                    if (slice.isFrameStart) {
                        readAheadLo = slice.hi;
                    }
                    final long needed = Math.max(0, readAheadLo - slice.lo);
                    final long expectedRows = needed == 0
                            ? 0
                            : Math.min(readAheadLo, Math.max(needed, Math.min(PAGE_FRAME_PREFETCH_ROWS, Math.max(slice.prefetchedRowsBefore, slice.batchRows))));
                    Assert.assertEquals(context + ", slice " + i + " [" + slice.lo + ", " + slice.hi + ')', expectedRows, slice.rowsRead);
                    readAheadLo -= slice.rowsRead;
                    Assert.assertTrue(context + ", slice " + i + " copies rows not read ahead", readAheadLo <= slice.lo);
                }
            }
        });
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
                state.beginStreamingPageFrame(1, factory, cursor, null, metadata.getColumnCount(), 0, "t", CompiledQuery.SELECT, false, true);
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
        final long[] partitionRows = new long[partitions];
        Arrays.fill(partitionRows, rowsPerPartition);
        createTable(partitionRows);
    }

    private static void createTable(long[] partitionRows) throws Exception {
        execute("""
                CREATE TABLE t (
                    ts TIMESTAMP, l LONG, sym SYMBOL, v VARCHAR, a DOUBLE[]
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""");
        // partition k, day k, holds partitionRows[k] rows
        for (int k = 0; k < partitionRows.length; k++) {
            execute("INSERT INTO t SELECT (" + k + " * 86_400_000_000 + x - 1)::TIMESTAMP, "
                    + "x, 's' || (x % 7), lpad(x::STRING, 20, '0')::VARCHAR, ARRAY[x::DOUBLE, 2.0] "
                    + "FROM long_sequence(" + partitionRows[k] + ")");
        }
        execute("ALTER TABLE t ADD COLUMN top VARCHAR");
    }

    // OS pages that hold the bytes [lo, hi) of a page-aligned vector
    private static long pages(long lo, long hi) {
        return hi > lo ? (hi - 1) / Files.PAGE_SIZE - lo / Files.PAGE_SIZE + 1 : 0;
    }

    // A read-ahead of the column pages of rows [lo, hi) before the slice
    // [sliceLo, sliceHi) of a frame of the createTable() table is copied.
    private static String readAhead(long sliceLo, long sliceHi, long lo, long hi) {
        return readAheadEvent(sliceLo, sliceHi, hi - lo, tablePages(lo, hi));
    }

    private static String readAheadEvent(long sliceLo, long sliceHi, long rows, long pages) {
        return "slice [" + sliceLo + ", " + sliceHi + ") read ahead " + rows + " rows, " + pages + " pages";
    }

    // OS pages that hold rows [lo, hi) of a native partition of the createTable()
    // table, each one starting at row 0 of the partition's column files: ts, l,
    // the sym keys, the aux and data vectors of v, whose values take 20 bytes, and
    // of a, whose values take 24 bytes: the 4-byte shape, padded to 8, and two
    // doubles. The column top of top has no pages.
    private static long tablePages(long lo, long hi) {
        return pages(lo * 8, hi * 8) // ts
                + pages(lo * 8, hi * 8) // l
                + pages(lo * 4, hi * 4) // sym keys
                + pages(lo * 16, hi * 16) + pages(lo * 20, hi * 20) // v aux, data
                + pages(lo * 16, hi * 16) + pages(lo * 24, hi * 24); // a aux, data
    }

    /**
     * Streams {@code sql}'s page frames in {@code order} through the state, as
     * {@code streamResults} does: it asks each slice for the rows the batch still
     * takes, and starts a fresh batch every {@code batchRows} rows.
     */
    private Stream stream(String sql, int order, int frameRows, int batchRows) throws Exception {
        sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
        RecordCursorFactory factory = null;
        PageFrameCursor cursor = null;
        try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
            factory = select(sql);
            Assert.assertEquals(order, QwpEgressUpgradeProcessor.pageFrameOrder(factory, false));
            cursor = factory.getPageFrameCursor(sqlExecutionContext, order);
            final RecordMetadata metadata = factory.getMetadata();
            final boolean isDescending = order == PartitionFrameCursorFactory.ORDER_DESC;
            state.beginStreamingPageFrame(1, factory, cursor, null, metadata.getColumnCount(), 0, sql, CompiledQuery.SELECT, false,
                    isDescending);
            factory = null;
            cursor = null;
            final QwpResultBatchBuffer buffer = beginBatch(state, metadata);
            final Stream stream = new Stream();
            boolean isFrameStart = true;
            PageFrame frame;
            while ((frame = state.advanceToPageFrame()) != null) {
                // the unconsumed rows of the frame; a descending stream takes its
                // slice from their top, an ascending one from their bottom
                final long lo = state.getStreamingPageFrameRow();
                final long hi = state.getStreamingPageFrameRowHi();
                final long rowsBefore = state.getStreamingPageFramePrefetchedRows();
                final long pagesBefore = state.getStreamingPageFramePrefetchedPages();
                final int batchRowsLeft = batchRows - buffer.getRowCount();
                final int sliceRows = state.appendPageFrameSlice(batchRowsLeft);
                stream.rows += sliceRows;
                final long sliceLo = isDescending ? hi - sliceRows : lo;
                stream.slices.add(new Slice(
                        isFrameStart,
                        frame.getFormat() == PartitionFormat.NATIVE,
                        sliceLo,
                        sliceLo + sliceRows,
                        batchRowsLeft,
                        rowsBefore,
                        state.getStreamingPageFramePrefetchedRows() - rowsBefore,
                        state.getStreamingPageFramePrefetchedPages() - pagesBefore
                ));
                // the next slice starts a frame once this one used up the frame's rows
                isFrameStart = isDescending ? sliceLo == lo : sliceLo + sliceRows == hi;
                if (buffer.getRowCount() >= batchRows) {
                    stream.batchReadAheadRows.add(state.getStreamingPageFramePrefetchedRows());
                    stream.batchReadAheadPages.add(state.getStreamingPageFramePrefetchedPages());
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

    // A slice of a frame the stream copied: rows [lo, hi) of the frame, the rows
    // its batch still took, the rows the stream had read ahead before it, and what
    // the slice read ahead.
    private record Slice(
            boolean isFrameStart,
            boolean isNative,
            long lo,
            long hi,
            long batchRows,
            long prefetchedRowsBefore,
            long rowsRead,
            long pagesRead
    ) {
    }

    private static class Stream {
        // pages and rows the stream had read ahead when each batch filled up
        final LongList batchReadAheadPages = new LongList();
        final LongList batchReadAheadRows = new LongList();
        // every slice the stream copied, in stream order
        final ObjList<Slice> slices = new ObjList<>();
        long prefetchedPages;
        long prefetchedRows;
        long rows;

        // the slices that read ahead, with what they read, in stream order
        ObjList<String> readAheads() {
            final ObjList<String> readAheads = new ObjList<>();
            for (int i = 0, n = slices.size(); i < n; i++) {
                final Slice slice = slices.getQuick(i);
                if (slice.rowsRead != 0 || slice.pagesRead != 0) {
                    readAheads.add(readAheadEvent(slice.lo, slice.hi, slice.rowsRead, slice.pagesRead));
                }
            }
            return readAheads;
        }
    }
}
