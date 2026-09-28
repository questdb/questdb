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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.closeStreamingParquetWriter;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.createStreamingParquetWriter;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.finishStreamingParquetWrite;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.flushRowGroup;
import static io.questdb.griffin.engine.table.parquet.PartitionEncoder.writeStreamingParquetChunk;

/**
 * Covers {@link io.questdb.griffin.engine.table.parquet.PartitionEncoder#flushRowGroup(long, long)}:
 * the caller, not the configured row group size, decides where a row group ends.
 */
public class ParquetRowGroupFlushTest extends AbstractCairoTest {
    // Header the streaming writer prepends to every emitted buffer:
    // [8 bytes data length][8 bytes rows written to row groups].
    private static final int BUFFER_HEADER_SIZE = 16;
    private static final int DATA_PAGE_SIZE = 1024 * 1024;
    // Flush after every page frame, draining each time.
    private static final int FLUSH_AFTER_EACH_FRAME = 0;
    // Flush before the first chunk is written, when nothing is pending.
    private static final int FLUSH_BEFORE_ANY_ROWS = 1;
    // Flush after the first frame, write the second frame, then finish without draining.
    // Both frames are submitted with no drain, so 8 rows are pending, and the
    // boundary is then declared at 4 -- inside frame 2. The whole-buffer form of
    // flushRowGroup could not express a cut there.
    private static final int FLUSH_BOTH_FRAMES_THEN_AT_FOUR = 7;
    // Both frames submitted with no drain, then a boundary declared far beyond the 8
    // pending rows. The clamp must reduce it to 8, so one row group results. Without
    // the clamp the writer would be asked for rows that do not exist.
    private static final int FLUSH_BOTH_FRAMES_THEN_OVERSIZED = 8;
    private static final int FLUSH_MID_STREAM_NO_DRAIN = 2;
    // Flush after the first frame, then finish without writing anything else.
    private static final int FLUSH_MID_STREAM_THEN_FINISH = 3;
    // Flush after the first frame, write the second frame, then drain.
    private static final int FLUSH_MID_STREAM_THEN_WRITE = 4;
    // Never flush, draining after every page frame: every boundary comes from the fixed
    // row group size, exactly as the copy export task drives the writer.
    private static final int NO_FLUSH_DRAIN_EACH_FRAME = 5;
    // Never flush, and never drain either: the caller only appends the buffer each write
    // hands back, so a row group the threshold makes due is only emitted if the write that
    // makes it due closes it there and then.
    private static final int NO_FLUSH_NO_DRAIN = 6;
    // Large enough that the fixed-size path cannot split the 8 test rows: every
    // row group boundary in the flush tests comes from flushRowGroup().
    private static final long ROW_GROUP_SIZE = 1_000_000;

    @Test
    public void testFlushAtRowCountCutsInsideAChunk() throws Exception {
        // Frames of 3 and 5 rows are both submitted before any flush, so 8 rows are
        // pending. Declaring the boundary at 4 cuts inside frame 2: the captured group
        // takes all of frame 1 plus one row of frame 2, and finish emits the rest.
        assertRowGroupSizes(FLUSH_BOTH_FRAMES_THEN_AT_FOUR, "flush_mid_chunk.parquet", 4, 4);
    }

    @Test
    public void testFlushAtRowCountIsClampedToPending() throws Exception {
        // 8 rows pending, boundary declared at 1_000. The clamp reduces it to 8, so the
        // whole thing is one row group. Without the clamp the writer would be asked for
        // 1_000 rows it does not have.
        assertRowGroupSizes(FLUSH_BOTH_FRAMES_THEN_OVERSIZED, "flush_oversized.parquet", 8);
    }

    @Test
    public void testFlushBeforeAnyRowsDoesNotArmBoundary() throws Exception {
        // Nothing is pending, so the flush captures nothing: the whole table lands in a
        // single row group and no zero-row row group is emitted.
        assertRowGroupSizes(FLUSH_BEFORE_ANY_ROWS, "flush_before_any_rows.parquet", 8);
    }

    @Test
    public void testFlushRowGroupClosesRowGroupAtCallerBoundary() throws Exception {
        assertRowGroupSizes(FLUSH_AFTER_EACH_FRAME, "row_group_flush.parquet", 3, 5);
    }

    @Test
    public void testFlushThenFinishEmitsCapturedBoundaryOnce() throws Exception {
        // The capture covers every pending row, so finish must emit it once and must not
        // follow it with an empty row group for the zero rows that remain.
        assertRowGroupSizes(FLUSH_MID_STREAM_THEN_FINISH, "flush_then_finish.parquet", 3);
    }

    @Test
    public void testFlushThenFinishWithoutDrainKeepsBoundary() throws Exception {
        // The caller never drains between the flush and finish: the captured boundary must
        // still split the two key runs instead of merging them into one row group.
        assertRowGroupSizes(FLUSH_MID_STREAM_NO_DRAIN, "flush_no_drain.parquet", 3, 5);
    }

    @Test
    public void testFlushThenWriteMoreRowsKeepsCapturedRowCount() throws Exception {
        // Rows written after the flush must not join the captured row group: the boundary
        // is the row count pending at the moment of the flush, not at drain time.
        assertRowGroupSizes(FLUSH_MID_STREAM_THEN_WRITE, "flush_then_write.parquet", 3, 5);
    }

    @Test
    public void testThresholdClosesRowGroupOnTheWriteThatMakesItDue() throws Exception {
        // Same threshold of 3, but the caller never drains: it only appends the one buffer
        // each write hands back. This pins WHEN the threshold fires, which the draining tests
        // above cannot see: a row group must be closed by the write that first brings the
        // pending count up TO the threshold, not by a later one that pushes it past.
        // The first frame reaches 3 pending rows exactly, so its own write must close them
        // and hand back a row group of 3. The second frame's write then closes 3 of its
        // 5 rows and finish emits the remaining 2.
        // Were the threshold only to fire once the pending count exceeded 3, the first
        // frame's write would hand back nothing, its 3 rows would still be pending when the
        // second frame arrived, and with no drain to close the backlog finish would emit the
        // whole 5-row remainder as one group, giving 3, 5 instead.
        assertRowGroupSizes(NO_FLUSH_NO_DRAIN, 3, "threshold_no_drain.parquet", 3, 3, 2);
    }

    @Test
    public void testThresholdSplitsFramesAtFixedRowGroupSize() throws Exception {
        // flushRowGroup() is never called, so every boundary below comes from the threshold.
        // The frames are 3 and 5 rows wide and the threshold is 3, which does not divide the
        // 8 rows. The first frame's write reaches 3 pending rows exactly and closes them; the
        // drain that follows finds nothing pending. The second frame's write closes the first
        // 3 of its 5 rows, the drain that follows finds 2 pending, which is below the
        // threshold, and finish emits those 2 as a short final row group.
        assertRowGroupSizes(NO_FLUSH_DRAIN_EACH_FRAME, 3, "threshold_uneven.parquet", 3, 3, 2);
    }

    @Test
    public void testThresholdWithRowCountAMultipleOfRowGroupSize() throws Exception {
        // A threshold that does divide the 8 rows, leaving no tail for finish. The first
        // frame leaves 3 rows pending, below the threshold, so no row group is due and the
        // drain returns nothing. The second frame brings the pending count to 8: its write
        // closes 4 rows, which span the whole first frame plus the first row of the second,
        // and the drain that follows closes the remaining 4. Row groups are always exactly
        // the threshold, so the boundary falls inside a frame rather than at its edge.
        assertRowGroupSizes(NO_FLUSH_DRAIN_EACH_FRAME, 4, "threshold_even.parquet", 4, 4);
    }

    private static long appendBuffer(FilesFacade ff, long fd, long buffer, long fileOffset) {
        final long dataSize = Unsafe.getLong(buffer);
        if (dataSize > 0) {
            final long written = ff.write(fd, buffer + BUFFER_HEADER_SIZE, dataSize, fileOffset);
            Assert.assertEquals(dataSize, written);
        }
        return fileOffset + dataSize;
    }

    /**
     * Whether the given flush mode captures a row group boundary once the first page frame
     * has been written. The threshold-only modes capture nothing at all.
     */
    private static boolean armsBoundaryAfterFirstFrame(int flushMode) {
        return flushMode == FLUSH_MID_STREAM_NO_DRAIN
                || flushMode == FLUSH_MID_STREAM_THEN_FINISH
                || flushMode == FLUSH_MID_STREAM_THEN_WRITE;
    }

    /**
     * Drains every row group the writer has ready, appending each to the output file.
     */
    private static long drain(long writerPtr, FilesFacade ff, long fd, long fileOffset) {
        long buffer = writeStreamingParquetChunk(writerPtr, 0, 0);
        while (buffer != 0) {
            fileOffset = appendBuffer(ff, fd, buffer, fileOffset);
            buffer = writeStreamingParquetChunk(writerPtr, 0, 0);
        }
        return fileOffset;
    }

    private static long generateParquetMeta(FilesFacade ff, Path parquetPath, long parquetFileSize, Path parquetMetaPath) {
        final long parquetFd = ff.openRO(parquetPath.$());
        Assert.assertTrue(parquetFd >= 0);
        try {
            ff.remove(parquetMetaPath.$());
            final long parquetMetaFd = ff.openRW(parquetMetaPath.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(parquetMetaFd >= 0);
            try {
                final long parquetMetaFileSize = ParquetMetadataWriter.generate(
                        Unsafe.getNativeAllocator(MemoryTag.NATIVE_DEFAULT),
                        Files.toOsFd(parquetFd),
                        parquetFileSize,
                        Files.toOsFd(parquetMetaFd)
                );
                Assert.assertTrue(parquetMetaFileSize > 0);
                return parquetMetaFileSize;
            } finally {
                ff.close(parquetMetaFd);
            }
        } finally {
            ff.close(parquetFd);
        }
    }

    /**
     * Streams the test table into a parquet file under the given flush mode and asserts the
     * row group sizes the file ends up with.
     */
    private void assertRowGroupSizes(int flushMode, String fileName, int... expectedRowGroupSizes) throws Exception {
        assertRowGroupSizes(flushMode, ROW_GROUP_SIZE, fileName, expectedRowGroupSizes);
    }

    /**
     * Streams the test table into a parquet file under the given flush mode and configured row
     * group size, and asserts the row group sizes the file ends up with.
     */
    private void assertRowGroupSizes(int flushMode, long rowGroupSize, String fileName, int... expectedRowGroupSizes) throws Exception {
        assertMemoryLeak(TestFilesFacadeImpl.INSTANCE, () -> {
            execute("CREATE TABLE t (id INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES" +
                    "(1, '2024-06-10T00:00:00.000000Z')," +
                    "(2, '2024-06-10T01:00:00.000000Z')," +
                    "(3, '2024-06-10T02:00:00.000000Z')");
            execute("INSERT INTO t VALUES" +
                    "(4, '2024-06-11T00:00:00.000000Z')," +
                    "(5, '2024-06-11T01:00:00.000000Z')," +
                    "(6, '2024-06-11T02:00:00.000000Z')," +
                    "(7, '2024-06-11T03:00:00.000000Z')," +
                    "(8, '2024-06-11T04:00:00.000000Z')");

            final FilesFacade ff = configuration.getFilesFacade();
            try (Path parquetPath = new Path().of(root).concat(fileName);
                 Path parquetMetaPath = new Path().of(root).concat(fileName + "._pm")) {
                final long parquetFileSize = streamExport(ff, parquetPath, flushMode, rowGroupSize);
                Assert.assertTrue(parquetFileSize > 0);

                final long parquetMetaFileSize = generateParquetMeta(ff, parquetPath, parquetFileSize, parquetMetaPath);
                final long parquetMetaAddr = TableUtils.mapRO(ff, parquetMetaPath.$(), LOG, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    final ParquetMetaFileReader meta = new ParquetMetaFileReader();
                    try {
                        meta.of(parquetMetaAddr, parquetMetaFileSize);
                        Assert.assertTrue(meta.resolveFooter(parquetFileSize));

                        Assert.assertEquals(expectedRowGroupSizes.length, meta.getRowGroupCount());
                        for (int i = 0; i < expectedRowGroupSizes.length; i++) {
                            Assert.assertEquals("row group " + i, expectedRowGroupSizes[i], meta.getRowGroupSize(i));
                        }
                    } finally {
                        meta.clear();
                    }
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
    }

    /**
     * Streams the whole table through the streaming parquet writer, closing row groups where
     * the given flush mode says. The table holds one daily partition of 3 rows and one of
     * 5 rows, so the page frames are 3 and 5 rows wide.
     *
     * @return the size of the written parquet file
     */
    private long streamExport(FilesFacade ff, Path parquetPath, int flushMode, long rowGroupSize) throws Exception {
        ff.remove(parquetPath.$());
        final long fd = ff.openRW(parquetPath.$(), CairoConfiguration.O_NONE);
        Assert.assertTrue(fd >= 0);
        try {
            long fileOffset = 0;
            try (RecordCursorFactory factory = select("SELECT * FROM t")) {
                final RecordMetadata metadata = factory.getMetadata();
                try (PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                     DirectUtf8Sink columnNames = new DirectUtf8Sink(64, false, MemoryTag.NATIVE_PARQUET_EXPORTER);
                     DirectLongList columnMetadata = new DirectLongList(8, MemoryTag.NATIVE_PARQUET_EXPORTER, true);
                     DirectLongList columnData = new DirectLongList(16, MemoryTag.NATIVE_PARQUET_EXPORTER, true)) {

                    columnNames.reopen();
                    columnMetadata.reopen();
                    columnData.reopen();

                    for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                        final int startSize = columnNames.size();
                        columnNames.put(metadata.getColumnName(i));
                        columnMetadata.add(columnNames.size() - startSize);
                        final int columnType = metadata.getColumnType(i);
                        Assert.assertFalse(ColumnType.isSymbol(columnType));
                        // GenericRecordMetadata on the cursor path reports no writer index.
                        int writerIndex = metadata.getWriterIndex(i);
                        if (writerIndex < 0) {
                            writerIndex = i;
                        }
                        columnMetadata.add((long) writerIndex << 32 | (columnType & 0xFFFFFFFFL));
                        columnMetadata.add(metadata.getColumnMetadata(i).getParquetEncodingConfig());
                    }

                    final long writerPtr = createStreamingParquetWriter(
                            Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_EXPORTER),
                            metadata.getColumnCount(),
                            columnNames.ptr(),
                            columnNames.size(),
                            columnMetadata.getAddress(),
                            metadata.getTimestampIndex(),
                            false,
                            ParquetCompression.packCompressionCodecLevel(ParquetCompression.COMPRESSION_UNCOMPRESSED, 0),
                            true,
                            false,
                            rowGroupSize,
                            DATA_PAGE_SIZE,
                            ParquetVersion.PARQUET_VERSION_V2,
                            0,
                            0,
                            0.0,
                            0.0
                    );
                    try {
                        if (flushMode == FLUSH_BEFORE_ANY_ROWS) {
                            // Nothing is pending, so this must capture nothing: no row group is
                            // due, and the first chunk must not be forced into a group of its own.
                            flushRowGroup(writerPtr, 0);
                            Assert.assertEquals(0, writeStreamingParquetChunk(writerPtr, 0, 0));
                        }

                        int frameIndex = 0;
                        PageFrame frame;
                        while ((frame = pageFrameCursor.next()) != null) {
                            Assert.assertEquals(PartitionFormat.NATIVE, frame.getFormat());
                            final long frameRowCount = frame.getPartitionHi() - frame.getPartitionLo();
                            Assert.assertEquals(frameIndex == 0 ? 3 : 5, frameRowCount);

                            columnData.clear();
                            for (int i = 0, n = frame.getColumnCount(); i < n; i++) {
                                final long pageAddress = frame.getPageAddress(i);
                                columnData.add(pageAddress > 0 ? 0 : frameRowCount);
                                columnData.add(pageAddress);
                                columnData.add(frame.getPageSize(i));
                                columnData.add(frame.getAuxPageAddress(i));
                                columnData.add(frame.getAuxPageSize(i));
                                columnData.add(0L);
                                columnData.add(0L);
                            }

                            long buffer = writeStreamingParquetChunk(writerPtr, columnData.getAddress(), frameRowCount);
                            if ((flushMode == FLUSH_MID_STREAM_NO_DRAIN && frameIndex == 1)
                                    || flushMode == NO_FLUSH_NO_DRAIN
                                    || flushMode == FLUSH_BOTH_FRAMES_THEN_AT_FOUR
                                    || flushMode == FLUSH_BOTH_FRAMES_THEN_OVERSIZED) {
                                // Take only what the write itself handed back: no drain call
                                // to close anything the write left pending.
                                if (buffer != 0) {
                                    fileOffset = appendBuffer(ff, fd, buffer, fileOffset);
                                }
                            } else {
                                while (buffer != 0) {
                                    fileOffset = appendBuffer(ff, fd, buffer, fileOffset);
                                    buffer = writeStreamingParquetChunk(writerPtr, 0, 0);
                                }
                            }

                            if (flushMode == FLUSH_AFTER_EACH_FRAME) {
                                flushRowGroup(writerPtr, frameRowCount);
                                fileOffset = drain(writerPtr, ff, fd, fileOffset);

                                // Flushing again with nothing pending must not emit an empty row
                                // group: ParquetMetaFileReader treats a zero-row group as corruption.
                                flushRowGroup(writerPtr, 0);
                                fileOffset = drain(writerPtr, ff, fd, fileOffset);
                            } else if (frameIndex == 0 && armsBoundaryAfterFirstFrame(flushMode)) {
                                // Capture the boundary between the two key runs, then leave it to
                                // the next write or to finish to emit it.
                                flushRowGroup(writerPtr, frameRowCount);
                            }

                            frameIndex++;
                            if (flushMode == FLUSH_MID_STREAM_THEN_FINISH) {
                                break;
                            }
                        }
                        Assert.assertEquals(flushMode == FLUSH_MID_STREAM_THEN_FINISH ? 1 : 2, frameIndex);

                        if (flushMode == FLUSH_BOTH_FRAMES_THEN_AT_FOUR) {
                            // 8 rows pending across two frames; the boundary lands inside
                            // the second one, which only the row-count form can name.
                            flushRowGroup(writerPtr, 4);
                        } else if (flushMode == FLUSH_BOTH_FRAMES_THEN_OVERSIZED) {
                            flushRowGroup(writerPtr, 1_000);
                        }

                        fileOffset = appendBuffer(ff, fd, finishStreamingParquetWrite(writerPtr), fileOffset);
                    } finally {
                        closeStreamingParquetWriter(writerPtr);
                    }
                }
            }
            return fileOffset;
        } finally {
            ff.close(fd);
        }
    }
}
