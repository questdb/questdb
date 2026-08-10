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
 * Covers {@link io.questdb.griffin.engine.table.parquet.PartitionEncoder#flushRowGroup(long)}:
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
    private static final int FLUSH_MID_STREAM_NO_DRAIN = 2;
    // Flush after the first frame, then finish without writing anything else.
    private static final int FLUSH_MID_STREAM_THEN_FINISH = 3;
    // Flush after the first frame, write the second frame, then drain.
    private static final int FLUSH_MID_STREAM_THEN_WRITE = 4;
    // Large enough that the fixed-size path cannot split the 8 test rows: every
    // row group boundary in this test comes from flushRowGroup().
    private static final long ROW_GROUP_SIZE = 1_000_000;

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

    private static long appendBuffer(FilesFacade ff, long fd, long buffer, long fileOffset) {
        final long dataSize = Unsafe.getLong(buffer);
        if (dataSize > 0) {
            final long written = ff.write(fd, buffer + BUFFER_HEADER_SIZE, dataSize, fileOffset);
            Assert.assertEquals(dataSize, written);
        }
        return fileOffset + dataSize;
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
                final long parquetFileSize = streamExport(ff, parquetPath, flushMode);
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
    private long streamExport(FilesFacade ff, Path parquetPath, int flushMode) throws Exception {
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
                            ROW_GROUP_SIZE,
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
                            flushRowGroup(writerPtr);
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
                            if (flushMode == FLUSH_MID_STREAM_NO_DRAIN && frameIndex == 1) {
                                // Take only what the write itself handed back: no drain call
                                // between the flush and finish.
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
                                flushRowGroup(writerPtr);
                                fileOffset = drain(writerPtr, ff, fd, fileOffset);

                                // Flushing again with nothing pending must not emit an empty row
                                // group: ParquetMetaFileReader treats a zero-row group as corruption.
                                flushRowGroup(writerPtr);
                                fileOffset = drain(writerPtr, ff, fd, fileOffset);
                            } else if (frameIndex == 0 && flushMode != FLUSH_BEFORE_ANY_ROWS) {
                                // Capture the boundary between the two key runs, then leave it to
                                // the next write or to finish to emit it.
                                flushRowGroup(writerPtr);
                            }

                            frameIndex++;
                            if (flushMode == FLUSH_MID_STREAM_THEN_FINISH) {
                                break;
                            }
                        }
                        Assert.assertEquals(flushMode == FLUSH_MID_STREAM_THEN_FINISH ? 1 : 2, frameIndex);

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
