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
    // Large enough that the fixed-size path cannot split the 8 test rows: every
    // row group boundary in this test comes from flushRowGroup().
    private static final long ROW_GROUP_SIZE = 1_000_000;

    @Test
    public void testFlushRowGroupClosesRowGroupAtCallerBoundary() throws Exception {
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
            try (Path parquetPath = new Path().of(root).concat("row_group_flush.parquet");
                 Path parquetMetaPath = new Path().of(root).concat("row_group_flush.parquet._pm")) {
                final long parquetFileSize = streamExport(ff, parquetPath);
                Assert.assertTrue(parquetFileSize > 0);

                final long parquetMetaFileSize = generateParquetMeta(ff, parquetPath, parquetFileSize, parquetMetaPath);
                final long parquetMetaAddr = TableUtils.mapRO(ff, parquetMetaPath.$(), LOG, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    final ParquetMetaFileReader meta = new ParquetMetaFileReader();
                    try {
                        meta.of(parquetMetaAddr, parquetMetaFileSize);
                        Assert.assertTrue(meta.resolveFooter(parquetFileSize));

                        Assert.assertEquals(2, meta.getRowGroupCount());
                        Assert.assertEquals(3, meta.getRowGroupSize(0));
                        Assert.assertEquals(5, meta.getRowGroupSize(1));
                    } finally {
                        meta.clear();
                    }
                } finally {
                    ff.munmap(parquetMetaAddr, parquetMetaFileSize, MemoryTag.MMAP_DEFAULT);
                }
            }
        });
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
     * Streams the whole table through the streaming parquet writer, closing a row group
     * after every page frame. The table holds one daily partition of 3 rows and one of
     * 5 rows, so the frames are 3 and 5 rows wide and the file must end up with row
     * groups of exactly those sizes.
     *
     * @return the size of the written parquet file
     */
    private long streamExport(FilesFacade ff, Path parquetPath) throws Exception {
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
                            while (buffer != 0) {
                                fileOffset = appendBuffer(ff, fd, buffer, fileOffset);
                                buffer = writeStreamingParquetChunk(writerPtr, 0, 0);
                            }

                            flushRowGroup(writerPtr);
                            fileOffset = drain(writerPtr, ff, fd, fileOffset);

                            // Flushing again with nothing pending must not emit an empty row
                            // group: ParquetMetaFileReader treats a zero-row group as corruption.
                            flushRowGroup(writerPtr);
                            fileOffset = drain(writerPtr, ff, fd, fileOffset);

                            frameIndex++;
                        }
                        Assert.assertEquals(2, frameIndex);

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
