/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;

/**
 * External merge sort for parquet files that are too large to sort in one pass.
 * <p>
 * Phase A (this class): stream the source one row group at a time, sort each RG
 * internally by the timestamp column, and persist the sorted RG as a standalone
 * parquet file (a "run") via the provided {@link FilesFacade}. When the facade
 * is a {@link io.questdb.std.MemFdFilesFacade}, runs live entirely in anonymous
 * RAM.
 * <p>
 * Phase B (to follow in a later milestone) will k-way merge the runs into a
 * single sorted output stream, dispatched to a pluggable sink.
 * <p>
 * Memory profile per RG worker (single-threaded in Phase A by default):
 * <ul>
 *   <li>One {@link RowGroupBuffers} for the decoded RG (decompressed size)</li>
 *   <li>One 16 B/row sort index (ts, localRow) pairs</li>
 *   <li>One set of output column buffers of the same shape as decoded
 *       (scatter destination; var-columns grow on demand)</li>
 *   <li>The streaming parquet encoder's internal buffer (~tens of MB)</li>
 * </ul>
 * All native allocations use {@link MemoryTag#NATIVE_IMPORT} so leaks surface
 * in tag-based assertions.
 */
public class ExternalSortedParquetImporter {

    private static final Log LOG = LogFactory.getLog(ExternalSortedParquetImporter.class);
    // Each run holds exactly one source row group worth of data in a single
    // output row group, so the streaming writer flushes everything in one shot.
    private static final long RUN_DATA_PAGE_SIZE = 0L; // default
    // TODO (M2): make the run-path namespace a caller-supplied prefix so that
    // concurrent imports on the same MemFdFilesFacade don't collide on
    // "run_<i>.parquet". For M1 there is a single caller per facade.
    private static final String RUN_PATH_PREFIX = "run_";
    private static final String RUN_PATH_SUFFIX = ".parquet";

    /**
     * Phase A: produce one sorted parquet run per source row group.
     * <p>
     * Runs are written to {@code runFf} at synthetic paths of the form
     * "run_&lt;sourceRgIndex&gt;.parquet". The caller owns the returned list
     * and must close each {@link RunHandle} (which removes the run from the
     * facade and releases its backing memory when the facade is memfd-backed).
     *
     * @param srcFf          facade for reading the source parquet
     * @param runFf          facade that will host the run files (typically a
     *                       {@link io.questdb.std.MemFdFilesFacade})
     * @param srcPath        path to the source parquet file
     * @param tsColumnName   name of the timestamp column to sort by
     * @param tsColumnType   QuestDB column type for the timestamp column in the
     *                       output; 0 means "use the source column type".
     *                       Currently only TIMESTAMP source types are supported.
     * @return list of run handles, one per source row group, in source-RG order
     */
    public static ObjList<RunHandle> phaseA(
            FilesFacade srcFf,
            FilesFacade runFf,
            CharSequence srcPath,
            CharSequence tsColumnName,
            int tsColumnType
    ) {
        final int memoryTag = MemoryTag.NATIVE_IMPORT;
        final ObjList<RunHandle> runs = new ObjList<>();

        long srcFd = -1;
        long srcAddr = 0;
        long srcFileSize = 0;
        PartitionDecoder decoder = null;
        DirectIntList columns = null;

        try {
            try (Path p = new Path()) {
                p.put(srcPath);
                srcFd = TableUtils.openRO(srcFf, p.$(), LOG);
                srcFileSize = srcFf.length(srcFd);
                srcAddr = TableUtils.mapRO(srcFf, srcFd, srcFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }

            decoder = new PartitionDecoder();
            decoder.of(srcAddr, srcFileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            final PartitionDecoder.Metadata meta = decoder.metadata();
            final int columnCount = meta.getColumnCount();
            final int rowGroupCount = meta.getRowGroupCount();
            final long totalRows = meta.getRowCount();

            final int tsColumnIndex = meta.getColumnIndex(tsColumnName);
            if (tsColumnIndex < 0) {
                throw CairoException.nonCritical()
                        .put("timestamp column not found in parquet file [column=")
                        .put(tsColumnName)
                        .put(']');
            }

            final int srcTsColType = meta.getColumnType(tsColumnIndex);
            if (!ColumnType.isTimestamp(srcTsColType)) {
                // Phase A M1 milestone: TIMESTAMP source only. LONG and DATE handling
                // will follow once the base path is validated.
                throw CairoException.nonCritical()
                        .put("phaseA currently requires TIMESTAMP source type [column=")
                        .put(tsColumnName)
                        .put(", type=").put(ColumnType.nameOf(srcTsColType))
                        .put(']');
            }
            final int effectiveTsType = tsColumnType > 0 ? tsColumnType : srcTsColType;

            columns = new DirectIntList(2L * columnCount, memoryTag);
            for (int i = 0; i < columnCount; i++) {
                columns.add(i);
                columns.add(meta.getColumnType(i));
            }

            LOG.info()
                    .$("phaseA start [src=").$(srcPath)
                    .$(", rows=").$(totalRows)
                    .$(", rowGroups=").$(rowGroupCount)
                    .$(", columns=").$(columnCount)
                    .$(", tsColumn=").$(tsColumnName)
                    .I$();

            for (int rg = 0; rg < rowGroupCount; rg++) {
                final int rgRows = meta.getRowGroupSize(rg);
                final String runPath = RUN_PATH_PREFIX + rg + RUN_PATH_SUFFIX;
                final RunHandle run = sortAndWriteRun(
                        runFf, decoder, columns, meta,
                        rg, rgRows, tsColumnIndex, effectiveTsType,
                        runPath, columnCount, memoryTag
                );
                runs.add(run);
            }

            LOG.info()
                    .$("phaseA done [runs=").$(runs.size())
                    .$(", rows=").$(totalRows)
                    .I$();

            return runs;
        } catch (Throwable t) {
            // On any failure, release every run we already produced so memfd
            // pages are reclaimed before the exception propagates.
            Misc.freeObjListAndClear(runs);
            throw t;
        } finally {
            Misc.free(columns);
            Misc.free(decoder);
            if (srcAddr != 0) {
                srcFf.munmap(srcAddr, srcFileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (srcFd != -1) {
                srcFf.close(srcFd);
            }
        }
    }

    /**
     * Test helper: walks the ts column of a run and returns -1 if every value
     * is monotonically non-decreasing, or the row index of the first offending
     * pair otherwise. Opens and maps the run via {@code runFf}, decodes only
     * the ts column, and releases everything before returning.
     */
    public static long verifyRunMonotonic(
            FilesFacade runFf,
            RunHandle run,
            CharSequence tsColumnName
    ) {
        final int memoryTag = MemoryTag.NATIVE_IMPORT;

        long fd = -1;
        long addr = 0;
        long size = 0;
        PartitionDecoder decoder = null;
        DirectIntList tsCols = null;
        RowGroupBuffers buffers = null;

        try {
            try (Path p = new Path()) {
                p.put(run.path);
                fd = TableUtils.openRO(runFf, p.$(), LOG);
                size = runFf.length(fd);
                addr = TableUtils.mapRO(runFf, fd, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }

            decoder = new PartitionDecoder();
            decoder.of(addr, size, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            final PartitionDecoder.Metadata meta = decoder.metadata();
            final int tsIdx = meta.getColumnIndex(tsColumnName);
            if (tsIdx < 0) {
                throw CairoException.nonCritical()
                        .put("timestamp column not found in run [column=")
                        .put(tsColumnName)
                        .put(']');
            }

            tsCols = new DirectIntList(2, memoryTag);
            tsCols.add(tsIdx);
            tsCols.add(meta.getColumnType(tsIdx));

            buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            long globalRow = 0;
            long prev = Long.MIN_VALUE;
            final int rgCount = meta.getRowGroupCount();
            for (int rg = 0; rg < rgCount; rg++) {
                final int rgRows = meta.getRowGroupSize(rg);
                decoder.decodeRowGroup(buffers, tsCols, rg, 0, rgRows);
                final long ptr = buffers.getChunkDataPtr(0);
                for (int i = 0; i < rgRows; i++) {
                    final long ts = Unsafe.getUnsafe().getLong(ptr + i * 8L);
                    if (globalRow > 0 && ts < prev) {
                        return globalRow;
                    }
                    prev = ts;
                    globalRow++;
                }
            }
            return -1;
        } finally {
            Misc.free(buffers);
            Misc.free(tsCols);
            Misc.free(decoder);
            if (addr != 0) {
                runFf.munmap(addr, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                runFf.close(fd);
            }
        }
    }

    private static RunHandle sortAndWriteRun(
            FilesFacade runFf,
            PartitionDecoder decoder,
            DirectIntList columns,
            PartitionDecoder.Metadata meta,
            int rgIndex,
            int rgRows,
            int tsColumnIndex,
            int effectiveTsType,
            String runPath,
            int columnCount,
            int memoryTag
    ) {
        if (rgRows <= 0) {
            throw CairoException.nonCritical()
                    .put("cannot produce a run from an empty row group [rg=")
                    .put(rgIndex)
                    .put(']');
        }

        // Column classification, mirroring BulkSortedParquetWriter.kWayMergeCluster.
        final int[] colTypes = new int[columnCount];
        final int[] colSizes = new int[columnCount];
        final boolean[] isVarCol = new boolean[columnCount];
        for (int c = 0; c < columnCount; c++) {
            colTypes[c] = meta.getColumnType(c);
            if (ColumnType.isVarSize(colTypes[c])) {
                isVarCol[c] = true;
                colSizes[c] = -1;
            } else if (ColumnType.isSymbol(colTypes[c])) {
                colSizes[c] = Integer.BYTES;
            } else {
                colSizes[c] = ColumnType.sizeOf(colTypes[c]);
            }
        }

        RowGroupBuffers srcBuf = null;
        long sortArray = 0;
        final long[] outDataAddrs = new long[columnCount];
        final long[] outDataSizes = new long[columnCount];
        final long[] outAuxAddrs = new long[columnCount];
        final long[] outAuxSizes = new long[columnCount];
        final long[] varDataOffsets = new long[columnCount];
        final long[] varDataCapacities = new long[columnCount];

        long writerPtr = 0;
        long runFd = -1;
        long fileOffset = 0;
        boolean isRegistered = false;

        try {
            // 1. Decode the full RG into native buffers.
            srcBuf = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            decoder.decodeRowGroup(srcBuf, columns, rgIndex, 0, rgRows);

            // 2. Build (ts, localRow) index and radix-sort by ts.
            sortArray = Unsafe.malloc(16L * rgRows, memoryTag);
            final long tsPtr = srcBuf.getChunkDataPtr(tsColumnIndex);
            for (int r = 0; r < rgRows; r++) {
                Unsafe.getUnsafe().putLong(sortArray + r * 16L, Unsafe.getUnsafe().getLong(tsPtr + r * 8L));
                Unsafe.getUnsafe().putLong(sortArray + r * 16L + 8, r);
            }
            Vect.sortLongIndexAscInPlace(sortArray, rgRows);

            final long runMinTs = Unsafe.getUnsafe().getLong(sortArray);
            final long runMaxTs = Unsafe.getUnsafe().getLong(sortArray + (long) (rgRows - 1) * 16L);

            // 3. Allocate output chunk buffers (one chunk == one output RG).
            for (int c = 0; c < columnCount; c++) {
                if (isVarCol[c]) {
                    final long auxEntrySize = BulkSortedParquetWriter.getAuxEntrySize(colTypes[c]);
                    final long auxSize = auxEntrySize * rgRows;
                    outAuxAddrs[c] = Unsafe.calloc(auxSize, memoryTag);
                    outAuxSizes[c] = auxSize;
                    // Heuristic initial capacity for var data. Grows on demand via
                    // ensureVarCapacity. 64 B/row is a rough-and-ready starting point.
                    final long initDataSize = 64L * rgRows;
                    outDataAddrs[c] = Unsafe.malloc(initDataSize, memoryTag);
                    outDataSizes[c] = initDataSize;
                    varDataCapacities[c] = initDataSize;
                } else if (colSizes[c] > 0) {
                    final long bufSize = (long) colSizes[c] * rgRows;
                    outDataAddrs[c] = Unsafe.malloc(bufSize, memoryTag);
                    outDataSizes[c] = bufSize;
                }
            }

            // 4. Scatter rows into output buffers in sorted order.
            for (int outPos = 0; outPos < rgRows; outPos++) {
                final int origRow = (int) Unsafe.getUnsafe().getLong(sortArray + outPos * 16L + 8);
                for (int c = 0; c < columnCount; c++) {
                    if (isVarCol[c]) {
                        BulkSortedParquetWriter.copyVarValue(
                                srcBuf, c, origRow,
                                outDataAddrs, outAuxAddrs,
                                varDataOffsets, varDataCapacities,
                                outPos, colTypes[c], memoryTag
                        );
                    } else if (colSizes[c] > 0) {
                        final int elemSize = colSizes[c];
                        final long srcPtr = srcBuf.getChunkDataPtr(c) + (long) origRow * elemSize;
                        final long dstPtr = outDataAddrs[c] + (long) outPos * elemSize;
                        switch (elemSize) {
                            case 1 -> Unsafe.getUnsafe().putByte(dstPtr, Unsafe.getUnsafe().getByte(srcPtr));
                            case 2 -> Unsafe.getUnsafe().putShort(dstPtr, Unsafe.getUnsafe().getShort(srcPtr));
                            case 4 -> Unsafe.getUnsafe().putInt(dstPtr, Unsafe.getUnsafe().getInt(srcPtr));
                            case 8 -> Unsafe.getUnsafe().putLong(dstPtr, Unsafe.getUnsafe().getLong(srcPtr));
                            case 16 -> {
                                Unsafe.getUnsafe().putLong(dstPtr, Unsafe.getUnsafe().getLong(srcPtr));
                                Unsafe.getUnsafe().putLong(dstPtr + 8, Unsafe.getUnsafe().getLong(srcPtr + 8));
                            }
                            default -> Vect.memcpy(dstPtr, srcPtr, elemSize);
                        }
                    }
                }
            }

            // Source RG buffers no longer needed — release before encode to cap peak RSS.
            Misc.free(srcBuf);
            srcBuf = null;
            Unsafe.free(sortArray, 16L * rgRows, memoryTag);
            sortArray = 0;

            // 5. Create streaming writer sized to exactly one row group of rgRows.
            writerPtr = createRunWriter(
                    meta, tsColumnIndex, effectiveTsType, columnCount, rgRows, memoryTag
            );

            // 6. Open the destination fd via runFf and drain the encoder into it.
            try (Path dstP = new Path()) {
                dstP.put(runPath);
                runFd = runFf.openRW(dstP.$(), CairoConfiguration.O_NONE);
                if (runFd < 0) {
                    throw CairoException.critical(runFf.errno())
                            .put("cannot create run file [path=")
                            .put(runPath)
                            .put(']');
                }
                isRegistered = true;

                fileOffset = BulkSortedParquetWriter.flushOutputChunk(
                        runFf, runFd, fileOffset, writerPtr,
                        outDataAddrs, outDataSizes, outAuxAddrs, outAuxSizes,
                        varDataOffsets, isVarCol, columnCount, rgRows, memoryTag,
                        null // not the last chunk in a merge; we still own the buffers
                );

                // Finalise the parquet footer and drain it before we free buffers.
                // Because run_rg_size == rgRows, everything has been flushed by now,
                // so keeping the buffers alive past finishStreamingParquetWrite is safe.
                fileOffset = BulkSortedParquetWriter.drainBuffer(
                        runFf, runFd, fileOffset,
                        PartitionEncoder.finishStreamingParquetWrite(writerPtr)
                );
                runFf.truncate(runFd, fileOffset);
            }

            final long finalSize = fileOffset;
            LOG.info()
                    .$("run written [rg=").$(rgIndex)
                    .$(", path=").$(runPath)
                    .$(", rows=").$(rgRows)
                    .$(", minTs=").$(runMinTs)
                    .$(", maxTs=").$(runMaxTs)
                    .$(", fileSize=").$(finalSize)
                    .I$();

            return new RunHandle(runFf, runPath, rgRows, runMinTs, runMaxTs, finalSize);
        } catch (Throwable t) {
            // If we opened and registered a run fd before failing, remove it so
            // the memfd pages drop before we propagate the exception.
            if (isRegistered) {
                try (Path dstP = new Path()) {
                    dstP.put(runPath);
                    runFf.remove(dstP.$());
                } catch (Throwable ignore) {
                    // swallow — primary exception wins
                }
            }
            throw t;
        } finally {
            if (writerPtr != 0) {
                PartitionEncoder.closeStreamingParquetWriter(writerPtr);
            }
            if (runFd >= 0) {
                runFf.close(runFd);
            }
            if (sortArray != 0) {
                Unsafe.free(sortArray, 16L * rgRows, memoryTag);
            }
            if (srcBuf != null) {
                Misc.free(srcBuf);
            }
            for (int c = 0; c < columnCount; c++) {
                if (outDataAddrs[c] != 0) {
                    final long cap = varDataCapacities[c] > 0 ? varDataCapacities[c] : outDataSizes[c];
                    Unsafe.free(outDataAddrs[c], cap, memoryTag);
                }
                if (outAuxAddrs[c] != 0) {
                    Unsafe.free(outAuxAddrs[c], outAuxSizes[c], memoryTag);
                }
            }
        }
    }

    private static long createRunWriter(
            PartitionDecoder.Metadata meta,
            int tsColumnIndex,
            int effectiveTsType,
            int columnCount,
            int rgRows,
            int memoryTag
    ) {
        final long allocator = Unsafe.getNativeAllocator(memoryTag);
        try (
                DirectUtf8Sink columnNames = new DirectUtf8Sink(256);
                DirectLongList columnMetadata = new DirectLongList(2L * columnCount, memoryTag)
        ) {
            for (int c = 0; c < columnCount; c++) {
                final int startSize = columnNames.size();
                columnNames.put(meta.getColumnName(c));
                final int nameSize = columnNames.size() - startSize;
                columnMetadata.add(nameSize);
                final int colType = (c == tsColumnIndex) ? effectiveTsType : meta.getColumnType(c);
                columnMetadata.add((long) c << 32 | (colType & 0xFFFFFFFFL));
            }

            return PartitionEncoder.createStreamingParquetWriter(
                    allocator,
                    columnCount,
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    tsColumnIndex,
                    false, // ascending
                    ParquetCompression.WRITER_COMPRESSION_SNAPPY,
                    true,  // statistics enabled
                    false, // raw array encoding
                    (long) rgRows,
                    RUN_DATA_PAGE_SIZE,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0L,
                    0,
                    PartitionEncoder.DEFAULT_BLOOM_FILTER_FPP,
                    0.0
            );
        }
    }

    /**
     * Handle to a single sorted run file. Closing it removes the run from the
     * backing files facade so any memfd pages are freed. Idempotent close.
     */
    public static class RunHandle implements QuietCloseable {
        public final long fileSize;
        public final long maxTs;
        public final long minTs;
        public final String path;
        public final long rowCount;
        private final FilesFacade runFf;
        private boolean isClosed;

        RunHandle(
                FilesFacade runFf,
                String path,
                long rowCount,
                long minTs,
                long maxTs,
                long fileSize
        ) {
            this.runFf = runFf;
            this.path = path;
            this.rowCount = rowCount;
            this.minTs = minTs;
            this.maxTs = maxTs;
            this.fileSize = fileSize;
        }

        @Override
        public void close() {
            if (isClosed) {
                return;
            }
            isClosed = true;
            try (Path p = new Path()) {
                p.put(path);
                runFf.remove(p.$());
            }
        }
    }
}
