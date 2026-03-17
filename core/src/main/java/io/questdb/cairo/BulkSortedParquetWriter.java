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
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;

/**
 * Bulk sorted parquet import: reads a source parquet file, sorts by a timestamp
 * column, and writes a new sorted parquet file.
 * <p>
 * Uses a statistics-driven approach (Approach C from the plan) with a
 * source-order scatter fallback (Approach D) for overlapping row groups:
 * <ol>
 *   <li>Read row group min/max timestamp statistics (microseconds, no decode)</li>
 *   <li>Sort row groups by min timestamp</li>
 *   <li>Non-overlapping row groups: direct copy via
 *       {@code writeStreamingParquetChunkFromRowGroup} (no decode/encode)</li>
 *   <li>Overlapping clusters: decode, radix sort, scatter-write to output
 *       buffers, write via {@code writeStreamingParquetChunk}</li>
 * </ol>
 */
public class BulkSortedParquetWriter {
    private static final Log LOG = LogFactory.getLog(BulkSortedParquetWriter.class);
    private static final int BUFFER_HEADER_SIZE = 2 * Long.BYTES;
    private static final int CHUNK_COL_ENTRY_SIZE = 7; // entries per column for writeStreamingParquetChunk

    /**
     * Execute a bulk sorted parquet write using the same FilesFacade for source and destination.
     */
    public static void execute(
            FilesFacade ff,
            CharSequence srcPath,
            CharSequence dstPath,
            CharSequence tsColumnName
    ) {
        execute(ff, ff, srcPath, dstPath, tsColumnName, -1);
    }

    /**
     * Execute a bulk sorted parquet write.
     *
     * @param srcFf         files facade for reading the source parquet
     * @param dstFf         files facade for writing the output parquet
     * @param srcPath       path to source parquet file
     * @param dstPath       path to destination parquet file
     * @param tsColumnName  name of the timestamp column to sort by
     * @param tsColumnIndex index of the timestamp column in the parquet metadata, or -1 to look up by name
     */
    public static void execute(
            FilesFacade srcFf,
            FilesFacade dstFf,
            CharSequence srcPath,
            CharSequence dstPath,
            CharSequence tsColumnName,
            int tsColumnIndex
    ) {
        final int memoryTag = MemoryTag.NATIVE_IMPORT;

        long fd = -1;
        long addr = 0;
        long fileSize = 0;
        PartitionDecoder decoder = null;
        DirectIntList columns = null;

        try {
            // Open and mmap source parquet file (always via srcFf)
            try (Path p = new Path()) {
                p.put(srcPath);
                fd = TableUtils.openRO(srcFf, p.$(), LOG);
                fileSize = srcFf.length(fd);
                addr = TableUtils.mapRO(srcFf, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }

            decoder = new PartitionDecoder();
            decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

            final PartitionDecoder.Metadata meta = decoder.metadata();
            final int columnCount = meta.getColumnCount();
            final int rowGroupCount = meta.getRowGroupCount();
            final long totalRows = meta.getRowCount();

            // Resolve timestamp column index
            if (tsColumnIndex < 0) {
                tsColumnIndex = meta.getColumnIndex(tsColumnName);
                if (tsColumnIndex < 0) {
                    throw CairoException.nonCritical()
                            .put("timestamp column not found in parquet file [column=")
                            .put(tsColumnName)
                            .put(']');
                }
            }

            // Verify timestamp column type
            final int tsColType = meta.getColumnType(tsColumnIndex);
            if (!ColumnType.isTimestamp(tsColType) && tsColType != ColumnType.LONG && tsColType != ColumnType.DATE) {
                throw CairoException.nonCritical()
                        .put("timestamp column must be TIMESTAMP, LONG, or DATE [column=")
                        .put(tsColumnName)
                        .put(", type=").put(ColumnType.nameOf(tsColType))
                        .put(']');
            }

            // Build column projection: [parquet_index, column_type] pairs
            columns = new DirectIntList(2L * columnCount, memoryTag);
            for (int i = 0; i < columnCount; i++) {
                columns.add(i);
                columns.add(meta.getColumnType(i));
            }

            LOG.info()
                    .$("starting bulk sorted parquet write [src=").$(srcPath)
                    .$(", dst=").$(dstPath)
                    .$(", rows=").$(totalRows)
                    .$(", rowGroups=").$(rowGroupCount)
                    .$(", columns=").$(columnCount)
                    .$(", tsColumn=").$(tsColumnName)
                    .I$();

            if (rowGroupCount == 0 || totalRows == 0) {
                LOG.info().$("source parquet is empty, nothing to do").$();
                return;
            }

            // Phase 1: Read row group statistics and sort
            final long[] rgOrigIndex = new long[rowGroupCount];
            final long[] rgMinTs = new long[rowGroupCount];
            final long[] rgMaxTs = new long[rowGroupCount];
            final int[] rgSize = new int[rowGroupCount];

            for (int i = 0; i < rowGroupCount; i++) {
                rgOrigIndex[i] = i;
                rgMinTs[i] = decoder.rowGroupMinTimestamp(i, tsColumnIndex);
                rgMaxTs[i] = decoder.rowGroupMaxTimestamp(i, tsColumnIndex);
                rgSize[i] = meta.getRowGroupSize(i);
            }

            // Sort row groups by min timestamp (insertion sort — typically <1000 RGs)
            for (int i = 1; i < rowGroupCount; i++) {
                long keyMin = rgMinTs[i];
                long keyMax = rgMaxTs[i];
                long keyIdx = rgOrigIndex[i];
                int keySize = rgSize[i];
                int j = i - 1;
                while (j >= 0 && rgMinTs[j] > keyMin) {
                    rgMinTs[j + 1] = rgMinTs[j];
                    rgMaxTs[j + 1] = rgMaxTs[j];
                    rgOrigIndex[j + 1] = rgOrigIndex[j];
                    rgSize[j + 1] = rgSize[j];
                    j--;
                }
                rgMinTs[j + 1] = keyMin;
                rgMaxTs[j + 1] = keyMax;
                rgOrigIndex[j + 1] = keyIdx;
                rgSize[j + 1] = keySize;
            }

            // Phase 2: Detect overlaps and classify row groups
            int directCopyCount = 0;
            int overlapClusterCount = 0;
            long overlapRows = 0;
            long directCopyRows = 0;

            // Mark which sorted row groups overlap with the next one
            boolean[] isOverlapStart = new boolean[rowGroupCount];
            for (int i = 0; i < rowGroupCount - 1; i++) {
                if (rgMaxTs[i] > rgMinTs[i + 1]) {
                    isOverlapStart[i] = true;
                }
            }

            // Count overlapping clusters for logging
            int i = 0;
            while (i < rowGroupCount) {
                if (i < rowGroupCount - 1 && isOverlapStart[i]) {
                    // Start of an overlap cluster
                    int clusterEnd = i + 1;
                    while (clusterEnd < rowGroupCount - 1 && isOverlapStart[clusterEnd]) {
                        clusterEnd++;
                    }
                    clusterEnd++; // inclusive
                    overlapClusterCount++;
                    for (int k = i; k < clusterEnd; k++) {
                        overlapRows += rgSize[k];
                    }
                    i = clusterEnd;
                } else {
                    directCopyCount++;
                    directCopyRows += rgSize[i];
                    i++;
                }
            }

            LOG.info()
                    .$("row group analysis [directCopy=").$(directCopyCount)
                    .$(", directCopyRows=").$(directCopyRows)
                    .$(", overlapClusters=").$(overlapClusterCount)
                    .$(", overlapRows=").$(overlapRows)
                    .I$();

            // Phase 3: Create streaming parquet writer and process row groups
            writeOutput(
                    dstFf, decoder, columns, meta,
                    dstPath, tsColumnIndex,
                    rgOrigIndex, rgMinTs, rgMaxTs, rgSize,
                    isOverlapStart, rowGroupCount, columnCount, totalRows,
                    memoryTag
            );

            LOG.info()
                    .$("bulk sorted parquet write complete [dst=").$(dstPath)
                    .$(", rows=").$(totalRows)
                    .I$();
        } finally {
            Misc.free(columns);
            Misc.free(decoder);
            if (addr != 0) {
                srcFf.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (fd != -1) {
                srcFf.close(fd);
            }
        }
    }

    private static void writeOutput(
            FilesFacade ff,
            PartitionDecoder decoder,
            DirectIntList columns,
            PartitionDecoder.Metadata meta,
            CharSequence dstPath,
            int tsColumnIndex,
            long[] rgOrigIndex,
            long[] rgMinTs,
            long[] rgMaxTs,
            int[] rgSize,
            boolean[] isOverlapStart,
            int rowGroupCount,
            int columnCount,
            long totalRows,
            int memoryTag
    ) {
        long writerPtr = 0;
        long allocator = Unsafe.getNativeAllocator(memoryTag);
        long outputFd = -1;
        long fileOffset = 0;

        try (
                DirectUtf8Sink columnNames = new DirectUtf8Sink(256);
                DirectLongList columnMetadata = new DirectLongList(2L * columnCount, memoryTag);
                Path dstP = new Path()
        ) {
            // Build column names and metadata for the streaming writer.
            // Use sequential column IDs (0, 1, 2, ...) to match QuestDB's
            // writer index convention for the destination table.
            for (int c = 0; c < columnCount; c++) {
                int startSize = columnNames.size();
                columnNames.put(meta.getColumnName(c));
                int nameSize = columnNames.size() - startSize;
                columnMetadata.add(nameSize);
                int colType = meta.getColumnType(c);
                columnMetadata.add((long) c << 32 | (colType & 0xFFFFFFFFL));
            }

            writerPtr = PartitionEncoder.createStreamingParquetWriter(
                    allocator,
                    columnCount,
                    columnNames.ptr(),
                    columnNames.size(),
                    columnMetadata.getAddress(),
                    tsColumnIndex,
                    false, // ascending
                    ParquetCompression.COMPRESSION_UNCOMPRESSED,
                    true,  // statistics enabled
                    false, // raw array encoding
                    0,     // default row group size
                    0,     // default data page size
                    ParquetVersion.PARQUET_VERSION_V1
            );

            // Open output file
            dstP.put(dstPath);
            outputFd = ff.openRW(dstP.$(), CairoConfiguration.O_NONE);
            if (outputFd < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot create destination parquet file [path=")
                        .put(dstPath)
                        .put(']');
            }

            // Global sort: treat all row groups as one overlapping cluster.
            // The cluster's column data buffers must stay alive until
            // finishStreamingParquetWrite, which is when the Rust encoder
            // actually reads them.
            //
            // deferredFrees collects (addr, size) pairs for buffers whose
            // lifetime must extend past the cluster method.
            DirectLongList deferredFrees = new DirectLongList(32, memoryTag);
            try {
                fileOffset = writeOverlappingCluster(
                        ff, outputFd, fileOffset,
                        decoder, columns, meta,
                        writerPtr,
                        tsColumnIndex,
                        rgOrigIndex, rgSize,
                        0, rowGroupCount,
                        columnCount, memoryTag,
                        deferredFrees
                );

                // Finish: writes final partial row group + file footer.
                // This is when the Rust encoder reads the column data buffers.
                fileOffset = drainBuffer(ff, outputFd, fileOffset,
                        PartitionEncoder.finishStreamingParquetWrite(writerPtr));
                ff.truncate(outputFd, fileOffset);
            } finally {
                // Free deferred column data buffers now that finish is done.
                for (long i = 0, n = deferredFrees.size(); i < n; i += 2) {
                    long addr = deferredFrees.get(i);
                    long size = deferredFrees.get(i + 1);
                    if (addr != 0) {
                        Unsafe.free(addr, size, memoryTag);
                    }
                }
                Misc.free(deferredFrees);
            }
        } finally {
            if (writerPtr != 0) {
                PartitionEncoder.closeStreamingParquetWriter(writerPtr);
            }
            if (outputFd >= 0) {
                ff.close(outputFd);
            }
        }
    }

    private static long drainBuffer(FilesFacade ff, long fd, long fileOffset, long buffer) {
        if (buffer == 0) {
            return fileOffset;
        }
        long dataSize = Unsafe.getUnsafe().getLong(buffer);
        long dataPtr = buffer + BUFFER_HEADER_SIZE;
        if (dataSize > 0) {
            long written = ff.write(fd, dataPtr, dataSize, fileOffset);
            if (written != dataSize) {
                throw CairoException.critical(ff.errno())
                        .put("failed to write parquet data [expected=")
                        .put(dataSize)
                        .put(", written=").put(written)
                        .put(']');
            }
            fileOffset += dataSize;
        }
        return fileOffset;
    }

    /**
     * Decode + sort + write an overlapping cluster via the streaming parquet writer.
     * Uses source-order scatter: iterates source row groups in order, writing each
     * decoded row to its final sorted position in pre-allocated output buffers.
     */
    private static long writeOverlappingCluster(
            FilesFacade ff,
            long outputFd,
            long fileOffset,
            PartitionDecoder decoder,
            DirectIntList columns,
            PartitionDecoder.Metadata meta,
            long writerPtr,
            int tsColumnIndex,
            long[] rgOrigIndex,
            int[] rgSize,
            int clusterStart,
            int clusterEnd,
            int columnCount,
            int memoryTag,
            DirectLongList deferredFrees
    ) {
        final int clusterRgCount = clusterEnd - clusterStart;

        // Compute total rows in cluster and row group start offsets
        long clusterTotalRows = 0;
        int[] rgStartRow = new int[clusterRgCount];
        for (int k = 0; k < clusterRgCount; k++) {
            rgStartRow[k] = (int) clusterTotalRows;
            clusterTotalRows += rgSize[clusterStart + k];
        }

        LOG.info()
                .$("processing overlapping cluster [rgCount=").$(clusterRgCount)
                .$(", totalRows=").$(clusterTotalRows)
                .I$();

        if (clusterTotalRows > Integer.MAX_VALUE) {
            throw CairoException.nonCritical()
                    .put("overlapping cluster too large [rows=")
                    .put(clusterTotalRows)
                    .put(']');
        }
        final int totalRows = (int) clusterTotalRows;

        // Step 1: Decode only the timestamp column from all RGs in the cluster,
        // build a (value, globalRowId) array, and radix sort it.
        DirectIntList tsOnlyColumns = null;
        long sortArray = 0;

        try {
            tsOnlyColumns = new DirectIntList(2, memoryTag);
            tsOnlyColumns.add(columns.get(tsColumnIndex * 2));
            tsOnlyColumns.add(columns.get(tsColumnIndex * 2 + 1));

            // Allocate sort array: (value, rowId) pairs = 16 bytes each
            final long sortArraySize = 16L * totalRows;
            sortArray = Unsafe.malloc(sortArraySize, memoryTag);

            long sortPos = sortArray;
            for (int k = 0; k < clusterRgCount; k++) {
                int origRg = (int) rgOrigIndex[clusterStart + k];
                int rgRows = rgSize[clusterStart + k];

                try (RowGroupBuffers tsBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)) {
                    decoder.decodeRowGroup(tsBuffers, tsOnlyColumns, origRg, 0, rgRows);
                    long tsDataPtr = tsBuffers.getChunkDataPtr(0);

                    int globalStart = rgStartRow[k];
                    for (int r = 0; r < rgRows; r++) {
                        long tsValue = Unsafe.getUnsafe().getLong(tsDataPtr + r * 8L);
                        Unsafe.getUnsafe().putLong(sortPos, tsValue);
                        Unsafe.getUnsafe().putLong(sortPos + 8, globalStart + r);
                        sortPos += 16;
                    }
                }
            }

            // Sort (value, rowId) pairs by value (ascending).
            // Uses quicksort for <600 entries, radix sort for >=600.
            // QuestDB timestamps are positive longs — unsigned sort is correct.
            Vect.sortLongIndexAscInPlace(sortArray, totalRows);

            // Build reverse map: for each source global row, what is its output position?
            // destPos[globalRow] = outputPos
            long destPosArray = 0;
            try {
                destPosArray = Unsafe.malloc(4L * totalRows, memoryTag);

                for (int r = 0; r < totalRows; r++) {
                    int globalRow = (int) Unsafe.getUnsafe().getLong(sortArray + r * 16L + 8);
                    Unsafe.getUnsafe().putInt(destPosArray + globalRow * 4L, r);
                }

                // Step 2: For each column, allocate output buffer, then iterate source RGs
                // in order and scatter-write to sorted positions.
                fileOffset = scatterWriteCluster(
                        ff, outputFd, fileOffset,
                        decoder, columns, meta,
                        writerPtr, tsColumnIndex,
                        rgOrigIndex, rgSize,
                        clusterStart, clusterEnd,
                        rgStartRow, destPosArray,
                        columnCount, totalRows,
                        memoryTag, deferredFrees
                );
            } finally {
                if (destPosArray != 0) {
                    Unsafe.free(destPosArray, 4L * totalRows, memoryTag);
                }
            }
        } finally {
            Misc.free(tsOnlyColumns);
            if (sortArray != 0) {
                Unsafe.free(sortArray, 16L * totalRows, memoryTag);
            }
        }
        return fileOffset;
    }

    /**
     * Scatter-write all columns for an overlapping cluster. Decodes each source
     * row group once (in source order) and writes each value to its final sorted
     * position in pre-allocated output buffers. Then writes the output buffers
     * to the streaming parquet writer.
     */
    private static long scatterWriteCluster(
            FilesFacade ff,
            long outputFd,
            long fileOffset,
            PartitionDecoder decoder,
            DirectIntList columns,
            PartitionDecoder.Metadata meta,
            long writerPtr,
            int tsColumnIndex,
            long[] rgOrigIndex,
            int[] rgSize,
            int clusterStart,
            int clusterEnd,
            int[] rgStartRow,
            long destPosArray,
            int columnCount,
            int totalRows,
            int memoryTag,
            DirectLongList deferredFrees
    ) {
        final int clusterRgCount = clusterEnd - clusterStart;

        // Classify columns
        int[] colTypes = new int[columnCount];
        int[] colSizes = new int[columnCount]; // byte size for fixed columns, -1 for var
        boolean[] isVarCol = new boolean[columnCount];
        for (int c = 0; c < columnCount; c++) {
            colTypes[c] = meta.getColumnType(c);
            if (ColumnType.isVarSize(colTypes[c])) {
                isVarCol[c] = true;
                colSizes[c] = -1;
            } else if (ColumnType.isSymbol(colTypes[c])) {
                // Parquet decodes symbols as INT (symbol key) — 4 bytes
                colSizes[c] = Integer.BYTES;
            } else {
                colSizes[c] = ColumnType.sizeOf(colTypes[c]);
            }
        }

        // Phase A: Pre-scan variable-width columns to compute output sizes.
        // For each var-width column, we need to know the data size for each output
        // position so we can compute offsets.
        //
        // valueSizes[outputPos] = size of the value's data payload (for VARCHAR: inline or data)
        // We process all var columns together to avoid multiple RG decode passes.

        // Count var columns
        int varColCount = 0;
        int[] varColIndices = new int[columnCount];
        for (int c = 0; c < columnCount; c++) {
            if (isVarCol[c]) {
                varColIndices[varColCount++] = c;
            }
        }

        // For var columns, build two things:
        // 1. Per-column data offsets array: dataOffset[outputPos] for scatter writing
        // 2. Per-column total data size for allocation

        // Output buffers
        long[] outputDataAddrs = new long[columnCount];
        long[] outputDataSizes = new long[columnCount];
        long[] outputAuxAddrs = new long[columnCount];
        long[] outputAuxSizes = new long[columnCount];

        // For var columns: offset arrays (computed from pre-scan)
        long[] varDataOffsetAddrs = new long[columnCount]; // only for var cols

        try {
            // Allocate fixed-column output buffers
            for (int c = 0; c < columnCount; c++) {
                if (!isVarCol[c]) {
                    int elemSize = colSizes[c];
                    if (elemSize > 0) {
                        long bufSize = (long) elemSize * totalRows;
                        outputDataAddrs[c] = Unsafe.malloc(bufSize, memoryTag);
                        outputDataSizes[c] = bufSize;
                    }
                }
            }

            // Pre-scan var columns: decode each RG, read aux headers to get value sizes
            if (varColCount > 0) {
                precomputeVarColumnOffsets(
                        decoder, columns, meta,
                        rgOrigIndex, rgSize,
                        clusterStart, clusterEnd,
                        rgStartRow, destPosArray,
                        varColIndices, varColCount,
                        columnCount, totalRows,
                        outputDataAddrs, outputDataSizes,
                        outputAuxAddrs, outputAuxSizes,
                        varDataOffsetAddrs,
                        memoryTag
                );
            }

            // Phase B: Decode all columns from each source RG and scatter-write
            for (int k = 0; k < clusterRgCount; k++) {
                int origRg = (int) rgOrigIndex[clusterStart + k];
                int rgRows = rgSize[clusterStart + k];
                int globalStart = rgStartRow[k];

                try (RowGroupBuffers rgBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)) {
                    decoder.decodeRowGroup(rgBuffers, columns, origRg, 0, rgRows);

                    // Scatter fixed-width columns
                    for (int c = 0; c < columnCount; c++) {
                        if (isVarCol[c]) {
                            continue;
                        }
                        int elemSize = colSizes[c];
                        if (elemSize <= 0) {
                            continue;
                        }

                        long srcPtr = rgBuffers.getChunkDataPtr(c);
                        long dstPtr = outputDataAddrs[c];

                        for (int r = 0; r < rgRows; r++) {
                            int outPos = Unsafe.getUnsafe().getInt(destPosArray + (globalStart + r) * 4L);
                            long srcOff = (long) r * elemSize;
                            long dstOff = (long) outPos * elemSize;

                            switch (elemSize) {
                                case 1 -> Unsafe.getUnsafe().putByte(
                                        dstPtr + dstOff,
                                        Unsafe.getUnsafe().getByte(srcPtr + srcOff)
                                );
                                case 2 -> Unsafe.getUnsafe().putShort(
                                        dstPtr + dstOff,
                                        Unsafe.getUnsafe().getShort(srcPtr + srcOff)
                                );
                                case 4 -> Unsafe.getUnsafe().putInt(
                                        dstPtr + dstOff,
                                        Unsafe.getUnsafe().getInt(srcPtr + srcOff)
                                );
                                case 8 -> Unsafe.getUnsafe().putLong(
                                        dstPtr + dstOff,
                                        Unsafe.getUnsafe().getLong(srcPtr + srcOff)
                                );
                                case 16 -> {
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff, Unsafe.getUnsafe().getLong(srcPtr + srcOff));
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff + 8, Unsafe.getUnsafe().getLong(srcPtr + srcOff + 8));
                                }
                                case 32 -> {
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff, Unsafe.getUnsafe().getLong(srcPtr + srcOff));
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff + 8, Unsafe.getUnsafe().getLong(srcPtr + srcOff + 8));
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff + 16, Unsafe.getUnsafe().getLong(srcPtr + srcOff + 16));
                                    Unsafe.getUnsafe().putLong(dstPtr + dstOff + 24, Unsafe.getUnsafe().getLong(srcPtr + srcOff + 24));
                                }
                                default -> Vect.memcpy(dstPtr + dstOff, srcPtr + srcOff, elemSize);
                            }
                        }
                    }

                    // Scatter variable-width columns
                    scatterVarColumns(
                            rgBuffers, meta,
                            varColIndices, varColCount,
                            globalStart, rgRows,
                            destPosArray,
                            outputDataAddrs, outputAuxAddrs,
                            varDataOffsetAddrs,
                            columnCount
                    );
                }
            }

            // Phase C: Submit the sorted output buffers to the streaming writer.
            // IMPORTANT: the streaming writer holds references to the column data
            // buffers (outputDataAddrs, outputAuxAddrs) until finishStreamingParquetWrite.
            // Do NOT free them until after finish. We register the drain call here but
            // defer buffer cleanup to the caller (writeOutput's finally block via the
            // returned arrays).
            try (DirectLongList chunkData = new DirectLongList(CHUNK_COL_ENTRY_SIZE * (long) columnCount, memoryTag)) {
                for (int c = 0; c < columnCount; c++) {
                    chunkData.add(0); // col_top
                    if (isVarCol[c]) {
                        chunkData.add(outputDataAddrs[c]);
                        chunkData.add(outputDataSizes[c]);
                        chunkData.add(outputAuxAddrs[c]);
                        chunkData.add(outputAuxSizes[c]);
                    } else {
                        chunkData.add(outputDataAddrs[c]);
                        chunkData.add(outputDataSizes[c]);
                        chunkData.add(0);
                        chunkData.add(0);
                    }
                    chunkData.add(0); // symbol_offset_addr
                    chunkData.add(0); // symbol_offset_size
                }

                fileOffset = drainBuffer(ff, outputFd, fileOffset,
                        PartitionEncoder.writeStreamingParquetChunk(writerPtr, chunkData.getAddress(), totalRows));
            }

            // Register column data/aux buffers for deferred cleanup.
            // The streaming writer holds references to these until finishStreamingParquetWrite.
            for (int c = 0; c < columnCount; c++) {
                if (outputDataAddrs[c] != 0) {
                    deferredFrees.add(outputDataAddrs[c]);
                    deferredFrees.add(outputDataSizes[c]);
                    outputDataAddrs[c] = 0; // prevent double-free
                }
                if (outputAuxAddrs[c] != 0) {
                    deferredFrees.add(outputAuxAddrs[c]);
                    deferredFrees.add(outputAuxSizes[c]);
                    outputAuxAddrs[c] = 0;
                }
                // Offset arrays are not referenced by the writer — free immediately.
                if (varDataOffsetAddrs[c] != 0) {
                    Unsafe.free(varDataOffsetAddrs[c], 8L * totalRows, memoryTag);
                    varDataOffsetAddrs[c] = 0;
                }
            }
        } catch (Throwable t) {
            // On error, free everything that wasn't deferred
            for (int c = 0; c < columnCount; c++) {
                if (outputDataAddrs[c] != 0) {
                    Unsafe.free(outputDataAddrs[c], outputDataSizes[c], memoryTag);
                }
                if (outputAuxAddrs[c] != 0) {
                    Unsafe.free(outputAuxAddrs[c], outputAuxSizes[c], memoryTag);
                }
                if (varDataOffsetAddrs[c] != 0) {
                    Unsafe.free(varDataOffsetAddrs[c], 8L * totalRows, memoryTag);
                }
            }
            throw t;
        }
        return fileOffset;
    }

    /**
     * Pre-scan variable-width columns to compute output data offsets.
     * <p>
     * For STRING columns: aux is 8-byte offsets into data; data is
     * [int32 len][UTF-16 chars]. We read the length from data to get value size.
     * <p>
     * For VARCHAR columns: aux is 16-byte entries per row; data is UTF-8 bytes.
     * The aux entry encodes whether the value is inline or in the data area.
     * We use VarcharTypeDriver.getValueSize() to determine the data footprint.
     */
    private static void precomputeVarColumnOffsets(
            PartitionDecoder decoder,
            DirectIntList columns,
            PartitionDecoder.Metadata meta,
            long[] rgOrigIndex,
            int[] rgSize,
            int clusterStart,
            int clusterEnd,
            int[] rgStartRow,
            long destPosArray,
            int[] varColIndices,
            int varColCount,
            int columnCount,
            int totalRows,
            long[] outputDataAddrs,
            long[] outputDataSizes,
            long[] outputAuxAddrs,
            long[] outputAuxSizes,
            long[] varDataOffsetAddrs,
            int memoryTag
    ) {
        final int clusterRgCount = clusterEnd - clusterStart;

        // Allocate per-output-position value size arrays for each var column.
        // valueSizes[c][outputPos] = number of data bytes for this value
        long[] valueSizeAddrs = new long[varColCount];
        try {
            for (int v = 0; v < varColCount; v++) {
                valueSizeAddrs[v] = Unsafe.calloc(4L * totalRows, memoryTag);
            }

            // Decode each RG (full columns — we need aux data), read sizes
            for (int k = 0; k < clusterRgCount; k++) {
                int origRg = (int) rgOrigIndex[clusterStart + k];
                int rgRows = rgSize[clusterStart + k];
                int globalStart = rgStartRow[k];

                try (RowGroupBuffers rgBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)) {
                    decoder.decodeRowGroup(rgBuffers, columns, origRg, 0, rgRows);

                    for (int v = 0; v < varColCount; v++) {
                        int c = varColIndices[v];
                        int colType = meta.getColumnType(c);
                        long auxPtr = rgBuffers.getChunkAuxPtr(c);
                        long dataPtr = rgBuffers.getChunkDataPtr(c);

                        for (int r = 0; r < rgRows; r++) {
                            int outPos = Unsafe.getUnsafe().getInt(destPosArray + (globalStart + r) * 4L);
                            int valueSize = getVarValueDataSize(colType, auxPtr, dataPtr, r);
                            Unsafe.getUnsafe().putInt(valueSizeAddrs[v] + outPos * 4L, valueSize);
                        }
                    }
                }
            }

            // Compute prefix-sum offsets and allocate output buffers
            for (int v = 0; v < varColCount; v++) {
                int c = varColIndices[v];
                int colType = meta.getColumnType(c);

                // Compute data offsets via prefix sum
                long offsetAddr = Unsafe.malloc(8L * totalRows, memoryTag);
                varDataOffsetAddrs[c] = offsetAddr;

                long runningOffset = 0;
                for (int r = 0; r < totalRows; r++) {
                    Unsafe.getUnsafe().putLong(offsetAddr + r * 8L, runningOffset);
                    int valueSize = Unsafe.getUnsafe().getInt(valueSizeAddrs[v] + r * 4L);
                    runningOffset += valueSize;
                }

                // Allocate data buffer
                long totalDataSize = runningOffset;
                if (totalDataSize > 0) {
                    outputDataAddrs[c] = Unsafe.malloc(totalDataSize, memoryTag);
                    outputDataSizes[c] = totalDataSize;
                }

                // Allocate aux buffer
                long auxEntrySize = getAuxEntrySize(colType);
                long auxSize = auxEntrySize * totalRows;
                outputAuxAddrs[c] = Unsafe.calloc(auxSize, memoryTag);
                outputAuxSizes[c] = auxSize;
            }
        } finally {
            for (int v = 0; v < varColCount; v++) {
                if (valueSizeAddrs[v] != 0) {
                    Unsafe.free(valueSizeAddrs[v], 4L * totalRows, memoryTag);
                }
            }
        }
    }

    /**
     * Scatter variable-width column data from a decoded row group to the output buffers.
     */
    private static void scatterVarColumns(
            RowGroupBuffers rgBuffers,
            PartitionDecoder.Metadata meta,
            int[] varColIndices,
            int varColCount,
            int globalStart,
            int rgRows,
            long destPosArray,
            long[] outputDataAddrs,
            long[] outputAuxAddrs,
            long[] varDataOffsetAddrs,
            int columnCount
    ) {
        for (int v = 0; v < varColCount; v++) {
            int c = varColIndices[v];
            int colType = meta.getColumnType(c);
            long srcAuxPtr = rgBuffers.getChunkAuxPtr(c);
            long srcDataPtr = rgBuffers.getChunkDataPtr(c);
            long dstDataPtr = outputDataAddrs[c];
            long dstAuxPtr = outputAuxAddrs[c];
            long offsetsPtr = varDataOffsetAddrs[c];

            if (colType == ColumnType.STRING) {
                scatterStringColumn(
                        srcAuxPtr, srcDataPtr,
                        dstAuxPtr, dstDataPtr, offsetsPtr,
                        globalStart, rgRows, destPosArray
                );
            } else if (colType == ColumnType.VARCHAR) {
                scatterVarcharColumn(
                        srcAuxPtr, srcDataPtr,
                        dstAuxPtr, dstDataPtr, offsetsPtr,
                        globalStart, rgRows, destPosArray
                );
            } else {
                // BINARY or ARRAY — use generic var-size scatter
                scatterGenericVarColumn(
                        colType, srcAuxPtr, srcDataPtr,
                        dstAuxPtr, dstDataPtr, offsetsPtr,
                        globalStart, rgRows, destPosArray
                );
            }
        }
    }

    /**
     * Scatter STRING column data.
     * STRING aux format: 8-byte offset per row into the data area.
     * STRING data format: [int32 length][UTF-16 char data] at each offset.
     * NULL: length == -1 (TableUtils.NULL_LEN).
     */
    private static void scatterStringColumn(
            long srcAuxPtr,
            long srcDataPtr,
            long dstAuxPtr,
            long dstDataPtr,
            long offsetsPtr,
            int globalStart,
            int rgRows,
            long destPosArray
    ) {
        for (int r = 0; r < rgRows; r++) {
            int outPos = Unsafe.getUnsafe().getInt(destPosArray + (globalStart + r) * 4L);
            long dstDataOffset = Unsafe.getUnsafe().getLong(offsetsPtr + outPos * 8L);

            // Write aux: output offset
            Unsafe.getUnsafe().putLong(dstAuxPtr + outPos * 8L, dstDataOffset);

            // Read source data
            long srcOffset = Unsafe.getUnsafe().getLong(srcAuxPtr + r * 8L);
            long srcAddr = srcDataPtr + srcOffset;
            int len = Unsafe.getUnsafe().getInt(srcAddr); // char count or NULL_LEN

            if (len == TableUtils.NULL_LEN) {
                // Write NULL marker
                Unsafe.getUnsafe().putInt(dstDataPtr + dstDataOffset, TableUtils.NULL_LEN);
            } else {
                // Copy: [int32 len][len*2 bytes of UTF-16 data]
                int totalBytes = Integer.BYTES + len * 2;
                Vect.memcpy(dstDataPtr + dstDataOffset, srcAddr, totalBytes);
            }
        }
    }

    /**
     * Scatter VARCHAR column data.
     * VARCHAR aux format: 16-byte entries per row.
     * VARCHAR can store values inline in aux (small strings) or in the data area.
     */
    private static void scatterVarcharColumn(
            long srcAuxPtr,
            long srcDataPtr,
            long dstAuxPtr,
            long dstDataPtr,
            long offsetsPtr,
            int globalStart,
            int rgRows,
            long destPosArray
    ) {
        for (int r = 0; r < rgRows; r++) {
            int outPos = Unsafe.getUnsafe().getInt(destPosArray + (globalStart + r) * 4L);
            long dstDataOffset = Unsafe.getUnsafe().getLong(offsetsPtr + outPos * 8L);

            long srcAuxEntry = srcAuxPtr + r * 16L;
            long dstAuxEntry = dstAuxPtr + outPos * 16L;

            int valueSize = VarcharTypeDriver.getValueSize(srcAuxPtr, r);
            int raw = Unsafe.getUnsafe().getInt(srcAuxEntry);
            boolean isInlined = (raw & 1) == 1; // HEADER_FLAG_INLINED

            if (valueSize == TableUtils.NULL_LEN || isInlined) {
                // NULL or inlined: entire value is in the 16-byte aux entry, no data to copy
                Unsafe.getUnsafe().putLong(dstAuxEntry, Unsafe.getUnsafe().getLong(srcAuxEntry));
                Unsafe.getUnsafe().putLong(dstAuxEntry + 8, Unsafe.getUnsafe().getLong(srcAuxEntry + 8));
            } else {
                // Not inlined: aux points to data area
                int size = valueSize;
                // Read source data offset from aux (bytes 8-15)
                long srcDataOffset = Unsafe.getUnsafe().getLong(srcAuxEntry + 8);

                // Write updated aux with new data offset
                // Copy first 8 bytes (header + size + prefix), update the offset
                Unsafe.getUnsafe().putLong(dstAuxEntry, Unsafe.getUnsafe().getLong(srcAuxEntry));
                Unsafe.getUnsafe().putLong(dstAuxEntry + 8, dstDataOffset);

                // Copy data
                if (size > 0) {
                    Vect.memcpy(dstDataPtr + dstDataOffset, srcDataPtr + srcDataOffset, size);
                }
            }
        }
    }

    /**
     * Generic scatter for BINARY or ARRAY var-size columns.
     * BINARY aux format: 8-byte offsets. Data: [int64 length][bytes].
     */
    private static void scatterGenericVarColumn(
            int colType,
            long srcAuxPtr,
            long srcDataPtr,
            long dstAuxPtr,
            long dstDataPtr,
            long offsetsPtr,
            int globalStart,
            int rgRows,
            long destPosArray
    ) {
        for (int r = 0; r < rgRows; r++) {
            int outPos = Unsafe.getUnsafe().getInt(destPosArray + (globalStart + r) * 4L);
            long dstDataOffset = Unsafe.getUnsafe().getLong(offsetsPtr + outPos * 8L);

            // Write aux offset
            Unsafe.getUnsafe().putLong(dstAuxPtr + outPos * 8L, dstDataOffset);

            // Read source
            long srcOffset = Unsafe.getUnsafe().getLong(srcAuxPtr + r * 8L);
            long srcAddr = srcDataPtr + srcOffset;
            long len = Unsafe.getUnsafe().getLong(srcAddr);

            if (len == TableUtils.NULL_LEN) {
                Unsafe.getUnsafe().putLong(dstDataPtr + dstDataOffset, TableUtils.NULL_LEN);
            } else {
                long totalBytes = Long.BYTES + len;
                Vect.memcpy(dstDataPtr + dstDataOffset, srcAddr, totalBytes);
            }
        }
    }

    /**
     * Get the data size for a single variable-width value.
     */
    private static int getVarValueDataSize(int colType, long auxPtr, long dataPtr, int row) {
        if (colType == ColumnType.STRING) {
            long offset = Unsafe.getUnsafe().getLong(auxPtr + row * 8L);
            int charLen = Unsafe.getUnsafe().getInt(dataPtr + offset);
            if (charLen == TableUtils.NULL_LEN) {
                return Integer.BYTES; // just the null marker
            }
            return Integer.BYTES + charLen * 2;
        } else if (colType == ColumnType.VARCHAR) {
            int size = VarcharTypeDriver.getValueSize(auxPtr, row);
            if (size == TableUtils.NULL_LEN) {
                return 0; // no data for NULL
            }
            int raw = Unsafe.getUnsafe().getInt(auxPtr + row * 16L);
            if ((raw & 1) == 1) { // HEADER_FLAG_INLINED
                return 0; // inlined in aux, no data area usage
            }
            return Math.max(size, 0);
        } else {
            // BINARY or ARRAY
            long offset = Unsafe.getUnsafe().getLong(auxPtr + row * 8L);
            long len = Unsafe.getUnsafe().getLong(dataPtr + offset);
            if (len == TableUtils.NULL_LEN) {
                return Long.BYTES; // just the null marker
            }
            return (int) (Long.BYTES + len);
        }
    }

    /**
     * Get the aux entry size for a variable-width column type.
     */
    private static long getAuxEntrySize(int colType) {
        if (colType == ColumnType.VARCHAR) {
            return 16; // VarcharTypeDriver.AUX_WIDTH
        }
        return 8; // STRING, BINARY, ARRAY use 8-byte offsets
    }

}
