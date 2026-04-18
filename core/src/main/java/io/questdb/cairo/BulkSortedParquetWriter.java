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
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
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
    // Output row group size. Must match the streaming writer's row_group_size
    // so that each full chunk is completely flushed and its buffers can be freed.
    private static final int OUTPUT_RG_SIZE = 100_000;

    /**
     * Execute a bulk sorted parquet write using the same FilesFacade for source and destination.
     */
    public static void execute(
            FilesFacade ff,
            CharSequence srcPath,
            CharSequence dstPath,
            CharSequence tsColumnName
    ) {
        execute(ff, ff, srcPath, dstPath, tsColumnName, -1, 0);
    }

    /**
     * Execute a bulk sorted parquet write.
     *
     * @param srcFf          files facade for reading the source parquet
     * @param dstFf          files facade for writing the output parquet
     * @param srcPath        path to source parquet file
     * @param dstPath        path to destination parquet file
     * @param tsColumnName   name of the timestamp column to sort by
     * @param tsColumnIndex  index of the timestamp column, or -1 to look up by name
     * @param tsColumnType   QuestDB column type to use for the timestamp column in the
     *                       output (e.g. ColumnType.TIMESTAMP). 0 = use the source type.
     *                       This allows treating a LONG column as TIMESTAMP.
     */
    public static void execute(
            FilesFacade srcFf,
            FilesFacade dstFf,
            CharSequence srcPath,
            CharSequence dstPath,
            CharSequence tsColumnName,
            int tsColumnIndex,
            int tsColumnType
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

            // Verify and resolve timestamp column type
            final int srcTsColType = meta.getColumnType(tsColumnIndex);
            if (!ColumnType.isTimestamp(srcTsColType) && srcTsColType != ColumnType.LONG && srcTsColType != ColumnType.DATE) {
                throw CairoException.nonCritical()
                        .put("timestamp column must be TIMESTAMP, LONG, or DATE [column=")
                        .put(tsColumnName)
                        .put(", type=").put(ColumnType.nameOf(srcTsColType))
                        .put(']');
            }
            // Effective type for the timestamp column in the output parquet.
            // Allows treating a LONG column as TIMESTAMP.
            final int effectiveTsType = tsColumnType > 0 ? tsColumnType : srcTsColType;

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

            if (ColumnType.isTimestamp(srcTsColType)) {
                for (int i = 0; i < rowGroupCount; i++) {
                    rgOrigIndex[i] = i;
                    rgMinTs[i] = decoder.rowGroupMinTimestamp(i, tsColumnIndex);
                    rgMaxTs[i] = decoder.rowGroupMaxTimestamp(i, tsColumnIndex);
                    rgSize[i] = meta.getRowGroupSize(i);
                }
            } else {
                // LONG or DATE — read stats via RowGroupStatBuffers
                try (
                        RowGroupStatBuffers statBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        DirectIntList statColumns = new DirectIntList(2, memoryTag)
                ) {
                    statColumns.add(tsColumnIndex);
                    statColumns.add(srcTsColType);
                    for (int i = 0; i < rowGroupCount; i++) {
                        rgOrigIndex[i] = i;
                        decoder.readRowGroupStats(statBuffers, statColumns, i);
                        rgMinTs[i] = statBuffers.getMinValueLong(0);
                        rgMaxTs[i] = statBuffers.getMaxValueLong(0);
                        rgSize[i] = meta.getRowGroupSize(i);
                    }
                }
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
                    dstPath, tsColumnIndex, effectiveTsType,
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
            int effectiveTsType,
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
            for (int c = 0; c < columnCount; c++) {
                int startSize = columnNames.size();
                columnNames.put(meta.getColumnName(c));
                int nameSize = columnNames.size() - startSize;
                columnMetadata.add(nameSize);
                int colType = (c == tsColumnIndex) ? effectiveTsType : meta.getColumnType(c);
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
                    ParquetCompression.WRITER_COMPRESSION_SNAPPY,
                    true,  // statistics enabled
                    false, // raw array encoding
                    (long) OUTPUT_RG_SIZE,
                    0L,    // default data page size
                    ParquetVersion.PARQUET_VERSION_V1,
                    0L,    // bloom filter column indexes ptr (no bloom filters)
                    0,     // bloom filter column count
                    PartitionEncoder.DEFAULT_BLOOM_FILTER_FPP,
                    0.0    // min compression ratio (disabled)
            );

            dstP.put(dstPath);
            outputFd = ff.openRW(dstP.$(), CairoConfiguration.O_NONE);
            if (outputFd < 0) {
                throw CairoException.critical(ff.errno())
                        .put("cannot create destination parquet file [path=")
                        .put(dstPath)
                        .put(']');
            }

            // Process row groups in clusters: overlapping clusters are k-way merged,
            // non-overlapping singletons are merged as 1-RG clusters (sort + write).
            int i = 0;
            DirectLongList deferredFrees = new DirectLongList(32, memoryTag);
            try {
                while (i < rowGroupCount) {
                    int clusterEnd;
                    if (i < rowGroupCount - 1 && isOverlapStart[i]) {
                        clusterEnd = i + 1;
                        while (clusterEnd < rowGroupCount - 1 && isOverlapStart[clusterEnd]) {
                            clusterEnd++;
                        }
                        clusterEnd++;
                    } else {
                        clusterEnd = i + 1;
                    }
                    fileOffset = kWayMergeCluster(
                            ff, outputFd, fileOffset,
                            decoder, columns, meta,
                            writerPtr, tsColumnIndex,
                            rgOrigIndex, rgSize,
                            i, clusterEnd, columnCount,
                            memoryTag, deferredFrees
                    );
                    i = clusterEnd;
                }
                fileOffset = drainBuffer(ff, outputFd, fileOffset,
                        PartitionEncoder.finishStreamingParquetWrite(writerPtr));
                ff.truncate(outputFd, fileOffset);
            } finally {
                for (long j = 0, n = deferredFrees.size(); j < n; j += 2) {
                    long addr = deferredFrees.get(j);
                    long size = deferredFrees.get(j + 1);
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
     * K-way merge for an overlapping cluster. Decodes all RGs once, sorts each
     * by timestamp, then merges using a min-heap. Output is written sequentially
     * in chunks matching the streaming writer's row group size.
     * <p>
     * Memory: all cluster RGs decoded simultaneously (~350 MB per RG) plus
     * per-RG sort arrays (16 bytes/row) plus one output chunk (~100 MB).
     */
    private static long kWayMergeCluster(
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
        long clusterTotalRows = 0;
        for (int k = 0; k < clusterRgCount; k++) {
            clusterTotalRows += rgSize[clusterStart + k];
        }

        LOG.info()
                .$("k-way merge cluster [rgCount=").$(clusterRgCount)
                .$(", totalRows=").$(clusterTotalRows)
                .I$();

        // Classify columns
        int[] colTypes = new int[columnCount];
        int[] colSizes = new int[columnCount];
        boolean[] isVarCol = new boolean[columnCount];
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

        // Phase 1: Sort each RG by timestamp.
        // sortArrays[k] holds (timestamp, localRow) pairs in sorted order.
        long[] sortArrays = new long[clusterRgCount];
        RowGroupBuffers[] rgBuffers = new RowGroupBuffers[clusterRgCount];

        try {
            DirectIntList tsOnlyColumns = new DirectIntList(2, memoryTag);
            try {
                tsOnlyColumns.add(columns.get(tsColumnIndex * 2));
                tsOnlyColumns.add(columns.get(tsColumnIndex * 2 + 1));

                for (int k = 0; k < clusterRgCount; k++) {
                    int origRg = (int) rgOrigIndex[clusterStart + k];
                    int rgRows = rgSize[clusterStart + k];

                    try (RowGroupBuffers tsBuf = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)) {
                        decoder.decodeRowGroup(tsBuf, tsOnlyColumns, origRg, 0, rgRows);
                        long tsPtr = tsBuf.getChunkDataPtr(0);

                        long sortArray = Unsafe.malloc(16L * rgRows, memoryTag);
                        sortArrays[k] = sortArray;
                        for (int r = 0; r < rgRows; r++) {
                            Unsafe.getUnsafe().putLong(sortArray + r * 16L, Unsafe.getUnsafe().getLong(tsPtr + r * 8L));
                            Unsafe.getUnsafe().putLong(sortArray + r * 16L + 8, r);
                        }
                        Vect.sortLongIndexAscInPlace(sortArray, rgRows);
                    }
                }
            } finally {
                Misc.free(tsOnlyColumns);
            }

            LOG.info().$("per-RG timestamp sort complete, decoding all columns...").$();

            // Phase 2: Decode all RGs fully (all columns).
            for (int k = 0; k < clusterRgCount; k++) {
                int origRg = (int) rgOrigIndex[clusterStart + k];
                int rgRows = rgSize[clusterStart + k];
                rgBuffers[k] = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                decoder.decodeRowGroup(rgBuffers[k], columns, origRg, 0, rgRows);
            }

            LOG.info().$("all RGs decoded, starting merge...").$();

            // Phase 3: K-way merge using min-heap.
            fileOffset = mergeAndWrite(
                    ff, outputFd, fileOffset, writerPtr,
                    rgBuffers, sortArrays, rgSize,
                    clusterStart, clusterRgCount,
                    columnCount, colTypes, colSizes, isVarCol,
                    memoryTag, deferredFrees
            );
        } finally {
            for (int k = 0; k < clusterRgCount; k++) {
                if (rgBuffers[k] != null) {
                    Misc.free(rgBuffers[k]);
                }
                if (sortArrays[k] != 0) {
                    Unsafe.free(sortArrays[k], 16L * rgSize[clusterStart + k], memoryTag);
                }
            }
        }
        return fileOffset;
    }

    /**
     * Merge sorted RGs via min-heap and write output chunks to the streaming writer.
     */
    private static long mergeAndWrite(
            FilesFacade ff,
            long outputFd,
            long fileOffset,
            long writerPtr,
            RowGroupBuffers[] rgBuffers,
            long[] sortArrays,
            int[] rgSize,
            int clusterStart,
            int clusterRgCount,
            int columnCount,
            int[] colTypes,
            int[] colSizes,
            boolean[] isVarCol,
            int memoryTag,
            DirectLongList deferredFrees
    ) {
        // Min-heap (1-indexed). Stores (timestamp, rgLocalIndex).
        long[] heapTs = new long[clusterRgCount + 1];
        int[] heapIdx = new int[clusterRgCount + 1];
        int heapSize = 0;
        int[] cursors = new int[clusterRgCount];

        for (int k = 0; k < clusterRgCount; k++) {
            int rgRows = rgSize[clusterStart + k];
            if (rgRows > 0) {
                long ts = Unsafe.getUnsafe().getLong(sortArrays[k]);
                heapSize = heapPush(heapTs, heapIdx, heapSize, ts, k);
            }
        }

        // Allocate output chunk buffers (one output row group = OUTPUT_RG_SIZE rows).
        long[] outDataAddrs = new long[columnCount];
        long[] outDataSizes = new long[columnCount];
        long[] outAuxAddrs = new long[columnCount];
        long[] outAuxSizes = new long[columnCount];
        // Var column: running data offsets and growable data buffers.
        long[] varDataOffsets = new long[columnCount]; // running offset per var col
        long[] varDataCapacities = new long[columnCount]; // current capacity

        try {
            for (int c = 0; c < columnCount; c++) {
                if (isVarCol[c]) {
                    long auxEntrySize = getAuxEntrySize(colTypes[c]);
                    long auxSize = auxEntrySize * OUTPUT_RG_SIZE;
                    outAuxAddrs[c] = Unsafe.calloc(auxSize, memoryTag);
                    outAuxSizes[c] = auxSize;
                    // Initial var data estimate: 64 bytes per row per var column
                    long initDataSize = 64L * OUTPUT_RG_SIZE;
                    outDataAddrs[c] = Unsafe.malloc(initDataSize, memoryTag);
                    outDataSizes[c] = initDataSize;
                    varDataCapacities[c] = initDataSize;
                } else if (colSizes[c] > 0) {
                    long bufSize = (long) colSizes[c] * OUTPUT_RG_SIZE;
                    outDataAddrs[c] = Unsafe.malloc(bufSize, memoryTag);
                    outDataSizes[c] = bufSize;
                }
            }

            int outPos = 0;
            long rowsMerged = 0;

            while (heapSize > 0) {
                int k = heapIdx[1];
                heapSize = heapPop(heapTs, heapIdx, heapSize);

                int sortedIdx = cursors[k];
                int origRow = (int) Unsafe.getUnsafe().getLong(sortArrays[k] + sortedIdx * 16L + 8);

                // Copy one row from source RG to output buffer.
                for (int c = 0; c < columnCount; c++) {
                    if (isVarCol[c]) {
                        copyVarValue(
                                rgBuffers[k], c, origRow,
                                outDataAddrs, outAuxAddrs, varDataOffsets, varDataCapacities,
                                outPos, colTypes[c], memoryTag
                        );
                    } else if (colSizes[c] > 0) {
                        int elemSize = colSizes[c];
                        long srcPtr = rgBuffers[k].getChunkDataPtr(c) + (long) origRow * elemSize;
                        long dstPtr = outDataAddrs[c] + (long) outPos * elemSize;
                        switch (elemSize) {
                            case 1 -> Unsafe.getUnsafe().putByte(dstPtr, Unsafe.getUnsafe().getByte(srcPtr));
                            case 2 -> Unsafe.getUnsafe().putShort(dstPtr, Unsafe.getUnsafe().getShort(srcPtr));
                            case 4 -> Unsafe.getUnsafe().putInt(dstPtr, Unsafe.getUnsafe().getInt(srcPtr));
                            case 8 -> Unsafe.getUnsafe().putLong(dstPtr, Unsafe.getUnsafe().getLong(srcPtr));
                            case 16 -> {
                                Unsafe.getUnsafe().putLong(dstPtr, Unsafe.getUnsafe().getLong(srcPtr));
                                Unsafe.getUnsafe().putLong(dstPtr + 8, Unsafe.getUnsafe().getLong(srcPtr + 8));
                            }
                            case 32 -> {
                                Unsafe.getUnsafe().putLong(dstPtr, Unsafe.getUnsafe().getLong(srcPtr));
                                Unsafe.getUnsafe().putLong(dstPtr + 8, Unsafe.getUnsafe().getLong(srcPtr + 8));
                                Unsafe.getUnsafe().putLong(dstPtr + 16, Unsafe.getUnsafe().getLong(srcPtr + 16));
                                Unsafe.getUnsafe().putLong(dstPtr + 24, Unsafe.getUnsafe().getLong(srcPtr + 24));
                            }
                            default -> Vect.memcpy(dstPtr, srcPtr, elemSize);
                        }
                    }
                }
                outPos++;

                // Advance cursor; re-insert into heap if RG has more rows.
                cursors[k]++;
                if (cursors[k] < rgSize[clusterStart + k]) {
                    long nextTs = Unsafe.getUnsafe().getLong(sortArrays[k] + cursors[k] * 16L);
                    heapSize = heapPush(heapTs, heapIdx, heapSize, nextTs, k);
                }

                // Flush output chunk when full.
                if (outPos >= OUTPUT_RG_SIZE) {
                    rowsMerged += outPos;
                    fileOffset = flushOutputChunk(
                            ff, outputFd, fileOffset, writerPtr,
                            outDataAddrs, outDataSizes, outAuxAddrs, outAuxSizes,
                            varDataOffsets, isVarCol, columnCount, outPos, memoryTag,
                            null // not last — free immediately after drain
                    );
                    outPos = 0;
                    // Reset var data offsets for next chunk (reuse buffers)
                    for (int c = 0; c < columnCount; c++) {
                        varDataOffsets[c] = 0;
                        // Zero aux buffers for next chunk
                        if (isVarCol[c] && outAuxAddrs[c] != 0) {
                            Vect.memset(outAuxAddrs[c], outAuxSizes[c], 0);
                        }
                    }
                    if (rowsMerged % 10_000_000 == 0) {
                        LOG.info().$("merge progress [rows=").$(rowsMerged).I$();
                    }
                }
            }

            // Flush remaining rows (last partial chunk).
            if (outPos > 0) {
                rowsMerged += outPos;
                fileOffset = flushOutputChunk(
                        ff, outputFd, fileOffset, writerPtr,
                        outDataAddrs, outDataSizes, outAuxAddrs, outAuxSizes,
                        varDataOffsets, isVarCol, columnCount, outPos, memoryTag,
                        deferredFrees // last chunk — defer buffers for finishStreamingParquetWrite
                );
                // Buffers are now owned by deferredFrees; zero our references.
                for (int c = 0; c < columnCount; c++) {
                    outDataAddrs[c] = 0;
                    outAuxAddrs[c] = 0;
                }
            }

            LOG.info().$("merge complete [totalRows=").$(rowsMerged).I$();
        } finally {
            // Free any buffers that weren't deferred.
            for (int c = 0; c < columnCount; c++) {
                if (outDataAddrs[c] != 0) {
                    Unsafe.free(outDataAddrs[c], varDataCapacities[c] > 0 ? varDataCapacities[c] : outDataSizes[c], memoryTag);
                }
                if (outAuxAddrs[c] != 0) {
                    Unsafe.free(outAuxAddrs[c], outAuxSizes[c], memoryTag);
                }
            }
        }
        return fileOffset;
    }

    /**
     * Copy a variable-width value from a source RG buffer to the output chunk,
     * growing the output data buffer if needed.
     */
    private static void copyVarValue(
            RowGroupBuffers srcBuf,
            int col,
            int srcRow,
            long[] outDataAddrs,
            long[] outAuxAddrs,
            long[] varDataOffsets,
            long[] varDataCapacities,
            int outPos,
            int colType,
            int memoryTag
    ) {
        long srcAuxPtr = srcBuf.getChunkAuxPtr(col);
        long srcDataPtr = srcBuf.getChunkDataPtr(col);

        if (colType == ColumnType.STRING) {
            long srcOffset = Unsafe.getUnsafe().getLong(srcAuxPtr + srcRow * 8L);
            long srcAddr = srcDataPtr + srcOffset;
            int charLen = Unsafe.getUnsafe().getInt(srcAddr);
            int valueBytes = (charLen == TableUtils.NULL_LEN) ? Integer.BYTES : (Integer.BYTES + charLen * 2);

            long dstDataOffset = varDataOffsets[col];
            ensureVarCapacity(outDataAddrs, varDataCapacities, col, dstDataOffset + valueBytes, memoryTag);
            Vect.memcpy(outDataAddrs[col] + dstDataOffset, srcAddr, valueBytes);
            Unsafe.getUnsafe().putLong(outAuxAddrs[col] + outPos * 8L, dstDataOffset);
            varDataOffsets[col] = dstDataOffset + valueBytes;
        } else if (colType == ColumnType.VARCHAR) {
            long srcAuxEntry = srcAuxPtr + srcRow * 16L;
            long dstAuxEntry = outAuxAddrs[col] + outPos * 16L;
            int raw = Unsafe.getUnsafe().getInt(srcAuxEntry);
            boolean isInlined = (raw & 1) == 1;
            int valueSize = VarcharTypeDriver.getValueSize(srcAuxPtr, srcRow);

            if (valueSize == TableUtils.NULL_LEN || isInlined) {
                // NULL or inlined: copy aux entry as-is
                Unsafe.getUnsafe().putLong(dstAuxEntry, Unsafe.getUnsafe().getLong(srcAuxEntry));
                Unsafe.getUnsafe().putLong(dstAuxEntry + 8, Unsafe.getUnsafe().getLong(srcAuxEntry + 8));
            } else {
                long srcAuxWord1 = Unsafe.getUnsafe().getLong(srcAuxEntry + 8);
                long srcDataOffset = srcAuxWord1 >>> 16;
                long dstDataOffset = varDataOffsets[col];

                ensureVarCapacity(outDataAddrs, varDataCapacities, col, dstDataOffset + valueSize, memoryTag);
                if (valueSize > 0) {
                    Vect.memcpy(outDataAddrs[col] + dstDataOffset, srcDataPtr + srcDataOffset, valueSize);
                }
                // Copy first 8 bytes (header + prefix), fix data offset in second 8 bytes
                Unsafe.getUnsafe().putLong(dstAuxEntry, Unsafe.getUnsafe().getLong(srcAuxEntry));
                long dstAuxWord1 = (srcAuxWord1 & 0xFFFFL) | (dstDataOffset << 16);
                Unsafe.getUnsafe().putLong(dstAuxEntry + 8, dstAuxWord1);
                varDataOffsets[col] = dstDataOffset + valueSize;
            }
        } else {
            // BINARY or ARRAY
            long srcOffset = Unsafe.getUnsafe().getLong(srcAuxPtr + srcRow * 8L);
            long srcAddr = srcDataPtr + srcOffset;
            long len = Unsafe.getUnsafe().getLong(srcAddr);
            int valueBytes = (len == TableUtils.NULL_LEN) ? Long.BYTES : (int) (Long.BYTES + len);

            long dstDataOffset = varDataOffsets[col];
            ensureVarCapacity(outDataAddrs, varDataCapacities, col, dstDataOffset + valueBytes, memoryTag);
            Vect.memcpy(outDataAddrs[col] + dstDataOffset, srcAddr, valueBytes);
            Unsafe.getUnsafe().putLong(outAuxAddrs[col] + outPos * 8L, dstDataOffset);
            varDataOffsets[col] = dstDataOffset + valueBytes;
        }
    }

    /**
     * Grow a var-column data buffer if needed.
     */
    private static void ensureVarCapacity(long[] addrs, long[] capacities, int col, long needed, int memoryTag) {
        if (needed > capacities[col]) {
            long newCap = Math.max(capacities[col] * 2, needed);
            addrs[col] = Unsafe.realloc(addrs[col], capacities[col], newCap, memoryTag);
            capacities[col] = newCap;
        }
    }

    /**
     * Flush an output chunk to the streaming writer with drain loop,
     * then free or defer the buffers.
     */
    private static long flushOutputChunk(
            FilesFacade ff,
            long outputFd,
            long fileOffset,
            long writerPtr,
            long[] outDataAddrs,
            long[] outDataSizes,
            long[] outAuxAddrs,
            long[] outAuxSizes,
            long[] varDataOffsets,
            boolean[] isVarCol,
            int columnCount,
            int rowCount,
            int memoryTag,
            DirectLongList deferredFrees
    ) {
        try (DirectLongList chunkData = new DirectLongList(CHUNK_COL_ENTRY_SIZE * (long) columnCount, memoryTag)) {
            for (int c = 0; c < columnCount; c++) {
                chunkData.add(0); // col_top
                if (isVarCol[c]) {
                    chunkData.add(outDataAddrs[c]);
                    chunkData.add(varDataOffsets[c]); // actual data size, not capacity
                    chunkData.add(outAuxAddrs[c]);
                    chunkData.add(outAuxSizes[c]);
                } else {
                    chunkData.add(outDataAddrs[c]);
                    chunkData.add(outDataSizes[c]);
                    chunkData.add(0);
                    chunkData.add(0);
                }
                chunkData.add(0); // symbol_offset_addr
                chunkData.add(0); // symbol_offset_size
            }

            long buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, chunkData.getAddress(), rowCount);
            fileOffset = drainBuffer(ff, outputFd, fileOffset, buffer);
            buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
            while (buffer != 0) {
                fileOffset = drainBuffer(ff, outputFd, fileOffset, buffer);
                buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
            }
        }

        // The streaming writer's row_group_size == OUTPUT_RG_SIZE, so a full
        // chunk is fully flushed (no pending references). A partial last chunk
        // may leave rows pending until finishStreamingParquetWrite.
        if (deferredFrees != null) {
            for (int c = 0; c < columnCount; c++) {
                if (outDataAddrs[c] != 0) {
                    deferredFrees.add(outDataAddrs[c]);
                    deferredFrees.add(outDataSizes[c]);
                }
                if (outAuxAddrs[c] != 0) {
                    deferredFrees.add(outAuxAddrs[c]);
                    deferredFrees.add(outAuxSizes[c]);
                }
            }
        }
        return fileOffset;
    }

    // ---- Min-heap helpers (1-indexed) ----

    private static int heapPush(long[] ts, int[] idx, int size, long newTs, int newIdx) {
        size++;
        ts[size] = newTs;
        idx[size] = newIdx;
        int i = size;
        while (i > 1 && ts[i] < ts[i >> 1]) {
            heapSwap(ts, idx, i, i >> 1);
            i >>= 1;
        }
        return size;
    }

    private static int heapPop(long[] ts, int[] idx, int size) {
        ts[1] = ts[size];
        idx[1] = idx[size];
        size--;
        int i = 1;
        while (true) {
            int smallest = i;
            int left = i << 1;
            int right = left + 1;
            if (left <= size && ts[left] < ts[smallest]) {
                smallest = left;
            }
            if (right <= size && ts[right] < ts[smallest]) {
                smallest = right;
            }
            if (smallest == i) {
                break;
            }
            heapSwap(ts, idx, i, smallest);
            i = smallest;
        }
        return size;
    }

    private static void heapSwap(long[] ts, int[] idx, int a, int b) {
        long tmpTs = ts[a]; ts[a] = ts[b]; ts[b] = tmpTs;
        int tmpIdx = idx[a]; idx[a] = idx[b]; idx[b] = tmpIdx;
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
