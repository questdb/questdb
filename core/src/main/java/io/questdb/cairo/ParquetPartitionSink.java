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

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Path;

/**
 * {@link SortedRowSink} that writes the globally sorted row stream as per-partition
 * parquet files staged under the caller-supplied {@link TableWriter}'s
 * {@code .attachable} directory, then calls
 * {@link TableWriter#attachPartition(long, long)} to bring each partition online.
 * <p>
 * Partition boundaries are derived from the writer's partition-by setting and the
 * incoming timestamp value. When the floor of the incoming ts crosses into a new
 * partition the currently open parquet file is finalised, the partition is attached,
 * and a fresh file is opened for the new partition. On {@link #onFinish} the last
 * partition is attached.
 * <p>
 * Lifecycle: the sink is non-owning. The caller constructs the {@link TableWriter}
 * and is responsible for closing it. {@link TableWriter#attachPartition} commits the
 * writer as part of its own handling, so an explicit caller commit is not required,
 * but the caller may still wrap the whole import in a try/rollback to release a
 * partially staged partition on failure (see {@link #close}, which removes any
 * staged-but-not-attached directory).
 * <p>
 * Column type support: every primitive type, STRING, VARCHAR, BINARY, UUID, LONG256,
 * IPv4, GEOBYTE/SHORT/INT/LONG. SYMBOL is rejected in {@link #onStart} because
 * attached parquet partitions have no symbol map files and would break string
 * resolution at query time. ARRAY, DECIMAL and INTERVAL are rejected for the same
 * reason as in {@link TableWriterSink}.
 * <p>
 * Memory: output is written through a streaming parquet encoder in
 * {@value #OUTPUT_RG_SIZE}-row chunks, so per-partition heap usage is bounded by
 * one chunk's worth of column buffers regardless of partition row count. All
 * native allocations the sink performs itself live under
 * {@link MemoryTag#NATIVE_IMPORT} and return to baseline after {@link #close}.
 */
public class ParquetPartitionSink implements SortedRowSink {

    private static final int CHUNK_COL_ENTRY_SIZE = 7;
    private static final long DEFAULT_DATA_PAGE_SIZE = 0L;
    private static final Log LOG = LogFactory.getLog(ParquetPartitionSink.class);
    private static final int MEMORY_TAG = MemoryTag.NATIVE_IMPORT;
    private static final int OUTPUT_RG_SIZE = 100_000;
    private final Path attachDirPath = new Path();
    private final int outputRgSize;
    private final Path parquetFilePath = new Path();
    private final TableWriter writer;
    private int columnCount;
    private DirectLongList columnMetadataBuf;
    private DirectUtf8Sink columnNamesBuf;
    private int[] colSizes;
    private int[] colTypes;
    private long currentPartitionTs = Long.MIN_VALUE;
    private long fileOffset;
    private TimestampDriver.TimestampFloorMethod floorMethod;
    private boolean[] isVarCol;
    private long[] outAuxAddrs;
    private long[] outAuxSizes;
    private long[] outDataAddrs;
    private long[] outDataSizes;
    private long outputFd = -1;
    private int outPos;
    private boolean partitionOpen;
    private long partitionRowCount;
    private int partitionBy;
    private long totalRowsAttached;
    private int tsColumnIndex = -1;
    private int timestampType;
    private long[] varDataCapacities;
    private long[] varDataOffsets;
    private long writerPtr;

    public ParquetPartitionSink(TableWriter writer) {
        this(writer, OUTPUT_RG_SIZE);
    }

    /**
     * Test-only constructor that accepts a custom output row-group size. Lets
     * tests exercise the mid-partition chunk-flush path without staging 100k+
     * rows. Production callers should use {@link #ParquetPartitionSink(TableWriter)}
     * which uses the default {@value #OUTPUT_RG_SIZE}-row chunk size.
     */
    @org.jetbrains.annotations.TestOnly
    public ParquetPartitionSink(TableWriter writer, int outputRgSize) {
        if (writer == null) {
            throw CairoException.nonCritical().put("ParquetPartitionSink requires a non-null TableWriter");
        }
        if (!PartitionBy.isPartitioned(writer.getPartitionBy())) {
            throw CairoException.nonCritical()
                    .put("ParquetPartitionSink requires a partitioned destination table [table=")
                    .put(writer.getTableToken().getTableName())
                    .put(']');
        }
        if (outputRgSize <= 0) {
            throw CairoException.nonCritical()
                    .put("ParquetPartitionSink outputRgSize must be positive [got=")
                    .put(outputRgSize).put(']');
        }
        this.writer = writer;
        this.outputRgSize = outputRgSize;
    }

    @Override
    public void acceptRow(ColumnBlockSource src, int row, long ts) {
        final long partitionTs = floorMethod.floor(ts);
        if (partitionTs != currentPartitionTs) {
            if (partitionOpen) {
                closePartitionAndAttach();
            }
            openPartition(partitionTs);
        }
        appendRow(src, row);
        outPos++;
        partitionRowCount++;
        if (outPos >= outputRgSize) {
            writeChunk();
            resetChunkBuffers();
        }
    }

    @Override
    public void close() {
        try {
            if (partitionOpen) {
                discardOpenPartition();
            }
        } finally {
            freeOutputBuffers();
            Misc.free(columnNamesBuf);
            columnNamesBuf = null;
            Misc.free(columnMetadataBuf);
            columnMetadataBuf = null;
            attachDirPath.close();
            parquetFilePath.close();
            colTypes = null;
            colSizes = null;
            isVarCol = null;
        }
    }

    @Override
    public void onFinish() {
        if (partitionOpen) {
            closePartitionAndAttach();
        }
        LOG.info()
                .$("parquet sink done [table=").$(writer.getTableToken().getTableName())
                .$(", rowsAttached=").$(totalRowsAttached)
                .I$();
    }

    @Override
    public void onStart(SortedStreamMetadata meta, int tsColIdx, long totalRows) {
        this.tsColumnIndex = tsColIdx;
        this.columnCount = meta.getColumnCount();
        this.colTypes = new int[columnCount];
        this.colSizes = new int[columnCount];
        this.isVarCol = new boolean[columnCount];
        this.partitionBy = writer.getPartitionBy();
        this.timestampType = writer.getTimestampType();
        this.floorMethod = PartitionBy.getPartitionFloorMethod(timestampType, partitionBy);

        final TableMetadata writerMeta = writer.getMetadata();
        final int writerTsIdx = writerMeta.getTimestampIndex();
        if (writerTsIdx < 0) {
            throw CairoException.nonCritical()
                    .put("destination table has no designated timestamp column");
        }
        final CharSequence parquetTsName = meta.getColumnName(tsColIdx);
        final CharSequence writerTsName = writerMeta.getColumnName(writerTsIdx);
        if (!Chars.equalsNullable(parquetTsName, writerTsName)) {
            throw CairoException.nonCritical()
                    .put("timestamp column name mismatch [parquet=")
                    .put(parquetTsName)
                    .put(", writer=")
                    .put(writerTsName)
                    .put(']');
        }

        columnNamesBuf = new DirectUtf8Sink(256);
        columnMetadataBuf = new DirectLongList(2L * columnCount, MEMORY_TAG);

        for (int c = 0; c < columnCount; c++) {
            final int parquetType = meta.getColumnType(c);
            colTypes[c] = parquetType;
            if (ColumnType.isVarSize(parquetType)) {
                isVarCol[c] = true;
                colSizes[c] = -1;
            } else {
                colSizes[c] = ColumnType.sizeOf(parquetType);
            }
            final CharSequence name = meta.getColumnName(c);
            if (c != tsColIdx) {
                final int writerCol = writerMeta.getColumnIndexQuiet(name);
                if (writerCol < 0) {
                    throw CairoException.nonCritical()
                            .put("parquet column not found in destination table [column=")
                            .put(name)
                            .put(']');
                }
                final int writerType = writerMeta.getColumnType(writerCol);
                if (writerType != parquetType) {
                    throw CairoException.nonCritical()
                            .put("column type mismatch [column=").put(name)
                            .put(", parquet=").put(ColumnType.nameOf(parquetType))
                            .put(", writer=").put(ColumnType.nameOf(writerType))
                            .put(']');
                }
                ensureSupported(name, parquetType);
            }

            final int nameStart = columnNamesBuf.size();
            columnNamesBuf.put(name);
            final int nameSize = columnNamesBuf.size() - nameStart;
            columnMetadataBuf.add(nameSize);
            columnMetadataBuf.add((long) c << 32 | (parquetType & 0xFFFFFFFFL));
        }

        allocOutputBuffers();

        LOG.info()
                .$("parquet sink start [table=").$(writer.getTableToken().getTableName())
                .$(", columns=").$(columnCount)
                .$(", totalRows=").$(totalRows)
                .$(", partitionBy=").$(PartitionBy.toString(partitionBy))
                .I$();
    }

    private void allocOutputBuffers() {
        outDataAddrs = new long[columnCount];
        outDataSizes = new long[columnCount];
        outAuxAddrs = new long[columnCount];
        outAuxSizes = new long[columnCount];
        varDataOffsets = new long[columnCount];
        varDataCapacities = new long[columnCount];
        for (int c = 0; c < columnCount; c++) {
            if (isVarCol[c]) {
                final long auxEntrySize = BulkSortedParquetWriter.getAuxEntrySize(colTypes[c]);
                final long auxSize = auxEntrySize * outputRgSize;
                outAuxAddrs[c] = Unsafe.calloc(auxSize, MEMORY_TAG);
                outAuxSizes[c] = auxSize;
                final long initDataSize = 64L * outputRgSize;
                outDataAddrs[c] = Unsafe.malloc(initDataSize, MEMORY_TAG);
                outDataSizes[c] = initDataSize;
                varDataCapacities[c] = initDataSize;
            } else if (colSizes[c] > 0) {
                final long bufSize = (long) colSizes[c] * outputRgSize;
                outDataAddrs[c] = Unsafe.malloc(bufSize, MEMORY_TAG);
                outDataSizes[c] = bufSize;
            }
        }
    }

    private void appendRow(ColumnBlockSource src, int row) {
        for (int c = 0; c < columnCount; c++) {
            if (isVarCol[c]) {
                BulkSortedParquetWriter.copyVarValue(
                        src, c, row,
                        outDataAddrs, outAuxAddrs,
                        varDataOffsets, varDataCapacities,
                        outPos, colTypes[c], MEMORY_TAG
                );
            } else if (colSizes[c] > 0) {
                final int elemSize = colSizes[c];
                final long srcPtr = src.getChunkDataPtr(c) + (long) row * elemSize;
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
    }

    private void buildAttachDirPath(long partitionTs) {
        final CairoConfiguration cfg = writer.getConfiguration();
        attachDirPath.of(cfg.getDbRoot()).concat(writer.getTableToken());
        TableUtils.setPathForNativePartition(attachDirPath, timestampType, partitionBy, partitionTs, -1L);
        attachDirPath.put(cfg.getAttachPartitionSuffix());
    }

    private void closePartitionAndAttach() {
        final FilesFacade ff = writer.getFilesFacade();
        final long ts = currentPartitionTs;
        final long rowsInPartition = partitionRowCount;
        try {
            if (outPos > 0) {
                // Partial last chunk: write it, but do NOT reset the buffers. The
                // streaming writer's row_group_size is outputRgSize, so a partial
                // chunk does not get fully flushed until finishStreamingParquetWrite
                // is called — which reads from our buffers. Zeroing them in between
                // would produce empty values for every row in the last RG.
                writeChunk();
            }
            final long finishBuf = PartitionEncoder.finishStreamingParquetWrite(writerPtr);
            fileOffset = BulkSortedParquetWriter.drainBuffer(ff, outputFd, fileOffset, finishBuf);
            ff.truncate(outputFd, fileOffset);
        } finally {
            // Unconditionally release the encoder and fd. If anything above threw,
            // the staging dir still contains a half-written parquet file that we
            // scrub in the outer catch/try-finally below.
            if (writerPtr != 0) {
                try {
                    PartitionEncoder.closeStreamingParquetWriter(writerPtr);
                } catch (Throwable ignore) {
                    // best-effort
                }
                writerPtr = 0;
            }
            if (outputFd >= 0) {
                try {
                    ff.close(outputFd);
                } catch (Throwable ignore) {
                    // best-effort
                }
                outputFd = -1;
            }
        }

        // Reset state before attach — attach may throw and close() must not double-free.
        partitionOpen = false;
        partitionRowCount = 0;
        currentPartitionTs = Long.MIN_VALUE;

        AttachDetachStatus status = null;
        Throwable attachError = null;
        try {
            status = writer.attachPartition(ts, rowsInPartition);
        } catch (Throwable t) {
            attachError = t;
        }
        if (status != AttachDetachStatus.OK) {
            // Any failure — thrown or returned — leaves the staging dir in place.
            // Scrub it so the three-tag leak detector and test reruns see a clean
            // state. rebuildAttachDirPath is safe here: we haven't freed anything
            // from onStart yet.
            try {
                buildAttachDirPath(ts);
                ff.rmdir(attachDirPath);
            } catch (Throwable ignore) {
                // best-effort
            }
            if (attachError != null) {
                if (attachError instanceof RuntimeException) {
                    throw (RuntimeException) attachError;
                }
                throw new RuntimeException(attachError);
            }
            throw status.getException(0, status, writer.getTableToken(), timestampType, partitionBy, ts);
        }
        totalRowsAttached += rowsInPartition;
        LOG.info()
                .$("partition attached [table=").$(writer.getTableToken().getTableName())
                .$(", partitionTs=").$(ts)
                .$(", rows=").$(rowsInPartition)
                .I$();
    }

    private void discardOpenPartition() {
        final FilesFacade ff = writer.getFilesFacade();
        if (writerPtr != 0) {
            try {
                PartitionEncoder.closeStreamingParquetWriter(writerPtr);
            } catch (Throwable ignore) {
                // best-effort
            }
            writerPtr = 0;
        }
        if (outputFd >= 0) {
            try {
                ff.close(outputFd);
            } catch (Throwable ignore) {
                // best-effort
            }
            outputFd = -1;
        }
        if (currentPartitionTs != Long.MIN_VALUE) {
            try {
                buildAttachDirPath(currentPartitionTs);
                ff.rmdir(attachDirPath);
            } catch (Throwable ignore) {
                // best-effort
            }
        }
        partitionOpen = false;
        partitionRowCount = 0;
        currentPartitionTs = Long.MIN_VALUE;
        outPos = 0;
    }

    private void ensureSupported(CharSequence name, int type) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN,
                 ColumnType.BYTE,
                 ColumnType.SHORT,
                 ColumnType.CHAR,
                 ColumnType.INT,
                 ColumnType.LONG,
                 ColumnType.DATE,
                 ColumnType.TIMESTAMP,
                 ColumnType.FLOAT,
                 ColumnType.DOUBLE,
                 ColumnType.IPv4,
                 ColumnType.GEOBYTE,
                 ColumnType.GEOSHORT,
                 ColumnType.GEOINT,
                 ColumnType.GEOLONG,
                 ColumnType.UUID,
                 ColumnType.LONG256,
                 ColumnType.STRING,
                 ColumnType.VARCHAR,
                 ColumnType.BINARY -> {
            }
            default -> throw CairoException.nonCritical()
                    .put("unsupported column type in ParquetPartitionSink [column=")
                    .put(name)
                    .put(", type=").put(ColumnType.nameOf(type))
                    .put(']');
        }
    }

    private void freeOutputBuffers() {
        if (outDataAddrs == null) {
            return;
        }
        for (int c = 0; c < columnCount; c++) {
            if (outDataAddrs[c] != 0) {
                final long cap = varDataCapacities != null && varDataCapacities[c] > 0
                        ? varDataCapacities[c]
                        : outDataSizes[c];
                Unsafe.free(outDataAddrs[c], cap, MEMORY_TAG);
                outDataAddrs[c] = 0;
            }
            if (outAuxAddrs[c] != 0) {
                Unsafe.free(outAuxAddrs[c], outAuxSizes[c], MEMORY_TAG);
                outAuxAddrs[c] = 0;
            }
        }
        outDataAddrs = null;
        outDataSizes = null;
        outAuxAddrs = null;
        outAuxSizes = null;
        varDataOffsets = null;
        varDataCapacities = null;
    }

    private void openPartition(long partitionTs) {
        final CairoConfiguration cfg = writer.getConfiguration();
        final FilesFacade ff = writer.getFilesFacade();

        // Publish the partition ts up front so any failure below can be cleaned
        // up by the caller via discardOpenPartition(). partitionOpen is flipped
        // true only after every fallible step succeeds.
        currentPartitionTs = partitionTs;
        partitionRowCount = 0;
        fileOffset = 0;
        outPos = 0;
        for (int c = 0; c < columnCount; c++) {
            varDataOffsets[c] = 0;
            if (isVarCol[c] && outAuxAddrs[c] != 0) {
                Vect.memset(outAuxAddrs[c], outAuxSizes[c], 0);
            }
        }

        buildAttachDirPath(partitionTs);
        if (ff.mkdirs(attachDirPath.slash(), cfg.getMkDirMode()) != 0) {
            throw CairoException.critical(ff.errno())
                    .put("cannot create attach staging dir [path=")
                    .put(attachDirPath)
                    .put(']');
        }
        // mkdirs leaves a trailing slash; re-anchor attachDirPath to the dir itself
        // for any later rmdir on the error path.
        buildAttachDirPath(partitionTs);

        parquetFilePath.of(attachDirPath).concat(TableUtils.PARQUET_PARTITION_NAME).$();

        outputFd = ff.openRW(parquetFilePath.$(), CairoConfiguration.O_NONE);
        if (outputFd < 0) {
            final int errno = ff.errno();
            try {
                ff.rmdir(attachDirPath);
            } catch (Throwable ignore) {
                // best-effort
            }
            currentPartitionTs = Long.MIN_VALUE;
            throw CairoException.critical(errno)
                    .put("cannot create partition parquet file [path=")
                    .put(parquetFilePath)
                    .put(']');
        }

        try {
            writerPtr = PartitionEncoder.createStreamingParquetWriter(
                    Unsafe.getNativeAllocator(MEMORY_TAG),
                    columnCount,
                    columnNamesBuf.ptr(),
                    columnNamesBuf.size(),
                    columnMetadataBuf.getAddress(),
                    tsColumnIndex,
                    false,
                    ParquetCompression.WRITER_COMPRESSION_SNAPPY,
                    true,
                    false,
                    (long) outputRgSize,
                    DEFAULT_DATA_PAGE_SIZE,
                    ParquetVersion.PARQUET_VERSION_V1,
                    0L,
                    0,
                    PartitionEncoder.DEFAULT_BLOOM_FILTER_FPP,
                    0.0
            );
        } catch (Throwable t) {
            try {
                ff.close(outputFd);
            } catch (Throwable ignore) {
                // best-effort
            }
            outputFd = -1;
            try {
                ff.rmdir(attachDirPath);
            } catch (Throwable ignore) {
                // best-effort
            }
            currentPartitionTs = Long.MIN_VALUE;
            throw t;
        }

        partitionOpen = true;
    }

    private void resetChunkBuffers() {
        outPos = 0;
        for (int c = 0; c < columnCount; c++) {
            varDataOffsets[c] = 0;
            if (isVarCol[c] && outAuxAddrs[c] != 0) {
                Vect.memset(outAuxAddrs[c], outAuxSizes[c], 0);
            }
        }
    }

    private void writeChunk() {
        final FilesFacade ff = writer.getFilesFacade();
        try (DirectLongList chunkData = new DirectLongList(CHUNK_COL_ENTRY_SIZE * (long) columnCount, MEMORY_TAG)) {
            for (int c = 0; c < columnCount; c++) {
                chunkData.add(0);
                if (isVarCol[c]) {
                    chunkData.add(outDataAddrs[c]);
                    chunkData.add(varDataOffsets[c]);
                    chunkData.add(outAuxAddrs[c]);
                    chunkData.add(outAuxSizes[c]);
                } else {
                    chunkData.add(outDataAddrs[c]);
                    chunkData.add(outDataSizes[c]);
                    chunkData.add(0);
                    chunkData.add(0);
                }
                chunkData.add(0);
                chunkData.add(0);
            }

            long buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, chunkData.getAddress(), outPos);
            fileOffset = BulkSortedParquetWriter.drainBuffer(ff, outputFd, fileOffset, buffer);
            buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
            while (buffer != 0) {
                fileOffset = BulkSortedParquetWriter.drainBuffer(ff, outputFd, fileOffset, buffer);
                buffer = PartitionEncoder.writeStreamingParquetChunk(writerPtr, 0, 0);
            }
        }
    }
}
