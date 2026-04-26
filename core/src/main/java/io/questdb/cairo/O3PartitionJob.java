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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetMetaPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetMetadataWriter;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectIntList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.tasks.O3OpenColumnTask;
import io.questdb.tasks.O3PartitionTask;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.O3OpenColumnJob.*;
import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableWriter.*;

public class O3PartitionJob extends AbstractQueueConsumerJob<O3PartitionTask> {

    private static final Log LOG = LogFactory.getLog(O3PartitionJob.class);
    private static final io.questdb.std.ThreadLocal<O3ParquetMergeContext> PARQUET_MERGE_CONTEXT =
            new io.questdb.std.ThreadLocal<>(O3ParquetMergeContext::new);
    public static final Closeable THREAD_LOCAL_CLEANER = PARQUET_MERGE_CONTEXT;
    // High bit set on the column type signals the Rust parquet encoder that the
    // symbol column contains no nulls, so it can emit an all-ones RLE run for
    // definition levels instead of checking each row.  This is a write-time hint
    // only — it does NOT change the parquet schema Repetition (always Optional).
    private static final int PARQUET_SYMBOL_NOT_NULL_HINT = Integer.MIN_VALUE;

    public O3PartitionJob(MessageBus messageBus) {
        super(messageBus.getO3PartitionQueue(), messageBus.getO3PartitionSubSeq());
    }

    public static void processParquetPartition(
            Path pathToTable,
            int timestampType,
            int partitionBy,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long partitionTimestamp,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            long srcNameTxn,
            long txn,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr,
            long o3TimestampMin,
            O3Basket o3Basket,
            long newPartitionSize,
            long oldPartitionSize
    ) {
        // Number of rows to insert from the O3 segment into this partition.
        final TableRecordMetadata tableWriterMetadata = tableWriter.getMetadata();
        Path path = Path.getThreadLocal(pathToTable);
        setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);

        final int partitionIndex = tableWriter.getPartitionIndexByTimestamp(partitionTimestamp);
        final long parquetFileSize = tableWriter.getPartitionParquetFileSize(partitionIndex);
        long duplicateCount = 0;
        long newParquetSize = -1;
        long newParquetMetaFileSize = -1;
        boolean isRewrite = false;
        CairoConfiguration cairoConfiguration = tableWriter.getConfiguration();
        FilesFacade ff = tableWriter.getFilesFacade();
        final O3ParquetMergeContext ctx = PARQUET_MERGE_CONTEXT.get();
        ctx.clear();
        final ParquetMetaPartitionDecoder partitionDecoder = ctx.getPartitionDecoder();
        final RowGroupBuffers rowGroupBuffers = ctx.getRowGroupBuffers();
        final DirectIntList parquetColumns = ctx.getParquetColumns();
        final ParquetMetaFileReader parquetMetaReader = ctx.getParquetMetaReader();
        final PartitionUpdater partitionUpdater = ctx.getPartitionUpdater();
        final PartitionDescriptor partitionDescriptor = ctx.getPartitionDescriptor();
        long parquetSize = parquetFileSize;
        try {
            int parquetNameLen = path.size();
            int partitionDirLen = parquetNameLen - TableUtils.PARQUET_PARTITION_NAME.length() - 1;
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();

            // The _pm file _might_ need to be regenerated, we can check that by resolving the footer, if it exists
            // for a specific parquet file then we're fine.
            parquetMetaReader.openAndMapRO(ff, path.$());
            boolean needsRegenerating = parquetMetaReader.getAddr() == 0;
            try {
                if (!needsRegenerating) {
                    needsRegenerating = !parquetMetaReader.resolveFooter(parquetFileSize);
                }
            } catch (CairoException ignored) {
                needsRegenerating = true;
            }
            if (needsRegenerating) {
                parquetMetaReader.unmapAndClear(ff);
                LOG.info()
                        .$("regenerating stale _pm [path=").$(path)
                        .$(", parquetFileSize=").$(parquetFileSize)
                        .I$();
                regenerateParquetMetadata(ff, path, partitionDirLen, parquetFileSize, cairoConfiguration);
                path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                parquetMetaReader.openAndMapRO(ff, path.$());
                if (parquetMetaReader.getAddr() == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                    throw CairoException.critical(0)
                            .put("regenerated _pm is missing or invalid [path=").put(path).put(']');
                }
            }
            parquetSize = parquetMetaReader.getParquetFileSize();
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();

            long parquetAddr = 0;
            try {
                parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                partitionDecoder.of(
                        parquetMetaReader,
                        parquetAddr,
                        parquetSize,
                        MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER
                );

                // Build column-ID mapping between table schema and parquet file.
                // This allows O3 merge to work after ADD/DROP COLUMN: columns are
                // matched by their writer index (stored as field_id in parquet),
                // not by position.
                final ParquetMetaFileReader parquetMeta = partitionDecoder.metadata();
                final int parquetColumnCount = parquetMeta.getColumnCount();
                final int columnCount = tableWriterMetadata.getColumnCount();
                final IntIntHashMap parquetColIdToIdx = ctx.getParquetColIdToIdx();
                for (int i = 0; i < parquetColumnCount; i++) {
                    parquetColIdToIdx.put(parquetMeta.getColumnId(i), i);
                }
                final IntList tableToParquetIdx = ctx.getTableToParquetIdx(columnCount);
                boolean hasMissingColumns = false;
                int mappedParquetColumns = 0;
                for (int i = 0; i < columnCount; i++) {
                    if (tableWriterMetadata.getColumnType(i) < 0) {
                        continue;
                    }
                    final int writerIndex = tableWriterMetadata.getColumnMetadata(i).getWriterIndex();
                    final int parquetIdx = parquetColIdToIdx.get(writerIndex);
                    tableToParquetIdx.setQuick(i, parquetIdx);
                    if (parquetIdx < 0) {
                        hasMissingColumns = true;
                    } else {
                        mappedParquetColumns++;
                    }
                }
                // Detect schema changes: missing columns (ADD COLUMN) or extra
                // columns in the parquet file (DROP COLUMN). Both require a
                // rewrite with the updated target schema.
                final boolean hasExtraColumns = mappedParquetColumns < parquetColumnCount;
                final boolean hasSchemaChange = hasMissingColumns || hasExtraColumns;

                final int rowGroupCount = partitionDecoder.metadata().getRowGroupCount();
                assert rowGroupCount > 0;

                final int timestampIndex = tableWriterMetadata.getTimestampIndex();
                final int timestampColumnType = tableWriterMetadata.getColumnType(timestampIndex);
                assert ColumnType.isTimestamp(timestampColumnType);

                // for API completeness, we'll use the same configuration as the initial partition encoder.
                final int compressionCodec = cairoConfiguration.getPartitionEncoderParquetCompressionCodec();
                final int compressionLevel = cairoConfiguration.getPartitionEncoderParquetCompressionLevel();
                final int rowGroupSize = cairoConfiguration.getPartitionEncoderParquetRowGroupSize();
                assert rowGroupSize >= 4;
                final int dataPageSize = cairoConfiguration.getPartitionEncoderParquetDataPageSize();
                final boolean statisticsEnabled = cairoConfiguration.isPartitionEncoderParquetStatisticsEnabled();
                final boolean rawArrayEncoding = cairoConfiguration.isPartitionEncoderParquetRawArrayEncoding();
                final double bloomFilterFpp = cairoConfiguration.getPartitionEncoderParquetBloomFilterFpp();
                final double minCompressionRatio = cairoConfiguration.getPartitionEncoderParquetMinCompressionRatio();

                // Decide whether to rewrite the file or update in-place.
                // A single-row-group file always triggers a rewrite: any O3 merge
                // replaces its only row group, leaving 100% of the original payload
                // as dead bytes.
                // Schema mismatch (hasSchemaChange) also forces rewrite: in update
                // mode, untouched row groups would retain the old column layout while
                // the footer schema uses the new target schema, producing a malformed
                // Parquet file.
                final long unusedBytes = partitionDecoder.metadata().getUnusedBytes();
                isRewrite = hasSchemaChange
                        || rowGroupCount == 1
                        || (parquetSize > 0 && (double) unusedBytes / parquetSize > cairoConfiguration.getPartitionEncoderParquetO3RewriteUnusedRatio())
                        || unusedBytes > cairoConfiguration.getPartitionEncoderParquetO3RewriteUnusedMaxBytes();

                if (isRewrite) {
                    LOG.info().$("parquet o3 partition rewrite [table=").$(tableWriter.getTableToken())
                            .$(", partition=").$ts(partitionTimestamp)
                            .$(", fileSize=").$size(parquetSize)
                            .$(", unusedBytes=").$size(unusedBytes)
                            .$(", unusedPct=").$(parquetSize > 0 ? (100.0 * unusedBytes / parquetSize) : 0)
                            .$(", hasSchemaChange=").$(hasSchemaChange)
                            .I$();
                }

                final int opts = cairoConfiguration.getWriterFileOpenOpts();
                // Two separate file descriptors are required: one for reading (metadata,
                // row group slicing) and one for writing (appending new row groups).
                // They must be distinct OS fds even when pointing to the same file,
                // because the reader and writer maintain independent cursor positions.
                // Rust closes both fds when the ParquetUpdater is dropped.
                int readerFdOs = -1, writerFdOs = -1, parquetMetaFdOs = -1;
                long readerFd = -1, writerFd = -1, parquetMetaFd = -1;
                final long writeFileSize;
                long updaterParquetMetaFileSize = 0;
                try {
                    readerFd = TableUtils.openRONoCache(ff, path.$(), LOG);
                    readerFdOs = Files.detach(readerFd);
                    readerFd = -1;
                    if (isRewrite) {
                        // Rewrite mode: write to a new partition directory named by txn.
                        // The old directory (srcNameTxn) is left intact and queued for removal on commit.
                        Path newPath = Path.getThreadLocal2(pathToTable);
                        setPathForNativePartition(newPath, timestampType, partitionBy, partitionTimestamp, txn);
                        ff.mkdirs(newPath.slash(), cairoConfiguration.getMkDirMode());
                        newPath.concat(PARQUET_PARTITION_NAME).$();
                        writerFd = TableUtils.openRW(ff, newPath.$(), LOG, opts);
                        writerFdOs = Files.detach(writerFd);
                        writerFd = -1;
                        writeFileSize = 0;
                        // Open _pm in the new directory.
                        newPath.parent().concat(PARQUET_METADATA_FILE_NAME).$();
                        parquetMetaFd = TableUtils.openRW(ff, newPath.$(), LOG, opts);
                        parquetMetaFdOs = Files.detach(parquetMetaFd);
                        parquetMetaFd = -1;
                    } else {
                        writerFd = TableUtils.openRW(ff, path.$(), LOG, opts);
                        writerFdOs = Files.detach(writerFd);
                        writerFd = -1;
                        writeFileSize = parquetSize;
                        // Open existing _pm for update.
                        path.trimTo(partitionDirLen).concat(PARQUET_METADATA_FILE_NAME).$();
                        parquetMetaFd = TableUtils.openRW(ff, path.$(), LOG, opts);
                        parquetMetaFdOs = Files.detach(parquetMetaFd);
                        parquetMetaFd = -1;
                        updaterParquetMetaFileSize = parquetMetaReader.getFileSize();
                        // Restore path to parquet file.
                        path.trimTo(partitionDirLen).concat(PARQUET_PARTITION_NAME).$();
                    }
                } catch (Throwable e) {
                    O3Utils.close(ff, readerFd);
                    if (readerFdOs != -1) {
                        Files.closeDetached(readerFdOs);
                    }
                    O3Utils.close(ff, writerFd);
                    if (writerFdOs != -1) {
                        Files.closeDetached(writerFdOs);
                    }
                    O3Utils.close(ff, parquetMetaFd);
                    if (parquetMetaFdOs != -1) {
                        Files.closeDetached(parquetMetaFdOs);
                    }
                    throw e;
                }

                // partitionUpdater.of() transfers fd ownership to Rust.
                // Rust closes all fds when destroy() is called (via close()).
                // The outer catch calls partitionUpdater.close() on error.
                partitionUpdater.of(
                        path.$(),
                        readerFdOs,
                        parquetSize,
                        writerFdOs,
                        writeFileSize,
                        timestampIndex,
                        ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                        statisticsEnabled,
                        rawArrayEncoding,
                        rowGroupSize,
                        dataPageSize,
                        bloomFilterFpp,
                        minCompressionRatio,
                        parquetMetaFdOs,
                        updaterParquetMetaFileSize,
                        parquetFileSize
                );

                if (hasSchemaChange) {
                    // Table schema differs from parquet file schema (ADD COLUMN,
                    // DROP COLUMN, or both). Pass the full target schema to Rust
                    // so the output file footer, column remapping, and null column
                    // chunks use the new schema.
                    // For SYMBOL columns, set the high bit on the column type
                    // when the symbol map has no null flag — this is a write-time
                    // hint for the Rust encoder to emit a fast all-ones RLE run
                    // for definition levels (symbols are always Optional in the schema).
                    final PartitionDescriptor schemaDesc = ctx.getChunkDescriptor();
                    schemaDesc.of(tableWriter.getTableToken().getTableName(), 0, timestampIndex);
                    for (int i = 0; i < columnCount; i++) {
                        int colType = tableWriterMetadata.getColumnType(i);
                        if (colType < 0) {
                            continue;
                        }
                        // The high bit is a write-time hint telling the Rust encoder
                        // that this symbol column has no nulls, so it can emit a fast
                        // all-ones RLE run for definition levels. It does NOT change
                        // the parquet schema Repetition — symbols are always Optional.
                        if (ColumnType.isSymbol(colType) && !tableWriter.getSymbolMapWriter(i).getNullFlag()) {
                            colType |= PARQUET_SYMBOL_NOT_NULL_HINT;
                        }
                        final int colId = tableWriterMetadata.getColumnMetadata(i).getWriterIndex();
                        final int parquetEncodingConfig = tableWriterMetadata.getColumnMetadata(i).getParquetEncodingConfig();
                        schemaDesc.addColumn(
                                tableWriterMetadata.getColumnName(i),
                                colType,
                                colId,
                                0,
                                parquetEncodingConfig
                        );
                    }
                    partitionUpdater.setTargetSchema(schemaDesc);
                    schemaDesc.clear();
                }

                // Build row group bounds for merge strategy computation.
                // Use the parquet-side column index (not the table-side index)
                // because the decoder resolves columns by their position in the
                // parquet file schema, which may differ after ADD/DROP COLUMN.
                // Read timestamp statistics when available (fast path). Fall back
                // to rowGroupMinTimestamp/rowGroupMaxTimestamp which decode actual
                // data when statistics are absent
                final int timestampParquetIdx = tableToParquetIdx.getQuick(timestampIndex);
                assert timestampParquetIdx >= 0 : "timestamp column missing from parquet file";
                final LongList rowGroupBounds = ctx.getRowGroupBounds();
                parquetColumns.clear();
                parquetColumns.add(timestampParquetIdx);
                parquetColumns.add(timestampColumnType);

                for (int rg = 0; rg < rowGroupCount; rg++) {
                    final long rgMin, rgMax;
                    final ParquetMetaFileReader meta = partitionDecoder.metadata();
                    final int statFlags = meta.getChunkStatFlags(rg, timestampParquetIdx);
                    final boolean hasTimestampStats = (statFlags & 0x03) == 0x03 && (statFlags & 0x18) == 0x18;
                    if (hasTimestampStats) {
                        rgMin = meta.getChunkMinStat(rg, timestampParquetIdx);
                        rgMax = meta.getChunkMaxStat(rg, timestampParquetIdx);
                    } else {
                        rgMin = partitionDecoder.rowGroupMinTimestamp(rg, timestampParquetIdx);
                        rgMax = partitionDecoder.rowGroupMaxTimestamp(rg, timestampParquetIdx);
                    }
                    final long rgRowCount = meta.getRowGroupSize(rg);
                    O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, rgMin, rgMax, rgRowCount);
                }
                parquetColumns.clear();

                // Compute merge actions (scratch lists are reused across calls within the same partition)
                final ObjList<O3ParquetMergeStrategy.MergeAction> actionsBuf = ctx.getActionsBuf();
                final int actionCount = O3ParquetMergeStrategy.computeMergeActions(
                        rowGroupBounds,
                        sortedTimestampsAddr,
                        srcOooLo,
                        srcOooHi,
                        rowGroupSize / 4,
                        rowGroupSize,
                        actionsBuf,
                        ctx.getRgO3Ranges(),
                        ctx.getGapO3Ranges()
                );

                // Execute merge actions.
                // metadataPosition tracks the final row group index in the output file.
                // Actions are in timestamp order: each action occupies one position.
                // Replacements (MERGE) execute first in end(), then insertions (COPY_O3)
                // in ascending position order via Vec::insert.
                //
                // mergeDstBufs holds reusable destination buffers shared across MERGE actions.
                // Layout per column: [primaryAddr, primarySize, secondaryAddr, secondarySize].
                // Buffers grow as needed and are freed after all actions complete.
                final int colCount = tableWriterMetadata.getColumnCount();
                final LongList mergeDstBufs = ctx.getMergeDstBufs(colCount);
                final LongList nullBufs = ctx.getNullBufs(colCount);
                final LongList srcPtrs = ctx.getSrcPtrs(colCount);
                final PartitionDescriptor chunkDescriptor = ctx.getChunkDescriptor();
                int metadataPosition = 0;
                try {
                    for (int i = 0; i < actionCount; i++) {
                        final O3ParquetMergeStrategy.MergeAction action = actionsBuf.getQuick(i);
                        switch (action.type) {
                            case MERGE -> {
                                assert partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex) <= Integer.MAX_VALUE;
                                final int rgSize = (int) partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex);
                                LOG.info()
                                        .$("parquet merge row group [table=").$(tableWriter.getTableToken())
                                        .$(", partition=").$ts(partitionTimestamp)
                                        .$(", rg=").$(action.rowGroupIndex)
                                        .$(", dataRows=").$(rgSize)
                                        .$(", o3Rows=").$(action.o3Hi - action.o3Lo + 1)
                                        .$(", rgMin=").$ts(O3ParquetMergeStrategy.getRowGroupMin(rowGroupBounds, action.rowGroupIndex))
                                        .$(", rgMax=").$ts(O3ParquetMergeStrategy.getRowGroupMax(rowGroupBounds, action.rowGroupIndex))
                                        .I$();
                                final long mergeResult = mergeRowGroup(
                                        chunkDescriptor,
                                        partitionUpdater,
                                        parquetColumns,
                                        oooColumns,
                                        sortedTimestampsAddr,
                                        tableWriter,
                                        partitionDecoder,
                                        rowGroupBuffers,
                                        action.rowGroupIndex,
                                        timestampIndex,
                                        partitionTimestamp,
                                        action.o3Lo,
                                        action.o3Hi,
                                        tableWriterMetadata,
                                        dedupColSinkAddr,
                                        rowGroupSize,
                                        metadataPosition,
                                        mergeDstBufs,
                                        tableToParquetIdx,
                                        nullBufs,
                                        srcPtrs,
                                        ctx.getActiveToDecodeIdx(columnCount),
                                        ctx.getActiveColIndices(columnCount)
                                );
                                final int numOutputRGs = (int) (mergeResult >>> 32);
                                final long mergeDuplicates = mergeResult & 0xFFFFFFFFL;
                                duplicateCount += mergeDuplicates;
                                tableWriter.addPhysicallyWrittenRows(rgSize + (action.o3Hi - action.o3Lo + 1) - mergeDuplicates);
                                metadataPosition += numOutputRGs;
                            }
                            case COPY_ROW_GROUP_SLICE -> {
                                assert partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex) <= Integer.MAX_VALUE;
                                final int rgSize = (int) partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex);
                                // The merge strategy always produces full-range COPY_ROW_GROUP_SLICE
                                // actions (the entire row group). Partial slicing is not supported.
                                assert action.rgLo == 0 && action.rgHi == rgSize - 1
                                        : "partial row group slice not supported, rg=" + action.rowGroupIndex
                                        + " range=[" + action.rgLo + "," + action.rgHi + "] size=" + rgSize;
                                if (isRewrite) {
                                    // Rewrite mode: every row group must be written to the new file.
                                    LOG.info()
                                            .$("parquet copy row group [table=").$(tableWriter.getTableToken())
                                            .$(", partition=").$ts(partitionTimestamp)
                                            .$(", rg=").$(action.rowGroupIndex)
                                            .$(", rows=").$(rgSize)
                                            .$(", hasSchemaChange=").$(hasSchemaChange)
                                            .$(", rgMin=").$ts(O3ParquetMergeStrategy.getRowGroupMin(rowGroupBounds, action.rowGroupIndex))
                                            .$(", rgMax=").$ts(O3ParquetMergeStrategy.getRowGroupMax(rowGroupBounds, action.rowGroupIndex))
                                            .I$();
                                    if (hasSchemaChange) {
                                        copyRowGroupWithNullColumns(
                                                partitionUpdater,
                                                action.rowGroupIndex,
                                                tableWriterMetadata,
                                                tableToParquetIdx
                                        );
                                    } else {
                                        partitionUpdater.copyRowGroup(action.rowGroupIndex);
                                    }
                                    tableWriter.addPhysicallyWrittenRows(rgSize);
                                }
                                // Update mode: full row groups stay in place, nothing to do.
                                metadataPosition++;
                            }
                            case COPY_O3 -> {
                                LOG.info()
                                        .$("parquet add row group from o3 [table=").$(tableWriter.getTableToken())
                                        .$(", partition=").$ts(partitionTimestamp)
                                        .$(", rows=").$(action.o3Hi - action.o3Lo + 1)
                                        .$(", o3Min=").$ts(Unsafe.getLong(sortedTimestampsAddr + action.o3Lo * TIMESTAMP_MERGE_ENTRY_BYTES))
                                        .$(", o3Max=").$ts(Unsafe.getLong(sortedTimestampsAddr + action.o3Hi * TIMESTAMP_MERGE_ENTRY_BYTES))
                                        .I$();
                                copyO3ToRowGroup(
                                        partitionDescriptor,
                                        partitionUpdater,
                                        oooColumns,
                                        sortedTimestampsAddr,
                                        tableWriter,
                                        timestampIndex,
                                        action.o3Lo,
                                        action.o3Hi,
                                        tableWriterMetadata,
                                        metadataPosition
                                );
                                tableWriter.addPhysicallyWrittenRows(action.o3Hi - action.o3Lo + 1);
                                metadataPosition++;
                            }
                        }
                    }
                } finally {
                    chunkDescriptor.clear();
                    partitionDescriptor.clear();
                    for (int bufIdx = 0; bufIdx < colCount; bufIdx++) {
                        int bi4 = bufIdx * 4;
                        for (int slot = 0; slot < 4; slot += 2) {
                            if (mergeDstBufs.getQuick(bi4 + slot) != 0) {
                                Unsafe.free(mergeDstBufs.getQuick(bi4 + slot), mergeDstBufs.getQuick(bi4 + slot + 1), MemoryTag.NATIVE_O3);
                            }
                        }
                    }
                }
                newParquetSize = partitionUpdater.updateFileMetadata();
                newParquetMetaFileSize = partitionUpdater.getResultParquetMetaFileSize();
                final long resultUnusedBytes = partitionUpdater.getResultUnusedBytes();
                LOG.info()
                        .$("parquet o3 partition [table=").$(tableWriter.getTableToken())
                        .$(", partition=").$ts(partitionTimestamp)
                        .$(", rowGroups=").$(metadataPosition)
                        .$(", fileSize=").$size(newParquetSize)
                        .$(", unusedBytes=").$size(resultUnusedBytes)
                        .$(", unusedPct=").$(newParquetSize > 0 ? (100.0 * resultUnusedBytes / newParquetSize) : 0)
                        .$(", partitionMutates=").$(isRewrite)
                        .I$();

                // Update indexes.
                // In rewrite mode, the new file is in a txn-named directory.
                final long txnName = isRewrite ? txn : srcNameTxn;
                path.of(pathToTable);
                setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, txnName);
                updateParquetIndexes(
                        partitionBy,
                        partitionTimestamp,
                        tableWriter,
                        txnName,
                        o3Basket,
                        newPartitionSize,
                        newParquetSize,
                        newParquetMetaFileSize,
                        pathToTable,
                        path,
                        ff,
                        partitionDecoder,
                        tableWriterMetadata,
                        parquetColumns,
                        rowGroupBuffers,
                        isRewrite
                );
            } catch (Throwable e) {
                if (isRewrite) {
                    // Rewrite mode: original is intact. Remove the new directory.
                    Path newPath = Path.getThreadLocal2(pathToTable);
                    setPathForNativePartition(newPath, timestampType, partitionBy, partitionTimestamp, txn);
                    if (!ff.rmdir(newPath.slash())) {
                        LOG.error().$("could not remove new partition directory after failed rewrite [path=").$(newPath).I$();
                    }
                }
                throw e;
            } finally {
                if (parquetAddr != 0) {
                    ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                }
            }
        } catch (Throwable th) {
            LOG.error().$("process partition error [table=").$(tableWriter.getTableToken())
                    .$(", e=").$(th)
                    .I$();
            // Release the Rust-owned file descriptors immediately so that
            // the truncation below (update mode) does not compete with them,
            // and on Windows the file is not locked by stale fds.
            partitionUpdater.close();
            if (!isRewrite) {
                // Update mode: truncate both parquet and _pm to their pre-merge sizes.
                path.of(pathToTable);
                setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
                long fd = TableUtils.openRW(ff, path.$(), LOG, cairoConfiguration.getWriterFileOpenOpts());
                if (!ff.truncate(fd, parquetSize)) {
                    LOG.error().$("could not truncate partition file [path=").$(path).I$();
                }
                ff.close(fd);

                if (parquetMetaReader.getAddr() != 0) {
                    path.of(pathToTable);
                    setPathForParquetPartitionMetadata(path.slash(), timestampType, partitionBy, partitionTimestamp, srcNameTxn);
                    fd = TableUtils.openRW(ff, path.$(), LOG, cairoConfiguration.getWriterFileOpenOpts());
                    if (!ff.truncate(fd, parquetMetaReader.getFileSize())) {
                        LOG.error().$("could not truncate _pm file [path=").$(path).I$();
                    }
                    ff.close(fd);
                }
            }
            // Rewrite mode: original is intact, new dir already removed by the inner catch.
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(th));
        } finally {
            // Release the reader's native handle (which borrows from the mmap) before munmap.
            // See ParquetMetaFileReader lifecycle contract.
            ctx.releaseResources();
            parquetMetaReader.unmapAndClear(ff);
            path.of(pathToTable);
            if (isRewrite) {
                setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, txn);
            } else {
                setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
            }
            final long fileSize = Files.length(path.$());
            Unsafe.putLong(partitionUpdateSinkAddr, partitionTimestamp);
            Unsafe.putLong(partitionUpdateSinkAddr + Long.BYTES, o3TimestampMin);
            Unsafe.putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize - duplicateCount);
            Unsafe.putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
            // flags: lowInt = partitionMutates (0 when rewritten, 1 when mutated in place)
            Unsafe.putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, Numbers.encodeLowHighInts(isRewrite ? 0 : 1, 0));
            Unsafe.putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0); // o3SplitPartitionSize
            Unsafe.putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, fileSize); // update parquet partition file size


            tableWriter.o3CountDownDoneLatch();
            tableWriter.o3ClockDownPartitionUpdateCount();
        }
    }

    public static void processPartition(
            Path pathToTable,
            int partitionBy,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long o3TimestampMin,
            long partitionTimestamp,
            long maxTimestamp,
            long srcDataMax,
            long srcNameTxn,
            boolean last,
            long txn,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long newPartitionSize,
            final long oldPartitionSize,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr,
            boolean isParquet,
            long o3TimestampLo,
            long o3TimestampHi
    ) {
        // is out of order data hitting the last partition?
        // if so we do not need to re-open files and write to existing file descriptors
        final RecordMetadata metadata = tableWriter.getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(metadata.getTimestampType());

        if (isParquet) {
            processParquetPartition(
                    pathToTable,
                    tableWriter.getMetadata().getTimestampType(),
                    partitionBy,
                    oooColumns,
                    srcOooLo,
                    srcOooHi,
                    partitionTimestamp,
                    sortedTimestampsAddr,
                    tableWriter,
                    srcNameTxn,
                    txn,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr,
                    o3TimestampMin,
                    o3Basket,
                    newPartitionSize,
                    oldPartitionSize
            );
            return;
        }

        final Path path = Path.getThreadLocal(pathToTable);

        long srcTimestampFd = 0;
        long dataTimestampLo;
        long dataTimestampHi;
        final FilesFacade ff = tableWriter.getFilesFacade();
        long oldPartitionTimestamp;

        // partition might be subject to split
        // if this happens the size of the original (srcDataPartition) partition will decrease
        // and the size of new (o3Partition) will be non-zero
        long srcDataNewPartitionSize = newPartitionSize;

        long o3SplitPartitionSize = 0;

        if (srcDataMax < 1) {
            // This has to be a brand-new partition for any of three cases:
            // - This partition is above min partition of the table.
            // - This partition is below max partition of the table.
            // - This is last partition that is empty.
            // pure OOO data copy into new partition

            if (!last) {
                try {
                    LOG.debug().$("would create [path=").$(path.slash$()).I$();
                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            txn - 1
                    );
                    createDirsOrFail(ff, path, tableWriter.getConfiguration().getMkDirMode());
                } catch (Throwable e) {
                    LOG.error().$("process new partition error [table=").$(tableWriter.getTableToken())
                            .$(", e=").$(e)
                            .I$();
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    tableWriter.o3ClockDownPartitionUpdateCount();
                    tableWriter.o3CountDownDoneLatch();
                    throw e;
                }
            }

            assert oldPartitionSize == 0;

            if (tableWriter.isCommitReplaceMode()) {
                assert srcOooLo <= srcOooHi;
                // Recalculate the resulting min timestamp to be the first row
                // of the new data.
                // o3TimestampMin is the min replaceRangeLo timestamp
                o3TimestampMin = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
            }

            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    o3TimestampMin,
                    partitionTimestamp,
                    partitionTimestamp,
                    // below parameters are unused by this type of append
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    srcNameTxn,
                    OPEN_NEW_PARTITION_FOR_APPEND,
                    0,  // timestamp fd
                    0,
                    0,
                    timestampIndex,
                    timestampDriver,
                    sortedTimestampsAddr,
                    newPartitionSize,
                    oldPartitionSize,
                    0,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
            );
        } else {
            long srcTimestampAddr = 0;
            long srcTimestampSize = 0;
            int prefixType;
            long prefixLo;
            long prefixHi;
            int mergeType;
            long mergeDataLo;
            long mergeDataHi;
            long mergeO3Lo;
            long mergeO3Hi;
            int suffixType;
            long suffixLo;
            long suffixHi;
            final int openColumnMode;
            long newMinPartitionTimestamp;

            try {
                // out of order is hitting existing partition
                // partitionTimestamp is in fact a ceil of ooo timestamp value for the given partition
                // so this check is for matching ceilings
                if (last) {
                    dataTimestampHi = maxTimestamp;
                    srcTimestampSize = srcDataMax * 8L;
                    // negative fd indicates descriptor reuse
                    srcTimestampFd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                    srcTimestampAddr = mapRW(ff, -srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                } else {
                    srcTimestampSize = srcDataMax * 8L;
                    // out of order data is going into archive partition
                    // we need to read "low" and "high" boundaries of the partition. "low" being the oldest timestamp
                    // and "high" being newest

                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            srcNameTxn
                    );

                    // also track the fd that we need to eventually close
                    // Open src timestamp column as RW in case append happens
                    srcTimestampFd = openRW(ff, dFile(path, metadata.getColumnName(timestampIndex), COLUMN_NAME_TXN_NONE), LOG, tableWriter.getConfiguration().getWriterFileOpenOpts());
                    srcTimestampAddr = mapRW(ff, srcTimestampFd, srcTimestampSize, MemoryTag.MMAP_O3);
                    dataTimestampHi = Unsafe.getLong(srcTimestampAddr + srcTimestampSize - Long.BYTES);
                }
                dataTimestampLo = Unsafe.getLong(srcTimestampAddr);

                // create copy jobs
                // we will have maximum of 3 stages:
                // - prefix data
                // - merge job
                // - suffix data
                //
                // prefix and suffix can be sourced either from OO fully or from Data (written to disk) fully
                // so for prefix and suffix we will need a flag indicating source of the data
                // as well as range of rows in that source

                mergeType = O3_BLOCK_NONE;
                mergeDataLo = -1;
                mergeDataHi = -1;
                mergeO3Lo = -1;
                mergeO3Hi = -1;
                suffixType = O3_BLOCK_NONE;
                suffixLo = -1;
                suffixHi = -1;

                assert srcTimestampFd != -1 && srcTimestampFd != 1;

                int branch;

                // When deduplication is enabled, we want to take into the merge
                // the rows from the partition which equals to the O3 min and O3 max.
                // Without taking equal rows, deduplication will be incorrect.
                // Without deduplication, taking timestamp == o3TimestampHi into merge
                // can result into unnecessary partition rewrites, when instead of appending
                // rows with equal timestamp a merge is triggered.
                long mergeEquals = tableWriter.isCommitDedupMode() || tableWriter.isCommitReplaceMode() ? 1 : 0;

                if (o3TimestampLo >= dataTimestampLo) {
                    //   +------+
                    //   | data |  +-----+
                    //   |      |  | OOO |
                    //   |      |  |     |

                    // When deduplication is enabled, take into the merge the rows which are equals
                    // to the dataTimestampHi in the else block
                    if (o3TimestampLo >= dataTimestampHi + mergeEquals) {

                        // +------+
                        // | data |
                        // |      |
                        // +------+
                        //
                        //           +-----+
                        //           | OOO |
                        //           |     |
                        //
                        branch = 1;
                        suffixType = O3_BLOCK_O3;
                        suffixLo = srcOooLo;
                        suffixHi = srcOooHi;

                        prefixType = O3_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = srcDataMax - 1;
                    } else {

                        //
                        // +------+
                        // |      |
                        // |      | +-----+
                        // | data | | OOO |
                        // +------+

                        prefixLo = 0;
                        // When deduplication is enabled, take into the merge the rows which are equals
                        // to the o3TimestampLo in the else block, e.g. reduce the prefix size
                        prefixHi = Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo - mergeEquals,
                                0,
                                srcDataMax - 1,
                                Vect.BIN_SEARCH_SCAN_DOWN
                        );
                        prefixType = prefixLo <= prefixHi ? O3_BLOCK_DATA : O3_BLOCK_NONE;
                        mergeDataLo = prefixHi + 1;
                        mergeO3Lo = srcOooLo;

                        if (o3TimestampHi < dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | +-----+
                            // +------+

                            branch = 2;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    mergeDataLo,
                                    srcDataMax - 1,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );
                            assert mergeDataHi > -1;

                            if (mergeDataLo > mergeDataHi) {
                                // the o3 data implodes right between rows of existing data
                                // so we will have both data prefix and suffix and the middle bit
                                // is the out of order
                                mergeType = O3_BLOCK_O3;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            suffixType = O3_BLOCK_DATA;
                            suffixLo = mergeDataHi + 1;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi;

                        } else if (o3TimestampHi > dataTimestampHi) {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ |     |
                            //          |     |
                            //          +-----+

                            branch = 3;
                            // When deduplication is enabled, take in to the merge
                            // all o3 rows that are equal to the last row in the data
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi,
                                    srcOooLo,
                                    srcOooHi,
                                    tableWriter.isCommitDedupMode() ? Vect.BIN_SEARCH_SCAN_DOWN : Vect.BIN_SEARCH_SCAN_UP
                            );

                            mergeDataHi = srcDataMax - 1;
                            assert mergeDataLo <= mergeDataHi;

                            mergeType = O3_BLOCK_MERGE;
                            suffixType = O3_BLOCK_O3;
                            suffixLo = mergeO3Hi + 1;
                            if (suffixLo > srcOooHi && tableWriter.isCommitReplaceMode()) {
                                // In replace mode o3TimestampHi can be greater than the highest timestamp in the o3 data
                                // This means that the suffix has to include all the o3 data
                                suffixLo = srcOooHi;
                            }
                            suffixHi = srcOooHi;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
                        } else {

                            //
                            // |      | +-----+
                            // | data | | OOO |
                            // |      | |     |
                            // +------+ +-----+
                            //

                            branch = 4;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                            assert mergeDataLo <= mergeDataHi;
                        }
                    }
                } else {

                    //            +-----+
                    //            | OOO |
                    //
                    //  +------+
                    //  | data |

                    prefixType = O3_BLOCK_O3;
                    prefixLo = srcOooLo;
                    if (dataTimestampLo <= o3TimestampHi) {

                        //
                        //  +------+  | OOO |
                        //  | data |  +-----+
                        //  |      |

                        mergeDataLo = 0;
                        // To make inserts stable o3 rows with timestamp == dataTimestampLo
                        // should go into the merge section.
                        prefixHi = Vect.boundedBinarySearchIndexT(
                                sortedTimestampsAddr,
                                dataTimestampLo - 1,
                                srcOooLo,
                                srcOooHi,
                                Vect.BIN_SEARCH_SCAN_DOWN
                        );
                        mergeO3Lo = prefixHi + 1;

                        if (o3TimestampHi < dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | +-----+
                            // |      |
                            // +------+

                            branch = 5;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            // To make inserts stable table rows with timestamp == o3TimestampHi
                            // should go into the merge section.
                            mergeDataHi = Vect.boundedBinarySearch64Bit(
                                    srcTimestampAddr,
                                    o3TimestampHi,
                                    0,
                                    srcDataMax - 1,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );

                            suffixLo = mergeDataHi + 1;
                            suffixType = O3_BLOCK_DATA;
                            suffixHi = srcDataMax - 1;
                            assert suffixLo <= suffixHi : String.format("Branch %,d suffixLo %,d > suffixHi %,d",
                                    branch, suffixLo, suffixHi);
                        } else if (o3TimestampHi > dataTimestampHi) {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ |     |
                            //          +-----+

                            branch = 6;
                            mergeDataHi = srcDataMax - 1;
                            // To deduplicate O3 rows with timestamp == dataTimestampHi
                            // should go into the merge section.
                            mergeO3Hi = Vect.boundedBinarySearchIndexT(
                                    sortedTimestampsAddr,
                                    dataTimestampHi - 1 + mergeEquals,
                                    mergeO3Lo,
                                    srcOooHi,
                                    Vect.BIN_SEARCH_SCAN_DOWN
                            );

                            if (mergeO3Lo > mergeO3Hi && !tableWriter.isCommitReplaceMode()) {
                                mergeType = O3_BLOCK_DATA;
                            } else {
                                mergeType = O3_BLOCK_MERGE;
                            }

                            if (mergeO3Hi < srcOooHi) {
                                suffixLo = mergeO3Hi + 1;
                                suffixType = O3_BLOCK_O3;
                                suffixHi = Math.max(suffixLo, srcOooHi);
                            }
                        } else {

                            // |      | |     |
                            // |      | | OOO |
                            // | data | |     |
                            // +------+ +-----+

                            branch = 7;
                            mergeType = O3_BLOCK_MERGE;
                            mergeO3Hi = srcOooHi;
                            mergeDataHi = srcDataMax - 1;
                        }
                    } else {
                        //            +-----+
                        //            | OOO |
                        //            +-----+
                        //
                        //  +------+
                        //  | data |
                        //
                        branch = 8;
                        prefixHi = srcOooHi;
                        suffixType = O3_BLOCK_DATA;
                        suffixLo = 0;
                        suffixHi = srcDataMax - 1;
                    }
                }

                // Save initial overlap state, mergeType can be re-written in commit replace mode
                boolean overlaps = mergeType == O3_BLOCK_MERGE;
                if (tableWriter.isCommitReplaceMode()) {
                    if (prefixHi < prefixLo) {
                        prefixType = O3_BLOCK_NONE;
                        // prefixType == O3_BLOCK_NONE and prefixHi >= also is used
                        // to indicate split partition. To avoid that, set prefixHi to -1.
                        prefixLo = 0;
                        prefixHi = -1;
                    }
                    if (suffixHi < suffixLo) {
                        suffixType = O3_BLOCK_NONE;
                    }
                }

                // Recalculate min timestamp value for this partition, it can be used
                // to replace table min timestamp value if it's the first partition.
                // Do not take existing data timestamp min value if it's being fully replaced.
                if (!tableWriter.isCommitReplaceMode()) {
                    newMinPartitionTimestamp = Math.min(o3TimestampMin, dataTimestampLo);
                } else {
                    newMinPartitionTimestamp = calculateMinDataTimestampAfterReplacement(
                            srcTimestampAddr,
                            sortedTimestampsAddr,
                            prefixType,
                            suffixType,
                            prefixLo,
                            suffixLo,
                            srcOooLo,
                            srcOooHi
                    );
                }

                if (tableWriter.isCommitReplaceMode()) {

                    if (mergeType == O3_BLOCK_MERGE) {
                        // When replace range deduplication mode is enabled, we need to take into the merge
                        // prefix and suffix it's O3 type.
                        newPartitionSize -= mergeDataHi - mergeDataLo + 1;
                        srcDataNewPartitionSize -= mergeDataHi - mergeDataLo + 1;
                    }

                    if (srcOooLo <= srcOooHi) {
                        if (mergeType == O3_BLOCK_MERGE) {

                            long removedDataRangeLo, removedDataRangeHi, o3RangeLo, o3RangeHi;
                            if (prefixType == O3_BLOCK_O3) {
                                // O3 in prefix, partition data in the suffix.
                                prefixHi = mergeO3Hi;
                                mergeType = O3_BLOCK_NONE;
                                mergeO3Hi = -1;
                                mergeO3Lo = -1;
                                mergeDataHi = -1;
                                mergeDataLo = -1;

                                removedDataRangeLo = 0;
                                removedDataRangeHi = suffixLo - 1;
                                o3RangeLo = prefixLo;
                                o3RangeHi = prefixHi;
                            } else if (suffixType == O3_BLOCK_O3) {
                                // Partition data in the prefix, O3 in suffix.
                                suffixLo = mergeO3Lo;
                                mergeType = O3_BLOCK_NONE;
                                mergeO3Hi = -1;
                                mergeO3Lo = -1;
                                mergeDataHi = -1;
                                mergeDataLo = -1;

                                removedDataRangeLo = prefixHi + 1;
                                removedDataRangeHi = srcDataMax - 1;
                                o3RangeLo = suffixLo;
                                o3RangeHi = suffixHi;
                            } else {
                                // Replacing partition data with new data in the middle of the partition.
                                removedDataRangeLo = mergeDataLo;
                                removedDataRangeHi = mergeDataHi;
                                o3RangeLo = mergeO3Lo;
                                o3RangeHi = mergeO3Hi;
                            }

                            if (removedDataRangeHi - removedDataRangeLo > 0 && removedDataRangeHi - removedDataRangeLo == o3RangeHi - o3RangeLo) {

                                // Check that replace first timestamp matches exactly the first timestamp in the partition
                                // and the last timestamp matches the last timestamp in the partition.
                                if (Unsafe.getLong(srcTimestampAddr + removedDataRangeLo * Long.BYTES)
                                        == getTimestampIndexValue(sortedTimestampsAddr, o3RangeLo)
                                        && Unsafe.getLong(srcTimestampAddr + removedDataRangeHi * Long.BYTES)
                                        == getTimestampIndexValue(sortedTimestampsAddr, o3RangeHi)) {

                                    // We are replacing with exactly the same number of rows
                                    // Maybe the rows are of the same data, then we don't need to rewrite the partition
                                    if (tableWriter.checkReplaceCommitIdenticalToPartition(
                                            partitionTimestamp,
                                            srcNameTxn,
                                            srcDataMax,
                                            removedDataRangeLo,
                                            removedDataRangeHi,
                                            o3RangeLo,
                                            o3RangeHi
                                    )) {
                                        LOG.info().$("replace commit resulted in identical data [table=").$(tableWriter.getTableToken())
                                                .$(", partitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                                                .$(", srcNameTxn=").$(srcNameTxn)
                                                .I$();
                                        // No need to update partition, it is identical to the existing one
                                        updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, oldPartitionSize, oldPartitionSize, 0);
                                        return;
                                    }
                                }
                            }
                        }
                    } else {
                        // Replacing data with no O3 data, e.g. effectively deleting a part of the partition.

                        // O3 data is supposed to be merged into the middle of an existing partition
                        // but there is no O3 data, it's a replacing commit with no new rows, just the range.
                        // At the end we have existing column data prefix, suffix and nothing to insert in between.
                        // We can finish here without modifying this partition.
                        boolean noop = mergeType == O3_BLOCK_O3 && prefixType == O3_BLOCK_DATA && suffixType == O3_BLOCK_DATA;

                        // No intersection with existing partition data, the whole replace range is before the existing partition
                        noop |= suffixType == O3_BLOCK_DATA && suffixLo == 0 && suffixHi == srcDataMax - 1;

                        // No intersection with existing partition data, the whole replace range is after the existing partition
                        noop |= prefixType == O3_BLOCK_DATA && prefixLo == 0 && prefixHi == srcDataMax - 1;

                        if (noop) {
                            updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, oldPartitionSize, oldPartitionSize, 0);
                            return;
                        }

                        // srcOooLo > srcOooHi means that O3 data is empty
                        if (prefixType == O3_BLOCK_O3) {
                            prefixType = O3_BLOCK_NONE;
                            // prefixType == O3_BLOCK_NONE and prefixHi >= also is used
                            // to indicate split partition. To avoid that, set prefixHi to -1.
                            prefixLo = 0;
                            prefixHi = -1;
                        }

                        // srcOooLo > srcOooHi means that O3 data is empty
                        mergeType = O3_BLOCK_NONE;
                        if (suffixType == O3_BLOCK_O3) {
                            suffixType = O3_BLOCK_NONE;
                        }

                        if (prefixType == O3_BLOCK_NONE && suffixType == O3_BLOCK_NONE) {
                            // full partition removal
                            updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, Long.MAX_VALUE, 0, oldPartitionSize, 1);
                            return;
                        }

                        if (prefixType == O3_BLOCK_DATA && prefixHi >= prefixLo && suffixType == O3_BLOCK_NONE) {
                            // No merge, no suffix, only data prefix

                            if (prefixHi - prefixLo + 1 == srcDataMax) {
                                // If the number of rows is the same as the number of rows in the partition
                                // There is nothing to do

                                // Nothing to do, use the existing partition to the prefix size
                                updatePartition(ff, srcTimestampAddr, srcTimestampSize, srcTimestampFd, tableWriter, partitionUpdateSinkAddr, partitionTimestamp, newMinPartitionTimestamp, prefixHi + 1, oldPartitionSize, 0);
                                return;
                            }

                            // The number of rows in the partition is lower.
                            // We cannot simply trim the partition to lower row numbers
                            // because the next commit can start overwriting the tail of the partition
                            // while there can be old readers that still read the data
                            // Proceed with another copy of the partition, but instead of
                            // copying, spit the last line into the suffix so that if it's economical
                            // to split the partition, it will be split.
                            // Split the last line of the partition
                            if (prefixHi > prefixLo) {
                                suffixHi = suffixLo = prefixHi;
                                suffixType = O3_BLOCK_DATA;
                                prefixHi--;
                            }
                        }
                    }
                }

                LOG.debug()
                        .$("o3 merge [branch=").$(branch)
                        .$(", prefixType=").$(prefixType)
                        .$(", prefixLo=").$(prefixLo)
                        .$(", prefixHi=").$(prefixHi)
                        .$(", o3TimestampLo=").$ts(timestampDriver, o3TimestampLo)
                        .$(", o3TimestampHi=").$ts(timestampDriver, o3TimestampHi)
                        .$(", o3TimestampMin=").$ts(timestampDriver, o3TimestampMin)
                        .$(", dataTimestampLo=").$ts(timestampDriver, dataTimestampLo)
                        .$(", dataTimestampHi=").$ts(timestampDriver, dataTimestampHi)
                        .$(", partitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                        .$(", srcDataMax=").$(srcDataMax)
                        .$(", mergeType=").$(mergeType)
                        .$(", mergeDataLo=").$(mergeDataLo)
                        .$(", mergeDataHi=").$(mergeDataHi)
                        .$(", mergeO3Lo=").$(mergeO3Lo)
                        .$(", mergeO3Hi=").$(mergeO3Hi)
                        .$(", suffixType=").$(suffixType)
                        .$(", suffixLo=").$(suffixLo)
                        .$(", suffixHi=").$(suffixHi)
                        .$(", table=").$(pathToTable)
                        .I$();

                oldPartitionTimestamp = partitionTimestamp;
                boolean partitionSplit = false;

                // Split partition if the prefix is large enough (relatively and absolutely)
                if (
                        prefixType == O3_BLOCK_DATA
                                && (mergeType == O3_BLOCK_MERGE || mergeType == O3_BLOCK_O3)
                                && prefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                && prefixHi > 2 * (mergeDataHi - mergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                ) {
                    // large prefix copy, better to split the partition
                    long maxSourceTimestamp = Unsafe.getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                    assert maxSourceTimestamp <= o3TimestampLo;
                    boolean canSplit = true;

                    if (maxSourceTimestamp == o3TimestampLo) {
                        // We cannot split the partition if existing data has timestamp with exactly same value
                        // because 2 partition parts cannot have data with exactly same timestamp.
                        // To make this work, we can reduce the prefix by the size of the rows which equals to o3TimestampLo.
                        long newPrefixHi = -1 + Vect.boundedBinarySearch64Bit(
                                srcTimestampAddr,
                                o3TimestampLo,
                                prefixLo,
                                prefixHi - 1,
                                Vect.BIN_SEARCH_SCAN_UP
                        );

                        if (newPrefixHi > -1L) {
                            long shiftLeft = prefixHi - newPrefixHi;
                            long newMergeDataLo = mergeDataLo - shiftLeft;
                            // Check that splitting still makes sense
                            if (newPrefixHi >= tableWriter.getPartitionO3SplitThreshold()
                                    && newPrefixHi > 2 * (mergeDataHi - newMergeDataLo + suffixHi - suffixLo + mergeO3Hi - mergeO3Lo)
                            ) {
                                prefixHi = newPrefixHi;
                                mergeDataLo = newMergeDataLo;
                                maxSourceTimestamp = Unsafe.getLong(srcTimestampAddr + prefixHi * Long.BYTES);
                                mergeType = O3_BLOCK_MERGE;
                                assert maxSourceTimestamp < o3TimestampLo;
                            } else {
                                canSplit = false;
                            }
                        } else {
                            canSplit = false;
                        }
                    }

                    if (canSplit) {
                        partitionSplit = true;
                        partitionTimestamp = maxSourceTimestamp + 1;
                        prefixType = O3_BLOCK_NONE;

                        // we are splitting existing partition along the "prefix" line
                        // this action creates two partitions:
                        // 1. Prefix of the srcDataPartition
                        // 2. Merge and suffix of srcDataPartition

                        // size of old data partition will be reduced
                        srcDataNewPartitionSize = prefixHi + 1;

                        // size of the new partition, old (0) and new
                        o3SplitPartitionSize = newPartitionSize - srcDataNewPartitionSize;

                        // large prefix copy, better to split the partition
                        LOG.info().$("o3 split partition [table=").$(tableWriter.getTableToken())
                                .$(", timestamp=").$ts(timestampDriver, oldPartitionTimestamp)
                                .$(", nameTxn=").$(srcNameTxn)
                                .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                                .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                                .$(", newPartitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                                .$(", nameTxn=").$(txn)
                                .I$();
                    }
                }

                boolean canAppendOnly = !partitionSplit;
                if (tableWriter.isCommitReplaceMode()) {
                    canAppendOnly &= (!overlaps && suffixType == O3_BLOCK_O3);
                } else {
                    canAppendOnly &= mergeType == O3_BLOCK_NONE && (prefixType == O3_BLOCK_NONE || prefixType == O3_BLOCK_DATA);
                }
                if (canAppendOnly) {
                    // We do not need to create a copy of partition when we simply need to append
                    // to the existing one.
                    openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                } else {
                    TableUtils.setPathForNativePartition(
                            path.trimTo(pathToTable.size()),
                            tableWriter.getMetadata().getTimestampType(),
                            partitionBy,
                            partitionTimestamp,
                            txn
                    );
                    createDirsOrFail(ff, path, tableWriter.getConfiguration().getMkDirMode());
                    if (last) {
                        openColumnMode = OPEN_LAST_PARTITION_FOR_MERGE;
                    } else {
                        openColumnMode = OPEN_MID_PARTITION_FOR_MERGE;
                    }
                }
            } catch (Throwable e) {
                LOG.error().$("process existing partition error [table=").$(tableWriter.getTableToken())
                        .$(", e=").$(e)
                        .I$();
                O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
                O3Utils.close(ff, srcTimestampFd);
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                tableWriter.o3ClockDownPartitionUpdateCount();
                tableWriter.o3CountDownDoneLatch();
                throw e;
            }

            // Compute max timestamp as maximum of out of order data and
            // data in existing partition.
            // When partition is new, the data timestamp is MIN_LONG

            Unsafe.putLong(partitionUpdateSinkAddr, partitionTimestamp);
            publishOpenColumnTasks(
                    txn,
                    columns,
                    oooColumns,
                    pathToTable,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    newMinPartitionTimestamp,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeDataLo,
                    mergeDataHi,
                    mergeO3Lo,
                    mergeO3Hi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    srcDataMax,
                    srcNameTxn,
                    openColumnMode,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    timestampIndex,
                    timestampDriver,
                    sortedTimestampsAddr,
                    srcDataNewPartitionSize,
                    oldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    columnCounter,
                    o3Basket,
                    partitionUpdateSinkAddr,
                    dedupColSinkAddr
            );
        }
    }

    public static void processPartition(
            O3PartitionTask task,
            long cursor,
            Sequence subSeq
    ) {
        // find "current" partition boundary in the out-of-order data
        // once we know the boundary we can move on to calculating another one
        // srcOooHi is index inclusive of value
        final Path pathToTable = task.getPathToTable();
        final int partitionBy = task.getPartitionBy();
        final ObjList<MemoryMA> columns = task.getColumns();
        final ReadOnlyObjList<? extends MemoryCR> oooColumns = task.getO3Columns();
        final long srcOooLo = task.getSrcOooLo();
        final long srcOooHi = task.getSrcOooHi();
        final long srcOooMax = task.getSrcOooMax();
        final long oooTimestampMin = task.getOooTimestampMin();
        final long partitionTimestamp = task.getPartitionTimestamp();
        final long maxTimestamp = task.getMaxTimestamp();
        final long srcDataMax = task.getSrcDataMax();
        final long srcNameTxn = task.getSrcNameTxn();
        final boolean last = task.isLast();
        final long txn = task.getTxn();
        final long sortedTimestampsAddr = task.getSortedTimestampsAddr();
        final TableWriter tableWriter = task.getTableWriter();
        final AtomicInteger columnCounter = task.getColumnCounter();
        final O3Basket o3Basket = task.getO3Basket();
        final long newPartitionSize = task.getNewPartitionSize();
        final long oldPartitionSize = task.getOldPartitionSize();
        final long partitionUpdateSinkAddr = task.getPartitionUpdateSinkAddr();
        final long dedupColSinkAddr = task.getDedupColSinkAddr();
        final boolean isParquet = task.isParquet();
        final long o3TimestampLo = task.getO3TimestampLo();
        final long o3TimestampHi = task.getO3TimestampHi();

        subSeq.done(cursor);

        processPartition(
                pathToTable,
                partitionBy,
                columns,
                oooColumns,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                partitionTimestamp,
                maxTimestamp,
                srcDataMax,
                srcNameTxn,
                last,
                txn,
                sortedTimestampsAddr,
                tableWriter,
                columnCounter,
                o3Basket,
                newPartitionSize,
                oldPartitionSize,
                partitionUpdateSinkAddr,
                dedupColSinkAddr,
                isParquet,
                o3TimestampLo,
                o3TimestampHi
        );
    }

    private static long calculateMinDataTimestampAfterReplacement(
            long srcDataTimestampAddr,
            long o3TimestampsAddr,
            int prefixType,
            int suffixType,
            long prefixLo,
            long suffixLo,
            long srcOooLo,
            long srcOooHi
    ) {
        if (prefixType == O3_BLOCK_DATA) {
            return Unsafe.getLong(srcDataTimestampAddr + prefixLo * Long.BYTES);
        }
        if (srcOooLo <= srcOooHi) {
            // If there is O3 data, it will replace the partition data in merge section
            return getTimestampIndexValue(o3TimestampsAddr, srcOooLo);
        }
        if (suffixType == O3_BLOCK_DATA) {
            // No prefix, no merge, just suffix from the partition data
            return Unsafe.getLong(srcDataTimestampAddr + suffixLo * Long.BYTES);
        }
        return Long.MAX_VALUE;
    }

    private static void copyO3ToRowGroup(
            PartitionDescriptor partitionDescriptor,
            PartitionUpdater partitionUpdater,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            int timestampIndex,
            long o3Lo,
            long o3Hi,
            TableRecordMetadata tableWriterMetadata,
            int metadataPosition
    ) {
        final long rowCount = o3Hi - o3Lo + 1;
        // Use the sorted timestamps directly as merge index.
        // After flattenIndex, each entry has [timestamp, sequential_index] with bit 63 = 0.
        // In the C++ shuffle functions, bit 63 = 0 selects sources[0] = src2 = srcOooFixAddr.
        final long mergeIndexAddr = sortedTimestampsAddr + o3Lo * TIMESTAMP_MERGE_ENTRY_BYTES;

        final int columnCount = tableWriterMetadata.getColumnCount();
        partitionDescriptor.of(tableWriter.getTableToken().getTableName(), rowCount, timestampIndex);

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = tableWriterMetadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            final String columnName = tableWriterMetadata.getColumnName(columnIndex);
            final int columnId = tableWriterMetadata.getColumnMetadata(columnIndex).getWriterIndex();
            final int parquetEncodingConfig = tableWriterMetadata.getColumnMetadata(columnIndex).getParquetEncodingConfig();
            final boolean notTheTimestamp = columnIndex != timestampIndex;
            final int columnOffset = getPrimaryColumnIndex(columnIndex);
            final MemoryCR oooMem1 = oooColumns.getQuick(columnOffset);
            final MemoryCR oooMem2 = oooColumns.getQuick(columnOffset + 1);

            if (ColumnType.isVarSize(columnType)) {
                final ColumnTypeDriver ctd = ColumnType.getDriver(columnType);
                final long srcOooAuxAddr = oooMem2.addressOf(0);
                final long srcOooDataAddr = oooMem1.addressOf(0);

                long dstAuxSize = ctd.getAuxVectorSize(rowCount);
                long dstDataSize = ctd.getDataVectorSize(srcOooAuxAddr, o3Lo, o3Hi);

                long dstAuxAddr = Unsafe.malloc(dstAuxSize, MemoryTag.NATIVE_O3);
                long dstDataAddr;
                try {
                    dstDataAddr = Unsafe.malloc(dstDataSize, MemoryTag.NATIVE_O3);
                } catch (Throwable th) {
                    Unsafe.free(dstAuxAddr, dstAuxSize, MemoryTag.NATIVE_O3);
                    throw th;
                }

                try {
                    O3CopyJob.mergeCopy(
                            columnType,
                            mergeIndexAddr,
                            rowCount,
                            0, // srcDataAuxAddr - not accessed (bit 63 = 0)
                            0, // srcDataVarAddr - not accessed
                            srcOooAuxAddr,
                            srcOooDataAddr,
                            dstAuxAddr,
                            dstDataAddr,
                            0
                    );
                } catch (Throwable th) {
                    Unsafe.free(dstAuxAddr, dstAuxSize, MemoryTag.NATIVE_O3);
                    Unsafe.free(dstDataAddr, dstDataSize, MemoryTag.NATIVE_O3);
                    throw th;
                }

                partitionDescriptor.addColumn(
                        columnName,
                        columnType,
                        columnId,
                        0,
                        dstDataAddr,
                        dstDataSize,
                        dstAuxAddr,
                        dstAuxSize,
                        0,
                        0,
                        parquetEncodingConfig
                );
                // Ownership transferred to partitionDescriptor, don't free on error.
            } else {
                final long srcOooFixAddr = oooMem1.addressOf(0);
                long dstFixSize = rowCount * ColumnType.sizeOf(columnType);
                long dstFixAddr = Unsafe.malloc(dstFixSize, MemoryTag.NATIVE_O3);

                try {
                    O3CopyJob.mergeCopy(
                            notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                            mergeIndexAddr,
                            rowCount,
                            0, // srcDataFixAddr - not accessed (bit 63 = 0)
                            0,
                            srcOooFixAddr,
                            0,
                            dstFixAddr,
                            0,
                            0
                    );
                } catch (Throwable th) {
                    Unsafe.free(dstFixAddr, dstFixSize, MemoryTag.NATIVE_O3);
                    throw th;
                }

                if (ColumnType.isSymbol(columnType)) {
                    final MemoryR offsetsMem;
                    final MemoryR valuesMem;
                    final int symbolCount;
                    final long valuesMemSize;
                    int encodeColumnType;
                    try {
                        final MapWriter symbolMapWriter = tableWriter.getSymbolMapWriter(columnIndex);
                        offsetsMem = symbolMapWriter.getSymbolOffsetsMemory();
                        valuesMem = symbolMapWriter.getSymbolValuesMemory();

                        symbolCount = symbolMapWriter.getSymbolCount();
                        final long offset = SymbolMapWriter.keyToOffset(symbolCount);
                        assert offset - SymbolMapWriter.HEADER_SIZE <= offsetsMem.size();
                        valuesMemSize = offsetsMem.getLong(offset);
                        assert valuesMemSize <= valuesMem.size();

                        // High bit = no-null hint for def level encoding, not schema Repetition.
                        encodeColumnType = columnType;
                        if (!symbolMapWriter.getNullFlag()) {
                            encodeColumnType |= PARQUET_SYMBOL_NOT_NULL_HINT;
                        }
                    } catch (Throwable th) {
                        Unsafe.free(dstFixAddr, dstFixSize, MemoryTag.NATIVE_O3);
                        throw th;
                    }
                    partitionDescriptor.addColumn(
                            columnName,
                            encodeColumnType,
                            columnId,
                            0,
                            dstFixAddr,
                            dstFixSize,
                            valuesMem.addressOf(0),
                            valuesMemSize,
                            // Skip header. Pass element count, not byte size.
                            offsetsMem.addressOf(SymbolMapWriter.HEADER_SIZE),
                            symbolCount,
                            parquetEncodingConfig
                    );
                } else {
                    partitionDescriptor.addColumn(
                            columnName,
                            columnType,
                            columnId,
                            0,
                            dstFixAddr,
                            dstFixSize,
                            0,
                            0,
                            0,
                            0,
                            parquetEncodingConfig
                    );
                }
            }
        }
        partitionUpdater.addRowGroup(metadataPosition, partitionDescriptor);
    }

    /**
     * Copies a row group from the source parquet file, appending null column
     * chunks for columns that exist in the current table schema but are missing
     * from the parquet file (ADD COLUMN case).
     */
    private static void copyRowGroupWithNullColumns(
            PartitionUpdater partitionUpdater,
            int rowGroupIndex,
            TableRecordMetadata tableWriterMetadata,
            IntList tableToParquetIdx
    ) {
        // Count missing columns (present in table but absent from parquet).
        // This may be zero in a DROP-only scenario — the function still
        // handles column remapping via field_id, dropping extra parquet
        // columns that no longer exist in the table schema.
        int nullColCount = 0;
        final int columnCount = tableWriterMetadata.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (tableWriterMetadata.getColumnType(i) >= 0 && tableToParquetIdx.getQuick(i) < 0) {
                nullColCount++;
            }
        }

        if (nullColCount == 0) {
            // DROP-only: no null columns needed, but the Rust function
            // still remaps existing columns by field_id, skipping dropped ones.
            partitionUpdater.copyRowGroupWithNullColumns(rowGroupIndex, 0, 0);
            return;
        }

        // Build the null column descriptor in native memory: pairs of
        // [targetSchemaPosition (long), columnType (long)] per null column.
        final long descSize = (long) nullColCount * 2 * Long.BYTES;
        final long descAddr = Unsafe.malloc(descSize, MemoryTag.NATIVE_O3);
        try {
            int targetPos = 0;
            int descIdx = 0;
            for (int i = 0; i < columnCount; i++) {
                final int colType = tableWriterMetadata.getColumnType(i);
                if (colType < 0) {
                    continue; // deleted column, not in target schema
                }
                if (tableToParquetIdx.getQuick(i) < 0) {
                    Unsafe.putLong(descAddr + (long) descIdx * Long.BYTES, targetPos);
                    Unsafe.putLong(descAddr + (long) (descIdx + 1) * Long.BYTES, colType);
                    descIdx += 2;
                }
                targetPos++;
            }
            partitionUpdater.copyRowGroupWithNullColumns(
                    rowGroupIndex,
                    descAddr,
                    nullColCount
            );
        } finally {
            Unsafe.free(descAddr, descSize, MemoryTag.NATIVE_O3);
        }
    }

    private static long createMergeIndex(
            long srcDataTimestampAddr,
            long sortedTimestampsAddr,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            long indexSize
    ) {
        // Create "index" for existing timestamp column. When we reshuffle timestamps during merge we will
        // have to go back and find data rows we need to move accordingly
        long timestampIndexAddr = Unsafe.malloc(indexSize, MemoryTag.NATIVE_O3);
        Vect.mergeTwoLongIndexesAsc(
                srcDataTimestampAddr,
                mergeDataLo,
                mergeDataHi - mergeDataLo + 1,
                sortedTimestampsAddr + mergeOOOLo * 16,
                mergeOOOHi - mergeOOOLo + 1,
                timestampIndexAddr
        );
        return timestampIndexAddr;
    }

    private static long getDedupRows(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        if (dedupCommitAddresses == null || dedupCommitAddresses.getColumnCount() == 0) {
            return Vect.mergeDedupTimestampWithLongIndexAsc(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr
            );
        } else {
            return getDedupRowsWithAdditionalKeys(
                    partitionTimestamp,
                    srcNameTxn,
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    oooColumns,
                    dedupCommitAddresses,
                    dedupColSinkAddr,
                    tableWriter,
                    tableRootPath,
                    tempIndexAddr
            );
        }
    }

    private static long getDedupRowsWithAdditionalKeys(
            long partitionTimestamp,
            long srcNameTxn,
            long srcTimestampAddr,
            long mergeDataLo,
            long mergeDataHi,
            long sortedTimestampsAddr,
            long mergeOOOLo,
            long mergeOOOHi,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            DedupColumnCommitAddresses dedupCommitAddresses,
            long dedupColSinkAddr,
            TableWriter tableWriter,
            Path tableRootPath,
            long tempIndexAddr
    ) {
        LOG.info().$("merge dedup with additional keys [table=").$(tableWriter.getTableToken())
                .$(", columnRowCount=").$(mergeDataHi - mergeDataLo + 1)
                .$(", o3RowCount=").$(mergeOOOHi - mergeOOOLo + 1)
                .I$();
        TableRecordMetadata metadata = tableWriter.getMetadata();
        int dedupColumnIndex = 0;
        int tableRootPathLen = tableRootPath.size();
        FilesFacade ff = tableWriter.getFilesFacade();

        int mapMemTag = MemoryTag.MMAP_O3;
        try {
            dedupCommitAddresses.clear(dedupColSinkAddr);
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (columnType > 0 && metadata.isDedupKey(i) && i != metadata.getTimestampIndex()) {
                    final int columnSize = !ColumnType.isVarSize(columnType) ? ColumnType.sizeOf(columnType) : -1;
                    final long columnTop = tableWriter.getColumnTop(partitionTimestamp, i, mergeDataHi + 1);
                    CharSequence columnName = metadata.getColumnName(i);
                    long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, i);

                    long addr = DedupColumnCommitAddresses.setColValues(
                            dedupColSinkAddr,
                            dedupColumnIndex,
                            columnType,
                            columnSize,
                            columnTop
                    );

                    if (columnTop > mergeDataHi) {
                        // column is all nulls because of column top
                        DedupColumnCommitAddresses.setColAddressValues(addr, DedupColumnCommitAddresses.NULL);

                        if (columnSize > 0) {
                            final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    -1,
                                    -1,
                                    -1
                            );
                        } else {
                            // Var len columns
                            final long oooVarColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            final long oooVarColSize = oooColumns.get(getPrimaryColumnIndex(i)).addressHi() - oooVarColAddress;
                            final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(i)).addressOf(0);

                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    -1,
                                    -1,
                                    -1
                            );
                            DedupColumnCommitAddresses.setReservedValuesSet2(
                                    addr,
                                    0,
                                    -1
                            );
                        }
                    } else { // if (columnTop > mergeDataHi)
                        if (columnSize > 0) {
                            // Fixed length column
                            TableUtils.setSinkForNativePartition(
                                    tableRootPath.trimTo(tableRootPathLen).slash(),
                                    tableWriter.getMetadata().getTimestampType(),
                                    tableWriter.getPartitionBy(),
                                    partitionTimestamp,
                                    srcNameTxn
                            );
                            long fd = TableUtils.openRO(ff, TableUtils.dFile(tableRootPath, columnName, columnNameTxn), LOG);

                            long fixMapSize = (mergeDataHi + 1 - columnTop) * columnSize;
                            long fixMappedAddress = TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    fd,
                                    0,
                                    fixMapSize,
                                    false,
                                    mapMemTag
                            );

                            DedupColumnCommitAddresses.setColAddressValues(addr, Math.abs(fixMappedAddress) - columnTop * columnSize);

                            final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(i)).addressOf(0);
                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    fixMappedAddress,
                                    fixMapSize,
                                    fd
                            );
                        } else {
                            // Variable length column
                            long rows = mergeDataHi + 1 - columnTop;
                            ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                            long auxMapSize = driver.getAuxVectorSize(rows);

                            TableUtils.setSinkForNativePartition(
                                    tableRootPath.trimTo(tableRootPathLen).slash(),
                                    tableWriter.getMetadata().getTimestampType(),
                                    tableWriter.getPartitionBy(),
                                    partitionTimestamp,
                                    srcNameTxn
                            );
                            long auxFd = TableUtils.openRO(ff, TableUtils.iFile(tableRootPath, columnName, columnNameTxn), LOG);
                            long auxMappedAddress = TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    auxFd,
                                    0,
                                    auxMapSize,
                                    false,
                                    mapMemTag
                            );

                            long varMapSize = driver.getDataVectorSizeAt(auxMappedAddress, rows - 1);
                            if (varMapSize > 0) {
                                TableUtils.setSinkForNativePartition(
                                        tableRootPath.trimTo(tableRootPathLen).slash(),
                                        tableWriter.getMetadata().getTimestampType(),
                                        tableWriter.getPartitionBy(),
                                        partitionTimestamp,
                                        srcNameTxn
                                );
                            }
                            long varFd = varMapSize > 0 ? TableUtils.openRO(ff, TableUtils.dFile(tableRootPath, columnName, columnNameTxn), LOG) : -1;
                            long varMappedAddress = varMapSize > 0 ? TableUtils.mapAppendColumnBuffer(
                                    ff,
                                    varFd,
                                    0,
                                    varMapSize,
                                    false,
                                    mapMemTag
                            ) : 0;

                            long auxRecSize = driver.auxRowsToBytes(1);
                            DedupColumnCommitAddresses.setColAddressValues(addr, auxMappedAddress - columnTop * auxRecSize, varMappedAddress, varMapSize);

                            MemoryCR oooVarCol = oooColumns.get(getPrimaryColumnIndex(i));
                            final long oooVarColAddress = oooVarCol.addressOf(0);
                            final long oooVarColSize = oooVarCol.addressHi() - oooVarColAddress;
                            final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(i)).addressOf(0);

                            DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            DedupColumnCommitAddresses.setReservedValuesSet1(
                                    addr,
                                    auxMappedAddress,
                                    auxMapSize,
                                    auxFd
                            );
                            DedupColumnCommitAddresses.setReservedValuesSet2(
                                    addr,
                                    varMappedAddress,
                                    varFd
                            );
                        }
                    }
                    dedupColumnIndex++;
                } // if (columnType > 0 && metadata.isDedupKey(i) && i != metadata.getTimestampIndex())
            }

            return Vect.mergeDedupTimestampWithLongIndexIntKeys(
                    srcTimestampAddr,
                    mergeDataLo,
                    mergeDataHi,
                    sortedTimestampsAddr,
                    mergeOOOLo,
                    mergeOOOHi,
                    tempIndexAddr,
                    dedupCommitAddresses.getColumnCount(),
                    DedupColumnCommitAddresses.getAddress(dedupColSinkAddr)
            );
        } finally {
            for (int i = 0, n = dedupCommitAddresses.getColumnCount(); i < n; i++) {
                final long mappedAddress = DedupColumnCommitAddresses.getColReserved1(dedupColSinkAddr, i);
                final long mappedAddressSize = DedupColumnCommitAddresses.getColReserved2(dedupColSinkAddr, i);
                if (mappedAddressSize > 0) {
                    TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, 0, mappedAddressSize, mapMemTag);
                    final long fd = DedupColumnCommitAddresses.getColReserved3(dedupColSinkAddr, i);
                    ff.close(fd);
                }

                final long varMappedAddress = DedupColumnCommitAddresses.getColReserved4(dedupColSinkAddr, i);
                if (varMappedAddress > 0) {
                    final long varMappedLength = DedupColumnCommitAddresses.getColVarDataLen(dedupColSinkAddr, i);
                    TableUtils.mapAppendColumnBufferRelease(ff, varMappedAddress, 0, varMappedLength, mapMemTag);
                    final long varFd = DedupColumnCommitAddresses.getColReserved5(dedupColSinkAddr, i);
                    ff.close(varFd);
                }
            }
        }
    }

    // returns packed long: (numOutputRowGroups << 32) | (duplicateCount & 0xFFFFFFFFL)
    private static long mergeRowGroup(
            PartitionDescriptor chunkDescriptor,
            PartitionUpdater partitionUpdater,
            DirectIntList parquetColumns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            ParquetMetaPartitionDecoder decoder,
            RowGroupBuffers rowGroupBuffers,
            int rowGroupIndex,
            int timestampIndex,
            long partitionTimestamp,
            long mergeRangeLo,
            long mergeRangeHi,
            TableRecordMetadata tableWriterMetadata,
            long dedupColSinkAddr,
            int maxRowGroupSize,
            int metadataPosition,
            LongList mergeDstBufs,
            IntList tableToParquetIdx,
            LongList nullBufs,
            LongList srcPtrs,
            IntList activeToDecodeIdx,
            IntList activeColIndices
    ) {
        // Build the decode list: only columns present in the parquet file.
        // Also build activeToDecodeIdx mapping: for each active column position,
        // store the decode buffer index (-1 if column is missing from parquet).
        parquetColumns.clear();
        int timestampColumnChunkIndex = -1;
        final int columnCount = tableWriterMetadata.getColumnCount();
        int activeColCount = 0;
        int decodeColCount = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = tableWriterMetadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            int parquetIdx = tableToParquetIdx.getQuick(columnIndex);
            if (parquetIdx >= 0) {
                if (columnIndex == timestampIndex) {
                    timestampColumnChunkIndex = decodeColCount;
                }
                parquetColumns.add(parquetIdx);
                parquetColumns.add(columnType);
                activeToDecodeIdx.setQuick(activeColCount, decodeColCount);
                decodeColCount++;
            } else {
                activeToDecodeIdx.setQuick(activeColCount, -1);
            }
            activeColIndices.setQuick(activeColCount, columnIndex);
            activeColCount++;
        }

        assert decoder.metadata().getRowGroupSize(rowGroupIndex) <= Integer.MAX_VALUE;
        final int rowGroupSize = (int) decoder.metadata().getRowGroupSize(rowGroupIndex);
        decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0, rowGroupSize);

        assert timestampColumnChunkIndex > -1;
        final long timestampDataPtr = rowGroupBuffers.getChunkDataPtr(timestampColumnChunkIndex);
        assert timestampDataPtr != 0;
        long mergeBatchRowCount = mergeRangeHi - mergeRangeLo + 1;
        long mergeRowCount = mergeBatchRowCount + rowGroupSize;
        long duplicateCount = 0;

        long timestampMergeIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
        long timestampMergeIndexAddr;
        if (!tableWriter.isCommitDedupMode()) {
            timestampMergeIndexAddr = createMergeIndex(
                    timestampDataPtr,
                    sortedTimestampsAddr,
                    0,
                    rowGroupSize - 1,
                    mergeRangeLo,
                    mergeRangeHi,
                    timestampMergeIndexSize
            );
        } else {
            final DedupColumnCommitAddresses dedupCommitAddresses = tableWriter.getDedupCommitAddresses();
            final long dedupRows;
            timestampMergeIndexAddr = Unsafe.malloc(timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            try {
                if (dedupCommitAddresses == null || dedupCommitAddresses.getColumnCount() == 0) {
                    dedupRows = Vect.mergeDedupTimestampWithLongIndexAsc(
                            timestampDataPtr,
                            0,
                            rowGroupSize - 1,
                            sortedTimestampsAddr,
                            mergeRangeLo,
                            mergeRangeHi,
                            timestampMergeIndexAddr
                    );
                } else {
                    int dedupColumnIndex = 0;
                    dedupCommitAddresses.clear(dedupColSinkAddr);
                    for (int ai = 0; ai < activeColCount; ai++) {
                        int columnIndex = activeColIndices.getQuick(ai);
                        int columnType = tableWriterMetadata.getColumnType(columnIndex);
                        int decodeIdx = activeToDecodeIdx.getQuick(ai);
                        assert columnIndex >= 0;
                        assert columnType >= 0;
                        if (tableWriterMetadata.isDedupKey(columnIndex) && columnIndex != timestampIndex) {
                            final int columnSize = !ColumnType.isVarSize(columnType) ? ColumnType.sizeOf(columnType) : -1;
                            final long columnTop = tableWriter.getColumnTop(partitionTimestamp, columnIndex, rowGroupSize);
                            long addr = DedupColumnCommitAddresses.setColValues(
                                    dedupColSinkAddr,
                                    dedupColumnIndex++,
                                    columnType,
                                    columnSize,
                                    columnTop
                            );
                            if (columnSize > 0) {
                                if (decodeIdx >= 0) {
                                    DedupColumnCommitAddresses.setColAddressValues(addr, rowGroupBuffers.getChunkDataPtr(decodeIdx));
                                } else {
                                    // Column missing from parquet (ADD COLUMN after partition
                                    // was created).  columnTop == rowGroupSize, so the native
                                    // dedup code treats all parquet values as NULL and will
                                    // not dereference the data pointer.
                                    assert columnTop == rowGroupSize : "missing column must have columnTop == rowGroupSize";
                                    DedupColumnCommitAddresses.setColAddressValues(addr, 0);
                                }
                                final long oooColAddress = oooColumns.get(getPrimaryColumnIndex(columnIndex)).addressOf(0);
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooColAddress);
                            } else {
                                if (decodeIdx >= 0) {
                                    DedupColumnCommitAddresses.setColAddressValues(
                                            addr,
                                            rowGroupBuffers.getChunkAuxPtr(decodeIdx),
                                            rowGroupBuffers.getChunkDataPtr(decodeIdx),
                                            rowGroupBuffers.getChunkDataSize(decodeIdx)
                                    );
                                } else {
                                    assert columnTop == rowGroupSize : "missing column must have columnTop == rowGroupSize";
                                    DedupColumnCommitAddresses.setColAddressValues(addr, 0, 0, 0);
                                }
                                MemoryCR oooVarCol = oooColumns.get(getPrimaryColumnIndex(columnIndex));
                                final long oooVarColAddress = oooVarCol.addressOf(0);
                                final long oooVarColSize = oooVarCol.addressHi() - oooVarColAddress;
                                final long oooAuxColAddress = oooColumns.get(getSecondaryColumnIndex(columnIndex)).addressOf(0);
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, oooAuxColAddress, oooVarColAddress, oooVarColSize);
                            }
                        }
                    }

                    dedupRows = Vect.mergeDedupTimestampWithLongIndexIntKeys(
                            timestampDataPtr,
                            0,
                            rowGroupSize - 1,
                            sortedTimestampsAddr,
                            mergeRangeLo,
                            mergeRangeHi,
                            timestampMergeIndexAddr,
                            dedupCommitAddresses.getColumnCount(),
                            DedupColumnCommitAddresses.getAddress(dedupColSinkAddr)
                    );
                }

                timestampMergeIndexAddr = Unsafe.realloc(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES,
                        MemoryTag.NATIVE_O3
                );
            } catch (Throwable e) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                throw e;
            }

            timestampMergeIndexSize = dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES;
            duplicateCount = mergeRowCount - dedupRows;
            if (duplicateCount > 0) {
                tableWriter.addDedupRowsRemoved(duplicateCount);
            }
            mergeRowCount = dedupRows;
        }

        assert timestampMergeIndexAddr != 0;

        // Even-split: when totalRows > 1.5x maxRowGroupSize, split into
        // ceil(totalRows / maxChunkTarget) chunks so that no chunk exceeds
        // maxChunkTarget = maxRowGroupSize + maxRowGroupSize / 2.
        // Integer division of maxRowGroupSize / 2 is intentional: the target
        // must be representable exactly in integer arithmetic to avoid off-by-one
        // overflows when distributing remainder rows across chunks.
        final long maxChunkTarget = (long) maxRowGroupSize + maxRowGroupSize / 2;
        int numChunks;
        if (mergeRowCount > maxChunkTarget) {
            numChunks = (int) ((mergeRowCount + maxChunkTarget - 1) / maxChunkTarget);
        } else {
            numChunks = 1;
        }
        final long maxChunkSize = (mergeRowCount + numChunks - 1) / numChunks;

        // Re-zero per-row-group buffers. The getters already zero them for the
        // first call, but mergeRowGroup is called in a loop (once per MERGE action),
        // so subsequent calls need stale values from the previous row group cleared.
        nullBufs.fill(0, activeColCount * 4, 0);
        srcPtrs.fill(0, activeColCount * 2, 0);

        try {
            // Phase 1: Ensure destination buffers in mergeDstBufs are large enough
            // for this merge. Grow if needed; reuse if already sufficient.
            // Also set up null source buffers for column-top and missing columns.
            for (int ai = 0; ai < activeColCount; ai++) {
                int columnIndex = activeColIndices.getQuick(ai);
                int columnType = tableWriterMetadata.getColumnType(columnIndex);
                int bi4 = ai * 4;
                int bi2 = ai * 2;
                int decodeIdx = activeToDecodeIdx.getQuick(ai);

                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver ctd = ColumnType.getDriver(columnType);
                    long columnDataPtr = decodeIdx >= 0 ? rowGroupBuffers.getChunkDataPtr(decodeIdx) : 0;
                    long columnAuxPtr = decodeIdx >= 0 ? rowGroupBuffers.getChunkAuxPtr(decodeIdx) : 0;

                    if (columnAuxPtr == 0) {
                        // Column top or missing from parquet: create null source buffers.
                        long nullAuxSize = ctd.getAuxVectorSize(rowGroupSize);
                        long nullAuxBuf = Unsafe.malloc(nullAuxSize, MemoryTag.NATIVE_O3);
                        ctd.setFullAuxVectorNull(nullAuxBuf, rowGroupSize);
                        columnAuxPtr = nullAuxBuf;
                        nullBufs.setQuick(bi4, nullAuxBuf);
                        nullBufs.setQuick(bi4 + 1, nullAuxSize);

                        long nullDataSize = ctd.getDataVectorSizeAt(nullAuxBuf, rowGroupSize - 1);
                        if (nullDataSize > 0) {
                            long nullDataBuf = Unsafe.malloc(nullDataSize, MemoryTag.NATIVE_O3);
                            ctd.setDataVectorEntriesToNull(nullDataBuf, rowGroupSize);
                            columnDataPtr = nullDataBuf;
                            nullBufs.setQuick(bi4 + 2, nullDataBuf);
                            nullBufs.setQuick(bi4 + 3, nullDataSize);
                        }
                    }
                    srcPtrs.setQuick(bi2, columnDataPtr);
                    srcPtrs.setQuick(bi2 + 1, columnAuxPtr);

                    final int columnOffset = getPrimaryColumnIndex(columnIndex);
                    final long srcOooFixAddr = oooColumns.getQuick(columnOffset + 1).addressOf(0);

                    // Aux buffer: bounded by maxChunkSize across all merges.
                    long neededAuxSize = ctd.getAuxVectorSize(maxChunkSize);
                    if (neededAuxSize > mergeDstBufs.getQuick(bi4 + 3)) {
                        if (mergeDstBufs.getQuick(bi4 + 2) != 0) {
                            Unsafe.free(mergeDstBufs.getQuick(bi4 + 2), mergeDstBufs.getQuick(bi4 + 3), MemoryTag.NATIVE_O3);
                            mergeDstBufs.setQuick(bi4 + 2, 0);
                            mergeDstBufs.setQuick(bi4 + 3, 0);
                        }
                        mergeDstBufs.setQuick(bi4 + 2, Unsafe.malloc(neededAuxSize, MemoryTag.NATIVE_O3));
                        mergeDstBufs.setQuick(bi4 + 3, neededAuxSize);
                    }

                    // Data buffer: overestimate varies per merge, grow if needed.
                    long neededDataSize = ctd.getDataVectorSize(srcOooFixAddr, mergeRangeLo, mergeRangeHi)
                            + ctd.getDataVectorSizeAt(columnAuxPtr, rowGroupSize - 1);
                    if (neededDataSize > mergeDstBufs.getQuick(bi4 + 1)) {
                        if (mergeDstBufs.getQuick(bi4) != 0) {
                            Unsafe.free(mergeDstBufs.getQuick(bi4), mergeDstBufs.getQuick(bi4 + 1), MemoryTag.NATIVE_O3);
                            mergeDstBufs.setQuick(bi4, 0);
                            mergeDstBufs.setQuick(bi4 + 1, 0);
                        }
                        mergeDstBufs.setQuick(bi4, Unsafe.malloc(neededDataSize, MemoryTag.NATIVE_O3));
                        mergeDstBufs.setQuick(bi4 + 1, neededDataSize);
                    }
                } else {
                    long columnDataPtr = decodeIdx >= 0 ? rowGroupBuffers.getChunkDataPtr(decodeIdx) : 0;
                    if (columnDataPtr == 0) {
                        // Column top or missing from parquet: create null source buffer.
                        long nullFixSize = (long) rowGroupSize * ColumnType.sizeOf(columnType);
                        long nullFixBuf = Unsafe.malloc(nullFixSize, MemoryTag.NATIVE_O3);
                        TableUtils.setNull(columnType, nullFixBuf, rowGroupSize);
                        columnDataPtr = nullFixBuf;
                        nullBufs.setQuick(bi4, nullFixBuf);
                        nullBufs.setQuick(bi4 + 1, nullFixSize);
                    }
                    srcPtrs.setQuick(bi2, columnDataPtr);

                    // Fixed-size buffer: bounded by maxChunkSize across all merges.
                    long neededFixSize = maxChunkSize * ColumnType.sizeOf(columnType);
                    if (neededFixSize > mergeDstBufs.getQuick(bi4 + 1)) {
                        if (mergeDstBufs.getQuick(bi4) != 0) {
                            Unsafe.free(mergeDstBufs.getQuick(bi4), mergeDstBufs.getQuick(bi4 + 1), MemoryTag.NATIVE_O3);
                            mergeDstBufs.setQuick(bi4, 0);
                            mergeDstBufs.setQuick(bi4 + 1, 0);
                        }
                        mergeDstBufs.setQuick(bi4, Unsafe.malloc(neededFixSize, MemoryTag.NATIVE_O3));
                        mergeDstBufs.setQuick(bi4 + 1, neededFixSize);
                    }
                }
            }

            // Phase 2: Process chunks, reusing destination buffers from mergeDstBufs.
            // Even distribution: first (mergeRowCount % numChunks) chunks get one extra row.
            final String tableName = tableWriter.getTableToken().getTableName();
            long baseChunkSize = mergeRowCount / numChunks;
            long extraRows = mergeRowCount % numChunks;
            long chunkLo = 0;
            for (int chunk = 0; chunk < numChunks; chunk++) {
                long chunkRowCount = baseChunkSize + (chunk < extraRows ? 1 : 0);
                long chunkMergeIndexAddr = timestampMergeIndexAddr + chunkLo * TIMESTAMP_MERGE_ENTRY_BYTES;

                chunkDescriptor.of(tableName, chunkRowCount, timestampIndex);

                for (int ai = 0; ai < activeColCount; ai++) {
                    int columnIndex = activeColIndices.getQuick(ai);
                    int columnType = tableWriterMetadata.getColumnType(columnIndex);
                    assert columnIndex >= 0;
                    assert columnType >= 0;
                    final String columnName = tableWriterMetadata.getColumnName(columnIndex);
                    final int columnId = tableWriterMetadata.getColumnMetadata(columnIndex).getWriterIndex();
                    final int parquetEncodingConfig = tableWriterMetadata.getColumnMetadata(columnIndex).getParquetEncodingConfig();

                    final boolean notTheTimestamp = columnIndex != timestampIndex;
                    final int columnOffset = getPrimaryColumnIndex(columnIndex);
                    int bi4 = ai * 4;
                    int bi2 = ai * 2;

                    if (ColumnType.isVarSize(columnType)) {
                        final long srcOooFixAddr = oooColumns.getQuick(columnOffset + 1).addressOf(0);
                        final long srcOooVarAddr = oooColumns.getQuick(columnOffset).addressOf(0);

                        long dstDataAddr = mergeDstBufs.getQuick(bi4);
                        long dstDataSize = mergeDstBufs.getQuick(bi4 + 1);
                        long dstAuxAddr = mergeDstBufs.getQuick(bi4 + 2);
                        long dstAuxSize = mergeDstBufs.getQuick(bi4 + 3);

                        // Note: dstDataSize/dstAuxSize are buffer capacities, not exact used sizes.
                        // The Rust encoder bounds all access by chunkRowCount, not by buffer size:
                        // aux is sliced to [0..chunkRowCount], data is read via offsets within that slice.

                        O3CopyJob.mergeCopy(
                                columnType,
                                chunkMergeIndexAddr,
                                chunkRowCount,
                                srcPtrs.getQuick(bi2 + 1), // columnAuxPtr
                                srcPtrs.getQuick(bi2),      // columnDataPtr
                                srcOooFixAddr,
                                srcOooVarAddr,
                                dstAuxAddr,
                                dstDataAddr,
                                0
                        );

                        chunkDescriptor.addColumn(
                                columnName,
                                columnType,
                                columnId,
                                0,
                                dstDataAddr,
                                dstDataSize,
                                dstAuxAddr,
                                dstAuxSize,
                                0,
                                0,
                                parquetEncodingConfig
                        );
                    } else {
                        final long srcOooFixAddr = oooColumns.getQuick(columnOffset).addressOf(0);
                        long dstFixAddr = mergeDstBufs.getQuick(bi4);
                        long dstFixSize = mergeDstBufs.getQuick(bi4 + 1);

                        // Note: dstFixSize is the buffer capacity, not exact used size.
                        // The Rust encoder slices data to [0..chunkRowCount] elements, ignoring extra capacity.

                        O3CopyJob.mergeCopy(
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                chunkMergeIndexAddr,
                                chunkRowCount,
                                srcPtrs.getQuick(bi2), // columnDataPtr
                                0,
                                srcOooFixAddr,
                                0,
                                dstFixAddr,
                                0,
                                0
                        );

                        if (ColumnType.isSymbol(columnType)) {
                            final MapWriter symbolMapWriter = tableWriter.getSymbolMapWriter(columnIndex);
                            final MemoryR offsetsMem = symbolMapWriter.getSymbolOffsetsMemory();
                            final MemoryR valuesMem = symbolMapWriter.getSymbolValuesMemory();

                            final int symbolCount = symbolMapWriter.getSymbolCount();
                            final long offset = SymbolMapWriter.keyToOffset(symbolCount);
                            assert offset - SymbolMapWriter.HEADER_SIZE <= offsetsMem.size();
                            final long valuesMemSize = offsetsMem.getLong(offset);
                            assert valuesMemSize <= valuesMem.size();

                            // High bit = no-null hint for def level encoding, not schema Repetition.
                            int encodeColumnType = columnType;
                            if (!symbolMapWriter.getNullFlag()) {
                                encodeColumnType |= PARQUET_SYMBOL_NOT_NULL_HINT;
                            }
                            chunkDescriptor.addColumn(
                                    columnName,
                                    encodeColumnType,
                                    columnId,
                                    0,
                                    dstFixAddr,
                                    dstFixSize,
                                    valuesMem.addressOf(0),
                                    valuesMemSize,
                                    // Skip header. Pass element count, not byte size.
                                    offsetsMem.addressOf(SymbolMapWriter.HEADER_SIZE),
                                    symbolCount,
                                    parquetEncodingConfig
                            );
                        } else {
                            chunkDescriptor.addColumn(
                                    columnName,
                                    columnType,
                                    columnId,
                                    0,
                                    dstFixAddr,
                                    dstFixSize,
                                    0,
                                    0,
                                    0,
                                    0,
                                    parquetEncodingConfig
                            );
                        }
                    }
                }

                if (chunk == 0) {
                    partitionUpdater.updateRowGroup(rowGroupIndex, chunkDescriptor);
                } else {
                    partitionUpdater.addRowGroup(metadataPosition + chunk, chunkDescriptor);
                }
                chunkLo += chunkRowCount;
            }
        } finally {
            // Free only per-merge null source buffers. Destination buffers in
            // mergeDstBufs are owned by the caller and freed after all merges.
            for (int ai = 0; ai < activeColCount; ai++) {
                int bi4 = ai * 4;
                for (int slot = 0; slot < 4; slot += 2) {
                    if (nullBufs.getQuick(bi4 + slot) != 0) {
                        Unsafe.free(nullBufs.getQuick(bi4 + slot), nullBufs.getQuick(bi4 + slot + 1), MemoryTag.NATIVE_O3);
                    }
                }
            }
            Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
        }

        return ((long) numChunks << 32) | (duplicateCount & 0xFFFFFFFFL);
    }

    private static void publishOpenColumnTaskContended(
            long cursor,
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int indexBlockCapacity,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        while (cursor == -2) {
            Os.pause();
            cursor = tableWriter.getO3OpenColumnPubSeq().next();
        }

        if (cursor > -1) {
            publishOpenColumnTaskHarmonized(
                    cursor,
                    openColumnMode,
                    pathToTable,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
                    txn,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeDataLo,
                    mergeDataHi,
                    mergeOOOLo,
                    mergeOOOHi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    indexBlockCapacity,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    activeFixFd,
                    activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
            );
        } else {
            O3OpenColumnJob.openColumn(
                    openColumnMode,
                    pathToTable,
                    columnName,
                    columnCounter,
                    partCounter,
                    columnType,
                    timestampMergeIndexAddr,
                    timestampMergeIndexSize,
                    srcOooFixAddr,
                    srcOooVarAddr,
                    srcOooLo,
                    srcOooHi,
                    srcOooMax,
                    oooTimestampMin,
                    partitionTimestamp,
                    oldPartitionTimestamp,
                    srcDataTop,
                    srcDataMax,
                    srcNameTxn,
                    txn,
                    prefixType,
                    prefixLo,
                    prefixHi,
                    mergeType,
                    mergeOOOLo,
                    mergeOOOHi,
                    mergeDataLo,
                    mergeDataHi,
                    suffixType,
                    suffixLo,
                    suffixHi,
                    srcTimestampFd,
                    srcTimestampAddr,
                    srcTimestampSize,
                    indexBlockCapacity,
                    activeFixFd,
                    activeVarFd,
                    srcDataNewPartitionSize,
                    srcDataOldPartitionSize,
                    o3SplitPartitionSize,
                    tableWriter,
                    indexWriter,
                    partitionUpdateSinkAddr,
                    columnIndex,
                    columnNameTxn
            );
        }
    }

    private static void publishOpenColumnTaskHarmonized(
            long cursor,
            int openColumnMode,
            Path pathToTable,
            CharSequence columnName,
            AtomicInteger columnCounter,
            AtomicInteger partCounter,
            int columnType,
            long timestampMergeIndexAddr,
            long timestampMergeIndexSize,
            long srcOooFixAddr,
            long srcOooVarAddr,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            long srcDataTop,
            long srcDataMax,
            long srcNameTxn,
            long txn,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            int indexBlockCapacity,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            long activeFixFd,
            long activeVarFd,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            BitmapIndexWriter indexWriter,
            long partitionUpdateSinkAddr,
            int columnIndex,
            long columnNameTxn
    ) {
        final O3OpenColumnTask openColumnTask = tableWriter.getO3OpenColumnQueue().get(cursor);
        openColumnTask.of(
                openColumnMode,
                pathToTable,
                columnName,
                columnCounter,
                partCounter,
                columnType,
                timestampMergeIndexAddr,
                timestampMergeIndexSize,
                srcOooFixAddr,
                srcOooVarAddr,
                srcOooLo,
                srcOooHi,
                srcOooMax,
                oooTimestampMin,
                partitionTimestamp,
                oldPartitionTimestamp,
                srcDataTop,
                srcDataMax,
                srcNameTxn,
                txn,
                prefixType,
                prefixLo,
                prefixHi,
                mergeType,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                suffixType,
                suffixLo,
                suffixHi,
                srcTimestampFd,
                srcTimestampAddr,
                srcTimestampSize,
                indexBlockCapacity,
                activeFixFd,
                activeVarFd,
                srcDataNewPartitionSize,
                srcDataOldPartitionSize,
                o3SplitPartitionSize,
                tableWriter,
                indexWriter,
                partitionUpdateSinkAddr,
                columnIndex,
                columnNameTxn
        );
        tableWriter.getO3OpenColumnPubSeq().done(cursor);
    }

    private static void publishOpenColumnTasks(
            long txn,
            ObjList<MemoryMA> columns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            Path pathToTable,
            long srcOooLo,
            long srcOooHi,
            long srcOooMax,
            long oooTimestampMin,
            long partitionTimestamp,
            long oldPartitionTimestamp,
            int prefixType,
            long prefixLo,
            long prefixHi,
            int mergeType,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            int suffixType,
            long suffixLo,
            long suffixHi,
            long srcDataMax,
            long srcNameTxn,
            int openColumnMode,
            long srcTimestampFd,
            long srcTimestampAddr,
            long srcTimestampSize,
            int timestampIndex,
            TimestampDriver timestampDriver,
            long sortedTimestampsAddr,
            long srcDataNewPartitionSize,
            long srcDataOldPartitionSize,
            long o3SplitPartitionSize,
            TableWriter tableWriter,
            AtomicInteger columnCounter,
            O3Basket o3Basket,
            long partitionUpdateSinkAddr,
            long dedupColSinkAddr
    ) {
        // Number of rows to insert from the O3 segment into this partition.
        final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

        long timestampMergeIndexAddr = 0;
        long timestampMergeIndexSize = 0;
        final TableRecordMetadata metadata = tableWriter.getMetadata();
        if (mergeType == O3_BLOCK_MERGE) {
            long mergeRowCount = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
            long tempIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
            assert tempIndexSize > 0; // avoid SIGSEGV

            try {
                if (tableWriter.isCommitPlainInsert()) {
                    timestampMergeIndexSize = tempIndexSize;

                    timestampMergeIndexAddr = createMergeIndex(
                            srcTimestampAddr,
                            sortedTimestampsAddr,
                            mergeDataLo,
                            mergeDataHi,
                            mergeOOOLo,
                            mergeOOOHi,
                            timestampMergeIndexSize
                    );
                } else if (tableWriter.isCommitDedupMode()) {
                    final long tempIndexAddr = Unsafe.malloc(tempIndexSize, MemoryTag.NATIVE_O3);
                    final DedupColumnCommitAddresses dedupCommitAddresses = tableWriter.getDedupCommitAddresses();
                    final Path tempTablePath = Path.getThreadLocal(tableWriter.getConfiguration().getDbRoot()).concat(tableWriter.getTableToken());

                    final long dedupRows = getDedupRows(
                            oldPartitionTimestamp,
                            srcNameTxn,
                            srcTimestampAddr,
                            mergeDataLo,
                            mergeDataHi,
                            sortedTimestampsAddr,
                            mergeOOOLo,
                            mergeOOOHi,
                            oooColumns,
                            dedupCommitAddresses,
                            dedupColSinkAddr,
                            tableWriter,
                            tempTablePath,
                            tempIndexAddr
                    );
                    timestampMergeIndexSize = dedupRows * TIMESTAMP_MERGE_ENTRY_BYTES;
                    timestampMergeIndexAddr = Unsafe.realloc(tempIndexAddr, tempIndexSize, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                    final long duplicateCount = mergeRowCount - dedupRows;
                    boolean appendOnly = false;
                    if (duplicateCount > 0) {
                        tableWriter.addDedupRowsRemoved(duplicateCount);
                        if (duplicateCount == mergeOOOHi - mergeOOOLo + 1 && prefixType != O3_BLOCK_O3) {

                            // All the rows are duplicates, the commit does not add any new lines.
                            // Check non-key columns if they are exactly the same as the rows they replace
                            if (tableWriter.checkDedupCommitIdenticalToPartition(
                                    oldPartitionTimestamp,
                                    srcNameTxn,
                                    srcDataOldPartitionSize,
                                    mergeDataLo,
                                    mergeDataHi,
                                    mergeOOOLo,
                                    mergeOOOHi,
                                    timestampMergeIndexAddr,
                                    dedupRows
                            )) {

                                if (suffixType != O3_BLOCK_O3) {
                                    LOG.info().$("deduplication resulted in noop [table=").$(tableWriter.getTableToken())
                                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                                            .I$();

                                    timestampMergeIndexAddr = Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);

                                    removePhantomPartitionDir(pathToTable, tableWriter, partitionTimestamp, txn);

                                    // nothing to do, skip the partition
                                    updatePartition(
                                            tableWriter.getFilesFacade(),
                                            srcTimestampAddr,
                                            srcTimestampSize,
                                            srcTimestampFd,
                                            tableWriter,
                                            partitionUpdateSinkAddr,
                                            oldPartitionTimestamp,
                                            Long.MAX_VALUE,
                                            -1,
                                            srcDataOldPartitionSize,
                                            0
                                    );

                                    return;
                                } else {
                                    // suffixType == O3_BLOCK_O3
                                    // we don't need to do the merge, but we need to append the suffix
                                    appendOnly = true;
                                }


                            }
                        }
                    } else if (suffixType != O3_BLOCK_DATA) {
                        // No duplicates.
                        // Maybe it's append only, if the OOO data "touches" the partition data then we
                        // do not need to merge, append is good enough
                        long dataMergeMaxTimestamp = Unsafe.getLong(srcTimestampAddr + mergeDataHi * Long.BYTES);
                        appendOnly = oooTimestampMin >= dataMergeMaxTimestamp;
                    }

                    if (appendOnly) {
                        mergeType = O3_BLOCK_NONE;
                        mergeDataHi = -1;
                        mergeDataLo = 0;
                        mergeOOOHi = -1;
                        mergeOOOLo = 0;

                        if (duplicateCount > 0) {
                            // Append procs may ignore suffixLo when appending using srcOooLo instead.
                            // Adjust suffixLo to match the srcOooLo.
                            srcOooLo = suffixLo;
                        } else {
                            suffixType = O3_BLOCK_O3;
                            suffixLo = srcOooLo;
                            suffixHi = srcOooHi;
                        }

                        if (o3SplitPartitionSize > 0) {
                            LOG.info().$("dedup resulted in no merge, undo partition split [table=")
                                    .$(tableWriter.getTableToken())
                                    .$(", partition=").$ts(timestampDriver, oldPartitionTimestamp)
                                    .$(", split=").$ts(timestampDriver, partitionTimestamp)
                                    .I$();
                            partitionTimestamp = oldPartitionTimestamp;
                            srcDataNewPartitionSize += o3SplitPartitionSize;
                            o3SplitPartitionSize = 0;
                        }

                        // No merge anymore, free the merge index
                        timestampMergeIndexAddr = Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);

                        removePhantomPartitionDir(pathToTable, tableWriter, partitionTimestamp, txn);

                        prefixType = O3_BLOCK_DATA;
                        prefixLo = 0;
                        prefixHi = srcDataMax - 1;

                        if (openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                            openColumnMode = OPEN_LAST_PARTITION_FOR_APPEND;
                        } else if (openColumnMode == OPEN_MID_PARTITION_FOR_MERGE) {
                            openColumnMode = OPEN_MID_PARTITION_FOR_APPEND;
                        } else {
                            assert false : "unexpected open column mode: " + openColumnMode;
                        }
                    }

                    // we could be de-duping a split partition
                    // in which case only its size will be affected
                    if (o3SplitPartitionSize > 0) {
                        o3SplitPartitionSize -= duplicateCount;
                    } else {
                        srcDataNewPartitionSize -= duplicateCount;
                    }
                    LOG.info()
                            .$("dedup row reduction [table=").$(tableWriter.getTableToken())
                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                            .$(", duplicateCount=").$(duplicateCount)
                            .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                            .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                            .$(", o3SplitPartitionSize=").$(srcDataOldPartitionSize)
                            .$(", mergeDataLo=").$(mergeDataLo)
                            .$(", mergeDataHi=").$(mergeDataHi)
                            .$(", mergeOOOLo=").$(mergeOOOLo)
                            .$(", mergeOOOHi=").$(mergeOOOHi)
                            .I$();
                } else if (tableWriter.isCommitReplaceMode()) {
                    // merge range is replaced by new data.
                    // Merge row count is the count of the new rows, compensated by 1 row that is start of the range
                    // and 1 row that is in the end of the range.
                    mergeType = O3_BLOCK_O3;
                } else {
                    throw new IllegalStateException("commit mode not supported");
                }
            } catch (Throwable e) {
                tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                LOG.critical().$("open column error [table=").$(tableWriter.getTableToken())
                        .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                        .$(", e=").$(e)
                        .I$();

                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
                O3CopyJob.closeColumnIdleQuick(
                        0,
                        0,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
                throw e;
            }
        }

        tableWriter.addPhysicallyWrittenRows(
                isOpenColumnModeForAppend(openColumnMode)
                        ? srcOooBatchRowSize
                        : o3SplitPartitionSize == 0 ? srcDataNewPartitionSize : o3SplitPartitionSize
        );

        final int columnCount = metadata.getColumnCount();
        columnCounter.set(compressColumnCount(metadata));
        int columnsInFlight = columnCount;
        if (openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE || openColumnMode == OPEN_MID_PARTITION_FOR_MERGE) {
            // Partition will be re-written. Jobs will set new column top values but by default they are 0
            Vect.memset(partitionUpdateSinkAddr + PARTITION_SINK_COL_TOP_OFFSET, (long) Long.BYTES * columnCount, 0);
        }

        try {
            for (int i = 0; i < columnCount; i++) {
                final int columnType = metadata.getColumnType(i);
                if (columnType < 0) {
                    continue;
                }
                final int colOffset = getPrimaryColumnIndex(i);
                final boolean notTheTimestamp = i != timestampIndex;
                final MemoryCR oooMem1 = oooColumns.getQuick(colOffset);
                final MemoryCR oooMem2 = oooColumns.getQuick(colOffset + 1);
                final MemoryMA mem1 = columns.getQuick(colOffset);
                final MemoryMA mem2 = columns.getQuick(colOffset + 1);
                final long activeFixFd;
                final long activeVarFd;
                final long srcDataTop;
                final long srcOooFixAddr;
                final long srcOooVarAddr;
                if (!ColumnType.isVarSize(columnType)) {
                    activeFixFd = mem1.getFd();
                    activeVarFd = 0;
                    srcOooFixAddr = (i == timestampIndex) ? sortedTimestampsAddr : oooMem1.addressOf(0);
                    srcOooVarAddr = 0;
                } else {
                    activeFixFd = mem2.getFd();
                    activeVarFd = mem1.getFd();
                    srcOooFixAddr = oooMem2.addressOf(0);
                    srcOooVarAddr = oooMem1.addressOf(0);
                }

                final CharSequence columnName = metadata.getColumnName(i);
                final boolean isIndexed = metadata.isColumnIndexed(i);
                final int indexBlockCapacity = isIndexed ? metadata.getIndexValueBlockCapacity(i) : -1;
                if (openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND || openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                    srcDataTop = tableWriter.getColumnTop(i);
                } else {
                    srcDataTop = -1; // column open job will have to find out if top exists and its value
                }

                final BitmapIndexWriter indexWriter;
                if (isIndexed) {
                    indexWriter = o3Basket.nextIndexer();
                } else {
                    indexWriter = null;
                }

                try {
                    final long cursor = tableWriter.getO3OpenColumnPubSeq().next();
                    final long columnNameTxn = tableWriter.getColumnNameTxn(oldPartitionTimestamp, i);
                    if (cursor > -1) {
                        publishOpenColumnTaskHarmonized(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
                                txn,
                                prefixType,
                                prefixLo,
                                prefixHi,
                                mergeType,
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
                                suffixType,
                                suffixLo,
                                suffixHi,
                                indexBlockCapacity,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    } else {
                        publishOpenColumnTaskContended(
                                cursor,
                                openColumnMode,
                                pathToTable,
                                columnName,
                                columnCounter,
                                o3Basket.nextPartCounter(),
                                notTheTimestamp ? columnType : ColumnType.setDesignatedTimestampBit(columnType, true),
                                timestampMergeIndexAddr,
                                timestampMergeIndexSize,
                                srcOooFixAddr,
                                srcOooVarAddr,
                                srcOooLo,
                                srcOooHi,
                                srcOooMax,
                                oooTimestampMin,
                                partitionTimestamp,
                                oldPartitionTimestamp,
                                srcDataTop,
                                srcDataMax,
                                srcNameTxn,
                                txn,
                                prefixType,
                                prefixLo,
                                prefixHi,
                                mergeType,
                                mergeDataLo,
                                mergeDataHi,
                                mergeOOOLo,
                                mergeOOOHi,
                                suffixType,
                                suffixLo,
                                suffixHi,
                                srcTimestampFd,
                                srcTimestampAddr,
                                srcTimestampSize,
                                indexBlockCapacity,
                                activeFixFd,
                                activeVarFd,
                                srcDataNewPartitionSize,
                                srcDataOldPartitionSize,
                                o3SplitPartitionSize,
                                tableWriter,
                                indexWriter,
                                partitionUpdateSinkAddr,
                                i,
                                columnNameTxn
                        );
                    }
                } catch (Throwable e) {
                    tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(e));
                    LOG.critical().$("open column error [table=").$(tableWriter.getTableToken())
                            .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                            .$(", e=").$(e)
                            .I$();
                    columnsInFlight = i + 1;
                    throw e;
                }
            }
        } finally {
            final int delta = columnsInFlight - columnCount;
            LOG.debug().$("idle [delta=").$(delta).I$();
            if (delta < 0 && columnCounter.addAndGet(delta) == 0) {
                O3CopyJob.closeColumnIdleQuick(
                        timestampMergeIndexAddr,
                        timestampMergeIndexSize,
                        srcTimestampFd,
                        srcTimestampAddr,
                        srcTimestampSize,
                        tableWriter
                );
            }
        }
    }

    private static void regenerateParquetMetadata(FilesFacade ff, Path path, int partitionDirLen, long parquetFileSize, CairoConfiguration configuration) {
        path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
        long parquetFd = TableUtils.openRO(ff, path.$(), LOG);
        try {
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            long parquetMetaFd = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
            try {
                if (!ff.truncate(parquetMetaFd, 0)) {
                    throw CairoException.critical(ff.errno()).put("could not truncate _pm [path=").put(path).put(']');
                }
                long parquetMetaAllocator = Unsafe.getNativeAllocator(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
                ParquetMetadataWriter.generate(parquetMetaAllocator, Files.toOsFd(parquetFd), parquetFileSize, Files.toOsFd(parquetMetaFd));
            } finally {
                ff.close(parquetMetaFd);
            }
        } finally {
            ff.close(parquetFd);
            path.trimTo(partitionDirLen);
        }
    }

    private static void removePhantomPartitionDir(
            Path pathToTable,
            TableWriter tableWriter,
            long partitionTimestamp,
            long txn
    ) {
        // Remove empty partition dir to not create a partition that is not used but can be counted
        // by partition purging logic as a valid version
        Path path = Path.getThreadLocal(pathToTable);
        setPathForNativePartition(path, tableWriter.getTimestampType(), tableWriter.getPartitionBy(), partitionTimestamp, txn);
        FilesFacade ff = tableWriter.getConfiguration().getFilesFacade();
        if (!ff.rmdir(path)) {
            // This is not critical, the read error will be transient
            LOG.error().$("could not remove phantom partition dir, it may cause transient missing file read errors [errno=").$(ff.errno()).$(", path=").$(path).I$();
        }
    }

    private static void updateParquetIndexes(
            int partitionBy,
            long partitionTimestamp,
            TableWriter tableWriter,
            long srcNameTxn,
            O3Basket o3Basket,
            long newPartitionSize,
            long newParquetSize,
            long newParquetMetaFileSize,
            Path pathToTable,
            Path path,
            FilesFacade ff,
            ParquetMetaPartitionDecoder partitionDecoder,
            TableRecordMetadata tableWriterMetadata,
            DirectIntList parquetColumns,
            RowGroupBuffers rowGroupBuffers,
            boolean isRewrite
    ) {
        long parquetAddr = 0;
        long parquetMetaAddr = 0;
        try {
            // Map the _pm sidecar file from the same directory as the parquet file.
            int parquetNameLen = path.size();
            int partitionDirLen = parquetNameLen - TableUtils.PARQUET_PARTITION_NAME.length() - 1;
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
            parquetMetaAddr = TableUtils.mapRO(ff, path.$(), LOG, newParquetMetaFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);

            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
            parquetAddr = TableUtils.mapRO(ff, path.$(), LOG, newParquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            partitionDecoder.of(
                    parquetMetaAddr,
                    newParquetMetaFileSize,
                    parquetAddr,
                    newParquetSize,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER
            );

            LOG.info()
                    .$("parquet o3 done [table=").$(tableWriter.getTableToken())
                    .$(", partition=").$(partitionTimestamp)
                    .$(", rowGroups=").$(partitionDecoder.metadata().getRowGroupCount())
                    .$(", fileSize=").$size(newParquetSize)
                    .I$();

            path.of(pathToTable);
            setPathForNativePartition(
                    path,
                    tableWriterMetadata.getTimestampType(),
                    partitionBy,
                    partitionTimestamp,
                    srcNameTxn
            );
            final int pLen = path.size();

            BitmapIndexWriter indexWriter = null;
            final int columnCount = tableWriterMetadata.getColumnCount();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (tableWriterMetadata.getColumnType(columnIndex) == ColumnType.SYMBOL && tableWriterMetadata.isColumnIndexed(columnIndex)) {
                    final int indexBlockCapacity = tableWriterMetadata.getIndexValueBlockCapacity(columnIndex);
                    if (indexBlockCapacity < 0) {
                        continue;
                    }

                    final CharSequence columnName = tableWriterMetadata.getColumnName(columnIndex);
                    final long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);

                    long kFd = 0;
                    long vFd = 0;
                    try {
                        kFd = openRW(
                                ff,
                                BitmapIndexUtils.keyFileName(path.trimTo(pLen), columnName, columnNameTxn),
                                LOG,
                                tableWriter.getConfiguration().getWriterFileOpenOpts()
                        );
                        vFd = openRW(
                                ff,
                                BitmapIndexUtils.valueFileName(path.trimTo(pLen), columnName, columnNameTxn),
                                LOG,
                                tableWriter.getConfiguration().getWriterFileOpenOpts()
                        );
                    } catch (Throwable th) {
                        O3Utils.close(ff, kFd);
                        O3Utils.close(ff, vFd);
                        throw th;
                    }

                    if (indexWriter == null) {
                        indexWriter = o3Basket.nextIndexer();
                    }

                    try {
                        final ParquetMetaFileReader parquetMetadata = partitionDecoder.metadata();

                        int parquetColumnIndex = -1;
                        final int writerIndex = tableWriterMetadata.getColumnMetadata(columnIndex).getWriterIndex();
                        for (int idx = 0, cnt = parquetMetadata.getColumnCount(); idx < cnt; idx++) {
                            if (parquetMetadata.getColumnId(idx) == writerIndex) {
                                parquetColumnIndex = idx;
                                break;
                            }
                        }
                        if (parquetColumnIndex == -1) {
                            path.trimTo(pLen);
                            LOG.error().$("could not find symbol column for indexing in parquet, skipping [path=").$(path)
                                    .$(", columnIndex=").$(columnIndex)
                                    .I$();
                            continue;
                        }

                        indexWriter.of(tableWriter.getConfiguration(), kFd, vFd, true, indexBlockCapacity);
                        vFd = kFd = -1;

                        // In rewrite mode all columns exist in the new parquet file
                        // (the Rust encoder fills missing columns with NULLs),
                        // so the index must cover all rows from row 0.
                        final long columnTop = isRewrite ? 0 : tableWriter.columnVersionReader().getColumnTop(partitionTimestamp, columnIndex);
                        if (columnTop > -1 && newPartitionSize > columnTop) {
                            parquetColumns.clear();
                            parquetColumns.add(parquetColumnIndex);
                            parquetColumns.add(ColumnType.SYMBOL);

                            long rowCount = 0;
                            final int rowGroupCount = parquetMetadata.getRowGroupCount();
                            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                                assert parquetMetadata.getRowGroupSize(rowGroupIndex) <= Integer.MAX_VALUE;
                                final int rowGroupSize = (int) parquetMetadata.getRowGroupSize(rowGroupIndex);
                                if (rowCount + rowGroupSize <= columnTop) {
                                    rowCount += rowGroupSize;
                                    continue;
                                }

                                partitionDecoder.decodeRowGroup(
                                        rowGroupBuffers,
                                        parquetColumns,
                                        rowGroupIndex,
                                        (int) Math.max(0, columnTop - rowCount),
                                        rowGroupSize
                                );

                                long rowId = Math.max(rowCount, columnTop);
                                final long addr = rowGroupBuffers.getChunkDataPtr(0);
                                final long size = rowGroupBuffers.getChunkDataSize(0);
                                for (long p = addr, lim = addr + size; p < lim; p += 4, rowId++) {
                                    indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(p)), rowId);
                                }

                                rowCount += rowGroupSize;
                            }
                            indexWriter.setMaxValue(newPartitionSize - 1);
                        }

                        indexWriter.commit();
                    } finally {
                        Misc.free(indexWriter);
                        O3Utils.close(ff, kFd);
                        O3Utils.close(ff, vFd);
                    }
                }
            }
        } finally {
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, newParquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            if (parquetMetaAddr != 0) {
                ff.munmap(parquetMetaAddr, newParquetMetaFileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    private static void updatePartition(
            FilesFacade ff,
            long srcTimestampAddr,
            long srcTimestampSize,
            long srcTimestampFd,
            TableWriter tableWriter,
            long partitionUpdateSinkAddr,
            long partitionTimestamp,
            long timestampMin,
            long newPartitionSize,
            long oldPartitionSize,
            int partitionMutates
    ) {
        updatePartitionSink(partitionUpdateSinkAddr, partitionTimestamp, timestampMin, newPartitionSize, oldPartitionSize, partitionMutates);

        O3Utils.unmap(ff, srcTimestampAddr, srcTimestampSize);
        O3Utils.close(ff, srcTimestampFd);

        tableWriter.o3ClockDownPartitionUpdateCount();
        tableWriter.o3CountDownDoneLatch();
    }

    private static void updatePartitionSink(long partitionUpdateSinkAddr, long partitionTimestamp, long o3TimestampMin, long newPartitionSize, long oldPartitionSize, long partitionMutates) {
        Unsafe.putLong(partitionUpdateSinkAddr, partitionTimestamp);
        Unsafe.putLong(partitionUpdateSinkAddr + Long.BYTES, o3TimestampMin);
        Unsafe.putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize); // new partition size
        Unsafe.putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, oldPartitionSize);
        Unsafe.putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, partitionMutates); // partitionMutates
        Unsafe.putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0); // o3SplitPartitionSize
        Unsafe.putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, -1); // update parquet partition file size
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        processPartition(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
