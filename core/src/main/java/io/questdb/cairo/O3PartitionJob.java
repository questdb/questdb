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
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryOM;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.PartitionUpdater;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.mp.Sequence;
import io.questdb.std.CarrierLocal;
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
    private static final CarrierLocal<O3ParquetMergeContext> PARQUET_MERGE_CONTEXT =
            new CarrierLocal<>(O3ParquetMergeContext::new);
    public static final Closeable THREAD_LOCAL_CLEANER = PARQUET_MERGE_CONTEXT::removeAndFree;
    // Tells the Rust encoder that the designated-timestamp column's
    // primary_data is laid out as 16-byte (ts, rowId) merge-index entries
    // (timestamp at offset 0). Lets the encoder read timestamps in place
    // instead of pre-extracting a flat ts vector. Supported by all three
    // encodings used for Timestamp (Plain, DeltaBinaryPacked, RleDictionary),
    // so callers can set this unconditionally on the designated-timestamp
    // column regardless of the user-configured encoding.
    //
    // Mirrors COLUMN_TYPE_STRIDED_TIMESTAMP_16_BIT in
    // core/rust/qdbr/src/parquet_write/schema.rs - keep in sync.
    private static final int PARQUET_TIMESTAMP_STRIDED_16 = 0x4000_0000;

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
            long seqTxn,
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
        long newParquetSize;
        long newParquetMetaFileSize;
        boolean isRewrite = false;
        CairoConfiguration cairoConfiguration = tableWriter.getConfiguration();
        FilesFacade ff = tableWriter.getFilesFacade();
        final O3ParquetMergeContext ctx = PARQUET_MERGE_CONTEXT.get();
        ctx.clear();
        final ParquetPartitionDecoder partitionDecoder = ctx.getPartitionDecoder(cairoConfiguration);
        final RowGroupBuffers rowGroupBuffers = ctx.getRowGroupBuffers();
        final DirectIntList parquetColumns = ctx.getParquetColumns();
        final ParquetMetaFileReader parquetMetaReader = ctx.getParquetMetaReader();
        final PartitionUpdater partitionUpdater = ctx.getPartitionUpdater();
        long parquetSize = parquetFileSize;
        try {
            int parquetNameLen = path.size();
            int partitionDirLen = parquetNameLen - TableUtils.PARQUET_PARTITION_NAME.length() - 1;
            path.trimTo(partitionDirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();

            ParquetMetaFileReader.openAndMapRO(ff, path.$(), parquetMetaReader);
            if (parquetMetaReader.getAddr() == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                throw CairoException.critical(0)
                        .put("_pm tail does not match current parquet file size [path=").put(path)
                        .put(", parquetFileSize=").put(parquetFileSize).put(']');
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
                final ParquetMetaFileReader parquetMeta = partitionDecoder.metadata();
                final int parquetColumnCount = parquetMeta.getColumnCount();
                final int rowGroupCount = parquetMeta.getRowGroupCount();
                final long unusedBytes = parquetMeta.getUnusedBytes();

                // Build column-ID mapping between table schema and parquet file.
                // This allows O3 merge to work after ADD/DROP COLUMN: columns are
                // matched by their writer index (stored as field_id in parquet),
                // not by position.
                final int columnCount = tableWriterMetadata.getColumnCount();
                final IntIntHashMap parquetColIdToIdx = ctx.getParquetColIdToIdx();
                for (int i = 0; i < parquetColumnCount; i++) {
                    parquetColIdToIdx.put(partitionDecoder.metadata().getColumnId(i), i);
                }
                final IntList tableToParquetIdx = ctx.getTableToParquetIdx(columnCount);
                boolean hasMissingColumns = false;
                boolean hasTypeConvertedColumns = false;
                int mappedParquetColumns = 0;
                for (int i = 0; i < columnCount; i++) {
                    if (tableWriterMetadata.getColumnType(i) < 0) {
                        continue;
                    }
                    final int origWriterIndex = tableWriterMetadata.getColumnMetadata(i).getOriginalWriterIndex();
                    int parquetIdx = parquetColIdToIdx.get(origWriterIndex);
                    if (parquetIdx >= 0 && origWriterIndex != tableWriterMetadata.getColumnMetadata(i).getWriterIndex()) {
                        hasTypeConvertedColumns = true;
                    }
                    tableToParquetIdx.setQuick(i, parquetIdx);
                    if (parquetIdx < 0) {
                        hasMissingColumns = true;
                    } else {
                        mappedParquetColumns++;
                    }
                }
                // Detect schema changes: missing columns (ADD COLUMN), extra
                // columns in the parquet file (DROP COLUMN), or type-converted
                // columns (ALTER COLUMN TYPE). All require a rewrite with the
                // updated target schema.
                final boolean hasExtraColumns = mappedParquetColumns < parquetColumnCount;
                final boolean hasSchemaChange = hasMissingColumns || hasExtraColumns || hasTypeConvertedColumns;
                // Legacy files (written before BOOLEAN/BYTE/SHORT/CHAR/SYMBOL became Optional) store these
                // no-null-sentinel columns as Required (parquet max def level 0, pages carry no
                // definition-level stream). A rewrite migrates the footer to Optional but raw-copies
                // non-overlapping row groups verbatim, leaving Required pages under an Optional footer,
                // which the reader then mis-decodes (value bytes consumed as def levels). When the source
                // carries any such column, force a full rewrite that re-encodes every row group through
                // the conversion path under the migrated Optional schema, so no Required page survives.
                // New (already Optional) files report a positive max def level and keep the raw-copy path.
                final boolean forceFullReencode = hasLegacyRequiredNoSentinelColumn(partitionDecoder.metadata(), parquetColumnCount);

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

                // When a timestamp value straddles a row-group boundary that the O3 batch
                // also lands on, computeMergeActions coalesces the tied row groups into a
                // single multi-group MERGE. That merge re-encodes fewer (or differently
                // sized) row groups than it consumed, which update mode cannot express
                // (it has no remove primitive), so it requires the rewrite path.
                //
                // Coalescing only matters for deduplicating commits: it exists so a dedup
                // key at the shared timestamp is compared against every existing copy
                // across the tied groups. Without dedup the tie is harmless (rows merge
                // correctly per-group), so a non-deduplicating commit must NOT pay the
                // forced rewrite. This gate must stay in lockstep with the
                // coalesceBoundaryTies flag passed to computeMergeActions below.
                final boolean isCommitDedup = tableWriter.isCommitDedupMode();
                final int tieTimestampParquetIdx = tableToParquetIdx.getQuick(timestampIndex);
                final boolean hasCoalescableTie = isCommitDedup
                        && tieTimestampParquetIdx >= 0
                        && hasCoalescableBoundaryTie(
                        partitionDecoder,
                        parquetMeta,
                        rowGroupCount,
                        tieTimestampParquetIdx,
                        sortedTimestampsAddr,
                        srcOooLo,
                        srcOooHi
                );

                // Decide whether to rewrite the file or update in-place.
                // A single-row-group file always triggers a rewrite: any O3 merge
                // replaces its only row group, leaving 100% of the original payload
                // as dead bytes.
                // Schema mismatch (hasSchemaChange) also forces rewrite: in update
                // mode, untouched row groups would retain the old column layout while
                // the footer schema uses the new target schema, producing a malformed
                // Parquet file.
                isRewrite = hasSchemaChange
                        || forceFullReencode
                        || rowGroupCount == 1
                        || hasCoalescableTie
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
                        // Parse anchor = committed head (resolved from _txn), not
                        // the raw header: a rolled-back update can leave the
                        // header ahead of _txn, and parsing from there would
                        // build on dead row groups. The append base, passed
                        // separately below as parquetMetaReader.getFileSize(), is
                        // the raw _pm header (offset 0) where the new bytes land.
                        updaterParquetMetaFileSize = parquetMetaReader.getResolvedFileSize();
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
                        updaterParquetMetaFileSize, // parse anchor (committed head from _txn)
                        parquetMetaReader.getFileSize(), // append base (_pm header at offset 0)
                        parquetFileSize, // existing parquet data-file size (gates first-time vs incremental)
                        seqTxn
                );

                if (hasSchemaChange || forceFullReencode) {
                    ParquetRowGroupMaterializer.setTargetSchema(
                            ctx,
                            partitionUpdater,
                            tableWriterMetadata,
                            tableWriter.getSymbolTableProvider()
                    );
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
                    O3ParquetMergeStrategy.addRowGroupBounds(rowGroupBounds, rgMin, rgMax, meta.getRowGroupSize(rg));
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
                        ctx.getGapO3Ranges(),
                        // Coalesce boundary ties only for dedup commits; must match the
                        // hasCoalescableTie rewrite gate above so a coalesced multi-group
                        // MERGE is never emitted in update mode (which cannot drop the
                        // absorbed row groups).
                        isCommitDedup
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
                                // A coalesced action spans row groups [rowGroupIndex..rowGroupIndexHi]
                                // (a single group when they are equal). Sum their row counts so the
                                // merge decodes and dedups the whole run as one unit.
                                long rgRowCount = 0;
                                for (int g = action.rowGroupIndex; g <= action.rowGroupIndexHi; g++) {
                                    rgRowCount += partitionDecoder.metadata().getRowGroupSize(g);
                                }
                                assert rgRowCount <= Integer.MAX_VALUE;
                                final int rgSize = (int) rgRowCount;
                                LOG.info()
                                        .$("parquet merge row group [table=").$(tableWriter.getTableToken())
                                        .$(", partition=").$ts(partitionTimestamp)
                                        .$(", rg=").$(action.rowGroupIndex)
                                        .$(", rgHi=").$(action.rowGroupIndexHi)
                                        .$(", dataRows=").$(rgSize)
                                        .$(", o3Rows=").$(action.o3Hi - action.o3Lo + 1)
                                        .$(", rgMin=").$ts(O3ParquetMergeStrategy.getRowGroupMin(rowGroupBounds, action.rowGroupIndex))
                                        .$(", rgMax=").$ts(O3ParquetMergeStrategy.getRowGroupMax(rowGroupBounds, action.rowGroupIndexHi))
                                        .I$();
                                final long mergeResult = mergeRowGroup(
                                        chunkDescriptor,
                                        partitionUpdater,
                                        parquetColumns,
                                        oooColumns,
                                        sortedTimestampsAddr,
                                        tableWriter,
                                        partitionDecoder,
                                        rgSize,
                                        rowGroupBuffers,
                                        action.rowGroupIndex,
                                        action.rowGroupIndexHi,
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
                                        ctx.getActiveColIndices(columnCount),
                                        ctx
                                );
                                final int numOutputRGs = (int) (mergeResult >>> 32);
                                final long mergeDuplicates = mergeResult & 0xFFFFFFFFL;
                                duplicateCount += mergeDuplicates;
                                tableWriter.addPhysicallyWrittenRows(rgSize + (action.o3Hi - action.o3Lo + 1) - mergeDuplicates);
                                metadataPosition += numOutputRGs;
                            }
                            case COPY_ROW_GROUP_SLICE -> {
                                final long copyRgRowCount = partitionDecoder.metadata().getRowGroupSize(action.rowGroupIndex);
                                assert copyRgRowCount <= Integer.MAX_VALUE;
                                final int rgSize = (int) copyRgRowCount;
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
                                    if (hasTypeConvertedColumns || forceFullReencode) {
                                        ParquetRowGroupMaterializer.materialize(
                                                ctx,
                                                partitionDecoder,
                                                partitionUpdater,
                                                action.rowGroupIndex,
                                                metadataPosition,
                                                tableWriterMetadata,
                                                tableToParquetIdx,
                                                tableWriter.getSymbolTableProvider()
                                        );
                                    } else if (hasSchemaChange) {
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
                                        ctx,
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
                    for (int bufIdx = 0; bufIdx < colCount; bufIdx++) {
                        int bi4 = bufIdx * 4;
                        for (int slot = 0; slot < 4; slot += 2) {
                            if (mergeDstBufs.getQuick(bi4 + slot) != 0) {
                                Unsafe.free(mergeDstBufs.getQuick(bi4 + slot), mergeDstBufs.getQuick(bi4 + slot + 1), MemoryTag.NATIVE_O3);
                                mergeDstBufs.setQuick(bi4 + slot, 0);
                                mergeDstBufs.setQuick(bi4 + slot + 1, 0);
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

                // Publish the new _pm last. In sync modes an incremental update
                // first fsyncs the appended footer while the old header is still
                // authoritative, patches the header (the MVCC commit signal),
                // then fsyncs again before _txn can commit. A first-barrier
                // failure leaves the old header intact. After the patch, the
                // appended footer is already durable, so a second-barrier failure
                // is safe whether the old or new header reaches disk: _txn is
                // unchanged and a pinned reader can walk back to the committed
                // footer (see commit_parquet_meta).
                partitionUpdater.commitParquetMeta(cairoConfiguration.getCommitMode() != CommitMode.NOSYNC);
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
                // Update mode: truncate the parquet data file back to its
                // pre-merge size. _pm is never truncated (see below).
                path.of(pathToTable);
                setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, srcNameTxn);
                try {
                    // Swallow any error (openRW throws on an FS fault): o3BumpErrorCount
                    // below must still run to suspend the table. A failed truncate leaves
                    // the failed update's appended bytes as a dead tail past the committed
                    // parquetSize, which is harmless -- the committed _txn size is unchanged,
                    // so readers ignore it and the next update overwrites it.
                    long fd = TableUtils.openRW(ff, path.$(), LOG, cairoConfiguration.getWriterFileOpenOpts());
                    if (!ff.truncate(fd, parquetSize)) {
                        LOG.error().$("could not truncate partition file [path=").$(path).I$();
                    }
                    ff.close(fd);
                } catch (Throwable dataErr) {
                    LOG.error().$("could not truncate parquet data file on rollback [path=").$(path).$(", e=").$(dataErr).I$();
                }

                if (parquetMetaReader.getAddr() != 0) {
                    // Leave _pm un-truncated, header unrestored. A failure before
                    // commitParquetMeta leaves the header at the committed footer;
                    // a failure inside it either leaves the old header (tail
                    // barrier failed) or may leave the header at the new, already
                    // durable footer (header barrier failed). Either way the
                    // committed _txn size is unchanged, so a reader walks the
                    // MVCC chain back to the committed footer and the failed
                    // update's bytes sit past it as a dead tail the next update
                    // overwrites.
                    // Truncating would pull pages from under a concurrent reader's
                    // mmap and SIGBUS the JVM -- the hazard this change removes.
                    path.of(pathToTable);
                    setPathForParquetPartitionMetadata(path.slash(), timestampType, partitionBy, partitionTimestamp, srcNameTxn);
                    try {
                        // Make the leftover _pm tail durable, but swallow any error:
                        // o3BumpErrorCount below must still run to suspend the table.
                        // fsyncAndClose closes the fd even on failure (no leak), and a
                        // non-durable dead tail is harmless -- the next update rewrites it.
                        long fd = TableUtils.openRW(ff, path.$(), LOG, cairoConfiguration.getWriterFileOpenOpts());
                        if (cairoConfiguration.getCommitMode() != CommitMode.NOSYNC) {
                            ff.fsyncAndClose(fd);
                        } else {
                            ff.close(fd);
                        }
                    } catch (Throwable pmErr) {
                        LOG.error().$("could not fsync _pm tail on rollback [path=").$(path).$(", e=").$(pmErr).I$();
                    }
                }
            }
            // Rewrite mode: original is intact, new dir already removed by the inner catch.
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(th));
        } finally {
            // Release the reader's native handle (which borrows from the mmap) before munmap.
            // See ParquetMetaFileReader lifecycle contract.
            ctx.releaseResources();
            final long parquetMetaAddr = parquetMetaReader.getAddr();
            final long parquetMetaSize = parquetMetaReader.getFileSize();
            parquetMetaReader.clear();
            if (parquetMetaAddr != 0) {
                ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
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
            long seqTxn,
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
            if (srcDataMax < 1) {
                // Brand-new partition on a FORMAT PARQUET table: there is no
                // existing parquet file to update, so the standard parquet O3
                // path (which assumes a file + _pm exist) cannot run. Emit a
                // fresh parquet file directly from the O3 buffers instead.
                writeFreshParquetFromO3(
                        pathToTable,
                        tableWriter.getMetadata().getTimestampType(),
                        partitionBy,
                        oooColumns,
                        srcOooLo,
                        srcOooHi,
                        o3TimestampMin,
                        partitionTimestamp,
                        sortedTimestampsAddr,
                        tableWriter,
                        txn,
                        seqTxn,
                        partitionUpdateSinkAddr,
                        o3Basket,
                        newPartitionSize
                );
                return;
            }
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
                    seqTxn,
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
        final long seqTxn = task.getSeqTxn();
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
                seqTxn,
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

    /**
     * Writes a brand-new row group from O3 input only — there is no
     * pre-existing parquet data to merge against. The O3 source buffers reach
     * this method already sorted/deduped/dense (see TableWriter.cthO3SortColumn
     * / swapO3ColumnsExcept), so we hand the encoder pointers into those
     * buffers directly with o3Lo as the slice offset. The only buffer we
     * allocate is the flattened timestamp column, because cthO3SortColumn
     * skips the timestamp (its sort key lives separately in
     * sortedTimestampsAddr as 16-byte (ts, rowId) pairs).
     */
    private static void copyO3ToRowGroup(
            O3ParquetMergeContext ctx,
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
        // Use the sorted timestamps directly as merge index for the timestamp
        // extraction below. Each entry is (ts, originalRowId); bit 63 of the
        // rowId is irrelevant for oooCopyIndex, which only reads timestamps.
        final long mergeIndexAddr = sortedTimestampsAddr + o3Lo * TIMESTAMP_MERGE_ENTRY_BYTES;

        // Non-owning descriptor: every column hands the encoder pointers into
        // O3 source buffers (sorted data, sorted aux, merge index for the
        // timestamp), so there's nothing to free on this path.
        final PartitionDescriptor descriptor = ctx.getFreshPartitionDescriptor();
        descriptor.clear();

        try {
            descriptor.of(tableWriter.getTableToken().getTableName(), rowCount, timestampIndex);
            populateO3DescriptorColumns(
                    ctx,
                    descriptor,
                    tableWriterMetadata,
                    oooColumns,
                    tableWriter,
                    o3Lo,
                    o3Hi,
                    mergeIndexAddr
            );
            partitionUpdater.addRowGroup(metadataPosition, descriptor);
        } finally {
            descriptor.clear();
        }
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
        // This may be zero in a DROP-only scenario - the function still
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

    /**
     * Returns true if any two adjacent row groups share a boundary timestamp
     * (rg[i].max == rg[i+1].min) that the sorted O3 batch [srcOooLo, srcOooHi] also
     * contains. In that case computeMergeActions will coalesce the tied row groups
     * into one multi-group MERGE, which requires the rewrite path.
     */
    private static boolean hasCoalescableBoundaryTie(
            ParquetPartitionDecoder partitionDecoder,
            ParquetMetaFileReader meta,
            int rowGroupCount,
            int timestampParquetIdx,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi
    ) {
        if (rowGroupCount < 2 || srcOooHi < srcOooLo) {
            return false;
        }
        long prevMax = readRowGroupTimestampBound(partitionDecoder, meta, 0, timestampParquetIdx, true);
        for (int rg = 1; rg < rowGroupCount; rg++) {
            final long curMin = readRowGroupTimestampBound(partitionDecoder, meta, rg, timestampParquetIdx, false);
            if (prevMax == curMin) {
                final long idx = Vect.boundedBinarySearchIndexT(sortedTimestampsAddr, curMin, srcOooLo, srcOooHi, Vect.BIN_SEARCH_SCAN_DOWN);
                if (idx >= srcOooLo && Unsafe.getLong(sortedTimestampsAddr + idx * TIMESTAMP_MERGE_ENTRY_BYTES) == curMin) {
                    return true;
                }
            }
            prevMax = readRowGroupTimestampBound(partitionDecoder, meta, rg, timestampParquetIdx, true);
        }
        return false;
    }

    // Detects a legacy parquet file that stores BOOLEAN/BYTE/SHORT/CHAR/SYMBOL as Required (max def
    // level 0, no definition-level stream). Such a file predates the convention that these
    // no-null-sentinel columns are Optional. Rewriting it migrates the footer to Optional while
    // raw-copying untouched row groups, leaving Required pages under an Optional footer (corrupt
    // reads). The caller forces a full re-encode when this returns true. Modern files report a
    // positive max def level for these columns and are left on the fast raw-copy path.
    private static boolean hasLegacyRequiredNoSentinelColumn(ParquetMetaFileReader meta, int parquetColumnCount) {
        for (int i = 0; i < parquetColumnCount; i++) {
            if (meta.getColumnMaxDefLevel(i) == 0) {
                final int srcTag = ColumnType.tagOf(meta.getColumnType(i));
                if (srcTag == ColumnType.BOOLEAN
                        || srcTag == ColumnType.BYTE
                        || srcTag == ColumnType.SHORT
                        || srcTag == ColumnType.CHAR
                        || srcTag == ColumnType.SYMBOL) {
                    return true;
                }
            }
        }
        return false;
    }

    // returns packed long: (numOutputRowGroups << 32) | (duplicateCount & 0xFFFFFFFFL)
    private static long mergeRowGroup(
            PartitionDescriptor chunkDescriptor,
            PartitionUpdater partitionUpdater,
            DirectIntList parquetColumns,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            ParquetPartitionDecoder decoder,
            int rowGroupSize,
            RowGroupBuffers rowGroupBuffers,
            int rowGroupIndex,
            int rowGroupIndexHi,
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
            IntList activeColIndices,
            O3ParquetMergeContext ctx
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
                // When the source parquet type differs from the target, request a decode
                // type that Rust can actually produce. Java batch-converts afterward.
                parquetColumns.add(ParquetColumnTypeConverter.chooseDecodeType(decoder, parquetIdx, columnType));
                activeToDecodeIdx.setQuick(activeColCount, decodeColCount);
                decodeColCount++;
            } else {
                activeToDecodeIdx.setQuick(activeColCount, -1);
            }
            activeColIndices.setQuick(activeColCount, columnIndex);
            activeColCount++;
        }

        if (rowGroupIndexHi > rowGroupIndex) {
            // Coalesced run: a timestamp value straddles the boundaries of row groups
            // [rowGroupIndex..rowGroupIndexHi]. Decode them all into one buffer so the
            // dedup compares the shared-timestamp key against every existing copy.
            final long decoded = decoder.decodeRowGroupRange(rowGroupBuffers, parquetColumns, rowGroupIndex, rowGroupIndexHi);
            assert decoded == rowGroupSize : "decoded " + decoded + " rows, expected " + rowGroupSize;
        } else {
            decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0, rowGroupSize);
        }

        assert timestampColumnChunkIndex > -1;
        final long timestampDataPtr = rowGroupBuffers.getChunkDataPtr(timestampColumnChunkIndex);
        assert timestampDataPtr != 0;
        long mergeBatchRowCount = mergeRangeHi - mergeRangeLo + 1;
        long mergeRowCount = mergeBatchRowCount + rowGroupSize;
        long duplicateCount = 0;

        long timestampMergeIndexSize = mergeRowCount * TIMESTAMP_MERGE_ENTRY_BYTES;
        long timestampMergeIndexAddr = 0;
        int numChunks;

        // Re-zero per-row-group buffers. The getters already zero them for the
        // first call, but mergeRowGroup is called in a loop (once per MERGE action),
        // so subsequent calls need stale values from the previous row group cleared.
        nullBufs.fill(0, activeColCount * 4, 0);
        srcPtrs.fill(0, activeColCount * 2, 0);

        try {
            // Phase 1a: prepare the per-column SOURCE pointers in srcPtrs (data at slot bi2, aux at
            // bi2+1). prepareSourceColumn batch-converts a fixed<->var/symbol crossing into
            // the target representation (buffers tracked in nullBufs for freeing), fills a null
            // source buffer for a column-top / missing column, or passes a target-typed decode
            // through. This MUST run before the dedup compare below: the native dedup comparer
            // reads these source pointers directly, and if it saw the raw cross-typed decode buffer
            // it would misread it -- a fixed->var key has a dangling/empty aux vector (SIGSEGV), and
            // a var/symbol->fixed key would read VARCHAR_SLICE bytes as fixed values (silent wrong
            // dedup). The destination sizing (Phase 1b) and merge copy (Phase 2) reuse the same
            // prepared pointers, so each crossing conversion runs exactly once.
            final LongList convertedPtrs = ctx.getConvertedPtrs(columnCount);
            for (int ai = 0; ai < activeColCount; ai++) {
                int columnIndex = activeColIndices.getQuick(ai);
                int columnType = tableWriterMetadata.getColumnType(columnIndex);
                int decodeIdx = activeToDecodeIdx.getQuick(ai);
                ParquetColumnTypeConverter.prepareSourceColumn(
                        decoder,
                        rowGroupBuffers,
                        tableToParquetIdx,
                        columnIndex,
                        columnType,
                        decodeIdx,
                        rowGroupSize,
                        ctx,
                        nullBufs,
                        convertedPtrs,
                        ai * 4
                );
                srcPtrs.setQuick(ai * 2, convertedPtrs.getQuick(ai * 4));        // dataPtr
                srcPtrs.setQuick(ai * 2 + 1, convertedPtrs.getQuick(ai * 4 + 2)); // auxPtr
            }

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
                        int bi2 = ai * 2;
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
                                    // Use the Phase-1a prepared source pointer: for a
                                    // var/symbol->fixed crossing this is the converted fixed
                                    // buffer, not the raw VARCHAR_SLICE decode the comparer
                                    // would otherwise misread as fixed values.
                                    DedupColumnCommitAddresses.setColAddressValues(addr, srcPtrs.getQuick(bi2));
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
                                    // Use the Phase-1a prepared source pointers: for a
                                    // fixed->var crossing these are the converted aux/data
                                    // buffers (the raw decode has a dangling/empty aux). The
                                    // var data length is the exact extent from the aux
                                    // vector's last entry (same computation as Phase 1b).
                                    final long srcAuxPtr = srcPtrs.getQuick(bi2 + 1);
                                    final long srcDataPtr = srcPtrs.getQuick(bi2);
                                    final long srcVarDataLen = ColumnType.getDriver(columnType)
                                            .getDataVectorSizeAt(srcAuxPtr, rowGroupSize - 1);
                                    DedupColumnCommitAddresses.setColAddressValues(
                                            addr,
                                            srcAuxPtr,
                                            srcDataPtr,
                                            srcVarDataLen
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
            if (mergeRowCount > maxChunkTarget) {
                numChunks = (int) ((mergeRowCount + maxChunkTarget - 1) / maxChunkTarget);
            } else {
                numChunks = 1;
            }
            final long maxChunkSize = (mergeRowCount + numChunks - 1) / numChunks;

            // Phase 1b: ensure destination buffers in mergeDstBufs are large enough
            // for this merge. Grow if needed; reuse if already sufficient. Reads the
            // Phase-1a prepared source aux pointer from srcPtrs to size var data.
            for (int ai = 0; ai < activeColCount; ai++) {
                int columnIndex = activeColIndices.getQuick(ai);
                int columnType = tableWriterMetadata.getColumnType(columnIndex);
                int bi4 = ai * 4;
                int bi2 = ai * 2;

                if (ColumnType.isVarSize(columnType)) {
                    final ColumnTypeDriver ctd = ColumnType.getDriver(columnType);
                    final long columnAuxPtr = srcPtrs.getQuick(bi2 + 1);

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
                    final int columnId = tableWriterMetadata.getColumnMetadata(columnIndex).getOriginalWriterIndex();
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
                                encodeColumnType |= ParquetColumnTypeConverter.PARQUET_SYMBOL_NOT_NULL_HINT;
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
                        nullBufs.setQuick(bi4 + slot, 0);
                        nullBufs.setQuick(bi4 + slot + 1, 0);
                    }
                }
            }
            // timestampMergeIndexAddr is 0 if Phase 1a or the merge-index build threw
            // before it was allocated.
            if (timestampMergeIndexAddr != 0) {
                Unsafe.free(timestampMergeIndexAddr, timestampMergeIndexSize, MemoryTag.NATIVE_O3);
            }
        }

        return ((long) numChunks << 32) | (duplicateCount & 0xFFFFFFFFL);
    }

    /**
     * Populates a non-owning {@link PartitionDescriptor} with one entry per
     * column of an O3-only partition slice, suitable for handing to either
     * {@link io.questdb.griffin.engine.table.parquet.PartitionEncoder} (for
     * fresh-parquet writes) or
     * {@link io.questdb.griffin.engine.table.parquet.PartitionUpdater#addRowGroup}
     * (for appending a row group to an existing parquet file).
     * <p>
     * All pointers handed to the descriptor reference O3 source memory (or,
     * for a rebased aux, the context scratch arena) owned by the
     * {@code TableWriter} for the duration of the encode call:
     * <ul>
     *     <li>Var-size columns: the data buffer as primary and the aux as
     *     secondary. A 0-based source (copy+sort path) passes the full data
     *     buffer with its absolute aux offsets. An offset-mapped WAL segment
     *     ({@link MemoryOM}, in-order block apply) has a data window that does
     *     not start at offset 0, so its aux holds offsets that overshoot the
     *     window; that aux is rebased to window-relative offsets in the context
     *     scratch arena so {@code primary_data} is a valid 0-based base.</li>
     *     <li>Designated timestamp: the merge-index slice as primary with
     *     {@code PARQUET_TIMESTAMP_STRIDED_16} set on the column type, so
     *     the Rust encoder reads timestamps in place from the 16-byte
     *     (ts, rowId) entries.</li>
     *     <li>Fixed-size and symbol columns: a pointer into the sorted O3
     *     buffer offset by {@code o3Lo} (symbol dictionaries come from the
     *     table-wide {@link SymbolMapWriter}).</li>
     * </ul>
     * Callers are responsible for calling {@link PartitionDescriptor#of} and
     * issuing the encode/addRowGroup, and for any descriptor cleanup.
     */
    private static void populateO3DescriptorColumns(
            O3ParquetMergeContext ctx,
            PartitionDescriptor descriptor,
            TableRecordMetadata metadata,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            TableWriter tableWriter,
            long o3Lo,
            long o3Hi,
            long mergeIndexAddr
    ) {
        final int timestampIndex = metadata.getTimestampIndex();
        final long rowCount = o3Hi - o3Lo + 1;
        final int columnCount = metadata.getColumnCount();

        // Size the rebase arena (see javadoc) once up front: the slots handed
        // out below must keep stable addresses across the loop, and a later
        // resize would move them.
        long rebaseArenaSize = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType > 0 && ColumnType.isVarSize(columnType)
                    && oooColumns.getQuick(getPrimaryColumnIndex(columnIndex)) instanceof MemoryOM) {
                rebaseArenaSize += ColumnType.getDriver(columnType).getAuxVectorSize(rowCount);
            }
        }
        long rebaseArenaBase = 0;
        long rebaseArenaBump = 0;
        if (rebaseArenaSize > 0) {
            rebaseArenaBase = ctx.getRebaseAuxMem().resize(rebaseArenaSize);
        }

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int columnType = metadata.getColumnType(columnIndex);
            if (columnType < 0) {
                continue;
            }
            final String columnName = metadata.getColumnName(columnIndex);
            // Field_id written to parquet must be the original writer index so the
            // column-ID mapping (parquetColIdToIdx in processParquetPartition) keeps
            // matching columns across ALTER COLUMN TYPE conversion chains.
            final int columnId = metadata.getColumnMetadata(columnIndex).getOriginalWriterIndex();
            final int parquetEncodingConfig = metadata.getColumnMetadata(columnIndex).getParquetEncodingConfig();

            if (ColumnType.isVarSize(columnType)) {
                final MemoryCR oooDataMem = oooColumns.getQuick(getPrimaryColumnIndex(columnIndex));
                final MemoryCR oooAuxMem = oooColumns.getQuick(getSecondaryColumnIndex(columnIndex));
                final long srcOooAuxAddr = oooAuxMem.addressOf(0);
                final ColumnTypeDriver ctd = ColumnType.getDriver(columnType);
                // Encoder reads rowCount aux entries; the N+1 sentinel that
                // string-like types include in getAuxVectorSize is unused here.
                final long auxSliceSize = ctd.auxRowsToBytes(rowCount);
                // Tight upper bound on data accessed via aux entries in this
                // slice. Encoder asserts offset+size <= data.len().
                final long dataExtent = ctd.getDataVectorSizeAt(srcOooAuxAddr, o3Hi);

                final long dataAddr;
                final long dataSize;
                final long auxAddr;
                if (oooDataMem instanceof MemoryOM) {
                    // Windowed source: hand the encoder the data window and
                    // rebase the aux by shift = dataLo (dest = src - shift).
                    final long dataLo = ctd.getDataVectorOffset(srcOooAuxAddr, o3Lo);
                    final long windowSize = dataExtent - dataLo;
                    final long rebasedAuxSize = ctd.getAuxVectorSize(rowCount);
                    final long scratch = rebaseArenaBase + rebaseArenaBump;
                    ctd.shiftCopyAuxVector(dataLo, srcOooAuxAddr, o3Lo, o3Hi, scratch, rebasedAuxSize);
                    rebaseArenaBump += rebasedAuxSize;
                    auxAddr = scratch;
                    // addressOf(dataLo) on an empty window extrapolates to a
                    // bogus pointer; pass null when there is nothing to read.
                    dataAddr = windowSize > 0 ? oooDataMem.addressOf(dataLo) : 0;
                    dataSize = windowSize;
                } else {
                    // 0-based source (copy+sort path): addressOf(0) is the real
                    // base and aux offsets are already correct.
                    auxAddr = srcOooAuxAddr + ctd.getAuxVectorOffset(o3Lo);
                    dataAddr = oooDataMem.addressOf(0);
                    dataSize = dataExtent;
                }

                descriptor.addColumn(
                        columnName,
                        columnType,
                        columnId,
                        0,
                        dataAddr,
                        dataSize,
                        auxAddr,
                        auxSliceSize,
                        0,
                        0,
                        parquetEncodingConfig
                );
                continue;
            }

            final long elemSize = ColumnType.sizeOf(columnType);
            final long dstFixSize = rowCount * elemSize;

            if (columnIndex == timestampIndex) {
                // The O3 buffer for the timestamp column is unsorted
                // (cthO3SortColumn skips it). Sorted timestamps live as
                // 16-byte (ts, rowId) entries in mergeIndexAddr; the Rust
                // encoder reads them in place when PARQUET_TIMESTAMP_STRIDED_16
                // is set. All three timestamp encodings (Plain,
                // DeltaBinaryPacked, RleDictionary) support the strided
                // layout, so no encoding-aware fallback is needed.
                descriptor.addColumn(
                        columnName,
                        ColumnType.setDesignatedTimestampBit(columnType, true)
                                | PARQUET_TIMESTAMP_STRIDED_16,
                        columnId,
                        0,
                        mergeIndexAddr,
                        rowCount * TableWriter.TIMESTAMP_MERGE_ENTRY_BYTES,
                        0,
                        0,
                        0,
                        0,
                        parquetEncodingConfig
                );
                continue;
            }

            final MemoryCR oooMem1 = oooColumns.getQuick(getPrimaryColumnIndex(columnIndex));
            final long srcFixSliceAddr = oooMem1.addressOf(0) + o3Lo * elemSize;

            if (ColumnType.isSymbol(columnType)) {
                // Symbol columns: int keys in the sorted O3 slice; symbol
                // dictionary (offsets + values) comes from the table-wide
                // SymbolMapWriter (WAL apply has already applied this
                // transaction's SymbolMapDiff).
                final MapWriter symbolMapWriter = tableWriter.getSymbolMapWriter(columnIndex);
                final MemoryR offsetsMem = symbolMapWriter.getSymbolOffsetsMemory();
                final MemoryR valuesMem = symbolMapWriter.getSymbolValuesMemory();
                final int symbolCount = symbolMapWriter.getSymbolCount();
                final long offset = SymbolMapWriter.keyToOffset(symbolCount);
                assert offset - SymbolMapWriter.HEADER_SIZE <= offsetsMem.size();
                final long valuesMemSize = offsetsMem.getLong(offset);
                assert valuesMemSize <= valuesMem.size();

                int encodeColumnType = columnType;
                if (!symbolMapWriter.getNullFlag()) {
                    encodeColumnType |= ParquetColumnTypeConverter.PARQUET_SYMBOL_NOT_NULL_HINT;
                }
                descriptor.addColumn(
                        columnName,
                        encodeColumnType,
                        columnId,
                        0,
                        srcFixSliceAddr,
                        dstFixSize,
                        valuesMem.addressOf(0),
                        valuesMemSize,
                        // Skip 8-byte header. Pass element count, not byte size.
                        offsetsMem.addressOf(SymbolMapWriter.HEADER_SIZE),
                        symbolCount,
                        parquetEncodingConfig
                );
            } else {
                descriptor.addColumn(
                        columnName,
                        columnType,
                        columnId,
                        0,
                        srcFixSliceAddr,
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
            IndexWriter indexWriter,
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
            IndexWriter indexWriter,
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
                final byte indexType = metadata.getColumnIndexType(i);
                if (openColumnMode == OPEN_LAST_PARTITION_FOR_APPEND || openColumnMode == OPEN_LAST_PARTITION_FOR_MERGE) {
                    srcDataTop = tableWriter.getColumnTop(i);
                } else {
                    srcDataTop = -1; // column open job will have to find out if top exists and its value
                }

                final IndexWriter indexWriter;
                if (isIndexed) {
                    indexWriter = o3Basket.nextIndexer(indexType);
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

    private static long readRowGroupTimestampBound(
            ParquetPartitionDecoder partitionDecoder,
            ParquetMetaFileReader meta,
            int rowGroupIndex,
            int timestampParquetIdx,
            boolean wantMax
    ) {
        final int statFlags = meta.getChunkStatFlags(rowGroupIndex, timestampParquetIdx);
        final boolean hasTimestampStats = (statFlags & 0x03) == 0x03 && (statFlags & 0x18) == 0x18;
        if (hasTimestampStats) {
            return wantMax
                    ? meta.getChunkMaxStat(rowGroupIndex, timestampParquetIdx)
                    : meta.getChunkMinStat(rowGroupIndex, timestampParquetIdx);
        }
        return wantMax
                ? partitionDecoder.rowGroupMaxTimestamp(rowGroupIndex, timestampParquetIdx)
                : partitionDecoder.rowGroupMinTimestamp(rowGroupIndex, timestampParquetIdx);
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
            ParquetPartitionDecoder partitionDecoder,
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

            IndexWriter indexWriter;
            final int columnCount = tableWriterMetadata.getColumnCount();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                if (tableWriterMetadata.getColumnType(columnIndex) == ColumnType.SYMBOL && tableWriterMetadata.isColumnIndexed(columnIndex)) {
                    final int indexBlockCapacity = tableWriterMetadata.getIndexValueBlockCapacity(columnIndex);
                    if (indexBlockCapacity < 0) {
                        continue;
                    }

                    final CharSequence columnName = tableWriterMetadata.getColumnName(columnIndex);
                    final long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);

                    byte indexType = tableWriterMetadata.getColumnIndexType(columnIndex);

                    // Get a fresh writer when index type changes so different index
                    // types can coexist across columns in the same partition.
                    indexWriter = o3Basket.nextIndexer(indexType);

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

                        indexWriter.of(path.trimTo(pLen), columnName, columnNameTxn, indexBlockCapacity);
                        // The 4-arg of(...) above does not carry the partition
                        // identity, but a deferred seal-purge needs it to find the
                        // .pv directory later. srcNameTxn is this rebuild's dir
                        // name-txn (the new txn dir in rewrite mode, srcNameTxn in
                        // update-in-place mode).
                        indexWriter.setPartitionContext(partitionTimestamp, srcNameTxn);

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
                                if (size == 0) {
                                    BitmapIndexUtils.addNullEntries(indexWriter, rowId, rowCount + rowGroupSize);
                                } else {
                                    for (long p = addr, lim = addr + size; p < lim; p += 4, rowId++) {
                                        indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(p)), rowId);
                                    }
                                }

                                rowCount += rowGroupSize;
                            }
                            indexWriter.setMaxValue(newPartitionSize - 1);
                        }

                        // Tag the chain entry with the upcoming txn so that if
                        // the surrounding O3 / parquet-rewrite path crashes
                        // before txWriter.commit lands, recoveryDropAbandoned
                        // on the next writer-open drops this entry. Without
                        // the tag, slot[0].TXN_AT_SEAL defaults to 0 and the
                        // entry would be undroppable, surfacing stale rows.
                        // No-op on BitmapIndexWriter.
                        indexWriter.setNextTxnAtSeal(tableWriter.getTxn() + 1L);
                        if (IndexType.isPosting(indexType)) {
                            // commitDense may seal (spill-driven reseal), rotating the
                            // .pv and recording a purge for the superseded file. The
                            // pooled writer is freed below without ever draining that
                            // outbox through a TableWriter (the native reseal does, this
                            // path didn't), so the superseded .pv would leak. Drain it in
                            // a finally: an I/O fault inside commitDense's seal/sync can
                            // throw AFTER the purge was recorded, and the defer must still
                            // hand the entry off rather than drop it (idempotent no-op on
                            // an empty outbox). The deferred entry is published after the
                            // commit and the scoreboard-gated job deletes the .pv only
                            // once no reader is pinned in its txn window -- safe for both
                            // the rewrite (fresh dir) and update-in-place (live committed
                            // dir) cases. Tagged with getTxn() as the current (pre-commit)
                            // txn so the seal's getTxn()+1 entry is treated as
                            // finite-future.
                            try {
                                indexWriter.commitDense();
                            } finally {
                                tableWriter.deferParquetPostingSealPurges(indexWriter, tableWriter.getTxn());
                            }
                        } else {
                            indexWriter.commit();
                        }
                    } finally {
                        Misc.free(indexWriter);
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

    /**
     * Emits a fresh parquet file for a brand-new partition on a FORMAT PARQUET
     * table, sourcing data directly from the O3 in-memory column buffers. Unlike
     * {@link #processParquetPartition}, this path does not assume an existing
     * parquet file or _pm sidecar.
     */
    private static void writeFreshParquetFromO3(
            Path pathToTable,
            int timestampType,
            int partitionBy,
            ReadOnlyObjList<? extends MemoryCR> oooColumns,
            long srcOooLo,
            long srcOooHi,
            long o3TimestampMin,
            long partitionTimestamp,
            long sortedTimestampsAddr,
            TableWriter tableWriter,
            long txn,
            long seqTxn,
            long partitionUpdateSinkAddr,
            O3Basket o3Basket,
            long newPartitionSize
    ) {
        assert !tableWriter.getTableToken().isMatView() : "FORMAT PARQUET should be rejected on mat views at SQL level";

        final TableRecordMetadata metadata = tableWriter.getMetadata();
        final long partitionRowCount = srcOooHi - srcOooLo + 1;
        final FilesFacade ff = tableWriter.getFilesFacade();
        final CairoConfiguration configuration = tableWriter.getConfiguration();
        final long partitionNameTxn = txn - 1;
        final long mergeIndexAddr = sortedTimestampsAddr + srcOooLo * TableWriter.TIMESTAMP_MERGE_ENTRY_BYTES;

        final Path path = Path.getThreadLocal(pathToTable);
        final int pathSize = path.size();
        final Path parquetPath = Path.getThreadLocal2(pathToTable);

        // Source O3 column buffers reach this method already sorted (see
        // TableWriter.cthO3SortColumn / swapO3ColumnsExcept) and deduped, so
        // there is no reorder work left to do here. Hand the encoder pointers
        // into those buffers directly, with srcOooLo as the slice offset.
        //
        // For the designated timestamp the O3 buffer itself is unsorted
        // (cthO3SortColumn skips it); sorted timestamps live as 16-byte
        // (ts, rowId) entries in sortedTimestampsAddr. The Rust encoder
        // accepts that strided layout directly when the column type carries
        // PARQUET_TIMESTAMP_STRIDED_16 — supported by all three timestamp
        // encodings (Plain, DeltaBinaryPacked, RleDictionary), so no
        // encoding-aware fallback is needed.
        //
        // populateO3DescriptorColumns handles var-column data/aux pointers
        // (including rebasing offset-mapped WAL aux into the context arena).
        //
        // The descriptor itself is non-owning and does not free anything on
        // clear -- every pointer it carries references O3 source memory (or the
        // context rebase arena) owned by the TableWriter for this call.
        final O3ParquetMergeContext ctx = PARQUET_MERGE_CONTEXT.get();
        final PartitionDescriptor descriptor = ctx.getFreshPartitionDescriptor();
        descriptor.clear();

        long parquetFileSize;
        long parquetMetaFd = -1;
        boolean partitionDirCreated = false;
        try {
            setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            createDirsOrFail(ff, path.slash(), configuration.getMkDirMode());
            partitionDirCreated = true;

            final int readerTimestampIndex = metadata.getTimestampIndex();
            final int writerTimestampIndex = readerTimestampIndex >= 0
                    ? metadata.getColumnMetadata(readerTimestampIndex).getWriterIndex()
                    : -1;
            descriptor.of(tableWriter.getTableToken().getTableName(), partitionRowCount, writerTimestampIndex);
            populateO3DescriptorColumns(
                    ctx,
                    descriptor,
                    metadata,
                    oooColumns,
                    tableWriter,
                    srcOooLo,
                    srcOooHi,
                    mergeIndexAddr
            );

            // Open _pm so the Rust encoder writes parquet metadata alongside
            // the data file.
            setPathForParquetPartitionMetadata(parquetPath.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            parquetMetaFd = TableUtils.openRW(ff, parquetPath.$(), LOG, configuration.getWriterFileOpenOpts());

            setPathForParquetPartition(parquetPath.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);

            final int compressionCodec = configuration.getPartitionEncoderParquetCompressionCodec();
            final int compressionLevel = configuration.getPartitionEncoderParquetCompressionLevel();
            final int rowGroupSize = configuration.getPartitionEncoderParquetRowGroupSize();
            final int dataPageSize = configuration.getPartitionEncoderParquetDataPageSize();
            final boolean statisticsEnabled = configuration.isPartitionEncoderParquetStatisticsEnabled();
            final boolean rawArrayEncoding = configuration.isPartitionEncoderParquetRawArrayEncoding();
            final int parquetVersion = configuration.getPartitionEncoderParquetVersion();
            final double minCompressionRatio = configuration.getPartitionEncoderParquetMinCompressionRatio();

            // Honor per-column bloom filter flags (bit 25 of the parquet encoding
            // config), exactly as the CONVERT path does in produceParquetFromNative.
            final DirectIntList bloomFilterIndexes = ctx.getBloomFilterColumns();
            bloomFilterIndexes.clear();
            TableUtils.deriveBloomFilterColumnIndexes(metadata, bloomFilterIndexes);
            long bloomFilterColumnIndexesPtr = 0;
            int bloomFilterColumnCount = 0;
            if (bloomFilterIndexes.size() > 0) {
                bloomFilterColumnIndexesPtr = bloomFilterIndexes.getAddress();
                bloomFilterColumnCount = (int) bloomFilterIndexes.size();
            }

            PartitionEncoder.encodeWithOptions(
                    descriptor,
                    parquetPath,
                    ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                    statisticsEnabled,
                    rawArrayEncoding,
                    rowGroupSize,
                    dataPageSize,
                    parquetVersion,
                    bloomFilterColumnIndexesPtr,
                    bloomFilterColumnCount,
                    configuration.getPartitionEncoderParquetBloomFilterFpp(),
                    minCompressionRatio,
                    Files.toOsFd(parquetMetaFd),
                    -1L,
                    seqTxn
            );

            parquetFileSize = ff.length(parquetPath.$());

            if (configuration.getCommitMode() != CommitMode.NOSYNC) {
                ff.fsync(parquetMetaFd);
            }

            // Stat _pm size and release the writer fd before mmap RO for indexing.
            setPathForParquetPartitionMetadata(parquetPath.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            final long parquetMetaFileSize = ff.length(parquetPath.$());
            ff.close(parquetMetaFd);
            parquetMetaFd = -1;

            // Build .pk/.k index files for indexed SYMBOL columns by decoding
            // the just-written parquet. processParquetPartition does this for
            // existing parquet partitions via updateParquetIndexes; the fresh
            // path must do the same or readers will SIGABRT trying to open
            // missing index files on the first query against this partition.
            setPathForParquetPartition(parquetPath.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
            // Encoder already cleared the descriptor; clearing the rest of the
            // context resets transient scratch lists before indexing.
            ctx.clear();
            updateParquetIndexes(
                    partitionBy,
                    partitionTimestamp,
                    tableWriter,
                    partitionNameTxn,
                    o3Basket,
                    partitionRowCount,
                    parquetFileSize,
                    parquetMetaFileSize,
                    pathToTable,
                    parquetPath,
                    ff,
                    ctx.getPartitionDecoder(configuration),
                    metadata,
                    ctx.getParquetColumns(),
                    ctx.getRowGroupBuffers(),
                    true
            );

            // Hand off to the consumer via the partition update sink. The
            // consumer (o3ConsumePartitionUpdateSink) will register the
            // partition in txWriter and mark it as parquet.
            Unsafe.putLong(partitionUpdateSinkAddr, partitionTimestamp);
            Unsafe.putLong(partitionUpdateSinkAddr + Long.BYTES, o3TimestampMin);
            Unsafe.putLong(partitionUpdateSinkAddr + 2 * Long.BYTES, newPartitionSize);
            Unsafe.putLong(partitionUpdateSinkAddr + 3 * Long.BYTES, 0L); // oldPartitionSize
            // partitionMutates=0 — this is a fresh write, not an in-place mutation.
            Unsafe.putLong(partitionUpdateSinkAddr + 4 * Long.BYTES, Numbers.encodeLowHighInts(0, 0));
            Unsafe.putLong(partitionUpdateSinkAddr + 5 * Long.BYTES, 0L); // o3SplitPartitionSize
            Unsafe.putLong(partitionUpdateSinkAddr + 7 * Long.BYTES, parquetFileSize);
        } catch (Throwable th) {
            LOG.error().$("could not write fresh parquet partition [table=").$(tableWriter.getTableToken())
                    .$(", ts=").$ts(partitionTimestamp)
                    .$(", e=").$(th)
                    .I$();
            // Close parquetMetaFd before rmdir; on Windows the open _pm file
            // would prevent the partition dir from being removed, leaving an
            // orphan directory behind. The finally block tolerates -1.
            if (parquetMetaFd != -1) {
                ff.close(parquetMetaFd);
                parquetMetaFd = -1;
            }
            if (partitionDirCreated) {
                setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
                if (!ff.rmdir(path.slash())) {
                    LOG.error().$("could not remove fresh parquet partition dir [path=").$(path).I$();
                }
            }
            tableWriter.o3BumpErrorCount(CairoException.isCairoOomError(th));
            throw th;
        } finally {
            // Encoder calls descriptor.clear() in its own finally; descriptor
            // is non-owning so this is just a metadata reset either way.
            descriptor.clear();
            if (parquetMetaFd != -1) {
                ff.close(parquetMetaFd);
            }
            path.trimTo(pathSize);
            parquetPath.trimTo(pathSize);
            tableWriter.o3ClockDownPartitionUpdateCount();
            tableWriter.o3CountDownDoneLatch();
        }
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        processPartition(queue.get(cursor), cursor, subSeq);
        return true;
    }
}
