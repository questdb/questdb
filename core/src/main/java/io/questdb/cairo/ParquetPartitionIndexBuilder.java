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

import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Builds indexes from row 0 off a Parquet partition directory's own {@code data.parquet}, including covered values.
 * The caller must own the directory exclusively: either a compaction staging directory or a partition under
 * checkpoint recovery.
 * <p>
 * Holds no {@link TableWriter}, so it is the caller's to make sure the result is what the partition will look like
 * once published: every column read from row 0, i.e. every column top zero. Posting chain entries are tagged with the
 * table txn the caller built off, which is never above the txn that later publishes the directory, so writer-open
 * recovery keeps them. Not thread-safe.
 */
final class ParquetPartitionIndexBuilder implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(ParquetPartitionIndexBuilder.class);
    private final CairoConfiguration configuration;
    private final ParquetCoveredColumnAccumulator coveredColumnAccumulator = new ParquetCoveredColumnAccumulator();
    private final MemoryMAR ddlMem;
    private final ParquetPartitionDecoder decoder;
    private final FilesFacade ff;
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private final RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
    private final LongList rowGroupIndexRowLos = new LongList();
    private final IntList rowGroupOrder = new IntList();
    private final PostingSupersededFileRemover supersededFileRemover;
    private int commitMode = CommitMode.NOSYNC;
    private DirectIntList decodeColumns;
    private SymbolColumnIndexer indexer;
    private byte indexerType = -1;

    ParquetPartitionIndexBuilder(CairoConfiguration configuration, FilesFacade ff) {
        this.configuration = configuration;
        this.ff = ff;
        this.ddlMem = Vm.getPMARInstance(configuration);
        this.decoder = configuration.newParquetPartitionDecoder();
        this.supersededFileRemover = new PostingSupersededFileRemover(ff);
    }

    /**
     * Builds the index of every indexed SYMBOL column of the partition in {@code partitionDir}.
     *
     * @param partitionDir      the partition directory holding {@code data.parquet} and {@code _pm}; the index files go
     *                          there too. Trimmed back on return
     * @param parquetFileSize   the committed size of the directory's {@code data.parquet}
     * @param metadata          the table metadata the parquet file was written under
     * @param columnVersions    column versions of the same snapshot, for the column name txns
     * @param partitionRowCount the partition's row count
     * @param tableTxn          the table txn the caller built off; tags the posting chain entries
     * @param commitMode        the commit mode the caller writes the partition's data under; the index files are
     *                          flushed as the writer flushes its column files under it
     */
    void buildIndexes(
            Path partitionDir,
            long parquetFileSize,
            TableRecordMetadata metadata,
            ColumnVersionReader columnVersions,
            long partitionTimestamp,
            long partitionRowCount,
            long tableTxn,
            int commitMode
    ) {
        buildIndexes(partitionDir, parquetFileSize, metadata, columnVersions, partitionTimestamp,
                partitionRowCount, tableTxn, commitMode, -1);
    }

    /**
     * Builds just {@code columnIndex}, or every indexed SYMBOL column when it is -1. Restore uses this
     * overload to repair one posting index without rewriting the partition's other indexes.
     */
    void buildIndexes(
            Path partitionDir,
            long parquetFileSize,
            TableRecordMetadata metadata,
            ColumnVersionReader columnVersions,
            long partitionTimestamp,
            long partitionRowCount,
            long tableTxn,
            int commitMode,
            int columnIndex
    ) {
        this.commitMode = commitMode;
        final int dirLen = partitionDir.size();
        long parquetAddr = 0;
        long parquetSize = 0;
        try {
            final int columnLo = Math.max(0, columnIndex);
            final int columnHi = columnIndex < 0 ? metadata.getColumnCount() : columnIndex + 1;
            for (int i = columnLo; i < columnHi; i++) {
                if (!ColumnType.isSymbol(metadata.getColumnType(i)) || !metadata.isColumnIndexed(i)) {
                    continue;
                }
                if (parquetAddr == 0) {
                    // Opened on the first indexed column: a partition with none pays nothing.
                    partitionDir.trimTo(dirLen).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$();
                    ParquetMetaFileReader.openAndMapRO(ff, partitionDir.$(), parquetMetaReader);
                    if (parquetMetaReader.getAddr() == 0 || !parquetMetaReader.resolveFooter(parquetFileSize)) {
                        throw CairoException.critical(0)
                                .put("_pm tail does not match parquet file size [path=").put(partitionDir)
                                .put(", parquetFileSize=").put(parquetFileSize).put(']');
                    }
                    parquetSize = parquetMetaReader.getParquetFileSize();
                    partitionDir.trimTo(dirLen).concat(TableUtils.PARQUET_PARTITION_NAME).$();
                    parquetAddr = TableUtils.mapRO(ff, partitionDir.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    decoder.of(parquetMetaReader, parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    rowGroupBuffers.reopen();
                    if (decodeColumns == null) {
                        decodeColumns = new DirectIntList(8, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    }
                }
                partitionDir.trimTo(dirLen);
                buildIndex(
                        partitionDir,
                        dirLen,
                        metadata,
                        i,
                        columnVersions,
                        partitionTimestamp,
                        partitionRowCount,
                        tableTxn
                );
            }
        } finally {
            partitionDir.trimTo(dirLen);
            // The decoder borrows both mappings: release it before unmapping.
            Misc.free(decoder);
            rowGroupBuffers.close();
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            final long parquetMetaAddr = parquetMetaReader.getAddr();
            final long parquetMetaSize = parquetMetaReader.getFileSize();
            parquetMetaReader.clear();
            if (parquetMetaAddr != 0) {
                ff.munmap(parquetMetaAddr, parquetMetaSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
            }
        }
    }

    @Override
    public void close() {
        Misc.free(indexer);
        indexer = null;
        indexerType = -1;
        Misc.free(decoder);
        rowGroupBuffers.close();
        decodeColumns = Misc.free(decodeColumns);
        ddlMem.close();
    }

    /**
     * Registers the covered columns of one COVERING index with {@code coveredColumnAccumulator}. The file holds every
     * live column from row 0, so each is decoded and read with a zero top.
     */
    private static void addCoveredSlots(
            ParquetCoveredColumnAccumulator coveredColumnAccumulator,
            RecordMetadata metadata,
            ParquetMetaFileReader parquetMetadata,
            IntList coveredWriterIndices,
            ColumnVersionReader columnVersions,
            long partitionTimestamp
    ) {
        coveredColumnAccumulator.clear();
        for (int slot = 0, n = coveredWriterIndices.size(); slot < n; slot++) {
            final int writerIndex = coveredWriterIndices.getQuick(slot);
            final int denseIndex = writerIndex < 0 ? -1 : denseColumnIndex(metadata, writerIndex);
            if (denseIndex < 0 || metadata.getColumnType(denseIndex) <= 0) {
                coveredColumnAccumulator.addDroppedSlot();
                continue;
            }
            coveredColumnAccumulator.addSlot(
                    writerIndex,
                    metadata.getColumnType(denseIndex),
                    metadata.getColumnName(denseIndex),
                    columnVersions.getColumnNameTxn(partitionTimestamp, writerIndex),
                    0,
                    parquetColumnIndex(parquetMetadata, metadata, denseIndex)
            );
        }
    }

    private static int denseColumnIndex(RecordMetadata metadata, int writerIndex) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
                return i;
            }
        }
        return -1;
    }

    private static int parquetColumnIndex(ParquetMetaFileReader parquetMetadata, RecordMetadata metadata, int columnIndex) {
        // The file carries each column under its original writer index, see O3PartitionJob.
        final int columnId = metadata.getColumnMetadata(columnIndex).getOriginalWriterIndex();
        for (int i = 0, n = parquetMetadata.getColumnCount(); i < n; i++) {
            if (parquetMetadata.getColumnId(i) == columnId) {
                return i;
            }
        }
        return -1;
    }

    private void buildIndex(
            Path partitionDir,
            int dirLen,
            TableRecordMetadata metadata,
            int columnIndex,
            ColumnVersionReader columnVersions,
            long partitionTimestamp,
            long partitionRowCount,
            long tableTxn
    ) {
        final CharSequence columnName = metadata.getColumnName(columnIndex);
        final int parquetIndex = parquetColumnIndex(decoder.metadata(), metadata, columnIndex);
        if (parquetIndex < 0) {
            throw CairoException.critical(0)
                    .put("indexed column is missing from parquet partition [path=").put(partitionDir)
                    .put(", column=").put(columnName).put(']');
        }
        final byte indexType = metadata.getColumnIndexType(columnIndex);
        final long columnNameTxn = columnVersions.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        createIndexFiles(partitionDir, dirLen, columnName, columnNameTxn, indexType, metadata.getIndexValueBlockCapacity(columnIndex));
        if (partitionRowCount == 0) {
            return;
        }

        if (indexer == null || indexerType != indexType) {
            indexer = Misc.free(indexer);
            indexer = new SymbolColumnIndexer(configuration, indexType);
            indexerType = indexType;
        }
        try {
            indexer.getWriter().setCurrentTableTxn(tableTxn);
            // No partition name txn: the directory gets its name when the swap publishes it, and a seal purge
            // recorded here is never published - there is no writer to hand it to, and no reader to wait for.
            indexer.configureWriter(partitionDir.trimTo(dirLen), columnName, columnNameTxn, 0, partitionTimestamp, -1L);
            // After configureWriter: its of() resets the pending tag.
            indexer.getWriter().setNextTxnAtSeal(tableTxn);
            indexColumn(
                    configuration,
                    ff,
                    partitionDir,
                    dirLen,
                    decoder,
                    rowGroupBuffers,
                    decodeColumns,
                    coveredColumnAccumulator,
                    rowGroupOrder,
                    rowGroupIndexRowLos,
                    indexer,
                    metadata,
                    columnVersions,
                    columnIndex,
                    parquetIndex,
                    0,
                    partitionTimestamp
            );
            // Flushed as the writer flushes its column files at commit: under SYNC and ASYNC. Under ADAPTIVE the save
            // point flushes them, under NOSYNC nothing does. The swap that names the directory follows.
            if (CommitMode.appliesColumnSync(commitMode)) {
                indexer.sync(commitMode == CommitMode.ASYNC);
            }
        } finally {
            indexer.clear();
            partitionDir.trimTo(dirLen);
        }
        if (IndexType.isPosting(indexType)) {
            supersededFileRemover.remove(partitionDir, dirLen, columnName, columnNameTxn);
        }
    }

    private void createIndexFiles(
            Path partitionDir,
            int dirLen,
            CharSequence columnName,
            long columnNameTxn,
            byte indexType,
            int indexValueBlockCapacity
    ) {
        try {
            IndexFactory.keyFileName(indexType, partitionDir.trimTo(dirLen), columnName, columnNameTxn);
            try {
                ddlMem.smallFile(ff, partitionDir.$(), MemoryTag.MMAP_TABLE_WRITER);
                IndexFactory.initKeyMemory(indexType, ddlMem, indexValueBlockCapacity);
                // An empty index is published as written here.
                if (CommitMode.appliesColumnSync(commitMode)) {
                    ddlMem.sync(commitMode == CommitMode.ASYNC);
                }
            } finally {
                ddlMem.close();
            }
            if (!ff.touch(IndexFactory.valueFileName(indexType, partitionDir.trimTo(dirLen), columnName, columnNameTxn, 0L))) {
                throw CairoException.critical(ff.errno()).put("could not create index [name=").put(partitionDir).put(']');
            }
        } finally {
            partitionDir.trimTo(dirLen);
        }
    }

    /**
     * Indexes a SYMBOL column of a Parquet partition off its {@code data.parquet} and seals the index, with
     * {@code indexer} already configured for the column. A COVERING posting index gets its covered values decoded
     * alongside the column and staged in scratch files in the partition directory. The file holds every column it
     * carries from row 0, so the covered columns are read with a zero top, while the rows of the indexed column below
     * {@code columnTop} are left out of the index.
     *
     * @param decodeColumns            scratch list of the columns to decode, cleared on entry
     * @param coveredColumnAccumulator stages the covered values; its scratch files are released on every exit path
     * @param rowGroupOrder            scratch list for the row groups in the order of their index rows
     * @param rowGroupIndexRowLos      scratch list for each row group's first index row
     */
    static void indexColumn(
            CairoConfiguration configuration,
            FilesFacade ff,
            Path partitionDir,
            int dirLen,
            ParquetPartitionDecoder decoder,
            RowGroupBuffers rowGroupBuffers,
            DirectIntList decodeColumns,
            ParquetCoveredColumnAccumulator coveredColumnAccumulator,
            IntList rowGroupOrder,
            LongList rowGroupIndexRowLos,
            SymbolColumnIndexer indexer,
            RecordMetadata metadata,
            ColumnVersionReader columnVersions,
            int columnIndex,
            int parquetColumnIndex,
            long columnTop,
            long partitionTimestamp
    ) {
        final ParquetMetaFileReader parquetMetadata = decoder.metadata();
        final IntList coveredWriterIndices = metadata.getColumnMetadata(columnIndex).getCoveringColumnIndices();
        final boolean hasCovering = coveredWriterIndices != null && coveredWriterIndices.size() > 0;
        try {
            decodeColumns.clear();
            decodeColumns.add(parquetColumnIndex);
            decodeColumns.add(ColumnType.SYMBOL);
            // The covered scratch files hold one entry per index row.
            final long indexRowCount = parquetMetadata.getIndexRowCount();
            int decodedCoveredCount = 0;
            if (hasCovering) {
                addCoveredSlots(coveredColumnAccumulator, metadata, parquetMetadata, coveredWriterIndices, columnVersions, partitionTimestamp);
                decodedCoveredCount = coveredColumnAccumulator.openScratchFiles(
                        configuration, ff, partitionDir, dirLen, parquetMetadata, indexRowCount, decodeColumns);
            }

            final IndexWriter indexWriter = indexer.getWriter();
            // Index values grow per key, so the row groups go in the order of their index rows.
            parquetMetadata.collectRowGroupsInIndexRowOrder(rowGroupOrder, rowGroupIndexRowLos);
            long nextIndexRow = 0;
            for (int i = 0, n = rowGroupOrder.size(); i < n; i++) {
                final int rowGroup = rowGroupOrder.getQuick(i);
                final long rowGroupSize = parquetMetadata.getRowGroupSize(rowGroup);
                final long indexRowLo = rowGroupIndexRowLos.getQuick(rowGroup);
                final boolean isBelowColumnTop = indexRowLo + rowGroupSize <= columnTop;
                if (isBelowColumnTop && decodedCoveredCount == 0) {
                    nextIndexRow = indexRowLo + rowGroupSize;
                    continue;
                }
                assert rowGroupSize <= Integer.MAX_VALUE;
                decoder.decodeRowGroup(rowGroupBuffers, decodeColumns, rowGroup, 0, (int) rowGroupSize);
                if (decodedCoveredCount > 0) {
                    // Index rows of replaced row groups: no entry points at them.
                    coveredColumnAccumulator.accumulateNulls(indexRowLo - nextIndexRow);
                    coveredColumnAccumulator.accumulate(rowGroupBuffers, rowGroupSize);
                }
                nextIndexRow = indexRowLo + rowGroupSize;
                if (!isBelowColumnTop) {
                    final long startOffset = Math.max(0, columnTop - indexRowLo);
                    long rowId = indexRowLo + startOffset;
                    final long addr = rowGroupBuffers.getChunkDataPtr(0);
                    final long size = rowGroupBuffers.getChunkDataSize(0);
                    if (size == 0) {
                        BitmapIndexUtils.addNullEntries(indexWriter, rowId, indexRowLo + rowGroupSize);
                    } else {
                        for (long p = addr + startOffset * Integer.BYTES, lim = addr + size; p < lim; p += Integer.BYTES, rowId++) {
                            indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(p)), rowId);
                        }
                    }
                }
            }
            if (hasCovering) {
                final int timestampIndex = metadata.getTimestampIndex();
                coveredColumnAccumulator.configureCovering(indexer, timestampIndex < 0 ? -1 : metadata.getWriterIndex(timestampIndex));
            }
            indexWriter.setMaxValue(indexRowCount - 1);
            indexer.seal();
        } finally {
            if (hasCovering) {
                coveredColumnAccumulator.releaseScratchFiles(indexer, ff, partitionDir, dirLen);
            }
        }
    }
}
