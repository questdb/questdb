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
import io.questdb.cairo.idx.PostingIndexUtils;
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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Builds, from row 0, the index of every indexed SYMBOL column of a Parquet partition directory that nobody reads
 * yet - a compaction staging directory - off the directory's own {@code data.parquet}, including the covered values of
 * a COVERING posting index.
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
    private final SupersededSealedFileRemover supersededSealedFileRemover = new SupersededSealedFileRemover();
    private DirectIntList decodeColumns;
    private SymbolColumnIndexer indexer;
    private byte indexerType = -1;

    ParquetPartitionIndexBuilder(CairoConfiguration configuration, FilesFacade ff) {
        this.configuration = configuration;
        this.ff = ff;
        this.ddlMem = Vm.getPMARInstance(configuration);
        this.decoder = configuration.newParquetPartitionDecoder();
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
     */
    void buildIndexes(
            Path partitionDir,
            long parquetFileSize,
            TableRecordMetadata metadata,
            ColumnVersionReader columnVersions,
            long partitionTimestamp,
            long partitionRowCount,
            long tableTxn
    ) {
        final int dirLen = partitionDir.size();
        long parquetAddr = 0;
        long parquetSize = 0;
        try {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
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

    private static int denseColumnIndex(TableRecordMetadata metadata, int writerIndex) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
                return i;
            }
        }
        return -1;
    }

    private static int parquetColumnIndex(ParquetMetaFileReader parquetMetadata, TableRecordMetadata metadata, int columnIndex) {
        // The file carries each column under its original writer index, see O3PartitionJob.
        final int columnId = metadata.getColumnMetadata(columnIndex).getOriginalWriterIndex();
        for (int i = 0, n = parquetMetadata.getColumnCount(); i < n; i++) {
            if (parquetMetadata.getColumnId(i) == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Registers the covered columns of one COVERING index with {@link #coveredColumnAccumulator}. The file holds
     * every live column from row 0, so each is decoded and read with a zero top.
     */
    private void addCoveredSlots(
            TableRecordMetadata metadata,
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
        final ParquetMetaFileReader parquetMetadata = decoder.metadata();
        final CharSequence columnName = metadata.getColumnName(columnIndex);
        final int parquetIndex = parquetColumnIndex(parquetMetadata, metadata, columnIndex);
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
        final IntList coveredWriterIndices = metadata.getColumnMetadata(columnIndex).getCoveringColumnIndices();
        final boolean hasCovering = coveredWriterIndices != null && coveredWriterIndices.size() > 0;
        try {
            indexer.getWriter().setCurrentTableTxn(tableTxn);
            // No partition name txn: the directory gets its name when the swap publishes it, and a seal purge
            // recorded here is never published - there is no writer to hand it to, and no reader to wait for.
            indexer.configureWriter(partitionDir.trimTo(dirLen), columnName, columnNameTxn, 0, partitionTimestamp, -1L);
            // After configureWriter: its of() resets the pending tag.
            indexer.getWriter().setNextTxnAtSeal(tableTxn);

            decodeColumns.clear();
            decodeColumns.add(parquetIndex);
            decodeColumns.add(ColumnType.SYMBOL);
            int decodedCoveredCount = 0;
            if (hasCovering) {
                addCoveredSlots(metadata, parquetMetadata, coveredWriterIndices, columnVersions, partitionTimestamp);
                decodedCoveredCount = coveredColumnAccumulator.openScratchFiles(
                        configuration, ff, partitionDir, dirLen, parquetMetadata, partitionRowCount, decodeColumns);
            }

            final IndexWriter indexWriter = indexer.getWriter();
            long rowCount = 0;
            for (int rowGroup = 0, n = parquetMetadata.getRowGroupCount(); rowGroup < n; rowGroup++) {
                final long rowGroupSize = parquetMetadata.getRowGroupSize(rowGroup);
                decoder.decodeRowGroup(rowGroupBuffers, decodeColumns, rowGroup, 0, (int) rowGroupSize);
                if (decodedCoveredCount > 0) {
                    coveredColumnAccumulator.accumulate(rowGroupBuffers, rowGroup, rowGroupSize);
                }
                final long addr = rowGroupBuffers.getChunkDataPtr(0);
                final long size = rowGroupBuffers.getChunkDataSize(0);
                if (size == 0) {
                    BitmapIndexUtils.addNullEntries(indexWriter, rowCount, rowCount + rowGroupSize);
                } else {
                    long rowId = rowCount;
                    for (long p = addr, lim = addr + size; p < lim; p += Integer.BYTES, rowId++) {
                        indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(p)), rowId);
                    }
                }
                rowCount += rowGroupSize;
            }
            if (hasCovering) {
                final int timestampIndex = metadata.getTimestampIndex();
                coveredColumnAccumulator.configureCovering(indexer, timestampIndex < 0 ? -1 : metadata.getWriterIndex(timestampIndex));
            }
            indexWriter.setMaxValue(partitionRowCount - 1);
            indexer.seal();
        } finally {
            if (hasCovering) {
                coveredColumnAccumulator.releaseScratchFiles(indexer, ff, partitionDir, dirLen);
            }
            indexer.clear();
            partitionDir.trimTo(dirLen);
        }
        if (IndexType.isPosting(indexType)) {
            supersededSealedFileRemover.remove(partitionDir, dirLen, columnName, columnNameTxn);
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
     * Deletes the sealed generations of one posting index that its live head superseded - the pre-seal files the
     * build started from. On a published partition that is the purge job's, once no reader can still pin them; here
     * nobody reads the directory yet, so they go straight away instead of being published into the partition.
     */
    private class SupersededSealedFileRemover implements PostingIndexUtils.SealedFileVisitor {
        private CharSequence columnName;
        private long columnNameTxn;
        private Path dir;
        private int dirLen;
        private long liveSealTxn;

        @Override
        public void onCoverDataFile(int includeIdx, long postingColumnNameTxn, long coveredColumnNameTxn, long sealTxn) {
            if (postingColumnNameTxn == columnNameTxn && sealTxn != liveSealTxn) {
                ff.removeQuiet(PostingIndexUtils.coverDataFileName(
                        dir.trimTo(dirLen), columnName, includeIdx, postingColumnNameTxn, coveredColumnNameTxn, sealTxn));
                dir.trimTo(dirLen);
            }
        }

        @Override
        public void onValueFile(long postingColumnNameTxn, long sealTxn) {
            if (postingColumnNameTxn == columnNameTxn && sealTxn != liveSealTxn) {
                ff.removeQuiet(PostingIndexUtils.valueFileName(dir.trimTo(dirLen), columnName, postingColumnNameTxn, sealTxn));
                dir.trimTo(dirLen);
            }
        }

        void remove(Path dir, int dirLen, CharSequence columnName, long columnNameTxn) {
            try {
                liveSealTxn = PostingIndexUtils.readSealTxnFromKeyFile(ff, PostingIndexUtils.keyFileName(dir.trimTo(dirLen), columnName, columnNameTxn));
                if (liveSealTxn < 0) {
                    // No readable head: keep everything rather than guess which generation is live.
                    return;
                }
                this.dir = dir;
                this.dirLen = dirLen;
                this.columnName = columnName;
                this.columnNameTxn = columnNameTxn;
                PostingIndexUtils.scanSealedFiles(ff, dir, dirLen, columnName, this);
            } finally {
                this.dir = null;
                this.columnName = null;
                dir.trimTo(dirLen);
            }
        }
    }
}
