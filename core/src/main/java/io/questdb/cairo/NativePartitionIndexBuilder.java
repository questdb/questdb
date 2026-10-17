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

import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Finishes the POSTING indexes of a native partition directory that nobody reads yet - the staging directory a
 * compaction REWRITE or a whole logical partition MERGE copied off a {@link TableReader} snapshot - so the swap that
 * publishes the directory stays metadata-only and the writer re-indexes nothing.
 * <p>
 * The copy itself already indexes every indexed column as it appends, but a POSTING index comes out of it as a run of
 * generations with no covered values. This does what the writer's swap-time reseal used to do, on the caller's
 * thread: it rebuilds each POSTING index from the column's data file into one dense chain entry and, for a COVERING
 * index, writes the covered values into its sidecars. BITMAP indexes need nothing more.
 * <p>
 * The chain entries are tagged with the table txn the caller built off. Nothing can read the directory before the
 * swap publishes it at a later txn, and the untagged generations the copy left behind carry txn 0, so the tag keeps
 * the chain non-decreasing and is never above the txn writer-open recovery checks it against. Not thread-safe.
 */
final class NativePartitionIndexBuilder implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(NativePartitionIndexBuilder.class);
    private final CairoConfiguration configuration;
    private final LongList coveredAddrs = new LongList();
    private final LongList coveredAuxAddrs = new LongList();
    private final LongList coveredAuxSizes = new LongList();
    private final LongList coveredNameTxns = new LongList();
    private final IntList coveredShifts = new IntList();
    private final LongList coveredSizes = new LongList();
    private final LongList coveredTops = new LongList();
    private final IntList coveredTypes = new IntList();
    private final FilesFacade ff;
    private final PostingSupersededFileRemover supersededFileRemover;
    private SymbolColumnIndexer indexer;
    private byte indexerType = -1;

    NativePartitionIndexBuilder(CairoConfiguration configuration, FilesFacade ff) {
        this.configuration = configuration;
        this.ff = ff;
        this.supersededFileRemover = new PostingSupersededFileRemover(ff);
    }

    /**
     * Finishes every POSTING index of the native partition in {@code partitionDir}.
     *
     * @param partitionDir       the staging directory the copy filled; trimmed back on return
     * @param metadata           the metadata the copy was built under
     * @param columnVersions     column versions of the same snapshot, for the name txns the copy wrote its files under
     * @param columnTops         the column tops the copy recorded, by writer index; a column that recorded none keeps
     *                           the one {@code columnVersions} holds, exactly as the swap publishes it
     * @param partitionTimestamp the timestamp the directory is published under
     * @param partitionRowCount  the rows the directory holds
     * @param tableTxn           the table txn the caller built off; tags the posting chain entries
     */
    void buildIndexes(
            Path partitionDir,
            TableRecordMetadata metadata,
            ColumnVersionReader columnVersions,
            ColumnTopRecorder columnTops,
            long partitionTimestamp,
            long partitionRowCount,
            long tableTxn
    ) {
        final int dirLen = partitionDir.size();
        try {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (!ColumnType.isSymbol(metadata.getColumnType(i)) || !metadata.isColumnIndexed(i)
                        || !IndexType.isPosting(metadata.getColumnIndexType(i))) {
                    continue;
                }
                final int writerIndex = metadata.getWriterIndex(i);
                final long columnTop = columnTop(columnTops, columnVersions, partitionTimestamp, writerIndex);
                // As the writer's reseal: a column with no rows in this directory has nothing to index.
                if (columnTop < 0 || columnTop >= partitionRowCount) {
                    continue;
                }
                buildIndex(
                        partitionDir,
                        dirLen,
                        metadata,
                        i,
                        columnVersions,
                        columnTops,
                        partitionTimestamp,
                        partitionRowCount,
                        columnTop,
                        tableTxn
                );
            }
        } finally {
            partitionDir.trimTo(dirLen);
        }
    }

    @Override
    public void close() {
        indexer = Misc.free(indexer);
        indexerType = -1;
    }

    private static long columnTop(ColumnTopRecorder columnTops, ColumnVersionReader columnVersions, long partitionTimestamp, int writerIndex) {
        final long recordedTop = columnTops.getColumnTop(writerIndex);
        return recordedTop >= 0 ? recordedTop : columnVersions.getColumnTop(partitionTimestamp, writerIndex);
    }

    private static int denseColumnIndex(TableRecordMetadata metadata, int writerIndex) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
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
            ColumnTopRecorder columnTops,
            long partitionTimestamp,
            long partitionRowCount,
            long columnTop,
            long tableTxn
    ) {
        final CharSequence columnName = metadata.getColumnName(columnIndex);
        final long columnNameTxn = columnVersions.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final byte indexType = metadata.getColumnIndexType(columnIndex);
        if (indexer == null || indexerType != indexType) {
            indexer = Misc.free(indexer);
            indexer = new SymbolColumnIndexer(configuration, indexType);
            indexerType = indexType;
        }
        final IntList coveredWriterIndices = metadata.getColumnMetadata(columnIndex).getCoveringColumnIndices();
        final int coverCount = coveredWriterIndices != null ? coveredWriterIndices.size() : 0;
        boolean isBuilt = false;
        try {
            if (coverCount > 0) {
                mapCoveredColumns(partitionDir, dirLen, metadata, coveredWriterIndices, columnVersions, columnTops, partitionTimestamp);
            }
            final IndexWriter writer = indexer.getWriter();
            writer.setCurrentTableTxn(tableTxn);
            // No partition name txn: the directory gets its name when the swap publishes it, and a seal purge
            // recorded here is never published - the superseded files are removed below instead.
            indexer.configureWriter(partitionDir.trimTo(dirLen), columnName, columnNameTxn, columnTop, partitionTimestamp, -1L);
            // After configureWriter: its of() resets the pending tag.
            writer.setNextTxnAtSeal(tableTxn);
            indexer.mergeTentativeIntoActiveIfAny();
            // Rebuild the chain from the column's data file into one dense entry, which supersedes every generation
            // the copy appended.
            writer.discardForRebuild();
            final long dataFd = TableUtils.openRO(ff, TableUtils.dFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn), LOG);
            try {
                indexer.index(ff, dataFd, columnTop, partitionRowCount);
            } finally {
                ff.close(dataFd);
            }
            writer.commitDense();
            if (coverCount > 0) {
                final int timestampIndex = metadata.getTimestampIndex();
                indexer.configureCovering(
                        coveredAddrs,
                        coveredAuxAddrs,
                        coveredTops,
                        coveredShifts,
                        coveredWriterIndices,
                        coveredTypes,
                        coverCount,
                        timestampIndex < 0 ? -1 : metadata.getWriterIndex(timestampIndex)
                );
                indexer.setCoveredColumnNameTxns(coveredNameTxns);
                indexer.setCoveredColumnAddrSizes(coveredSizes, coveredAuxSizes);
                writer.setNextTxnAtSeal(tableTxn);
                indexer.rebuildSidecars();
            }
            isBuilt = true;
        } finally {
            if (coverCount > 0) {
                if (indexer != null) {
                    indexer.releaseCoveredColumnReadMappings();
                }
                unmapCoveredColumns();
            }
            if (isBuilt) {
                indexer.clear();
            } else {
                // A failed configure or build can leave the indexer half closed: drop it, the next build makes another.
                indexer = Misc.free(indexer);
                indexerType = -1;
            }
            partitionDir.trimTo(dirLen);
        }
        supersededFileRemover.remove(partitionDir, dirLen, columnName, columnNameTxn);
    }

    /**
     * Maps the covered columns of one COVERING index out of {@code partitionDir}, the way the writer maps them for a
     * reseal. A slot whose column is gone keeps a zero address and type -1.
     */
    private void mapCoveredColumns(
            Path partitionDir,
            int dirLen,
            TableRecordMetadata metadata,
            IntList coveredWriterIndices,
            ColumnVersionReader columnVersions,
            ColumnTopRecorder columnTops,
            long partitionTimestamp
    ) {
        final int coverCount = coveredWriterIndices.size();
        coveredAddrs.setAll(coverCount, 0);
        coveredAuxAddrs.setAll(coverCount, 0);
        coveredSizes.setAll(coverCount, 0);
        coveredAuxSizes.setAll(coverCount, 0);
        coveredNameTxns.setAll(coverCount, TableUtils.COLUMN_NAME_TXN_NONE);
        coveredTops.setAll(coverCount, 0);
        coveredShifts.setAll(coverCount, 0);
        coveredTypes.setAll(coverCount, -1);
        for (int slot = 0; slot < coverCount; slot++) {
            final int writerIndex = coveredWriterIndices.getQuick(slot);
            final int denseIndex = writerIndex < 0 ? -1 : denseColumnIndex(metadata, writerIndex);
            if (denseIndex < 0 || metadata.getColumnType(denseIndex) <= 0) {
                continue;
            }
            final int columnType = metadata.getColumnType(denseIndex);
            final CharSequence columnName = metadata.getColumnName(denseIndex);
            final long columnNameTxn = columnVersions.getColumnNameTxn(partitionTimestamp, writerIndex);
            coveredTypes.setQuick(slot, columnType);
            coveredShifts.setQuick(slot, ColumnType.pow2SizeOf(columnType));
            coveredTops.setQuick(slot, Math.max(0, columnTop(columnTops, columnVersions, partitionTimestamp, writerIndex)));
            coveredNameTxns.setQuick(slot, columnNameTxn);
            mapCoveredFile(TableUtils.dFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn), coveredAddrs, coveredSizes, slot);
            if (ColumnType.isVarSize(columnType)) {
                mapCoveredFile(TableUtils.iFile(partitionDir.trimTo(dirLen), columnName, columnNameTxn), coveredAuxAddrs, coveredAuxSizes, slot);
            }
            partitionDir.trimTo(dirLen);
        }
    }

    private void mapCoveredFile(LPSZ file, LongList addrs, LongList sizes, int slot) {
        // A column whose rows all sit under its top has no file in the directory: nothing to map.
        if (!ff.exists(file)) {
            return;
        }
        final long fd = TableUtils.openRO(ff, file, LOG);
        try {
            final long size = ff.length(fd);
            if (size < 0) {
                throw CairoException.critical(ff.errno()).put("could not read covered column file size [path=").put(file).put(']');
            }
            if (size > 0) {
                addrs.setQuick(slot, TableUtils.mapRO(ff, fd, size, MemoryTag.MMAP_DEFAULT));
                sizes.setQuick(slot, size);
            }
        } finally {
            ff.close(fd);
        }
    }

    private void unmapCoveredColumns() {
        for (int slot = 0, n = coveredAddrs.size(); slot < n; slot++) {
            final long addr = coveredAddrs.getQuick(slot);
            if (addr != 0) {
                ff.munmap(addr, coveredSizes.getQuick(slot), MemoryTag.MMAP_DEFAULT);
            }
            final long auxAddr = coveredAuxAddrs.getQuick(slot);
            if (auxAddr != 0) {
                ff.munmap(auxAddr, coveredAuxSizes.getQuick(slot), MemoryTag.MMAP_DEFAULT);
            }
        }
        coveredAddrs.clear();
        coveredAuxAddrs.clear();
        coveredSizes.clear();
        coveredAuxSizes.clear();
    }
}
