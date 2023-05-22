/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final int PARTITIONS_SLOT_OFFSET_COLUMN_VERSION = 3;
    private static final int PARTITIONS_SLOT_OFFSET_NAME_TXN = 2;
    private static final int PARTITIONS_SLOT_OFFSET_SIZE = 1;
    private static final int PARTITIONS_SLOT_SIZE = 4;
    private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
    private final MillisecondClock clock;
    private final ColumnVersionReader columnVersionReader;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final int maxOpenPartitions;
    private final MessageBus messageBus;
    private final TableReaderMetadata metadata;
    private final LongList openPartitionInfo;
    private final int partitionBy;
    private final Path path;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final int rootLen;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final MemoryMR todoMem = Vm.getMRInstance();
    private final TxReader txFile;
    private final TxnScoreboard txnScoreboard;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountShl;
    private LongList columnTops;
    private ObjList<MemoryMR> columns;
    private int openPartitionCount;
    private int partitionCount;
    private long rowCount;
    private TableToken tableToken;
    private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_TABLE_READER);
    private long txColumnVersion = -1;
    private long txPartitionVersion = -1;
    private long txTruncateVersion = -1;
    private long txn = TableUtils.INITIAL_TXN;
    private boolean txnAcquired = false;

    public TableReader(CairoConfiguration configuration, TableToken tableToken) {
        this(configuration, tableToken, null);
    }

    public TableReader(CairoConfiguration configuration,
                       TableToken tableToken,
                       @Nullable MessageBus messageBus
    ) {
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.maxOpenPartitions = configuration.getInactiveReaderMaxOpenPartitions();
        this.ff = configuration.getFilesFacade();
        this.tableToken = tableToken;
        this.messageBus = messageBus;
        this.path = new Path();
        this.path.of(configuration.getRoot()).concat(this.tableToken.getDirName());
        this.rootLen = path.length();
        path.trimTo(rootLen);
        try {
            metadata = openMetaFile();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(rootLen));
            LOG.debug()
                    .$("open [id=").$(metadata.getTableId())
                    .$(", table=").utf8(this.tableToken.getTableName())
                    .$(", dirName=").utf8(this.tableToken.getDirName())
                    .I$();
            txFile = new TxReader(ff).ofRO(path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), partitionBy);
            path.trimTo(rootLen);
            reloadSlow(false);
            columnCount = metadata.getColumnCount();
            columnCountShl = getColumnBits(columnCount);
            openSymbolMaps();
            partitionCount = txFile.getPartitionCount();

            int capacity = getColumnBase(partitionCount);
            columns = new ObjList<>(capacity + 2);
            columns.setPos(capacity + 2);
            columns.setQuick(0, NullMemoryMR.INSTANCE);
            columns.setQuick(1, NullMemoryMR.INSTANCE);
            bitmapIndexes = new ObjList<>(capacity + 2);
            bitmapIndexes.setPos(capacity + 2);

            openPartitionInfo = new LongList(partitionCount * PARTITIONS_SLOT_SIZE);
            openPartitionInfo.setPos(partitionCount * PARTITIONS_SLOT_SIZE);
            for (int i = 0; i < partitionCount; i++) {
                // ts, number of rows, txn, column version for each partition
                // it is compared to attachedPartitions within the txn file to determine if a partition needs to be reloaded or not
                int baseOffset = i * PARTITIONS_SLOT_SIZE;
                openPartitionInfo.setQuick(baseOffset, txFile.getPartitionTimestampByIndex(i));
                openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_SIZE, -1L); // -1L means it is not open
                openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txFile.getPartitionNameTxn(i));
                openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, txFile.getPartitionColumnVersion(i));
            }
            columnTops = new LongList(capacity / 2);
            columnTops.setPos(capacity / 2);
            recordCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int base, int index) {
        return 2 + base + index * 2;
    }

    @TestOnly
    public int calculateOpenPartitionCount() {
        int openPartitionCount = 0;
        for (int partitionIndex = partitionCount - 1; partitionIndex > -1; partitionIndex--) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
            if (partitionSize > -1L) {
                ++openPartitionCount;
            }
        }
        return openPartitionCount;
    }

    @Override
    public void close() {
        if (isOpen()) {
            goPassive();
            freeSymbolMapReaders();
            freeBitmapIndexCache();
            Misc.free(metadata);
            Misc.free(txFile);
            Misc.free(todoMem);
            freeColumns();
            freeTempMem();
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(columnVersionReader);
            LOG.debug().$("closed '").utf8(tableToken.getTableName()).$('\'').$();
        }
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return txFile.getPartitionTimestampByTimestamp(timestamp);
    }

    public BitmapIndexReader getBitmapIndexReader(int partitionIndex, int columnIndex, int direction) {
        int columnBase = getColumnBase(partitionIndex);
        return getBitmapIndexReader(partitionIndex, columnBase, columnIndex, direction);
    }

    public BitmapIndexReader getBitmapIndexReader(int partitionIndex, int columnBase, int columnIndex, int direction) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
        final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final long partitionTxn = txFile.getPartitionNameTxn(partitionIndex);

        BitmapIndexReader reader = bitmapIndexes.getQuick(direction == BitmapIndexReader.DIR_BACKWARD ? index : index + 1);
        if (reader != null) {
            // make sure to reload the reader
            final String columnName = metadata.getColumnName(columnIndex);
            final long columnTop = getColumnTop(columnBase, columnIndex);
            Path path = pathGenPartitioned(partitionIndex);
            try {
                reader.of(configuration, path, columnName, columnNameTxn, columnTop);
            } finally {
                path.trimTo(rootLen);
            }
            return reader;
        }
        return createBitmapIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, partitionTxn);
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountShl;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getColumnTop(int base, int columnIndex) {
        return columnTops.getQuick(base / 2 + columnIndex);
    }

    public ColumnVersionReader getColumnVersionReader() {
        return columnVersionReader;
    }

    public TableReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long getDataVersion() {
        return this.txFile.getDataVersion();
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public int getMaxUncommittedRows() {
        return metadata.getMaxUncommittedRows();
    }

    public TableReaderMetadata getMetadata() {
        return metadata;
    }

    public long getMinTimestamp() {
        return txFile.getMinTimestamp();
    }

    public long getO3MaxLag() {
        return metadata.getO3MaxLag();
    }

    public int getOpenPartitionCount() {
        return openPartitionCount;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionIndexByTimestamp(long timestamp) {
        int end = openPartitionInfo.binarySearchBlock(PARTITIONS_SLOT_SIZE_MSB, timestamp, BinarySearch.SCAN_UP);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            return (-end - 2) / PARTITIONS_SLOT_SIZE;
        }
        return end / PARTITIONS_SLOT_SIZE;
    }

    public long getPartitionRowCount(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
    }

    public long getPartitionTimestampByIndex(int partitionIndex) {
        return txFile.getPartitionTimestampByIndex(partitionIndex);
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public long getTransientRowCount() {
        return txFile.getTransientRowCount();
    }

    public TxReader getTxFile() {
        return txFile;
    }

    public long getTxn() {
        return txn;
    }

    public long getTxnMetadataVersion() {
        return txFile.getMetadataVersion();
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    public long getVersion() {
        return txFile.getMetadataVersion();
    }

    public void goActive() {
        reload();
    }

    public void goPassive() {
        if (releaseTxn() && PartitionBy.isPartitioned(partitionBy)) {
            // check if reader unlocks a transaction in scoreboard
            // to house keep the partition versions
            checkSchedulePurgeO3Partitions();
        }
        // close all but N latest partitions
        if (PartitionBy.isPartitioned(partitionBy) && openPartitionCount > maxOpenPartitions) {
            final int originallyOpen = openPartitionCount;
            int openCount = 0;
            for (int partitionIndex = partitionCount - 1; partitionIndex > -1; partitionIndex--) {
                final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionSize > -1L && ++openCount > maxOpenPartitions) {
                    closePartition(partitionIndex);
                    if (openCount == originallyOpen) {
                        // ok, we've closed enough
                        break;
                    }
                }
            }
        }
    }

    public boolean isActive() {
        return txnAcquired;
    }

    public boolean isColumnCached(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex).isCached();
    }

    public boolean isOpen() {
        return tempMem8b != 0L;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

    /**
     * Opens given partition for reading.
     *
     * @param partitionIndex partition index
     * @return partition size in rows
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1L) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    public void reconcileOpenPartitionsFrom(int partitionIndex, boolean forceTruncate) {
        int txPartitionCount = txFile.getPartitionCount();
        int txPartitionIndex = partitionIndex;
        boolean changed = false;

        while (partitionIndex < partitionCount && txPartitionIndex < txPartitionCount) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            final long txPartTs = txFile.getPartitionTimestampByIndex(txPartitionIndex);
            final long openPartitionTimestamp = openPartitionInfo.getQuick(offset);

            if (openPartitionTimestamp < txPartTs) {
                // Deleted partitions
                // This will decrement partitionCount
                closeDeletedPartition(partitionIndex);
            } else if (openPartitionTimestamp > txPartTs) {
                // Insert partition
                insertPartition(partitionIndex, txPartTs);
                changed = true;
                txPartitionIndex++;
                partitionIndex++;
            } else {
                // Refresh partition
                final long newPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                final long txPartitionColumnVersion = txFile.getPartitionColumnVersion(partitionIndex);
                final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                final long openPartitionColumnVersion = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION);

                if (!forceTruncate) {
                    if (openPartitionNameTxn == txPartitionNameTxn && openPartitionColumnVersion == txPartitionColumnVersion) {
                        if (openPartitionSize != newPartitionSize) {
                            if (openPartitionSize > -1L) {
                                reloadPartition(partitionIndex, newPartitionSize, txPartitionNameTxn);
                                openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, newPartitionSize);
                                LOG.debug().$("updated partition size [partition=").$(openPartitionTimestamp).I$();
                            }
                            changed = true;
                        }
                    } else {
                        reopenPartition(offset, partitionIndex, txPartitionNameTxn);
                        changed = true;
                    }
                } else if (openPartitionSize > -1L && newPartitionSize > -1L) { // Don't force re-open if not yet opened
                    reopenPartition(offset, partitionIndex, txPartitionNameTxn);
                }
                txPartitionIndex++;
                partitionIndex++;
            }
        }

        // if while finished on txPartitionIndex == txPartitionCount condition
        // remove deleted opened partitions
        while (partitionIndex < partitionCount) {
            closeDeletedPartition(partitionIndex);
            changed = true;
        }

        // if while finished on partitionIndex == partitionCount condition
        // insert new partitions at the end
        for (; partitionIndex < txPartitionCount; partitionIndex++) {
            insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex));
            changed = true;
        }

        if (forceTruncate) {
            reloadAllSymbols();
        } else if (changed) {
            reloadSymbolMapCounts();
        }
    }

    public boolean reload() {
        if (acquireTxn()) {
            return false;
        }
        try {
            reloadSlow(true);
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            reconcileOpenPartitions(txPartitionVersion, txColumnVersion, txTruncateVersion);

            // Save transaction details which impact the reloading. Do not rely on txReader, it can be reloaded outside this method.
            txPartitionVersion = txFile.getPartitionTableVersion();
            txColumnVersion = txFile.getColumnVersion();
            txTruncateVersion = txFile.getTruncateVersion();

            // Useful for debugging
            // assert DebugUtils.reconcileColumnTops(PARTITIONS_SLOT_SIZE, openPartitionInfo, columnVersionReader, this);
            return true;
        } catch (Throwable e) {
            releaseTxn();
            throw e;
        }
    }

    public long size() {
        return rowCount;
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.updateTableToken(tableToken);
    }

    private static int getColumnBits(int columnCount) {
        return Numbers.msb(Numbers.ceilPow2(columnCount) * 2);
    }

    private static void growColumn(MemoryR mem1, MemoryR mem2, int type, long rowCount) {
        if (rowCount > 0) {
            if (ColumnType.isVariableLength(type)) {
                assert mem2 != null;
                mem2.extend((rowCount + 1) * 8);
                mem1.extend(mem2.getLong(rowCount * 8));
            } else {
                mem1.extend(rowCount << ColumnType.pow2SizeOf(type));
            }
        }
    }

    private boolean acquireTxn() {
        if (!txnAcquired) {
            if (txnScoreboard.acquireTxn(txn)) {
                txnAcquired = true;
            } else {
                return false;
            }
        }

        // txFile can also be reloaded in goPassive->checkSchedulePurgeO3Partitions
        // if txFile txn doesn't much reader txn, reader has to be slow reloaded
        if (txn == txFile.getTxn()) {
            // We have to be sure last txn is acquired in Scoreboard
            // otherwise writer can delete partition version files
            // between reading txn file and acquiring txn in the Scoreboard.
            Unsafe.getUnsafe().loadFence();
            return txFile.getVersion() == txFile.unsafeReadVersion();
        }
        return false;
    }

    private void checkSchedulePurgeO3Partitions() {
        long txnLocks = txnScoreboard.getActiveReaderCount(txn);
        long partitionTableVersion = txFile.getPartitionTableVersion();
        if (txnLocks == 0 && txFile.unsafeLoadAll() && txFile.getPartitionTableVersion() > partitionTableVersion) {
            // Last lock for this txn is released and this is not latest txn number
            // Schedule a job to clean up partition versions this reader may hold
            if (TableUtils.schedulePurgeO3Partitions(messageBus, tableToken, partitionBy)) {
                return;
            }

            LOG.error()
                    .$("could not queue purge partition task, queue is full [")
                    .$("dirName=").utf8(tableToken.getDirName())
                    .$(", txn=").$(txn)
                    .$(']').$();
        }
    }

    private void closeDeletedPartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        int columnBase = getColumnBase(partitionIndex);
        if (partitionSize > -1L) {
            for (int k = 0; k < columnCount; k++) {
                closePartitionColumnFile(columnBase, k);
            }
            openPartitionCount--;
        }
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);

        int colTopStart = columnBase / 2;
        int columnSlotSize = getColumnBase(1);
        columnTops.removeIndexBlock(colTopStart, columnSlotSize / 2);

        openPartitionInfo.removeIndexBlock(offset, PARTITIONS_SLOT_SIZE);
        LOG.info().$("closed deleted partition [table=").$(tableToken).$(", ts=").$ts(partitionTimestamp).$(", partitionIndex=").$(partitionIndex).I$();
        partitionCount--;
    }

    private void closePartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        int columnBase = getColumnBase(partitionIndex);
        if (partitionSize > -1L) {
            for (int k = 0; k < columnCount; k++) {
                closePartitionColumnFile(columnBase, k);
            }
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1L);
            openPartitionCount--;

            LOG.debug().$("closed partition [path=").$(path).$(", timestamp=").$ts(partitionTimestamp).I$();
        }
    }

    private void closePartitionColumnFile(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        Misc.free(columns.getAndSetQuick(index, NullMemoryMR.INSTANCE));
        Misc.free(columns.getAndSetQuick(index + 1, NullMemoryMR.INSTANCE));
        Misc.free(bitmapIndexes.getAndSetQuick(index, null));
        Misc.free(bitmapIndexes.getAndSetQuick(index + 1, null));
    }

    private long closeRewrittenPartitionFiles(int partitionIndex, int oldBase) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTs = openPartitionInfo.getQuick(offset);
        long existingPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        long newNameTxn = txFile.getPartitionNameTxnByPartitionTimestamp(partitionTs);
        long newSize = txFile.getPartitionSizeByPartitionTimestamp(partitionTs);
        if (existingPartitionNameTxn != newNameTxn || newSize < 0) {
            LOG.debug().$("close outdated partition files [table=").utf8(tableToken.getTableName()).$(", ts=").$ts(partitionTs).$(", nameTxn=").$(newNameTxn).$();
            // Close all columns, partition is overwritten. Partition reconciliation process will re-open correct files
            for (int i = 0; i < columnCount; i++) {
                closePartitionColumnFile(oldBase, i);
            }
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1L);
            openPartitionCount--;
            return -1;
        }
        pathGenPartitioned(partitionIndex);
        return newSize;
    }

    private void copyColumns(
            int fromBase,
            int fromColumnIndex,
            ObjList<MemoryMR> toColumns,
            LongList toColumnTops,
            ObjList<BitmapIndexReader> toIndexReaders,
            int toBase,
            int toColumnIndex
    ) {
        final int fromIndex = getPrimaryColumnIndex(fromBase, fromColumnIndex);
        final int toIndex = getPrimaryColumnIndex(toBase, toColumnIndex);

        toColumns.setQuick(toIndex, columns.getAndSetQuick(fromIndex, null));
        toColumns.setQuick(toIndex + 1, columns.getAndSetQuick(fromIndex + 1, null));
        toColumnTops.setQuick(toBase / 2 + toColumnIndex, columnTops.getQuick(fromBase / 2 + fromColumnIndex));
        toIndexReaders.setQuick(toIndex, bitmapIndexes.getAndSetQuick(fromIndex, null));
        toIndexReaders.setQuick(toIndex + 1, bitmapIndexes.getAndSetQuick(fromIndex + 1, null));
    }

    private void copyOrRenewSymbolMapReader(SymbolMapReader reader, int columnIndex) {
        if (reader != null && reader.isDeleted()) {
            reader = reloadSymbolMapReader(columnIndex, reader);
        }
        symbolMapReaders.setQuick(columnIndex, reader);
    }

    private BitmapIndexReader createBitmapIndexReaderAt(int globalIndex, int columnBase, int columnIndex, long columnNameTxn, int direction, long txn) {
        BitmapIndexReader reader;
        if (!metadata.isColumnIndexed(columnIndex)) {
            throw CairoException.critical(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
        }

        MemoryR col = columns.getQuick(globalIndex);
        if (col instanceof NullMemoryMR) {
            if (direction == BitmapIndexReader.DIR_BACKWARD) {
                reader = new BitmapIndexBwdNullReader();
                bitmapIndexes.setQuick(globalIndex, reader);
            } else {
                reader = new BitmapIndexFwdNullReader();
                bitmapIndexes.setQuick(globalIndex + 1, reader);
            }
        } else {
            Path path = pathGenPartitioned(getPartitionIndex(columnBase), txn);
            try {
                if (direction == BitmapIndexReader.DIR_BACKWARD) {
                    reader = new BitmapIndexBwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            getColumnTop(columnBase, columnIndex)
                    );
                    bitmapIndexes.setQuick(globalIndex, reader);
                } else {
                    reader = new BitmapIndexFwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            getColumnTop(columnBase, columnIndex)
                    );
                    bitmapIndexes.setQuick(globalIndex + 1, reader);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        return reader;
    }

    private void createNewColumnList(int columnCount, long pTransitionIndex, int columnCountShl) {
        LOG.debug().$("resizing columns file list [table=").utf8(tableToken.getTableName()).I$();
        int capacity = partitionCount << columnCountShl;
        final ObjList<MemoryMR> toColumns = new ObjList<>(capacity + 2);
        final LongList toColumnTops = new LongList(capacity / 2);
        final ObjList<BitmapIndexReader> toIndexReaders = new ObjList<>(capacity);
        toColumns.setPos(capacity + 2);
        toColumns.setQuick(0, NullMemoryMR.INSTANCE);
        toColumns.setQuick(1, NullMemoryMR.INSTANCE);
        toColumnTops.setPos(capacity / 2);
        toIndexReaders.setPos(capacity + 2);
        final long pIndexBase = pTransitionIndex + 8;
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int toBase = partitionIndex << columnCountShl;
            final int fromBase = partitionIndex << this.columnCountShl;

            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, fromBase)) > -1L) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
                        final int fromColumnIndex = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4L);

                        if (action == -1) {
                            closePartitionColumnFile(fromBase, i);
                        }

                        if (fromColumnIndex > -1) {
                            assert fromColumnIndex < this.columnCount;
                            copyColumns(fromBase, fromColumnIndex, toColumns, toColumnTops, toIndexReaders, toBase, i);
                        } else if (fromColumnIndex != Integer.MIN_VALUE) {
                            // new instance
                            reloadColumnAt(partitionIndex, path, toColumns, toColumnTops, toIndexReaders, toBase, i, partitionRowCount);
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        this.columns = toColumns;
        this.columnTops = toColumnTops;
        this.columnCountShl = columnCountShl;
        this.bitmapIndexes = toIndexReaders;
    }

    private void formatErrorPartitionDirName(int partitionIndex, CharSink sink) {
        TableUtils.setSinkForPartition(
                sink,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                -1L
        );
    }

    private void formatPartitionDirName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForPartition(
                sink,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn
        );
    }

    private void freeBitmapIndexCache() {
        Misc.freeObjList(bitmapIndexes);
    }

    private void freeColumns() {
        Misc.freeObjList(columns);
    }

    private void freeSymbolMapReaders() {
        for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
            Misc.freeIfCloseable(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
    }

    private void freeTempMem() {
        if (tempMem8b != 0L) {
            Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
            tempMem8b = 0L;
        }
    }

    private void insertPartition(int partitionIndex, long timestamp) {
        final int columnBase = getColumnBase(partitionIndex);
        final int columnSlotSize = getColumnBase(1);

        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize, NullMemoryMR.INSTANCE);
        bitmapIndexes.insert(idx, columnSlotSize, null);

        final int topBase = columnBase / 2;
        final int topSlotSize = columnSlotSize / 2;
        columnTops.insert(topBase, topSlotSize);
        columnTops.seed(topBase, topSlotSize, 0);

        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        openPartitionInfo.insert(offset, PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setQuick(offset, timestamp);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1L);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, -1L);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, -1L);
        partitionCount++;
        LOG.debug().$("inserted partition [index=").$(partitionIndex).$(", table=").$(tableToken).$(", timestamp=").$ts(timestamp).I$();
    }

    @NotNull
    // this method is not thread safe
    private SymbolMapReaderImpl newSymbolMapReader(int symbolColumnIndex, int columnIndex) {
        // symbol column index is the index of symbol column in dense array of symbol columns, e.g.
        // if table has only one symbol columns, the symbolColumnIndex is 0 regardless of column position
        // in the metadata.
        return new SymbolMapReaderImpl(
                configuration,
                path,
                metadata.getColumnName(columnIndex),
                columnVersionReader.getDefaultColumnNameTxn(metadata.getWriterIndex(columnIndex)),
                txFile.getSymbolValueCount(symbolColumnIndex)
        );
    }

    private TableReaderMetadata openMetaFile() {
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken);
        try {
            metadata.load();
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    @NotNull
    private MemoryMR openOrCreateMemory(
            Path path,
            ObjList<MemoryMR> columns,
            int primaryIndex,
            MemoryMR mem,
            long columnSize
    ) {
        if (mem != null && mem != NullMemoryMR.INSTANCE) {
            mem.of(ff, path, columnSize, columnSize, MemoryTag.MMAP_TABLE_READER);
        } else {
            mem = Vm.getMRInstance(ff, path, columnSize, MemoryTag.MMAP_TABLE_READER);
            columns.setQuick(primaryIndex, mem);
        }
        return mem;
    }

    private long openPartition0(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        final boolean isReopen = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE) > -1L;

        if (txFile.getPartitionCount() < 2 && txFile.getTransientRowCount() == 0) {
            // Empty single partition. Don't check that directory exists on the disk
            if (isReopen) {
                // We had this partition open, so close the column files.
                // We'll reopen them on a later attempt when there are some rows.
                closePartition(partitionIndex);
            }
            return -1;
        }

        try {
            final long partitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            Path path = pathGenPartitioned(partitionIndex, partitionNameTxn);

            if (ff.exists(path.$())) {
                final long partitionSize = txFile.getPartitionSize(partitionIndex);
                if (partitionSize > -1L) {
                    LOG.info()
                            .$("open partition ").utf8(path)
                            .$(" [rowCount=").$(partitionSize)
                            .$(", partitionNameTxn=").$(partitionNameTxn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", partitionIndex=").$(partitionIndex)
                            .$(", partitionCount=").$(partitionCount)
                            .I$();

                    openPartitionColumns(partitionIndex, path, getColumnBase(partitionIndex), partitionSize);
                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, partitionNameTxn);
                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, txFile.getPartitionColumnVersion(partitionIndex));
                    if (!isReopen) {
                        openPartitionCount++;
                    }
                }

                return partitionSize;
            }
            LOG.error().$("open partition failed, partition does not exist on the disk [path=").utf8(path).I$();

            if (PartitionBy.isPartitioned(getPartitionedBy())) {
                CairoException exception = CairoException.critical(0).put("Partition '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("' does not exist in table '")
                        .put(tableToken.getTableName())
                        .put("' directory. Run [ALTER TABLE ").put(tableToken.getTableName()).put(" DROP PARTITION LIST '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("'] to repair the table or restore the partition directory.");
                throw exception;
            } else {
                throw CairoException.critical(0).put("Table '").put(tableToken.getTableName())
                        .put("' data directory does not exist on the disk at ")
                        .put(path)
                        .put(". Restore data on disk or drop the table.");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(int partitionIndex, Path path, int columnBase, long partitionRowCount) {
        for (int i = 0; i < columnCount; i++) {
            reloadColumnAt(
                    partitionIndex,
                    path,
                    columns,
                    columnTops,
                    bitmapIndexes,
                    columnBase,
                    i,
                    partitionRowCount
            );
        }
    }

    private void openSymbolMaps() {
        int symbolColumnIndex = 0;
        final int columnCount = metadata.getColumnCount();
        symbolMapReaders.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                // symbol map index array is sparse
                symbolMapReaders.extendAndSet(i, newSymbolMapReader(symbolColumnIndex++, i));
            }
        }
    }

    private Path pathGenPartitioned(int partitionIndex) {
        long nameTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        return pathGenPartitioned(partitionIndex, nameTxn);
    }

    private Path pathGenPartitioned(int partitionIndex, long nameTxn) {
        formatPartitionDirName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private void readTxnSlow(long deadline) {
        int count = 0;

        while (true) {
            if (txFile.unsafeLoadAll()) {
                // good, very stable, congrats
                long txn = txFile.getTxn();
                releaseTxn();
                this.txn = txn;

                if (acquireTxn()) {
                    this.rowCount = txFile.getFixedRowCount() + txFile.getTransientRowCount();
                    LOG.debug()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", fixedRowCount=").$(txFile.getFixedRowCount())
                            .$(", maxTimestamp=").$ts(txFile.getMaxTimestamp())
                            .$(", attempts=").$(count)
                            .$(", thread=").$(Thread.currentThread().getName())
                            .I$();
                    break;
                }
            }
            // This is unlucky, sequences have changed while we were reading transaction data
            // We must discard and try again
            count++;
            if (clock.getTicks() > deadline) {
                LOG.error().$("tx read timeout [timeout=").$(configuration.getSpinLockTimeout()).$("ms]").$();
                throw CairoException.critical(0).put("Transaction read timeout");
            }
            Os.pause();
        }
    }

    private void reconcileOpenPartitions(long prevPartitionVersion, long prevColumnVersion, long prevTruncateVersion) {
        // Reconcile partition full or partial will only update row count of last partition and append new partitions
        boolean truncateHappened = txFile.getTruncateVersion() != prevTruncateVersion;
        if (txFile.getPartitionTableVersion() == prevPartitionVersion && txFile.getColumnVersion() == prevColumnVersion && !truncateHappened) {
            int partitionIndex = Math.max(0, partitionCount - 1);
            final int txPartitionCount = txFile.getPartitionCount();
            if (partitionIndex < txPartitionCount) {
                if (partitionIndex < partitionCount) {
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                    // we check that open partition size is non-negative to avoid loading
                    // partition that is not yet in memory
                    if (openPartitionSize > -1L) {
                        final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                        final long txPartitionSize = txFile.getPartitionSize(partitionIndex);
                        final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);

                        if (openPartitionNameTxn == txPartitionNameTxn) {
                            if (openPartitionSize != txPartitionSize) {
                                reloadPartition(partitionIndex, txPartitionSize, txPartitionNameTxn);
                                openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                LOG.debug().$("updated partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                            }
                        } else {
                            openPartition0(partitionIndex);
                        }
                    }
                    partitionIndex++;
                }
                for (; partitionIndex < txPartitionCount; partitionIndex++) {
                    insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex));
                }
                reloadSymbolMapCounts();
            }
            return;
        }
        reconcileOpenPartitionsFrom(0, truncateHappened);
    }

    private boolean releaseTxn() {
        if (txnAcquired) {
            long readerCount = txnScoreboard.releaseTxn(txn);
            txnAcquired = false;
            return readerCount == 0;
        }
        return false;
    }

    private void reloadAllSymbols() {
        int symbolMapIndex = 0;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
                if (symbolMapReader instanceof SymbolMapReaderImpl) {
                    final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
                    final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerColumnIndex);
                    int symbolCount = txFile.getSymbolValueCount(symbolMapIndex++);
                    ((SymbolMapReaderImpl) symbolMapReader).of(configuration, path, metadata.getColumnName(columnIndex), columnNameTxn, symbolCount);
                }
            }
        }
    }

    private void reloadColumnAt(
            int partitionIndex,
            Path path,
            ObjList<MemoryMR> columns,
            LongList columnTops,
            ObjList<BitmapIndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount
    ) {
        final int plen = path.length();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = primaryIndex + 1;

            MemoryMR mem1 = columns.getQuick(primaryIndex);
            MemoryMR mem2 = columns.getQuick(secondaryIndex);

            final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
            int writerIndex = metadata.getWriterIndex(columnIndex);
            final int versionRecordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex);
            final long columnTop = versionRecordIndex > -1L ? columnVersionReader.getColumnTopByIndex(versionRecordIndex) : 0L;
            long columnTxn = versionRecordIndex > -1L ? columnVersionReader.getColumnNameTxnByIndex(versionRecordIndex) : -1L;
            if (columnTxn == -1L) {
                // When column is added, column version will have txn number for the partition
                // where it's added. It will also have the txn number in the [default] partition
                columnTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
            }
            final long columnRowCount = partitionRowCount - columnTop;

            // When column is added mid-table existence the top record is only
            // created in the current partition. Older partitions would simply have no
            // column file. This makes it necessary to check the partition timestamp in Column Version file
            // of when the column was added.
            if (columnRowCount > 0 && (versionRecordIndex > -1L || columnVersionReader.getColumnTopPartitionTimestamp(writerIndex) <= partitionTimestamp)) {
                final int columnType = metadata.getColumnType(columnIndex);

                if (ColumnType.isVariableLength(columnType)) {
                    long columnSize = columnRowCount * 8L + 8L;
                    TableUtils.iFile(path.trimTo(plen), name, columnTxn);
                    mem2 = openOrCreateMemory(path, columns, secondaryIndex, mem2, columnSize);
                    long column2Size = mem2.getLong(columnRowCount * 8L);
                    if (column2Size <= 0 || column2Size >= (1L << 40)) {
                        LOG.critical().$("Invalid var len column size [column=").$(name).$(", size=").$(column2Size).$(", path=").$(path).I$();
                        throw CairoException.critical(0).put("Invalid column size [column=").put(path).put(", size=").put(column2Size).put(']');
                    }
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, column2Size);
                } else {
                    long columnSize = columnRowCount << ColumnType.pow2SizeOf(columnType);
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, columnSize);
                    Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    BitmapIndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader != null) {
                        indexReader.of(configuration, path.trimTo(plen), name, columnTxn, columnTop);
                    }
                } else {
                    Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullMemoryMR.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullMemoryMR.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not be always required
                Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));

                // Column is not present in the partition. Set column top to be the size of the partition.
                columnTops.setQuick(columnBase / 2 + columnIndex, partitionRowCount);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private boolean reloadColumnVersion(long columnVersion, long deadline) {
        if (columnVersionReader.getVersion() != columnVersion) {
            columnVersionReader.readSafe(clock, deadline);
        }
        return columnVersionReader.getVersion() == columnVersion;
    }

    private boolean reloadMetadata(int txnMetadataVersion, long deadline, boolean reshuffleColumns) {
        // create transition index, which will help us reuse already open resources
        if (txnMetadataVersion == metadata.getMetadataVersion()) {
            return true;
        }

        while (true) {
            long pTransitionIndex;
            try {
                pTransitionIndex = metadata.createTransitionIndex(txnMetadataVersion);
                if (pTransitionIndex < 0) {
                    if (clock.getTicks() < deadline) {
                        return false;
                    }
                    LOG.error().$("metadata read timeout [timeout=").$(configuration.getSpinLockTimeout()).utf8("ms, table=").$(tableToken.getTableName()).I$();
                    throw CairoException.critical(0).put("Metadata read timeout [table=").put(tableToken.getTableName()).put(']');
                }
            } catch (CairoException ex) {
                // This is temporary solution until we can get multiple version of metadata not overwriting each other
                TableUtils.handleMetadataLoadException(tableToken.getTableName(), deadline, ex, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                continue;
            }

            try {
                assert !reshuffleColumns || metadata.getColumnCount() == this.columnCount;
                metadata.applyTransitionIndex();
                if (reshuffleColumns) {
                    final int columnCount = metadata.getColumnCount();

                    int columnCountShl = getColumnBits(columnCount);
                    // when a column is added we cannot easily reshuffle columns in-place
                    // the reason is that we'd have to create gaps in columns list between
                    // partitions. It is possible in theory, but this could be an algo for
                    // another day.
                    if (columnCountShl > this.columnCountShl) {
                        createNewColumnList(columnCount, pTransitionIndex, columnCountShl);
                    } else {
                        reshuffleColumns(columnCount, pTransitionIndex);
                    }
                    // rearrange symbol map reader list
                    reshuffleSymbolMapReaders(pTransitionIndex, columnCount);
                    this.columnCount = columnCount;
                    reloadSymbolMapCounts();
                }
                return true;
            } finally {
                TableUtils.freeTransitionIndex(pTransitionIndex);
            }
        }
    }

    /**
     * Updates boundaries of all columns in partition.
     *
     * @param partitionIndex index of partition
     * @param rowCount       number of rows in partition
     */
    private void reloadPartition(int partitionIndex, long rowCount, long openPartitionNameTxn) {
        Path path = pathGenPartitioned(partitionIndex, openPartitionNameTxn);
        try {
            int symbolMapIndex = 0;
            int columnBase = getColumnBase(partitionIndex);
            for (int i = 0; i < columnCount; i++) {
                final int index = getPrimaryColumnIndex(columnBase, i);
                final MemoryMR mem1 = columns.getQuick(index);
                if (mem1 instanceof NullMemoryMR) {
                    reloadColumnAt(
                            partitionIndex,
                            path,
                            columns,
                            columnTops,
                            bitmapIndexes,
                            columnBase,
                            i,
                            rowCount
                    );
                } else {
                    growColumn(
                            mem1,
                            columns.getQuick(index + 1),
                            metadata.getColumnType(i),
                            rowCount - getColumnTop(columnBase, i)
                    );
                }

                // reload symbol map
                SymbolMapReader reader = symbolMapReaders.getQuick(i);
                if (reader == null) {
                    continue;
                }
                reader.updateSymbolCount(txFile.getSymbolValueCount(symbolMapIndex++));
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void reloadSlow(final boolean reshuffle) {
        final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
        do {
            // Reload txn
            readTxnSlow(deadline);
            // Reload _meta if structure version updated, reload _cv if column version updated
        } while (
            // Reload column versions, column version used in metadata reload column shuffle
                !reloadColumnVersion(txFile.getColumnVersion(), deadline)
                        // Start again if _meta with matching structure version cannot be loaded
                        || !reloadMetadata(txFile.getMetadataVersion(), deadline, reshuffle)
        );
    }

    private void reloadSymbolMapCounts() {
        int symbolMapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            if (!ColumnType.isSymbol(metadata.getColumnType(i))) {
                continue;
            }
            symbolMapReaders.getQuick(i).updateSymbolCount(txFile.getSymbolValueCount(symbolMapIndex++));
        }
    }

    private SymbolMapReader reloadSymbolMapReader(int columnIndex, SymbolMapReader reader) {
        if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
            final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
            final long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(writerColumnIndex);
            if (reader instanceof SymbolMapReaderImpl) {
                ((SymbolMapReaderImpl) reader).of(configuration, path, metadata.getColumnName(columnIndex), columnNameTxn, 0);
                return reader;
            }
            return new SymbolMapReaderImpl(configuration, path, metadata.getColumnName(columnIndex), columnNameTxn, 0);
        } else {
            return reader;
        }
    }

    private void reopenPartition(int offset, int partitionIndex, long txPartitionNameTxn) {
        openPartition0(partitionIndex);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txPartitionNameTxn);
    }

    private void reshuffleColumns(int columnCount, long pTransitionIndex) {
        LOG.debug().$("reshuffling columns file list [table=").utf8(tableToken.getTableName()).I$();
        final long pIndexBase = pTransitionIndex + 8;
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base)) > -1L) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4L);

                        if (action == -1) {
                            // This column is deleted (not moved).
                            // Close all files
                            closePartitionColumnFile(base, i);
                        }

                        // We should only remove columns from existing metadata if column count has reduced.
                        // And we should not attempt to reload columns, which have no matches in the metadata
                        if (i < columnCount) {
                            if (copyFrom == i) {
                                // It appears that column hasn't changed its position. There are three possibilities here:
                                // 1. Column has been forced out of the reader via closeColumnForRemove(). This is required
                                //    on Windows before column can be deleted. In this case we must check for marker
                                //    instance and the column from disk
                                // 2. Column hasn't been altered, and we can skip to next column.
                                MemoryMR col = columns.getQuick(getPrimaryColumnIndex(base, i));
                                if (col instanceof NullMemoryMR) {
                                    reloadColumnAt(
                                            partitionIndex,
                                            path,
                                            columns,
                                            columnTops,
                                            bitmapIndexes,
                                            base,
                                            i,
                                            partitionRowCount
                                    );
                                }
                            } else if (copyFrom > -1) {
                                copyColumns(base, copyFrom, columns, columnTops, bitmapIndexes, base, i);
                            } else if (copyFrom != Integer.MIN_VALUE) {
                                // new instance
                                reloadColumnAt(
                                        partitionIndex,
                                        path,
                                        columns,
                                        columnTops,
                                        bitmapIndexes,
                                        base,
                                        i,
                                        partitionRowCount
                                );
                            }
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void reshuffleSymbolMapReaders(long pTransitionIndex, int columnCount) {
        final long pIndexBase = pTransitionIndex + Long.BYTES;
        if (columnCount > this.columnCount) {
            symbolMapReaders.setPos(columnCount);
        }

        for (int i = 0, n = Math.max(columnCount, this.columnCount); i < n; i++) {
            long offset = pIndexBase + (long) i * Long.BYTES;
            final int action = Unsafe.getUnsafe().getInt(offset);
            final int copyFrom = Unsafe.getUnsafe().getInt(offset + 4L);

            if (action == -1) {
                // deleted
                Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(i, null));
            }

            // don't copy entries to themselves, unless symbol map was deleted
            if (copyFrom == i) {
                SymbolMapReader reader = symbolMapReaders.getQuick(copyFrom);
                if (reader != null && reader.isDeleted()) {
                    symbolMapReaders.setQuick(copyFrom, reloadSymbolMapReader(copyFrom, reader));
                }
            } else if (copyFrom > -1) {
                SymbolMapReader tmp = symbolMapReaders.getQuick(copyFrom);
                copyOrRenewSymbolMapReader(tmp, i);
            } else if (copyFrom != Integer.MIN_VALUE) {
                // New instance
                symbolMapReaders.getAndSetQuick(i, reloadSymbolMapReader(i, null));
            }
        }
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }
}
