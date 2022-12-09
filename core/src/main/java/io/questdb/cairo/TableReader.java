/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.INITIAL_TXN;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;
import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;

import static io.questdb.cairo.TableUtils.LONGS_PER_TX_ATTACHED_PARTITION;
import static io.questdb.cairo.TableUtils.LONGS_PER_TX_ATTACHED_PARTITION_MSB;
import static io.questdb.cairo.TxReader.*;


public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private final MillisecondClock clock;
    private final ColumnVersionReader columnVersionReader;
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final MessageBus messageBus;
    private final TableReaderMetadata metadata;
    private final LongList openPartitionInfo;
    private final int partitionBy;
    private final DateFormat partitionDirFormatMethod;
    private final PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private final Path path;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final int rootLen;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final String tableName;
    private final MemoryMR todoMem = Vm.getMRInstance();
    private final TxReader txFile;
    private final TxnScoreboard txnScoreboard;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountShl;
    private LongList columnTops;
    private ObjList<MemoryMR> columns;
    private int partitionCount;
    private long rowCount;
    private long tempMem8b = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
    private long txn = INITIAL_TXN;
    private boolean txnAcquired = false;

    public TableReader(CairoConfiguration configuration, CharSequence tableName) {
        this(configuration, tableName, null);
    }

    public TableReader(CairoConfiguration configuration, CharSequence tableName, @Nullable MessageBus messageBus) {
        this.configuration = configuration;
        this.tableName = Chars.toString(tableName);
        this.messageBus = messageBus;
        ff = configuration.getFilesFacade();
        clock = configuration.getMillisecondClock();
        path = new Path();
        path.of(configuration.getRoot()).concat(this.tableName);
        rootLen = path.length();
        try {
            metadata = openMetaFile();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(rootLen));
            LOG.debug()
                    .$("open [id=").$(metadata.getTableId())
                    .$(", table=").utf8(this.tableName)
                    .I$();
            this.txFile = new TxReader(ff).ofRO(path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), partitionBy);
            path.trimTo(rootLen);
            reloadSlow(false);
            columnCount = metadata.getColumnCount();
            columnCountShl = getColumnBits(columnCount);
            openSymbolMaps();
            partitionCount = txFile.getPartitionCount();
            partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
            partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);

            int capacity = getColumnBase(partitionCount);
            columns = new ObjList<>(capacity + 2);
            columns.setPos(capacity + 2);
            columns.setQuick(0, NullMemoryMR.INSTANCE);
            columns.setQuick(1, NullMemoryMR.INSTANCE);
            bitmapIndexes = new ObjList<>(capacity + 2);
            bitmapIndexes.setPos(capacity + 2);

            openPartitionInfo = new LongList(partitionCount * LONGS_PER_TX_ATTACHED_PARTITION);
            openPartitionInfo.setPos(partitionCount * LONGS_PER_TX_ATTACHED_PARTITION);
            for (int i = 0; i < partitionCount; i++) {
                // ts, number of rows, txn, column version for each partition
                // it is compared to attachedPartitions within the txn file to determine if a partition needs to be reloaded or not
                final int baseOffset = i * LONGS_PER_TX_ATTACHED_PARTITION;
                openPartitionInfo.setQuick(baseOffset + PARTITION_TS_OFFSET, txFile.getPartitionTimestamp(i));
                openPartitionInfo.setQuick(baseOffset + PARTITION_MASKED_SIZE_OFFSET, PARTITION_SIZE_MASK);
                openPartitionInfo.setQuick(baseOffset + PARTITION_NAME_TX_OFFSET, txFile.getPartitionNameTxn(i));
                openPartitionInfo.setQuick(baseOffset + PARTITION_COLUMN_VERSION_OFFSET, txFile.getPartitionColumnVersion(i));
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
            LOG.debug().$("closed '").utf8(tableName).$('\'').$();
        }
    }

    /**
     * Closed column files. Similarly to {@link #closeColumnForRemove(CharSequence)} closed reader column files before
     * column can be removed. This method takes column index usually resolved from column name by #TableReaderMetadata.
     * Bounds checking is performed via assertion.
     *
     * @param columnIndex column index
     */
    public void closeColumnForRemove(int columnIndex) {
        assert columnIndex > -1 && columnIndex < columnCount;
        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            // replace columns we force closed with special marker object
            // when we come to reloading table reader we would be able to
            // tell that column has to be attempted to be read from disk
            closePartitionColumnFile(getColumnBase(partitionIndex), columnIndex);
        }

        if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
            // same goes for symbol map reader - replace object with maker instance
            Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(columnIndex, EmptySymbolMapReader.INSTANCE));
        }
    }

    /**
     * Closes column files. This method should be used before call to TableWriter.removeColumn() on
     * Windows OS.
     *
     * @param columnName name of column to be closed.
     */
    public void closeColumnForRemove(CharSequence columnName) {
        closeColumnForRemove(metadata.getColumnIndex(columnName));
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return partitionFloorMethod.floor(timestamp);
    }

    public BitmapIndexReader getBitmapIndexReader(int partitionIndex, int columnIndex, int direction) {
        int columnBase = getColumnBase(partitionIndex);
        return getBitmapIndexReader(partitionIndex, columnBase, columnIndex, direction);
    }

    public BitmapIndexReader getBitmapIndexReader(int partitionIndex, int columnBase, int columnIndex, int direction) {
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        BitmapIndexReader reader = bitmapIndexes.getQuick(direction == BitmapIndexReader.DIR_BACKWARD ? index : index + 1);
        if (reader != null) {
            return reader;
        }
        long partitionTimestamp = txFile.getPartitionTimestamp(partitionIndex);
        long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        return createBitmapIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, txFile.getPartitionNameTxn(partitionIndex));
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountShl;
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

    public int getPartitionCount() {
        return partitionCount;
    }

    public int getPartitionIndexByTimestamp(long timestamp) {
        int end = openPartitionInfo.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, timestamp, BinarySearch.SCAN_UP);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            return (-end - 2) / LONGS_PER_TX_ATTACHED_PARTITION;
        }
        return end / LONGS_PER_TX_ATTACHED_PARTITION;
    }

    public long getPartitionTimestampByIndex(int partitionIndex) {
        return txFile.getPartitionTimestamp(partitionIndex);
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

    public String getTableName() {
        return tableName;
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

    public long getTxnStructureVersion() {
        return txFile.getStructureVersion();
    }

    public long getVersion() {
        return txFile.getStructureVersion();
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
    }

    public boolean isOpen() {
        return tempMem8b != 0L;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

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
            final int index = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
            final long txPartTs = txFile.getPartitionTimestamp(txPartitionIndex);
            final long openPartitionTimestamp = openPartitionInfo.getQuick(index);

            if (openPartitionTimestamp < txPartTs) {
                // Deleted partitions
                // This will decrement partitionCount
                deletePartition(partitionIndex);
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
                final long openPartitionNameTxn = openPartitionInfo.getQuick(index + PARTITION_NAME_TX_OFFSET);
                final long openPartitionColumnVersion = openPartitionInfo.getQuick(index + PARTITION_COLUMN_VERSION_OFFSET);
                final long openPartitionSize = getPartitionRowCount(partitionIndex);

                if (!forceTruncate) {
                    if (openPartitionNameTxn == txPartitionNameTxn && openPartitionColumnVersion == txPartitionColumnVersion) {
                        if (openPartitionSize != newPartitionSize) {
                            if (openPartitionSize > -1L) {
                                reloadPartition(partitionIndex, newPartitionSize, txPartitionNameTxn);
                                TxWriter.updatePartitionSizeByIndex(openPartitionInfo, index, newPartitionSize);
                                LOG.debug().$("updated partition size [partition=").$(openPartitionTimestamp).I$();
                            }
                            changed = true;
                        }
                    } else {
                        reOpenPartition(index, partitionIndex, txPartitionNameTxn);
                        changed = true;
                    }
                } else if (openPartitionSize > -1L && newPartitionSize > -1L) { // Don't force re-open if not yet opened
                    reOpenPartition(index, partitionIndex, txPartitionNameTxn);
                }
                txPartitionIndex++;
                partitionIndex++;
            }
        }

        // if while finished on txPartitionIndex == txPartitionCount condition
        // remove deleted opened partitions
        while (partitionIndex < partitionCount) {
            deletePartition(partitionIndex);
            changed = true;
        }

        // if while finished on partitionIndex == partitionCount condition
        // insert new partitions at the end
        for (; partitionIndex < txPartitionCount; partitionIndex++) {
            insertPartition(partitionIndex, txFile.getPartitionTimestamp(partitionIndex));
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
        final long prevPartitionVersion = txFile.getPartitionTableVersion();
        final long prevColumnVersion = txFile.getColumnVersion();
        final long prevTruncateVersion = txFile.getTruncateVersion();
        try {
            reloadSlow(true);
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            reconcileOpenPartitions(prevPartitionVersion, prevColumnVersion, prevTruncateVersion);
            return true;
        } catch (Throwable e) {
            releaseTxn();
            throw e;
        }
    }

    public long size() {
        return rowCount;
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
            if (TableUtils.schedulePurgeO3Partitions(messageBus, tableName, partitionBy)) {
                return;
            }

            LOG.error()
                    .$("could not queue purge partition task, queue is full [")
                    .$("table=").$(this.tableName)
                    .$(", txn=").$(txn)
                    .$(']').$();
        }
    }

    private void closePartitionColumnFile(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        Misc.free(columns.getAndSetQuick(index, NullMemoryMR.INSTANCE));
        Misc.free(columns.getAndSetQuick(index + 1, NullMemoryMR.INSTANCE));
        Misc.free(bitmapIndexes.getAndSetQuick(index, null));
        Misc.free(bitmapIndexes.getAndSetQuick(index + 1, null));
    }

    private long closeRewrittenPartitionFiles(int partitionIndex, int oldBase, Path path) {
        final int offset = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
        long partitionTs = openPartitionInfo.getQuick(offset);
        long existingPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITION_NAME_TX_OFFSET);
        long newNameTxn = txFile.getPartitionNameTxnByPartitionTimestamp(partitionTs);
        long newSize = txFile.getPartitionSizeByPartitionTimestamp(partitionTs);
        if (existingPartitionNameTxn != newNameTxn || newSize < 0) {
            LOG.debugW().$("close outdated partition files [table=").utf8(tableName).$(", ts=").$ts(partitionTs).$(", nameTxn=").$(newNameTxn).I$();
            // Close all columns, partition is overwritten. Partition reconciliation process will re-open correct files
            for (int i = 0; i < this.columnCount; i++) {
                closePartitionColumnFile(oldBase, i);
            }
            long maskedSize = openPartitionInfo.getQuick(offset + PARTITION_MASKED_SIZE_OFFSET);
            openPartitionInfo.setQuick(offset + PARTITION_MASKED_SIZE_OFFSET,
                    TxWriter.updatePartitionSize(maskedSize, PARTITION_SIZE_MASK)); // preserves current mask
            return -1;
        }
        pathGenPartitioned(partitionIndex);
        TableUtils.txnPartitionConditionally(path, existingPartitionNameTxn);
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
            Path path = pathGenPartitioned(getPartitionIndex(columnBase));
            try {
                if (direction == BitmapIndexReader.DIR_BACKWARD) {
                    reader = new BitmapIndexBwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            getColumnTop(columnBase, columnIndex),
                            txn
                    );
                    bitmapIndexes.setQuick(globalIndex, reader);
                } else {
                    reader = new BitmapIndexFwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            getColumnTop(columnBase, columnIndex),
                            txn
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
        LOG.debug().$("resizing columns file list [table=").$(tableName).I$();
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
                long partitionRowCount = getPartitionRowCount(partitionIndex);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, fromBase, path)) > -1L) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
                        final int fromColumnIndex = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4L);

                        if (action == -1) {
                            closePartitionColumnFile(fromBase, i);
                        }

                        if (fromColumnIndex > -1) {
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

    private void deletePartition(int partitionIndex) {
        final int offset = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = getPartitionRowCount(partitionIndex);
        int columnBase = getColumnBase(partitionIndex);
        if (partitionSize > -1L) {
            for (int k = 0; k < columnCount; k++) {
                closePartitionColumnFile(columnBase, k);
            }
        }
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);
        openPartitionInfo.removeIndexBlock(offset, LONGS_PER_TX_ATTACHED_PARTITION);

        LOG.info().$("deleted partition [path=").$(path).$(",timestamp=").$ts(partitionTimestamp).I$();
        partitionCount--;
    }

    private void formatPartitionDirName(int partitionIndex, CharSink sink) {
        partitionDirFormatMethod.format(
                openPartitionInfo.getQuick(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TS_OFFSET),
                null, // this format does not need locale access
                null,
                sink
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
        final int topBase = columnBase / 2;
        final int topSlotSize = columnSlotSize / 2;
        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize, NullMemoryMR.INSTANCE);
        bitmapIndexes.insert(idx, columnSlotSize, null);
        columnTops.insert(topBase, topSlotSize);
        columnTops.seed(topBase, topSlotSize, 0);

        final int offset = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
        openPartitionInfo.insert(offset, LONGS_PER_TX_ATTACHED_PARTITION);
        openPartitionInfo.setQuick(offset + PARTITION_TS_OFFSET, timestamp);
        openPartitionInfo.setQuick(offset + PARTITION_MASKED_SIZE_OFFSET, PARTITION_SIZE_MASK);
        openPartitionInfo.setQuick(offset + PARTITION_NAME_TX_OFFSET, -1L);
        openPartitionInfo.setQuick(offset + PARTITION_COLUMN_VERSION_OFFSET, -1L);
        partitionCount++;
        LOG.debug().$("inserted partition [index=").$(partitionIndex).$(", path=").$(path).$(", timestamp=").$ts(timestamp).I$();
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
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableName);
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
        if (txFile.getPartitionCount() < 2 && txFile.getTransientRowCount() == 0) {
            // Empty single partition. Don't check that directory exists on the disk
            return -1;
        }

        try {
            final long partitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            Path path = pathGenPartitioned(partitionIndex);
            TableUtils.txnPartitionConditionally(path, partitionNameTxn);

            if (ff.exists(path.$())) {

                final long partitionSize = txFile.getPartitionSize(partitionIndex);
                if (partitionSize > -1L) {
                    LOG.info()
                            .$("open partition ").utf8(path.$())
                            .$(" [rowCount=").$(partitionSize)
                            .$(", partitionNameTxn=").$(partitionNameTxn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", partitionIndex=").$(partitionIndex)
                            .$(", partitionCount=").$(partitionCount)
                            .$(']').$();

                    openPartitionColumns(partitionIndex, path, getColumnBase(partitionIndex), partitionSize);
                    final int offset = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
                    openPartitionInfo.setQuick(offset + PARTITION_MASKED_SIZE_OFFSET, txFile.getPartitionMaskedSize(partitionIndex));
                    openPartitionInfo.setQuick(offset + PARTITION_NAME_TX_OFFSET, txFile.getPartitionNameTxn(partitionIndex));
                    openPartitionInfo.setQuick(offset + PARTITION_COLUMN_VERSION_OFFSET, txFile.getPartitionColumnVersion(partitionIndex));
                }

                return partitionSize;
            }
            LOG.error().$("open partition failed, partition does not exist on the disk. [path=").utf8(path.$()).I$();

            if (PartitionBy.isPartitioned(getPartitionedBy())) {
                CairoException exception = CairoException.critical(0).put("Partition '");
                formatPartitionDirName(partitionIndex, exception.message);
                exception.put("' does not exist in table '")
                        .put(tableName)
                        .put("' directory. Run [ALTER TABLE ").put(tableName).put(" DROP PARTITION LIST '");
                formatPartitionDirName(partitionIndex, exception.message);
                exception.put("'] to repair the table or restore the partition directory.");
                throw exception;
            } else {
                throw CairoException.critical(0).put("Table '").put(tableName)
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
        formatPartitionDirName(partitionIndex, path.slash());
        return path;
    }

    private void reOpenPartition(int offset, int partitionIndex, long txPartitionNameTxn) {
        openPartitionInfo.setQuick(offset + PARTITION_MASKED_SIZE_OFFSET, PARTITION_SIZE_MASK);
        openPartition0(partitionIndex);
        openPartitionInfo.setQuick(offset + PARTITION_NAME_TX_OFFSET, txPartitionNameTxn);
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
        boolean truncateHappened = this.txFile.getTruncateVersion() != prevTruncateVersion;
        if (txFile.getPartitionTableVersion() == prevPartitionVersion && txFile.getColumnVersion() == prevColumnVersion && !truncateHappened) {
            int partitionIndex = Math.max(0, partitionCount - 1);
            final int txPartitionCount = txFile.getPartitionCount();
            if (partitionIndex < txPartitionCount) {
                if (partitionIndex < partitionCount) {
                    final int index = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
                    final long openPartitionSize = getPartitionRowCount(partitionIndex);
                    // we check that open partition size is non-negative to avoid loading
                    // partition that is not yet in memory
                    if (openPartitionSize > -1L) {
                        final long openPartitionNameTxn = openPartitionInfo.getQuick(index + PARTITION_NAME_TX_OFFSET);
                        final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                        if (openPartitionNameTxn == txPartitionNameTxn) {
                            final long txPartitionSize = txFile.getPartitionSize(partitionIndex);
                            if (openPartitionSize != txPartitionSize) {
                                reloadPartition(partitionIndex, txPartitionSize, txPartitionNameTxn);
                                TxWriter.updatePartitionSizeByIndex(openPartitionInfo, index, txPartitionSize);
                                LOG.debug().$("updated partition size [partition=").$(openPartitionInfo.getQuick(index)).I$();
                            }
                        } else {
                            openPartition0(partitionIndex);
                        }
                    }
                    partitionIndex++;
                }
                for (; partitionIndex < txPartitionCount; partitionIndex++) {
                    insertPartition(partitionIndex, txFile.getPartitionTimestamp(partitionIndex));
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

            final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION);
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
            assert partitionRowCount < 0 || columnRowCount >= 0;

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
                    columnSize = mem2.getLong(columnRowCount * 8L);
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, columnSize);
                } else {
                    long columnSize = columnRowCount << ColumnType.pow2SizeOf(columnType);
                    TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                    openOrCreateMemory(path, columns, primaryIndex, mem1, columnSize);
                    Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    BitmapIndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader instanceof BitmapIndexBwdReader) {
                        // name txn is -1 because the parent call sets up partition name for us
                        ((BitmapIndexBwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTxn, columnTop, -1);
                    }

                    indexReader = indexReaders.getQuick(secondaryIndex);
                    if (indexReader instanceof BitmapIndexFwdReader) {
                        ((BitmapIndexFwdReader) indexReader).of(configuration, path.trimTo(plen), name, columnTxn, columnTop, -1);
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

    private boolean reloadMetadata(long txnStructureVersion, long deadline, boolean reshuffleColumns) {
        // create transition index, which will help us reuse already open resources
        if (txnStructureVersion == metadata.getStructureVersion()) {
            return true;
        }

        while (true) {
            long pTransitionIndex;
            try {
                pTransitionIndex = metadata.createTransitionIndex(txnStructureVersion);
                if (pTransitionIndex < 0) {
                    if (clock.getTicks() < deadline) {
                        return false;
                    }
                    LOG.error().$("metadata read timeout [timeout=").$(configuration.getSpinLockTimeout()).utf8("ms, table=").$(tableName).I$();
                    throw CairoException.critical(0).put("Metadata read timeout [table=").put(tableName).put(']');
                }
            } catch (CairoException ex) {
                // This is temporary solution until we can get multiple version of metadata not overwriting each other
                TableUtils.handleMetadataLoadException(tableName, deadline, ex, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                continue;
            }

            try {
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
        Path path = pathGenPartitioned(partitionIndex);
        TableUtils.txnPartitionConditionally(path, openPartitionNameTxn);
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

    private void reloadSlow(boolean reshuffle) {
        final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
        do {
            // Reload txn
            readTxnSlow(deadline);
            // Reload _meta if structure version updated, reload _cv if column version updated
        } while (
            // Reload column versions, column version used in metadata reload column shuffle
                !reloadColumnVersion(txFile.getColumnVersion(), deadline)
                        // Start again if _meta with matching structure version cannot be loaded
                        || !reloadMetadata(txFile.getStructureVersion(), deadline, reshuffle)
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

    private void reshuffleColumns(int columnCount, long pTransitionIndex) {
        LOG.debug().$("reshuffling columns file list [table=").$(tableName).I$();
        final long pIndexBase = pTransitionIndex + Long.BYTES;
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                long partitionRowCount = getPartitionRowCount(partitionIndex);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base, path)) > -1L) {
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
            final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
            final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4L);

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

    int getColumnCount() {
        return columnCount;
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }

    long getPartitionRowCount(int partitionIndex) {
        long maskedSize = openPartitionInfo.getQuick(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_MASKED_SIZE_OFFSET);
        if (maskedSize < 0) {
            return -1L;
        }
        return maskedSize & PARTITION_SIZE_MASK;
    }

    TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    boolean isColumnCached(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex).isCached();
    }
}
