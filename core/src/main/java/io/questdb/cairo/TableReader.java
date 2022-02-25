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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.MemoryCMRImpl;
import io.questdb.cairo.vm.NullMemoryMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final int PARTITIONS_SLOT_SIZE = 4;
    private static final int PARTITIONS_SLOT_OFFSET_SIZE = 1;
    private static final int PARTITIONS_SLOT_OFFSET_NAME_TXN = 2;
    private static final int PARTITIONS_SLOT_OFFSET_DATA_TXN = 3;
    private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
    private final FilesFacade ff;
    private final Path path;
    private final int partitionBy;
    private final int rootLen;
    private final TableReaderMetadata metadata;
    private final DateFormat partitionDirFormatMethod;
    private final LongList openPartitionInfo;
    private final TableReaderRecordCursor recordCursor = new TableReaderRecordCursor();
    private final PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private final String tableName;
    private final MessageBus messageBus;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final TxReader txFile;
    private final MemoryMR todoMem = Vm.getMRInstance();
    private final TxnScoreboard txnScoreboard;
    private final ColumnVersionReader columnVersionReader;
    private int partitionCount;
    private LongList columnTops;
    private ObjList<MemoryMR> columns;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountShl;
    private long rowCount;
    private long txn = TableUtils.INITIAL_TXN;
    private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
    private boolean txnAcquired = false;

    public TableReader(CairoConfiguration configuration, CharSequence tableName) {
        this(configuration, tableName, null);
    }

    public TableReader(CairoConfiguration configuration, CharSequence tableName, @Nullable MessageBus messageBus) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.tableName = Chars.toString(tableName);
        this.messageBus = messageBus;
        this.path = new Path();
        this.path.of(configuration.getRoot()).concat(this.tableName);
        this.rootLen = path.length();
        try {
            this.metadata = openMetaFile();
            this.columnCount = this.metadata.getColumnCount();
            this.columnCountShl = getColumnBits(columnCount);
            this.partitionBy = this.metadata.getPartitionBy();
            this.columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            this.txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(rootLen));
            path.trimTo(rootLen);
            LOG.debug()
                    .$("open [id=").$(metadata.getId())
                    .$(", table=").$(this.tableName)
                    .I$();
            this.txFile = new TxReader(ff).ofRO(path, partitionBy);
            path.trimTo(rootLen);
            reloadSlow(metadata.getStructureVersion(), columnVersionReader.getVersion(), false);
            openSymbolMaps();
            partitionCount = txFile.getPartitionCount();
            partitionDirFormatMethod = PartitionBy.getPartitionDirFormatMethod(partitionBy);
            partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);

            int capacity = getColumnBase(partitionCount);
            this.columns = new ObjList<>(capacity);
            this.columns.setPos(capacity + 2);
            this.columns.setQuick(0, NullMemoryMR.INSTANCE);
            this.columns.setQuick(1, NullMemoryMR.INSTANCE);
            this.bitmapIndexes = new ObjList<>(capacity);
            this.bitmapIndexes.setPos(capacity + 2);

            this.openPartitionInfo = new LongList(partitionCount * PARTITIONS_SLOT_SIZE);
            this.openPartitionInfo.setPos(partitionCount * PARTITIONS_SLOT_SIZE);
            for (int i = 0; i < partitionCount; i++) {
                this.openPartitionInfo.setQuick(i * PARTITIONS_SLOT_SIZE, txFile.getPartitionTimestamp(i));
                this.openPartitionInfo.setQuick(i * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1); // size
                this.openPartitionInfo.setQuick(i * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN, txFile.getPartitionNameTxn(i)); // txn
                this.openPartitionInfo.setQuick(i * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_DATA_TXN, txFile.getPartitionDataTxn(i)); // txn
            }
            this.columnTops = new LongList(capacity / 2);
            this.columnTops.setPos(capacity / 2);
            this.recordCursor.of(this);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int base, int index) {
        return 2 + base + index * 2;
    }

    public double avgDouble(int columnIndex) {
        double result = 0;
        long countTotal = 0;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final MemoryR column = columns.getQuick(index);
            if (column != null) {
                final long count = column.getPageSize() / Double.BYTES;
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    result += Vect.avgDouble(column.getPageAddress(pageIndex), count);
                    countTotal++;
                }
            }
        }

        if (countTotal == 0) {
            return 0;
        }

        return result / countTotal;
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
            Misc.free(symbolMapReaders.getAndSetQuick(columnIndex, EmptySymbolMapReader.INSTANCE));
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
        long partitionTimestamp = txFile.getPartitionTimestamp(partitionIndex);
        long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        return reader == null ? createBitmapIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, txFile.getPartitionNameTxn(partitionIndex)) : reader;
    }

    public MemoryR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountShl;
    }

    public long getColumnTop(int base, int columnIndex) {
        return this.columnTops.getQuick(base / 2 + columnIndex);
    }

    public long getCommitLag() {
        return metadata.getCommitLag();
    }

    public TableReaderRecordCursor getCursor() {
        recordCursor.toTop();
        return recordCursor;
    }

    public long getDataVersion() {
        return this.txFile.getDataVersion();
    }

    public ColumnVersionReader getColumnVersionReader() {
        return columnVersionReader;
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
    public SymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    public String getTableName() {
        return tableName;
    }

    public long getTransientRowCount() {
        return txFile.getTransientRowCount();
    }

    public long getTxnStructureVersion() {
        return txFile.getStructureVersion();
    }

    public long getVersion() {
        return this.txFile.getStructureVersion();
    }

    public void goActive() {
        reload();
    }

    public void goPassive() {
        if (releaseTxn() && PartitionBy.isPartitioned(this.partitionBy)) {
            // check if reader unlocks a transaction in scoreboard
            // to house keep the partition versions
            checkSchedulePurgeO3Partitions();
        }
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    public double maxDouble(int columnIndex) {
        double max = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final MemoryR column = columns.getQuick(index);
            if (column != null) {
                final long count = column.getPageSize() / Double.BYTES;
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    double x = Vect.maxDouble(a, count);
                    if (x > max) {
                        max = x;
                    }
                }
            }
        }
        return max;
    }

    public double minDouble(int columnIndex) {
        double min = Double.POSITIVE_INFINITY;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final MemoryR column = columns.getQuick(index);
            if (column != null) {
                final long count = column.getPageSize() / Double.BYTES;
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    double x = Vect.minDouble(a, count);
                    if (x < min) {
                        min = x;
                    }
                }
            }
        }
        return min;
    }

    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    public void reconcileOpenPartitionsFrom(int partitionIndex) {
        int txPartitionCount = txFile.getPartitionCount();
        int txPartitionIndex = partitionIndex;
        boolean changed = false;

        while (partitionIndex < partitionCount && txPartitionIndex < txPartitionCount) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            final long openPartitionTimestamp = openPartitionInfo.getQuick(offset);
            final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
            final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);

            long txPartTs = txFile.getPartitionTimestamp(txPartitionIndex);

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
                long newPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                if (openPartitionNameTxn == txPartitionNameTxn) {
                    if (openPartitionSize != newPartitionSize) {
                        if (openPartitionSize > -1L) {
                            reloadPartition(partitionIndex, newPartitionSize, txPartitionNameTxn);
                            this.openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, newPartitionSize);
                            LOG.debug().$("updated partition size [partition=").$(openPartitionTimestamp).I$();
                        }
                        changed = true;
                    }
                } else {
                    // clear the partition size in case we truncated it
                    this.openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
                    openPartition0(partitionIndex);
                    this.openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txPartitionNameTxn);
                    changed = true;
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

        if (changed) {
            reloadSymbolMapCounts();
        }
    }

    public boolean reload() {
        if (acquireTxn()) {
            return false;
        }
        final long prevPartitionVersion = this.txFile.getPartitionTableVersion();
        try {
            reloadSlow(this.txFile.getStructureVersion(), this.columnVersionReader.getVersion(), true);
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            reconcileOpenPartitions(prevPartitionVersion);
            return true;
        } catch (Throwable e) {
            releaseTxn();
            throw e;
        }
    }

    public long size() {
        return rowCount;
    }

    public double sumDouble(int columnIndex) {
        double result = 0;
        for (int i = 0; i < partitionCount; i++) {
            openPartition(i);
            final int base = getColumnBase(i);
            final int index = getPrimaryColumnIndex(base, columnIndex);
            final MemoryR column = columns.getQuick(index);
            if (column != null) {
                final long count = column.getPageSize() / Double.BYTES;
                for (int pageIndex = 0, pageCount = column.getPageCount(); pageIndex < pageCount; pageIndex++) {
                    long a = column.getPageAddress(pageIndex);
                    result += Vect.sumDouble(a, count);
                }
            }
        }
        return result;
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

        // We have to be sure last txn is acquired in Scoreboard
        // otherwise writer can delete partition version files
        // between reading txn file and acquiring txn in the Scoreboard.
        Unsafe.getUnsafe().loadFence();
        return txFile.getVersion() == txFile.unsafeReadVersion();
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
        long partitionTs = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
        long exisingPartitionNameTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        long newNameTxn = txFile.getPartitionNameTxnByPartitionTimestamp(partitionTs);
        long newSize = txFile.getPartitionSizeByPartitionTimestamp(partitionTs);
        if (exisingPartitionNameTxn != newNameTxn || newSize < 0) {
            LOG.debugW().$("close outdated partition files [table=").$(tableName).$(", ts=").$ts(partitionTs).$(", nameTxn=").$(newNameTxn).$();
            // Close all columns, partition is overwritten. Partition reconciliation process will re-open correct files
            for (int i = 0; i < this.columnCount; i++) {
                closePartitionColumnFile(oldBase, i);
            }
            openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            return -1;
        }
        pathGenPartitioned(partitionIndex);
        TableUtils.txnPartitionConditionally(path, exisingPartitionNameTxn);
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

        toColumns.getAndSetQuick(toIndex, columns.getAndSetQuick(fromIndex, null));
        toColumns.getAndSetQuick(toIndex + 1, columns.getAndSetQuick(fromIndex + 1, null));
        toColumnTops.getAndSetQuick(toBase / 2 + toColumnIndex, columnTops.getQuick(fromBase / 2 + fromColumnIndex));
        toIndexReaders.getAndSetQuick(toIndex, bitmapIndexes.getAndSetQuick(fromIndex, null));
        toIndexReaders.getAndSetQuick(toIndex + 1, bitmapIndexes.getAndSetQuick(fromIndex + 1, null));
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
            throw CairoException.instance(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
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
        final ObjList<MemoryMR> columns = new ObjList<>(capacity + 2);
        final LongList columnTops = new LongList(capacity / 2);
        final ObjList<BitmapIndexReader> indexReaders = new ObjList<>(capacity);
        columns.setPos(capacity + 2);
        columns.setQuick(0, NullMemoryMR.INSTANCE);
        columns.setQuick(1, NullMemoryMR.INSTANCE);
        columnTops.setPos(capacity / 2);
        indexReaders.setPos(capacity + 2);
        final long pIndexBase = pTransitionIndex + 8;
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int base = partitionIndex << columnCountShl;
            final int oldBase = partitionIndex << this.columnCountShl;

            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base, path)) > -1L) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4);

                        if (action == -1) {
                            closePartitionColumnFile(oldBase, i);
                        }

                        if (copyFrom > -1) {
                            copyColumns(oldBase, copyFrom, columns, columnTops, indexReaders, base, i);
                        } else if (copyFrom != Integer.MIN_VALUE) {
                            // new instance
                            reloadColumnAt(partitionIndex, path, columns, columnTops, indexReaders, base, i, partitionRowCount);
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        this.columns = columns;
        this.columnTops = columnTops;
        this.columnCountShl = columnCountShl;
        this.bitmapIndexes = indexReaders;
    }

    private void deletePartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        int columnBase = getColumnBase(partitionIndex);
        if (partitionSize > -1L) {
            for (int k = 0; k < columnCount; k++) {
                closePartitionColumnFile(columnBase, k);
            }
        }
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);
        openPartitionInfo.removeIndexBlock(offset, PARTITIONS_SLOT_SIZE);

        LOG.info().$("deleted partition [path=").$(path).$(",timestamp=").$ts(partitionTimestamp).I$();
        partitionCount--;
    }

    private void formatPartitionDirName(int partitionIndex, CharSink sink) {
        partitionDirFormatMethod.format(
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
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
            Misc.free(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
    }

    private void freeTempMem() {
        if (tempMem8b != 0) {
            Unsafe.free(tempMem8b, 8, MemoryTag.NATIVE_DEFAULT);
            tempMem8b = 0;
        }
    }

    int getColumnCount() {
        return columnCount;
    }

    int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }

    long getPartitionRowCount(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
    }

    long getTxn() {
        return txn;
    }

    TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    private void handleMetadataLoadException(long deadline, CairoException ex) {
        // This is temporary solution until we can get multiple version of metadata not overwriting each other
        if (isMetaFileMissingFileSystemError(ex)) {
            if (configuration.getMicrosecondClock().getTicks() < deadline) {
                LOG.info().$("error reloading metadata [table=").$(tableName)
                        .$(", errno=").$(ex.getErrno())
                        .$(", error=").$(ex.getFlyweightMessage()).I$();
                Os.pause();
            } else {
                LOG.error().$("metadata read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                throw CairoException.instance(ex.getErrno()).put("Metadata read timeout. Last error: ").put(ex.getFlyweightMessage());
            }
        } else {
            throw ex;
        }
    }

    private void insertPartition(int partitionIndex, long timestamp) {
        final int columnBase = getColumnBase(partitionIndex);
        final int columnSlotSize = getColumnBase(1);
        final int topBase = columnBase / 2;
        final int topSlotSize = columnSlotSize / 2;
        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize);
        columns.set(idx, columnBase + columnSlotSize + 1, NullMemoryMR.INSTANCE);
        bitmapIndexes.insert(idx, columnSlotSize);
        bitmapIndexes.set(idx, columnBase + columnSlotSize, null);
        columnTops.insert(topBase, topSlotSize);
        columnTops.seed(topBase, topSlotSize, 0);

        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        openPartitionInfo.insert(offset, PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setQuick(offset, timestamp);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1L); // size
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, -1L); // name txn
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_DATA_TXN, -1L); // data txn
        partitionCount++;
        LOG.debug().$("inserted partition [index=").$(partitionIndex).$(", path=").$(path).$(", timestamp=").$ts(timestamp).I$();
    }

    boolean isColumnCached(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex).isCached();
    }

    private boolean isMetaFileMissingFileSystemError(CairoException ex) {
        int errno = ex.getErrno();
        return errno == CairoException.ERRNO_FILE_DOES_NOT_EXIST || errno == CairoException.METADATA_VALIDATION;
    }

    private TableReaderMetadata openMetaFile() {
        long deadline = this.configuration.getMicrosecondClock().getTicks() + this.configuration.getSpinLockTimeoutUs();
        TableReaderMetadata metadata = new TableReaderMetadata(ff);
        path.concat(TableUtils.META_FILE_NAME).$();
        try {
            while (true) {
                try {
                    return metadata.of(path, ColumnType.VERSION);
                } catch (CairoException ex) {
                    handleMetadataLoadException(deadline, ex);
                }
            }
        } finally {
            path.trimTo(rootLen);
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
            return -1;
        }

        try {
            final long partitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            Path path = pathGenPartitioned(partitionIndex);
            TableUtils.txnPartitionConditionally(path, partitionNameTxn);

            if (ff.exists(path.$())) {
                path.chop$();

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
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    this.openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                }

                return partitionSize;
            }
            LOG.error().$("open partition failed, partition does not exist on the disk. [path=").utf8(path.$()).I$();

            if (PartitionBy.isPartitioned(getPartitionedBy())) {
                CairoException exception = CairoException.instance(0).put("Partition '");
                formatPartitionDirName(partitionIndex, exception.message);
                TableUtils.txnPartitionConditionally(exception.message, partitionNameTxn);
                exception.put("' does not exist in table '")
                        .put(tableName)
                        .put("' directory. Run [ALTER TABLE ").put(tableName).put(" DROP PARTITION LIST '");
                formatPartitionDirName(partitionIndex, exception.message);
                TableUtils.txnPartitionConditionally(exception.message, partitionNameTxn);
                exception.put("'] to repair the table or restore the partition directory.");
                throw exception;
            } else {
                throw CairoException.instance(0).put("Table '").put(tableName)
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
                    this.columns,
                    this.columnTops,
                    this.bitmapIndexes,
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
                long columnNameTxn = columnVersionReader.getDefaultColumnNameTxn(metadata.getWriterIndex(i));
                SymbolMapReaderImpl symbolMapReader = new SymbolMapReaderImpl(configuration, path, metadata.getColumnName(i), columnNameTxn, txFile.getSymbolValueCount(symbolColumnIndex++));
                symbolMapReaders.extendAndSet(i, symbolMapReader);
            }
        }
    }

    private Path pathGenPartitioned(int partitionIndex) {
        formatPartitionDirName(partitionIndex, path.slash());
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
                            .$(']').$();
                    break;
                }
            }
            // This is unlucky, sequences have changed while we were reading transaction data
            // We must discard and try again
            count++;
            if (configuration.getMicrosecondClock().getTicks() > deadline) {
                LOG.error().$("tx read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                throw CairoException.instance(0).put("Transaction read timeout");
            }
            Os.pause();
        }
    }

    private void reconcileOpenPartitions(long prevPartitionVersion) {
        // Reconcile partition full or partial will only update row count of last partition and append new partitions
        if (this.txFile.getPartitionTableVersion() == prevPartitionVersion) {
            int partitionIndex = Math.max(0, partitionCount - 1);
            final int txPartitionCount = txFile.getPartitionCount();
            if (partitionIndex < txPartitionCount) {
                if (partitionIndex < partitionCount) {
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                    // we check that open partition size is non-negative to avoid loading
                    // partition that is not yet in memory
                    if (openPartitionSize > -1) {
                        final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                        final long txPartitionSize = txFile.getPartitionSize(partitionIndex);
                        final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);

                        if (openPartitionNameTxn == txPartitionNameTxn) {
                            if (openPartitionSize != txPartitionSize) {
                                reloadPartition(partitionIndex, txPartitionSize, txPartitionNameTxn);
                                this.openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                LOG.debug().$("updated partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                            }
                        } else {
                            openPartition0(partitionIndex);
                            this.openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txPartitionNameTxn);
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
        reconcileOpenPartitionsFrom(0);
    }

    private boolean releaseTxn() {
        if (txnAcquired) {
            long readerCount = txnScoreboard.releaseTxn(txn);
            txnAcquired = false;
            return readerCount == 0;
        }
        return false;
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
            assert partitionRowCount < 0 || columnRowCount >= 0;

            // When column is added mid-table existence the top record is only
            // created in the current partition. Older partitions would simply have no
            // column file. This makes it necessary to check the partition timestamp in Column Version file
            // of when the column was added.
            if (partitionRowCount > 0 && (versionRecordIndex > -1L || columnVersionReader.getColumnTopPartitionTimestamp(writerIndex) <= partitionTimestamp)) {
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
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private boolean reloadColumnVersion(long columnVersion, long deadline) {
        columnVersionReader.readSafe(configuration.getMicrosecondClock(), deadline);
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
                    if (configuration.getMicrosecondClock().getTicks() < deadline) {
                        return false;
                    }
                    LOG.error().$("metadata read timeout [timeout=").$(configuration.getSpinLockTimeoutUs()).utf8("μs]").$();
                    throw CairoException.instance(0).put("Metadata read timeout");
                }
            } catch (CairoException ex) {
                // This is temporary solution until we can get multiple version of metadata not overwriting each other
                handleMetadataLoadException(deadline, ex);
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

    private void reloadSlow(long prevStructureVersion, long prevColumnVersion, boolean reshuffle) {
        final long deadline = configuration.getMicrosecondClock().getTicks() + configuration.getSpinLockTimeoutUs();
        boolean structureVersionUpdated, columnVersionUpdated;
        do {
            // Reload txn
            readTxnSlow(deadline);
            structureVersionUpdated = prevStructureVersion != txFile.getStructureVersion();
            columnVersionUpdated = prevColumnVersion != txFile.getColumnVersion();
            // Reload _meta if structure version updated, reload _cv if column version updated
        } while ((columnVersionUpdated && !reloadColumnVersion(txFile.getColumnVersion(), deadline)) // Reload column versions, column version used in metadata reload column shuffle
                || (structureVersionUpdated && !reloadMetadata(txFile.getStructureVersion(), deadline, reshuffle)) // Start again if _meta with matching structure version cannot be loaded
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
        final long pIndexBase = pTransitionIndex + 8;
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1L && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base, path)) > -1L) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
                        final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4);

                        if (action == -1) {
                            // This column is deleted (not moved).
                            // Close all files
                            closePartitionColumnFile(base, i);
                        }

                        if (copyFrom == i) {
                            // It appears that column hasn't changed its position. There are three possibilities here:
                            // 1. Column has been forced out of the reader via closeColumnForRemove(). This is required
                            //    on Windows before column can be deleted. In this case we must check for marker
                            //    instance and the column from disk
                            // 2. Column hasn't been altered and we can skip to next column.
                            MemoryMR col = columns.getQuick(getPrimaryColumnIndex(base, i));
                            if (col instanceof NullColumn) {
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
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void reshuffleSymbolMapReaders(long pTransitionIndex, int columnCount) {
        final long pIndexBase = pTransitionIndex + 8;
        if (columnCount > this.columnCount) {
            symbolMapReaders.setPos(columnCount);
        }

        for (int i = 0, n = Math.max(columnCount, this.columnCount); i < n; i++) {
            final int action = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L);
            final int copyFrom = Unsafe.getUnsafe().getInt(pIndexBase + i * 8L + 4);

            if (action == -1) {
                // deleted
                Misc.free(symbolMapReaders.getAndSetQuick(i, null));
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
}
