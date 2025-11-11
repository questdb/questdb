/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.MemoryCMRDetachedImpl;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final int PARTITIONS_SLOT_OFFSET_SIZE = 1;
    private static final int PARTITIONS_SLOT_OFFSET_NAME_TXN = PARTITIONS_SLOT_OFFSET_SIZE + 1;
    private static final int PARTITIONS_SLOT_OFFSET_COLUMN_VERSION = PARTITIONS_SLOT_OFFSET_NAME_TXN + 1;
    private static final int PARTITIONS_SLOT_OFFSET_FORMAT = PARTITIONS_SLOT_OFFSET_COLUMN_VERSION + 1;
    private static final int PARTITIONS_SLOT_SIZE = 8; // must be power of 2
    private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
    private final MillisecondClock clock;
    private final ColumnVersionReader columnVersionReader;
    private final CairoConfiguration configuration;
    private final int dbRootSize;
    private final FilesFacade ff;
    private final int id;
    private final int maxOpenPartitions;
    private final MessageBus messageBus;
    private final TableReaderMetadata metadata;
    private final int partitionBy;
    private final PartitionOverwriteControl partitionOverwriteControl;
    private final Path path;
    private final int rootLen;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final int timestampType;
    private final TxReader txFile;
    private final TxnScoreboard txnScoreboard;
    private ObjList<BitmapIndexReader> bitmapIndexes;
    private int columnCount;
    private int columnCountShl;
    private LongList columnTops;
    private ObjList<MemoryCMR> columns;
    private int openPartitionCount;
    private LongList openPartitionInfo;
    private ObjList<MemoryCMR> parquetPartitions;
    private int partitionCount;
    private long rowCount;
    private TableToken tableToken;
    private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_TABLE_READER);
    private long txColumnVersion;
    private long txPartitionVersion;
    private long txTruncateVersion;
    private long txn = TableUtils.INITIAL_TXN;
    private boolean txnAcquired = false;

    public TableReader(
            int id,
            CairoConfiguration configuration,
            @NotNull TableToken tableToken,
            TxnScoreboardPool scoreboardFactory) {
        this(id, configuration, tableToken, scoreboardFactory, null, null);
    }

    // Don't forget to change TableReader srcReader overload when changing this constructor.
    public TableReader(
            int id,
            CairoConfiguration configuration,
            @NotNull TableToken tableToken,
            TxnScoreboardPool scoreboardPool,
            @Nullable MessageBus messageBus,
            @Nullable PartitionOverwriteControl partitionOverwriteControl
    ) {
        this.id = id;
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.maxOpenPartitions = configuration.getInactiveReaderMaxOpenPartitions();
        this.ff = configuration.getFilesFacade();
        this.tableToken = tableToken;
        this.messageBus = messageBus;
        try {
            this.path = new Path();
            this.path.of(configuration.getDbRoot());
            this.dbRootSize = path.size();
            path.concat(tableToken.getDirName());
            this.rootLen = path.size();
            path.trimTo(rootLen);
            metadata = openMetaFile();
            timestampType = metadata.getTimestampType();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = scoreboardPool.getTxnScoreboard(tableToken);
            LOG.debug()
                    .$("open [id=").$(metadata.getTableId())
                    .$(", table=").$(tableToken)
                    .I$();
            txFile = new TxReader(ff).ofRO(
                    path.trimTo(rootLen).concat(TXN_FILE_NAME).$(),
                    timestampType,
                    partitionBy
            );
            path.trimTo(rootLen);
            reloadSlow(false);
            init();

            this.partitionOverwriteControl = partitionOverwriteControl;
            if (partitionOverwriteControl != null) {
                partitionOverwriteControl.acquirePartitions(this);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    // copyOf constructor.
    public TableReader(
            int id,
            CairoConfiguration configuration,
            TableReader srcReader,
            TxnScoreboardPool scoreboardPool,
            @Nullable MessageBus messageBus,
            @Nullable PartitionOverwriteControl partitionOverwriteControl
    ) {
        assert srcReader.isOpen() && srcReader.isActive();
        this.id = id;
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.maxOpenPartitions = configuration.getInactiveReaderMaxOpenPartitions();
        this.ff = configuration.getFilesFacade();
        this.tableToken = srcReader.getTableToken();
        this.messageBus = messageBus;
        try {
            this.path = new Path();
            this.path.of(configuration.getDbRoot());
            this.dbRootSize = path.size();
            path.concat(tableToken.getDirName());
            this.rootLen = path.size();
            path.trimTo(rootLen);
            metadata = copyMeta(srcReader.metadata);
            timestampType = metadata.getTimestampType();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = scoreboardPool.getTxnScoreboard(tableToken);
            LOG.debug()
                    .$("open as copy [id=").$(metadata.getTableId())
                    .$(", table=").$(tableToken)
                    .$(", srcTxn=").$(srcReader.getTxn())
                    .I$();
            txFile = new TxReader(ff).ofRO(
                    path.trimTo(rootLen).concat(TXN_FILE_NAME).$(),
                    timestampType,
                    partitionBy
            );
            path.trimTo(rootLen);
            reloadAtTxn(srcReader, false);
            txPartitionVersion = txFile.getPartitionTableVersion();
            txColumnVersion = txFile.getColumnVersion();
            txTruncateVersion = txFile.getTruncateVersion();
            init();

            this.partitionOverwriteControl = partitionOverwriteControl;
            if (partitionOverwriteControl != null) {
                partitionOverwriteControl.acquirePartitions(this);
            }
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
            if (partitionSize > -1) {
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
            freeColumns();
            freeParquetPartitions();
            freeTempMem();
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(columnVersionReader);
            LOG.debug().$("closed [table=").$(tableToken).I$();
        }
    }

    public void dumpRawTxPartitionInfo(LongList container) {
        txFile.dumpRawTxPartitionInfo(container);
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return txFile.getPartitionTimestampByTimestamp(timestamp);
    }

    public BitmapIndexReader getBitmapIndexReader(int partitionIndex, int columnIndex, int direction) {
        final int columnBase = getColumnBase(partitionIndex);
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
        final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final long partitionTxn = txFile.getPartitionNameTxn(partitionIndex);
        BitmapIndexReader indexReader = getBitmapIndexReaderIfExists(partitionIndex, columnIndex, direction);
        if (indexReader != null) {
            if (
                    !indexReader.isOpen()
                            || indexReader.getColumnTxn() != columnNameTxn
                            || indexReader.getPartitionTxn() != partitionTxn
            ) {
                int plen = path.size();
                try {
                    indexReader.of(
                            configuration,
                            pathGenNativePartition(partitionIndex, partitionTxn),
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            partitionTxn,
                            getColumnTop(columnBase, columnIndex)
                    );
                } finally {
                    path.trimTo(plen);
                }
            } else {
                indexReader.reloadConditionally();
            }
            return indexReader;
        }
        return createBitmapIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, partitionTxn);
    }

    public BitmapIndexReader getBitmapIndexReaderIfExists(int partitionIndex, int columnIndex, int direction) {
        final int columnBase = getColumnBase(partitionIndex);
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final int indexIndex = direction == BitmapIndexReader.DIR_BACKWARD ? index : index + 1;
        return bitmapIndexes.getQuick(indexIndex);
    }

    public MemoryCR getColumn(int absoluteIndex) {
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

    public long getDataVersion() {
        return txFile.getDataVersion();
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

    public long getMetadataVersion() {
        return txFile.getMetadataVersion();
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

    /**
     * Returns previously open Parquet partition's mmapped address or 0 in case of a native partition.
     */
    public long getParquetAddr(int partitionIndex) {
        return parquetPartitions.getQuick(partitionIndex).addressOf(0);
    }

    /**
     * Returns previously open Parquet partition read size or -1 in case of a native partition.
     */
    public long getParquetFileSize(int partitionIndex) {
        return parquetPartitions.getQuick(partitionIndex).size();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public byte getPartitionFormat(int partitionIndex) {
        return (byte) openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_FORMAT);
    }

    public byte getPartitionFormatFromMetadata(int partitionIndex) {
        if (txFile.isPartitionParquet(partitionIndex)) {
            return PartitionFormat.PARQUET;
        }
        return PartitionFormat.NATIVE;
    }

    @TestOnly
    public int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }

    public int getPartitionIndexByTimestamp(long timestamp) {
        int end = openPartitionInfo.binarySearchBlock(PARTITIONS_SLOT_SIZE_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            return (-end - 2) / PARTITIONS_SLOT_SIZE;
        }
        return end / PARTITIONS_SLOT_SIZE;
    }

    /**
     * Pretty convoluted logic to calculate upper timestamp bound of the given partition, e.g., by partition index.
     * First thing of note - the upper-bound value is inclusive, it must be a value that will be able to reside in the
     * partition.
     * <p>
     * The value of the upper bound is COMPUTED, rather than retrieved from a store. The inputs for the computation are:
     * - min timestamp of the partition - this is a stored value
     * - timestamp ceil function, which depends on the partition type. E.g., you could round any timestamp to the
     * theoretical upper bound. But we cannot use only this method because of partition splits.
     * - min timestamp of the next partition, e.g., if partition was split, we cannot use ceil function but rather
     * the min timestamp of the next partition MINUS ONE (to ensure timestamp is inclusive). However, we cannot use this
     * method only because of gaps in partitions. E.g., daily partition could have a gap for weekend.
     * - we also cannot just grab the next partition, and we need to be mindful of the partition count. To avoid
     * index-out-of-bounds errors.
     * <p>
     * To calculate the bound we will:
     * 1. check if we can access the next partition. If we cannot - it means this is the last partition, and we can
     * use the ceil function and that would be it.
     * 2. We can access the next partition - great, we get its min timestamp but, next gotcha - there could be a gap,
     * so we take a min between the ceil value and the next timestamp value.
     * <p>
     * <p>
     * Clear?
     *
     * @param partitionIndex the index of the partition in question
     * @return upper bound of the timestamp that can possibly be stored in this partition, which is an inclusive value.
     */
    public long getPartitionMaxTimestampFromMetadata(int partitionIndex) {
        int next = partitionIndex + 1;
        long minTimestampCeil = txFile.getNextLogicalPartitionTimestamp(getPartitionMinTimestampFromMetadata(partitionIndex));
        return next < getPartitionCount() ? Math.min(getPartitionMinTimestampFromMetadata(next), minTimestampCeil) - 1 : minTimestampCeil;
    }

    public long getPartitionMinTimestampFromMetadata(int partitionIndex) {
        return txFile.getPartitionTimestampByIndex(partitionIndex);
    }

    public long getPartitionRowCount(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
    }

    public long getPartitionRowCountFromMetadata(int partitionIndex) {
        return txFile.getPartitionSize(partitionIndex);
    }

    public long getPartitionTimestampByIndex(int partitionIndex) {
        return txFile.getPartitionTimestampByIndex(partitionIndex);
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    public long getSeqTxn() {
        return txFile.getSeqTxn();
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

    public void goActive() {
        reload();
        if (partitionOverwriteControl != null) {
            partitionOverwriteControl.acquirePartitions(this);
        }
    }

    public void goActiveAtTxn(TableReader srcReader) {
        assert srcReader.isOpen() && srcReader.isActive();
        assert tableToken.equals(srcReader.getTableToken());

        // We may need to downgrade from newer txn to an older one.
        final boolean needsDowngrade = txn > srcReader.txn;
        if (needsDowngrade) {
            // Prepare for downgrade.
            if (partitionOverwriteControl != null) {
                // Mark partitions as unused before releasing txn in scoreboard
                // to avoid false positives in partition overwrite control
                partitionOverwriteControl.releasePartitions(this);
            }
            // close all latest partitions
            if (PartitionBy.isPartitioned(partitionBy)) {
                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                    if (partitionSize > -1) {
                        closePartition(partitionIndex);
                    }
                }
            }
            freeSymbolMapReaders();
            freeBitmapIndexCache();
            freeColumns();
            freeParquetPartitions();
            // Remember to copy source metadata upfront - we don't need to deal with metadata transition index.
            metadata.loadFrom(srcReader.metadata);
        }
        // Copy source reader's state.
        reloadAtTxn(srcReader, true);
        if (needsDowngrade) {
            // We need to re-init txn versions and all lists.
            init();
        }
        // Reload partitions.
        reconcileOpenPartitions(txPartitionVersion, txColumnVersion, txTruncateVersion);
        // Save transaction details which impact the reloading.
        // Do not rely on txReader, it can be reloaded outside this method.
        txPartitionVersion = txFile.getPartitionTableVersion();
        txColumnVersion = txFile.getColumnVersion();
        txTruncateVersion = txFile.getTruncateVersion();

        if (partitionOverwriteControl != null) {
            partitionOverwriteControl.acquirePartitions(this);
        }
    }

    public void goPassive() {
        if (!isActive()) {
            return;
        }
        if (partitionOverwriteControl != null) {
            // Mark partitions as unused before releasing txn in scoreboard
            // to avoid false positives in partition overwrite control
            partitionOverwriteControl.releasePartitions(this);
        }
        if (releaseTxn() && PartitionBy.isPartitioned(partitionBy)) {
            // check if the reader unlocks a transaction in the scoreboard to
            // house-keep the partition versions
            checkSchedulePurgeO3Partitions();
        }
        // close all but N latest partitions
        if (PartitionBy.isPartitioned(partitionBy) && openPartitionCount > maxOpenPartitions) {
            final int originallyOpen = openPartitionCount;
            int openCount = 0;
            for (int partitionIndex = partitionCount - 1; partitionIndex > -1; partitionIndex--) {
                final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionSize > -1 && ++openCount > maxOpenPartitions) {
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
        return tempMem8b != 0;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

    /**
     * Opens given partition for reading. Native partitions become immediately readable
     * after this call through mapped memory. For Parquet partitions, the file is open
     * for read with fd available via {@link #getParquetAddr(int)}} call.
     *
     * @param partitionIndex partition index
     * @return partition size in rows
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            return size;
        }
        return openPartition0(partitionIndex);
    }

    public boolean reload() {
        if (acquireTxn()) {
            return false;
        }
        try {
            long beforeReloadTicks = clock.getTicks();
            long i = reloadSlow(true);
            long afterReloadTicks = clock.getTicks();
            long reloadTicks = afterReloadTicks - beforeReloadTicks;
            if (reloadTicks > 50) {
                LOG.info().$("slow reader reload [table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(", millis=").$(reloadTicks)
                        .$(", iterations=").$(i)
                        .I$();
            }
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            boolean fastPath = reconcileOpenPartitions(txPartitionVersion, txColumnVersion, txTruncateVersion);
            long afterReconcileTicks = clock.getTicks();
            long reconcileTicks = afterReconcileTicks - afterReloadTicks;
            if (reconcileTicks > 50) {
                LOG.info().$("slow reader reconcile [table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(", millis=").$(reconcileTicks)
                        .$(", fastPath=").$(fastPath)
                        .I$();
            }

            // Save transaction details which impact the reloading.
            // Do not rely on txReader, it can be reloaded outside this method.
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
        return Math.max(Numbers.msb(Numbers.ceilPow2(columnCount) * 2), 0);
    }

    private static boolean growColumn(MemoryCMRDetachedImpl mem1, MemoryCMRDetachedImpl mem2, int columnType, long rowCount) {
        if (rowCount > 0) {
            if (ColumnType.isVarSize(columnType)) {
                if (mem2 == null) {
                    return false;
                }

                // Extend aux memory
                ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                long newSize = columnTypeDriver.getAuxVectorSize(rowCount);
                if (!mem2.tryChangeSize(newSize)) {
                    return false;
                }

                // Extend data memory
                long dataSize = columnTypeDriver.getDataVectorSizeAt(mem2.addressOf(0), rowCount - 1);
                if (mem1 != null) {
                    // because of dedup, the size of var data can grow or shrink
                    return mem1.tryChangeSize(dataSize);
                } else {
                    // dataSize can be 0 in case when it's a varchar column and all the values are inlined
                    // The data memory was not open, but now we need to open it. Mark the partition as not reloaded by returning false
                    return dataSize == 0;
                }
            } else {
                if (mem1 == null) {
                    return false;
                }

                return mem1.tryChangeSize(rowCount << ColumnType.pow2SizeOf(columnType));
            }
        }
        return true;
    }

    private boolean acquireTxn() {
        if (!txnAcquired) {
            try {
                if (txnScoreboard.acquireTxn(id, txn)) {
                    txnAcquired = true;
                } else {
                    return false;
                }
            } catch (CairoException ex) {
                // Scoreboard can be over allocated
                LOG.critical().$("cannot lock txn in scoreboard [table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(", error=").$safe(ex.getFlyweightMessage())
                        .I$();
                throw ex;
            }
        }

        // txFile can also be reloaded in goPassive->checkSchedulePurgeO3Partitions
        // if txFile txn doesn't match reader txn, reader has to be slow reloaded
        if (txn == txFile.getTxn()) {
            // We have to be sure the last txn is acquired in Scoreboard
            // otherwise the writer can delete partition version files
            // between reading txn file and acquiring txn in the Scoreboard.
            Unsafe.getUnsafe().loadFence();
            return txFile.getVersion() == txFile.unsafeReadVersion();
        }
        return false;
    }

    private void checkSchedulePurgeO3Partitions() {
        // In scoreboard V2, it is cheap to check that the txn released is not the max txn,
        // do it as a first step before more expensive checks.
        if (txnScoreboard.isOutdated(txn)) {
            long partitionTableVersion = txFile.getPartitionTableVersion();
            // In scoreboard V2 isTxnAvailable(txn) can be relatively expensive. We do this check at the end.
            if (txFile.unsafeLoadAll() && txFile.getPartitionTableVersion() > partitionTableVersion && txnScoreboard.isTxnAvailable(txn)) {
                // The last lock for this txn is released, and this is not the latest txn number
                // Schedule a job to clean up partition versions this reader may hold
                if (TableUtils.schedulePurgeO3Partitions(messageBus, tableToken, timestampType, partitionBy)) {
                    return;
                }

                LOG.error()
                        .$("could not queue purge partition task, queue is full [")
                        .$("table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(']').$();
            }
        }
    }

    private void closeDeletedPartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        long before = clock.getTicks();
        closePartitionResources(partitionIndex, offset);
        if (partitionSize > -1) {
            openPartitionCount--;
        }
        int columnBase = getColumnBase(partitionIndex);
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);
        bitmapIndexes.remove(baseIndex, newBaseIndex - 1);

        int colTopStart = columnBase / 2;
        int columnSlotSize = getColumnBase(1);
        columnTops.removeIndexBlock(colTopStart, columnSlotSize / 2);

        Misc.free(parquetPartitions.get(partitionIndex));
        parquetPartitions.remove(partitionIndex);
        openPartitionInfo.removeIndexBlock(offset, PARTITIONS_SLOT_SIZE);
        LOG.info().$("closed deleted partition [table=").$(tableToken)
                .$(", ts=").$ts(ColumnType.getTimestampDriver(timestampType), partitionTimestamp)
                .$(", partitionIndex=").$(partitionIndex)
                .$(", thread=").$(Thread.currentThread().getName())
                .$(", millis=").$(clock.getTicks() - before)
                .I$();
        partitionCount--;
    }

    private void closeIndexReader(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        Misc.free(bitmapIndexes.getQuick(index));
        Misc.free(bitmapIndexes.getQuick(index + 1));
    }

    private void closeParquetPartition(int partitionIndex) {
        Misc.free(parquetPartitions.getQuick(partitionIndex));
        int columnBase = getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            closeIndexReader(columnBase, i);
        }
    }

    private void closePartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        long before = clock.getTicks();
        closePartitionResources(partitionIndex, offset);
        long delta = clock.getTicks() - before;
        LOG.info().$("closed partition [path=").$substr(dbRootSize, path)
                .$(", timestamp=").$ts(ColumnType.getTimestampDriver(timestampType), partitionTimestamp)
                .$(", thread=").$(Thread.currentThread().getName())
                .$(", millis=").$(delta)
                .I$();
        if (partitionSize > -1) {
            openPartitionCount--;
        }
    }

    private void closePartitionColumn(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        long before = clock.getTicks();
        Misc.free(columns.get(index));
        Misc.free(columns.get(index + 1));
        long afterColumnClose = clock.getTicks();
        closeIndexReader(base, columnIndex);
        long afterIndexClose = clock.getTicks();

        long totalMillis = afterIndexClose - before;
        if (totalMillis > 50) {
            long indexCloseMillis = afterIndexClose - afterColumnClose;
            long columnCloseMillis = afterColumnClose - before;
            LOG.info().$("slow column partition close [table=").$(tableToken)
                    .$(", index=").$(index)
                    .$(", thread=").$(Thread.currentThread().getName())
                    .$(", millis=").$(totalMillis)
                    .$(", indexMillis=").$(indexCloseMillis)
                    .$(", columnMillis=").$(columnCloseMillis)
                    .I$();
        }
    }

    private void closePartitionColumns(int columnBase) {
        for (int i = 0; i < columnCount; i++) {
            closePartitionColumn(columnBase, i);
        }
    }

    private void closePartitionResources(int partitionIndex, int offset) {
        // we will call this method even if partition has been closed already, or it doesn't exist,
        // hence we ignore the "unknown" format
        final byte format = getPartitionFormat(partitionIndex);
        switch (format) {
            case PartitionFormat.PARQUET:
                closeParquetPartition(partitionIndex);
                break;
            case PartitionFormat.NATIVE:
                int columnBase = getColumnBase(partitionIndex);
                closePartitionColumns(columnBase);
                break;
            default:
                break;
        }
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
    }

    private long closeRewrittenPartitionFiles(int partitionIndex, int oldBase) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTs = openPartitionInfo.getQuick(offset);
        long existingPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        long newNameTxn = txFile.getPartitionNameTxnByPartitionTimestamp(partitionTs);
        long newSize = txFile.getPartitionRowCountByTimestamp(partitionTs);
        if (existingPartitionNameTxn != newNameTxn || newSize < 0) {
            LOG.debug().$("close outdated partition files [table=").$(tableToken).$(", ts=")
                    .$ts(ColumnType.getTimestampDriver(timestampType), partitionTs).$(", nameTxn=").$(newNameTxn).$();
            // Close all columns, partition is overwritten. Partition reconciliation process will re-open correct files
            if (getPartitionFormat(partitionIndex) == PartitionFormat.NATIVE) {
                for (int i = 0; i < columnCount; i++) {
                    closePartitionColumn(oldBase, i);
                }
            }
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            openPartitionCount--;
            return -1;
        }
        long nameTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        //noinspection resource
        pathGenNativePartition(partitionIndex, nameTxn);
        return newSize;
    }

    private void copyColumns(
            int fromBase,
            int fromColumnIndex,
            ObjList<MemoryCMR> toColumns,
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

    private TableReaderMetadata copyMeta(TableReaderMetadata srcMeta) {
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken);
        try {
            metadata.loadFrom(srcMeta);
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    private BitmapIndexReader createBitmapIndexReaderAt(int globalIndex, int columnBase, int columnIndex, long columnNameTxn, int direction, long partitionTxn) {
        BitmapIndexReader reader;
        if (!metadata.isColumnIndexed(columnIndex)) {
            throw CairoException.critical(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
        }
        MemoryR col = columns.getQuick(globalIndex);
        if (col instanceof NullMemoryCMR) {
            if (direction == BitmapIndexReader.DIR_BACKWARD) {
                reader = new BitmapIndexBwdNullReader(columnNameTxn, partitionTxn);
                bitmapIndexes.setQuick(globalIndex, reader);
            } else {
                reader = new BitmapIndexFwdNullReader(columnNameTxn, partitionTxn);
                bitmapIndexes.setQuick(globalIndex + 1, reader);
            }
        } else {
            Path path = pathGenNativePartition(getPartitionIndex(columnBase), partitionTxn);
            try {
                if (direction == BitmapIndexReader.DIR_BACKWARD) {
                    reader = new BitmapIndexBwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            partitionTxn,
                            getColumnTop(columnBase, columnIndex)
                    );
                    bitmapIndexes.setQuick(globalIndex, reader);
                } else {
                    reader = new BitmapIndexFwdReader(
                            configuration,
                            path,
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            partitionTxn,
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

    private void createNewColumnList(int columnCount, TableReaderMetadataTransitionIndex transitionIndex, int columnCountShl) {
        LOG.debug().$("resizing columns file list [table=").$(tableToken).I$();
        int capacity = partitionCount << columnCountShl;
        final ObjList<MemoryCMR> toColumns = new ObjList<>(capacity + 2);
        final LongList toColumnTops = new LongList(capacity / 2);
        final ObjList<BitmapIndexReader> toIndexReaders = new ObjList<>(capacity);
        toColumns.setPos(capacity + 2);
        toColumns.setQuick(0, NullMemoryCMR.INSTANCE);
        toColumns.setQuick(1, NullMemoryCMR.INSTANCE);
        toColumnTops.setPos(capacity / 2);
        toIndexReaders.setPos(capacity + 2);
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int toBase = partitionIndex << columnCountShl;
            final int fromBase = partitionIndex << this.columnCountShl;

            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1 && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, fromBase)) > -1) {
                    for (int i = 0; i < iterateCount; i++) {
                        if (transitionIndex.closeColumn(i)) {
                            closePartitionColumn(fromBase, i);
                        }

                        if (transitionIndex.replaceWithNew(i)) {
                            // new instance
                            reloadColumnAt(partitionIndex, path, toColumns, toColumnTops, toIndexReaders, toBase, i, partitionRowCount);
                        } else {
                            final int fromColumnIndex = transitionIndex.getCopyFromIndex(i);
                            assert fromColumnIndex < this.columnCount;
                            copyColumns(fromBase, fromColumnIndex, toColumns, toColumnTops, toIndexReaders, toBase, i);
                        }
                    }
                }
            } catch (Throwable th) {
                closePartitionColumns(fromBase);
                openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1);
                Misc.freeObjListIfCloseable(toColumns);
                throw th;
            } finally {
                path.trimTo(rootLen);
            }
        }
        this.columns = toColumns;
        this.columnTops = toColumnTops;
        this.columnCountShl = columnCountShl;
        this.bitmapIndexes = toIndexReaders;
    }

    private void formatErrorPartitionDirName(int partitionIndex, Utf16Sink sink) {
        TableUtils.setSinkForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                -1
        );
    }

    private void formatNativePartitionDirName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn
        );
    }

    private void formatParquetPartitionFileName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForParquetPartition(
                sink,
                timestampType,
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

    private void freeParquetPartitions() {
        Misc.freeObjList(parquetPartitions);
    }

    private void freeSymbolMapReaders() {
        for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
            Misc.freeIfCloseable(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
    }

    private void freeTempMem() {
        if (tempMem8b != 0) {
            tempMem8b = Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        }
    }

    private void init() {
        txPartitionVersion = txFile.getPartitionTableVersion();
        txColumnVersion = txFile.getColumnVersion();
        txTruncateVersion = txFile.getTruncateVersion();

        columnCount = metadata.getColumnCount();
        columnCountShl = getColumnBits(columnCount);
        openSymbolMaps();
        partitionCount = txFile.getPartitionCount();

        int capacity = getColumnBase(partitionCount);
        parquetPartitions = new ObjList<>(partitionCount);
        parquetPartitions.setAll(partitionCount, NullMemoryCMR.INSTANCE);
        columns = new ObjList<>(capacity + 2);
        columns.setPos(capacity + 2);
        columns.setQuick(0, NullMemoryCMR.INSTANCE);
        columns.setQuick(1, NullMemoryCMR.INSTANCE);
        bitmapIndexes = new ObjList<>(capacity + 2);
        bitmapIndexes.setPos(capacity + 2);

        openPartitionInfo = initOpenPartitionInfo();
        columnTops = new LongList(capacity / 2);
        columnTops.setPos(capacity / 2);
    }

    private @NotNull LongList initOpenPartitionInfo() {
        final LongList openPartitionInfo = new LongList(partitionCount * PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setPos(partitionCount * PARTITIONS_SLOT_SIZE);
        for (int i = 0; i < partitionCount; i++) {
            // ts, number of rows, txn, column version for each partition
            // it is compared to attachedPartitions within the txn file to determine if a partition needs to be reloaded or not
            final int baseOffset = i * PARTITIONS_SLOT_SIZE;
            final long partitionTimestamp = txFile.getPartitionTimestampByIndex(i);
            final boolean isParquet = txFile.isPartitionParquet(i);
            openPartitionInfo.setQuick(baseOffset, partitionTimestamp);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_SIZE, -1); // -1 means it is not open
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txFile.getPartitionNameTxn(i));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_FORMAT, isParquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE);
        }
        return openPartitionInfo;
    }

    private void insertPartition(int partitionIndex, long timestamp) {
        final int columnBase = getColumnBase(partitionIndex);
        final int columnSlotSize = getColumnBase(1);

        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize, NullMemoryCMR.INSTANCE);
        bitmapIndexes.insert(idx, columnSlotSize, null);
        parquetPartitions.insert(partitionIndex, 1, NullMemoryCMR.INSTANCE);

        final int topBase = columnBase / 2;
        final int topSlotSize = columnSlotSize / 2;
        columnTops.insert(topBase, topSlotSize);
        columnTops.seed(topBase, topSlotSize, 0);

        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        openPartitionInfo.insert(offset, PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setQuick(offset, timestamp);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, -1);
        partitionCount++;
        LOG.debug().$("inserted partition [index=").$(partitionIndex).$(", table=").$(tableToken)
                .$(", timestamp=").$ts(ColumnType.getTimestampDriver(timestampType), timestamp).I$();
    }

    // this method is not thread safe
    @NotNull
    private SymbolMapReaderImpl newSymbolMapReader(int symbolColumnIndex, int columnIndex) {
        // symbol column index is the index of symbol column in a dense array of symbol columns, e.g.,
        // if table has only one symbol columns, the symbolColumnIndex is 0 regardless of column position
        // in the metadata.
        return new SymbolMapReaderImpl(
                configuration,
                path,
                metadata.getColumnName(columnIndex),
                columnVersionReader.getSymbolTableNameTxn(metadata.getWriterIndex(columnIndex)),
                txFile.getSymbolValueCount(symbolColumnIndex)
        );
    }

    private TableReaderMetadata openMetaFile() {
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken);
        try {
            metadata.loadMetadata();
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    @NotNull
    private MemoryCMRDetachedImpl openOrCreateColumnMemory(
            Path path,
            ObjList<MemoryCMR> columns,
            int primaryIndex,
            @Nullable MemoryCMR mem,
            long columnSize,
            boolean keepFdOpen
    ) {
        MemoryCMRDetachedImpl memory;
        if (mem != null && mem != NullMemoryCMR.INSTANCE) {
            memory = (MemoryCMRDetachedImpl) mem;
            memory.of(ff, path.$(), columnSize, columnSize, MemoryTag.MMAP_TABLE_READER, 0, -1, keepFdOpen);
        } else {
            memory = new MemoryCMRDetachedImpl(ff, path.$(), columnSize, MemoryTag.MMAP_TABLE_READER, keepFdOpen);
            columns.setQuick(primaryIndex, memory);
        }
        return memory;
    }

    private long openPartition0(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        if (txFile.getPartitionCount() < 2 && txFile.getTransientRowCount() == 0) {
            return -1;
        }

        try {
            final long partitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);

            if (txFile.isPartitionParquet(partitionIndex)) {
                Path path = pathGenParquetPartition(partitionIndex, partitionNameTxn);
                if (ff.exists(path.$())) {
                    final long partitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                    if (partitionSize > -1) {
                        LOG.info()
                                .$("open partition [path=").$substr(dbRootSize, path)
                                .$(", rowCount=").$(partitionSize)
                                .$(", partitionIndex=").$(partitionIndex)
                                .$(", partitionCount=").$(partitionCount)
                                .$(", format=parquet")
                                .I$();

                        final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, partitionNameTxn);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, PartitionFormat.PARQUET);

                        final long parquetSize = txFile.getPartitionParquetFileSize(partitionIndex);
                        assert parquetSize > 0;
                        MemoryCMR parquetMem = parquetPartitions.getQuick(partitionIndex);
                        if (parquetMem != null && parquetMem != NullMemoryCMR.INSTANCE) {
                            parquetMem.of(ff, path.$(), parquetSize, parquetSize, MemoryTag.MMAP_TABLE_READER);
                        } else {
                            // Don't keep fd around to close/open reconciled parquet partitions instead of mremap'ping them.
                            parquetMem = new MemoryCMRDetachedImpl(ff, path.$(), parquetSize, MemoryTag.MMAP_TABLE_READER, false);
                            parquetPartitions.setQuick(partitionIndex, parquetMem);
                        }
                        openPartitionCount++;
                    }

                    return partitionSize;
                }
            } else { // native partition
                Path path = pathGenNativePartition(partitionIndex, partitionNameTxn);
                if (ff.exists(path.$())) {
                    final long partitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                    if (partitionSize > -1) {
                        LOG.debug()
                                .$("open partition [path=").$substr(dbRootSize, path)
                                .$(", rowCount=").$(partitionSize)
                                .$(", partitionIndex=").$(partitionIndex)
                                .$(", partitionCount=").$(partitionCount)
                                .$(", format=native")
                                .I$();

                        final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, partitionNameTxn);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, PartitionFormat.NATIVE);
                        openPartitionColumns(partitionIndex, path, getColumnBase(partitionIndex), partitionSize);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                        openPartitionCount++;
                    }

                    return partitionSize;
                }
            }
            LOG.error().$("open partition failed, partition does not exist on the disk [path=").$(path).I$();

            if (PartitionBy.isPartitioned(getPartitionedBy())) {
                CairoException exception = CairoException.critical(0).put("Partition '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("' does not exist in table '")
                        .put(tableToken.getTableName())
                        .put("' directory. Run [ALTER TABLE ").put(tableToken.getTableName()).put(" FORCE DROP PARTITION LIST '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("'] to repair the table or the database from the backup.");
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
        try {
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
        } catch (Throwable th) {
            closePartitionColumns(columnBase);
            openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            throw th;
        }
    }

    private void openSymbolMaps() {
        final int columnCount = metadata.getColumnCount();
        // ensure symbolMapReaders has capacity for columnCount entries
        symbolMapReaders.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                // symbolMapReaders is sparse
                symbolMapReaders.set(i, newSymbolMapReader(metadata.getDenseSymbolIndex(i), i));
            }
        }
    }

    private Path pathGenNativePartition(int partitionIndex, long nameTxn) {
        formatNativePartitionDirName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private Path pathGenParquetPartition(int partitionIndex, long nameTxn) {
        formatParquetPartitionFileName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private void prepareForLazyOpen(int partitionIndex) {
        closePartition(partitionIndex);
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
                            .$(", maxTimestamp=").$ts(ColumnType.getTimestampDriver(timestampType), txFile.getMaxTimestamp())
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
                throw CairoException.critical(0).put("Transaction read timeout [src=reader, table=").put(tableToken).put(", timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
            }
            Os.pause();
        }
    }

    private boolean reconcileOpenPartitions(long prevPartitionVersion, long prevColumnVersion, long prevTruncateVersion) {
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
                    if (openPartitionSize > -1) {
                        final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                        final long txPartitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                        final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                        if (openPartitionNameTxn == txPartitionNameTxn) {
                            // We used to skip reloading partition size if the row count is the same and name txn is the same.
                            // But in case of dedup, the row count can be same, but the data can be overwritten by splitting and squashing the partition back
                            // This is ok for fixed size columns but var length columns have to be re-mapped to the bigger / smaller sizes
                            final byte format = getPartitionFormat(partitionIndex);
                            assert format != -1;
                            if (format == PartitionFormat.NATIVE) {
                                if (reloadColumnFiles(partitionIndex, txPartitionSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                                } else {
                                    prepareForLazyOpen(partitionIndex);
                                }
                            } else {
                                // reload Parquet file
                                final long parquetSize = txFile.getPartitionParquetFileSize(partitionIndex);
                                if (reloadParquetFile(partitionIndex, parquetSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated parquet partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                                } else {
                                    prepareForLazyOpen(partitionIndex);
                                }
                            }
                        } else {
                            prepareForLazyOpen(partitionIndex);
                        }
                    }
                    partitionIndex++;
                }
                for (; partitionIndex < txPartitionCount; partitionIndex++) {
                    insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex));
                }
                reloadSymbolMapCounts();
            }
            return true;
        }
        reconcileOpenPartitions0(truncateHappened);
        return false;
    }

    private void reconcileOpenPartitions0(boolean forceTruncate) {
        int partitionIndex = 0;
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
                final long txPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                final long openPartitionColumnVersion = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION);

                if (!forceTruncate) {
                    if (openPartitionNameTxn == txPartitionNameTxn && openPartitionColumnVersion == columnVersionReader.getMaxPartitionVersion(txPartTs)) {
                        // We used to skip reloading partition size if the row count is the same and name txn is the same.
                        // But in case of dedup, the row count can be same, but the data can be overwritten by splitting and squashing the partition back
                        // This is ok for fixed size columns but var length columns have to be re-mapped to the bigger / smaller sizes
                        if (openPartitionSize > -1) {
                            final byte format = getPartitionFormat(partitionIndex);
                            assert format != -1;
                            if (format == PartitionFormat.NATIVE) {
                                if (reloadColumnFiles(partitionIndex, txPartitionSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated partition size [partition=").$(openPartitionTimestamp).I$();
                                } else {
                                    prepareForLazyOpen(partitionIndex);
                                }
                            } else {
                                // reload Parquet file
                                final long parquetSize = txFile.getPartitionParquetFileSize(partitionIndex);
                                if (reloadParquetFile(partitionIndex, parquetSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated parquet partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                                } else {
                                    prepareForLazyOpen(partitionIndex);
                                }
                            }
                        }
                    } else {
                        prepareForLazyOpen(partitionIndex);
                    }
                    changed = true;
                } else if (openPartitionSize > -1 && txPartitionSize > -1) { // Don't force re-open if not yet opened
                    prepareForLazyOpen(partitionIndex);
                }
                txPartitionIndex++;
                partitionIndex++;
            }
        }

        // if while finished on txPartitionIndex == txPartitionCount condition
        // removes deleted opened partitions
        while (partitionIndex < partitionCount) {
            closeDeletedPartition(partitionIndex);
            changed = true;
        }

        // if while finished on partitionIndex == partitionCount condition
        // inserts new partitions at the end
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

    private boolean releaseTxn() {
        if (txnAcquired) {
            long readerCount = txnScoreboard.releaseTxn(id, txn);
            txnAcquired = false;
            return readerCount == 0;
        }
        return false;
    }

    private void reloadAllSymbols() {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
                if (symbolMapReader instanceof SymbolMapReaderImpl) {
                    final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
                    final long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(writerColumnIndex);
                    int symbolCount = txFile.getSymbolValueCount(metadata.getDenseSymbolIndex(columnIndex));
                    ((SymbolMapReaderImpl) symbolMapReader).of(configuration, path, metadata.getColumnName(columnIndex), symbolTableNameTxn, symbolCount);
                }
            }
        }
    }

    private void reloadAtTxn(TableReader srcReader, boolean reshuffle) {
        releaseTxn();
        final long txn = srcReader.getTxn();
        if (!txnScoreboard.incrementTxn(id, txn)) {
            throw CairoException.critical(0).put("could not acquire txn for copy, source reader has to be active [table=")
                    .put(tableToken.getTableName()).put(", txn=").put(txn).put(']');
        }
        this.txn = txn;
        txnAcquired = true;
        txFile.loadAllFrom(srcReader.txFile);
        columnVersionReader.readFrom(srcReader.columnVersionReader);
        reloadMetadataFrom(srcReader.metadata, reshuffle);
    }

    private void reloadColumnAt(
            int partitionIndex,
            Path path,
            ObjList<MemoryCMR> columns,
            LongList columnTops,
            ObjList<BitmapIndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount
    ) {
        final int plen = path.size();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = primaryIndex + 1;
            final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
            final byte partitionFormat = (byte) openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_FORMAT);
            final long partitionTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
            int writerIndex = metadata.getWriterIndex(columnIndex);
            final int versionRecordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex);
            final long columnTop = versionRecordIndex > -1 ? columnVersionReader.getColumnTopByIndex(versionRecordIndex) : 0;
            long columnTxn = versionRecordIndex > -1 ? columnVersionReader.getColumnNameTxnByIndex(versionRecordIndex) : -1;
            if (columnTxn == -1) {
                // When a column is added, a column version will have txn number for the partition
                // where it's added. It will also have the txn number in the [default] partition
                columnTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
            }
            final long columnRowCount = partitionRowCount - columnTop;

            // When column is added mid-table existence, the top record is only
            // created in the current partition. Older partitions would simply have no
            // column file. This makes it necessary to check the partition timestamp in Column Version file
            // of when the column was added.
            if (columnRowCount > 0 && (versionRecordIndex > -1 || columnVersionReader.getColumnTopPartitionTimestamp(writerIndex) <= partitionTimestamp)) {
                if (partitionFormat == PartitionFormat.NATIVE) {
                    final int columnType = metadata.getColumnType(columnIndex);

                    final MemoryCMR dataMem = columns.getQuick(primaryIndex);
                    // We intend to keep the file handle open only for the last partition. All other
                    // partitions will have the file handle closed after memory is mapped. The potential knock-on
                    // effect of that is when user workload is such that it involved appending to non-last partition,
                    // the reader will incur file-reopen and re-map instead of "realloc" call
                    boolean lastPartition = partitionIndex == partitionCount - 1;
                    if (ColumnType.isVarSize(columnType)) {
                        final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                        long auxSize = columnTypeDriver.getAuxVectorSize(columnRowCount);
                        TableUtils.iFile(path.trimTo(plen), name, columnTxn);
                        MemoryCMR auxMem = columns.getQuick(secondaryIndex);
                        // Keep aux files fds open, they are read every time TableReader partition is reopened
                        // to find out what memory to map of the data file.
                        auxMem = openOrCreateColumnMemory(path, columns, secondaryIndex, auxMem, auxSize, lastPartition);
                        long dataSize = columnTypeDriver.getDataVectorSizeAt(auxMem.addressOf(0), columnRowCount - 1);
                        if (dataSize < columnTypeDriver.getDataVectorMinEntrySize() || dataSize >= (1L << 40)) {
                            LOG.critical().$("Invalid var len column size [column=").$safe(name)
                                    .$(", size=").$(dataSize)
                                    .$(", path=").$(path)
                                    .I$();
                            throw CairoException.critical(0).put("Invalid column size [column=").put(path)
                                    .put(", size=").put(dataSize)
                                    .put(']');
                        }
                        TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                        openOrCreateColumnMemory(path, columns, primaryIndex, dataMem, dataSize, lastPartition);
                    } else {
                        TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                        openOrCreateColumnMemory(
                                path,
                                columns,
                                primaryIndex,
                                dataMem,
                                columnRowCount << ColumnType.pow2SizeOf(columnType),
                                lastPartition
                        );
                        Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                    }
                } else {
                    assert partitionFormat == PartitionFormat.PARQUET;
                    Misc.free(columns.getAndSetQuick(primaryIndex, null));
                    Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    BitmapIndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader != null) {
                        indexReader.of(configuration, path.trimTo(plen), name, columnTxn, partitionTxn, columnTop);
                    }
                } else {
                    Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullMemoryCMR.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullMemoryCMR.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not always be required
                Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));

                // Column is not present in the partition. Set column top to be the size of the partition.
                columnTops.setQuick(columnBase / 2 + columnIndex, partitionRowCount);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Updates boundaries of all columns in partition.
     *
     * @param partitionIndex index of partition
     * @param rowCount       number of rows in partition
     */
    private boolean reloadColumnFiles(int partitionIndex, long rowCount) {
        int columnBase = getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            final int index = getPrimaryColumnIndex(columnBase, i);
            MemoryCMR mem1 = columns.getQuick(index);

            long columnFilesRowCount = rowCount - getColumnTop(columnBase, i);
            if (columnFilesRowCount > 0 &&
                    (mem1 == NullMemoryCMR.INSTANCE || !growColumn(
                            (MemoryCMRDetachedImpl) mem1,
                            (MemoryCMRDetachedImpl) columns.getQuick(index + 1),
                            metadata.getColumnType(i),
                            columnFilesRowCount
                    ))) {
                return false;
            }
            closeIndexReader(columnBase, i);

        }
        return true;
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
            try {
                if (!metadata.prepareTransition(txnMetadataVersion)) {
                    if (clock.getTicks() < deadline) {
                        return false;
                    }
                    throw CairoException.critical(0).put("Metadata read timeout [src=reader, timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
                }
            } catch (CairoException ex) {
                // This is a temporary solution until we can get multiple versions of metadata not overwriting each other
                TableUtils.handleMetadataLoadException(tableToken, deadline, ex, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                continue;
            }

            assert !reshuffleColumns || metadata.getColumnCount() == this.columnCount;
            final TableReaderMetadataTransitionIndex transitionIndex = metadata.applyTransition();
            if (reshuffleColumns) {
                reshuffleColumns(transitionIndex);
            }
            return true;
        }
    }

    private void reloadMetadataFrom(TableReaderMetadata srcMeta, boolean reshuffleColumns) {
        // create transition index, which will help us reuse already open resources
        if (srcMeta.getMetadataVersion() == metadata.getMetadataVersion()) {
            return;
        }

        assert !reshuffleColumns || metadata.getColumnCount() == this.columnCount;
        final TableReaderMetadataTransitionIndex transitionIndex = metadata.applyTransitionFrom(srcMeta);
        if (reshuffleColumns) {
            reshuffleColumns(transitionIndex);
        }
    }

    private boolean reloadParquetFile(int partitionIndex, long parquetSize) {
        MemoryCMR parquetMem = parquetPartitions.getQuick(partitionIndex);
        if (parquetMem == null || parquetMem == NullMemoryCMR.INSTANCE) {
            return false;
        }
        // We don't keep fd around when mmap'ping parquet files, so the only case
        // when the below call returns true is when the file size didn't change.
        return ((MemoryCMRDetachedImpl) parquetMem).tryChangeSize(parquetSize);
    }

    private long reloadSlow(boolean reshuffle) {
        final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
        long i = 0;
        do {
            i++;
            // Reload txn
            readTxnSlow(deadline);
            // Reload _meta if the structure version updated, reload _cv if column version updated
        } while (
            // Reload column versions, column version used in metadata reload column shuffle
                !reloadColumnVersion(txFile.getColumnVersion(), deadline)
                        // Start again if _meta with the matching structure version cannot be loaded
                        || !reloadMetadata(txFile.getMetadataVersion(), deadline, reshuffle)
        );
        return i;
    }

    private void reloadSymbolMapCounts() {
        for (int i = 0; i < columnCount; i++) {
            if (!ColumnType.isSymbol(metadata.getColumnType(i))) {
                continue;
            }
            symbolMapReaders.getQuick(i).updateSymbolCount(txFile.getSymbolValueCount(metadata.getDenseSymbolIndex(i)));
        }
    }

    private void renewSymbolMapReader(SymbolMapReader reader, int columnIndex) {
        if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
            final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
            final long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(writerColumnIndex);
            String columnName = metadata.getColumnName(columnIndex);
            if (!(reader instanceof SymbolMapReaderImpl symbolMapReader)) {
                reader = new SymbolMapReaderImpl(configuration, path, columnName, symbolTableNameTxn, 0);
            } else {
                // Fully reopen the symbol map reader only when necessary
                if (symbolMapReader.needsReopen(symbolTableNameTxn)) {
                    symbolMapReader.of(configuration, path, columnName, symbolTableNameTxn, 0);
                }
            }
        } else {
            if (reader instanceof SymbolMapReaderImpl) {
                ((SymbolMapReaderImpl) reader).close();
                reader = null;
            }
        }
        symbolMapReaders.setQuick(columnIndex, reader);
    }

    private void reshuffleColumns(TableReaderMetadataTransitionIndex transitionIndex) {
        final int columnCount = metadata.getColumnCount();

        int columnCountShl = getColumnBits(columnCount);
        // when a column is added, we cannot easily reshuffle columns in-place,
        // the reason is that we'd have to create gaps in the column list between
        // partitions. It is possible in theory, but this could be an algo for
        // another day.
        if (columnCountShl > this.columnCountShl) {
            createNewColumnList(columnCount, transitionIndex, columnCountShl);
        } else {
            reshuffleColumns(columnCount, transitionIndex);
        }
        // rearrange symbol map reader list
        reshuffleSymbolMapReaders(transitionIndex, columnCount);
        this.columnCount = columnCount;
        reloadSymbolMapCounts();
    }

    private void reshuffleColumns(int columnCount, TableReaderMetadataTransitionIndex transitionIndex) {
        LOG.debug().$("reshuffling columns file list [table=").$(tableToken).I$();
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1 && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base)) > -1) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int copyFrom = transitionIndex.getCopyFromIndex(i);

                        if (transitionIndex.closeColumn(i)) {
                            // This column is deleted (not moved).
                            // Close all files
                            closePartitionColumn(base, i);
                        }

                        // We should only remove columns from existing metadata if column count has reduced.
                        // And we should not attempt to reload columns, which have no matches in the metadata
                        if (i < columnCount) {
                            if (copyFrom == i) {
                                // It appears that the column hasn't changed its position. There are three possibilities here:
                                // 1. The column has been forced out of the reader via closeColumnForRemove(). This is required
                                //    on Windows before column can be deleted. In this case, we must check for marker
                                //    instance and the column from disk
                                // 2. The column hasn't been altered, and we can skip to the next column.
                                MemoryMR col = columns.getQuick(getPrimaryColumnIndex(base, i));
                                if (col instanceof NullMemoryCMR || (col != null && !col.isOpen())) {
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

    private void reshuffleSymbolMapReaders(TableReaderMetadataTransitionIndex transitionIndex, int columnCount) {
        if (columnCount > this.columnCount) {
            symbolMapReaders.setPos(columnCount);
        }

        // index structure is
        // [action: int, copy from:int]

        // action: if -1 then current column in slave is deleted or renamed, else it's reused
        // "copy from" >= 0 indicates that column is to be copied from slave position
        // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
        // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source

        for (int i = 0, n = Math.max(columnCount, this.columnCount); i < n; i++) {
            if (transitionIndex.closeColumn(i)) {
                // deleted
                Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(i, null));
            }

            final int replaceWith = transitionIndex.getCopyFromIndex(i);
            if (replaceWith > -1) {
                SymbolMapReader rdr = symbolMapReaders.getQuick(replaceWith);
                renewSymbolMapReader(rdr, i);
            } else if (replaceWith != Integer.MIN_VALUE) {
                // new instance
                renewSymbolMapReader(null, i);
            }
        }
    }
}
