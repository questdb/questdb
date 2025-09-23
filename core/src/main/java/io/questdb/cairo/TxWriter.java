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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public final class TxWriter extends TxReader implements Closeable, Mutable, SymbolValueCountCollector {
    private final CairoConfiguration configuration;
    private long baseVersion;
    private TableWriter.ExtensionListener extensionListener;
    private int lastRecordBaseOffset = -1;
    private long lastRecordStructureVersion = -1;
    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private long prevPartitionTableVersion = -1;
    private int prevRecordBaseOffset = -2;
    private long prevRecordStructureVersion = -2;
    private long prevTransientRowCount;
    private int readBaseOffset;
    private long readRecordSize;
    private long recordStructureVersion = 0;
    private MemoryCMARW txMemBase;
    private int txPartitionCount;
    private int writeAreaSize;
    private int writeBaseOffset;

    public TxWriter(FilesFacade ff, CairoConfiguration configuration) {
        super(ff);
        this.configuration = configuration;
    }

    public void append() {
        transientRowCount++;
    }

    public void beginPartitionSizeUpdate() {
        if (maxTimestamp != Long.MIN_VALUE) {
            // Last partition size is usually not stored in attached partitions list
            // but in transientRowCount only.
            // To resolve transientRowCount after out of order partition update
            // let's store it in attached partitions list
            // before out of order partition update happens
            updatePartitionSizeByTimestamp(maxTimestamp, transientRowCount);
        }
    }

    public void bumpColumnStructureVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        structureVersion = Numbers.encodeLowHighInts(getMetadataVersion(), getColumnStructureVersion() + 1);
        commit(denseSymbolMapWriters);
    }

    public void bumpMetadataAndColumnStructureVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        structureVersion = Numbers.decodeHighInt(structureVersion) != 0 ? Numbers.encodeLowHighInts(getMetadataVersion() + 1, getColumnStructureVersion() + 1) : structureVersion + 1;
        commit(denseSymbolMapWriters);
    }

    public void bumpMetadataVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        int colStoreVersion = getColumnStructureVersion();
        if (colStoreVersion == 0) {
            colStoreVersion = NONE_COL_STRUCTURE_VERSION;
        }
        structureVersion = Numbers.encodeLowHighInts(getMetadataVersion() + 1, colStoreVersion);
        commit(denseSymbolMapWriters);
    }

    public void bumpPartitionTableVersion() {
        recordStructureVersion++;
        partitionTableVersion++;
    }

    public void bumpTruncateVersion() {
        truncateVersion++;
    }

    public void cancelRow() {
        boolean allRowsCancelled = transientRowCount <= 1 && fixedRowCount == 0;
        if (transientRowCount == 1 && txPartitionCount > 1) {
            // we have to undo creation of partition
            txPartitionCount--;
            fixedRowCount -= prevTransientRowCount;
            transientRowCount = prevTransientRowCount + 1; // When row cancel finishes 1 is subtracted. Add 1 to compensate.
            attachedPartitions.setPos(attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION);
            prevTransientRowCount = getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
        }

        if (allRowsCancelled) {
            maxTimestamp = Long.MIN_VALUE;
            minTimestamp = Long.MAX_VALUE;
            prevMinTimestamp = minTimestamp;
            prevMaxTimestamp = maxTimestamp;
        } else {
            maxTimestamp = prevMaxTimestamp;
            minTimestamp = prevMinTimestamp;
        }

        recordStructureVersion++;
    }

    public long cancelToMaxTimestamp() {
        return prevMaxTimestamp;
    }

    public long cancelToTransientRowCount() {
        return prevTransientRowCount;
    }

    @Override
    public void clear() {
        clearData();
        if (txMemBase != null) {
            // Never trim _txn file to size. Size of the file can only grow up.
            txMemBase.close(false);
        }
        recordStructureVersion = 0L;
        lastRecordStructureVersion = -1L;
        prevRecordStructureVersion = -2L;
        lastRecordBaseOffset = -1;
        prevRecordBaseOffset = -2;
    }

    @Override
    public void close() {
        try {
            clear();
            txMemBase = null;
        } finally {
            super.close();
        }
    }

    @Override
    public void collectValueCount(int symbolIndexInTxWriter, int count) {
        writeTransientSymbolCount(symbolIndexInTxWriter, count);
    }

    public void commit(ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        if (prevRecordStructureVersion == recordStructureVersion && prevRecordBaseOffset > 0) {
            // Optimisation for the case where commit appends rows to the last partition only
            // In this case all to be changed is TX_OFFSET_MAX_TIMESTAMP_64 and TX_OFFSET_TRANSIENT_ROW_COUNT_64
            writeBaseOffset = prevRecordBaseOffset;
            putLong(TX_OFFSET_TXN_64, ++txn);
            putLong(TX_OFFSET_SEQ_TXN_64, seqTxn);
            putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
            putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
            putLagValues();

            // Store symbol counts. Unfortunately we cannot skip it in here
            storeSymbolCounts(symbolCountProviders);

            Unsafe.getUnsafe().storeFence();
            txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

            super.switchRecord(writeBaseOffset, writeAreaSize); // writeAreaSize should be between records
            readBaseOffset = writeBaseOffset;

            prevTransientRowCount = transientRowCount;
            prevMinTimestamp = minTimestamp;
            prevMaxTimestamp = maxTimestamp;

            prevRecordBaseOffset = lastRecordBaseOffset;
            lastRecordBaseOffset = writeBaseOffset;
            prevPartitionTableVersion = partitionTableVersion;
            int commitMode = configuration.getCommitMode();
            if (commitMode != CommitMode.NOSYNC) {
                txMemBase.sync(commitMode == CommitMode.ASYNC);
            }
        } else {
            // Slow path, record structure changed
            commitFullRecord(configuration.getCommitMode(), symbolCountProviders);
        }
    }

    public void finishPartitionSizeUpdate(long minTimestamp, long maxTimestamp) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        finishPartitionSizeUpdate();
    }

    public void finishPartitionSizeUpdate() {
        recordStructureVersion++;
        int numPartitions = getPartitionCount();
        transientRowCount = numPartitions > 0 ? getPartitionSize(numPartitions - 1) : 0L;
        fixedRowCount = 0L;
        txPartitionCount = getPartitionCount();
        for (int i = 0, hi = txPartitionCount - 1; i < hi; i++) {
            fixedRowCount += getPartitionSize(i);
        }
    }

    public int getAppendedPartitionCount() {
        return txPartitionCount;
    }

    public long getLastTxSize() {
        return txPartitionCount == 1 ? transientRowCount - prevTransientRowCount : transientRowCount;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount || prevPartitionTableVersion != partitionTableVersion;
    }

    public void initLastPartition(long timestamp) {
        txPartitionCount = 1;
        updateAttachedPartitionSizeByTimestamp(timestamp, 0L, txn - 1);
    }

    public void insertPartition(int index, long partitionTimestamp, long size, long nameTxn) {
        insertPartitionSizeByTimestamp(index * LONGS_PER_TX_ATTACHED_PARTITION, partitionTimestamp, size, nameTxn);
    }

    public boolean isInsideExistingPartition(long timestamp) {
        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1 && index < attachedPartitions.size()) {
            return true;
        }

        int prevPartition = (-index - 1) - LONGS_PER_TX_ATTACHED_PARTITION;
        if (prevPartition > -1) {
            long prevPartitionTs = attachedPartitions.getQuick(prevPartition + PARTITION_TS_OFFSET);
            return getPartitionFloor(prevPartitionTs) == getPartitionFloor(timestamp);
        }
        return false;
    }

    @Override
    public TxWriter ofRO(@Transient LPSZ path, int timestampType, int partitionBy) {
        throw new IllegalStateException();
    }

    public TxWriter ofRW(@Transient LPSZ path, int timestampType, int partitionBy) {
        clear();
        openTxnFile(ff, path);
        try {
            super.initRO(txMemBase, timestampType, partitionBy);
            unsafeLoadAll();
        } catch (Throwable e) {
            if (txMemBase != null) {
                // Do not truncate in case the file cannot be read
                txMemBase.close(false);
                txMemBase = null;
            }
            super.close();
            throw e;
        }
        return this;
    }

    public void removeAllPartitions() {
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        attachedPartitions.clear();
        recordStructureVersion++;
        truncateVersion++;
        partitionTableVersion++;
        dataVersion++;
    }

    public int removeAttachedPartitions(long timestamp) {
        recordStructureVersion++;
        final long partitionTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(partitionTimestampLo);
        if (indexRaw > -1) {
            final int size = attachedPartitions.size();
            final int lim = size - LONGS_PER_TX_ATTACHED_PARTITION;
            if (indexRaw < lim) {
                attachedPartitions.arrayCopy(indexRaw + LONGS_PER_TX_ATTACHED_PARTITION, indexRaw, lim - indexRaw);
            }
            attachedPartitions.setPos(lim);
            partitionTableVersion++;
            return indexRaw / LONGS_PER_TX_ATTACHED_PARTITION;
        } else {
            assert false;
            return -1;
        }
    }

    public void reset(
            long fixedRowCount,
            long transientRowCount,
            long maxTimestamp,
            ObjList<? extends SymbolCountProvider> symbolCountProviders
    ) {
        recordStructureVersion++;
        this.fixedRowCount = fixedRowCount;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        commit(symbolCountProviders);
    }

    public void resetLagAppliedRows() {
        txMemBase.putInt(readBaseOffset + TX_OFFSET_LAG_TXN_COUNT_32, 0);
        txMemBase.putInt(readBaseOffset + TX_OFFSET_LAG_ROW_COUNT_32, 0);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, Long.MAX_VALUE);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, Long.MIN_VALUE);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0));
    }

    public void resetLagValuesUnsafe() {
        txMemBase.putLong(readBaseOffset + TX_OFFSET_SEQ_TXN_64, 0);
        txMemBase.putInt(readBaseOffset + TX_OFFSET_CHECKSUM_32, 0);
        resetLagAppliedRows();
    }

    public void resetPartitionParquetFormat(long timestamp) {
        setPartitionParquetFormat(timestamp, -1, false);
    }

    public void resetStructureVersionUnsafe() {
        txMemBase.putLong(readBaseOffset + TX_OFFSET_STRUCT_VERSION_64, 0);
    }

    public void resetTimestamp() {
        recordStructureVersion++;
        prevMaxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void setColumnVersion(long newVersion) {
        if (columnVersion != newVersion) {
            recordStructureVersion++;
            columnVersion = newVersion;
        }
    }

    public void setExtensionListener(TableWriter.ExtensionListener extensionListener) {
        this.extensionListener = extensionListener;
    }

    public void setLagMaxTimestamp(long timestamp) {
        lagMaxTimestamp = timestamp;
    }

    public void setLagMinTimestamp(long timestamp) {
        lagMinTimestamp = timestamp;
    }

    public void setLagOrdered(boolean ordered) {
        lagOrdered = ordered;
    }

    public void setLagRowCount(int rowCount) {
        lagRowCount = rowCount;
    }

    public void setLagTxnCount(int txnCount) {
        lagTxnCount = txnCount;
    }

    public void setMaxTimestamp(long timestamp) {
        this.maxTimestamp = timestamp;
    }

    public void setMinTimestamp(long timestamp) {
        recordStructureVersion++;
        minTimestamp = timestamp;
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void setPartitionParquetFormat(long timestamp, long fileLength) {
        setPartitionParquetFormat(timestamp, fileLength, true);
    }

    public void setPartitionParquetFormat(long timestamp, long fileLength, boolean isParquetFormat) {
        int indexRaw = findAttachedPartitionRawIndex(timestamp);
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        int offset = indexRaw + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);

        maskedSize = updatePartitionHasParquetFormat(maskedSize, isParquetFormat);

        attachedPartitions.setQuick(offset, maskedSize);

        int fileLenOffset = indexRaw + PARTITION_PARQUET_FILE_SIZE_OFFSET;
        attachedPartitions.setQuick(fileLenOffset, fileLength);
    }

    public void setPartitionReadOnly(int partitionIndex, boolean isReadOnly) {
        setPartitionReadOnlyByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, isReadOnly);
    }

    public void setPartitionReadOnlyByRawIndex(int indexRaw, boolean isReadOnly) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        int offset = indexRaw + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);
        attachedPartitions.setQuick(offset, updatePartitionIsReadOnly(maskedSize, isReadOnly));
    }

    public void setPartitionReadOnlyByTimestamp(long timestamp, boolean isReadOnly) {
        setPartitionReadOnlyByRawIndex(findAttachedPartitionRawIndex(timestamp), isReadOnly);
    }

    public void setSeqTxn(long seqTxn) {
        this.seqTxn = seqTxn;
    }

    public void switchPartitions(long timestamp) {
        recordStructureVersion++;
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        long partitionTimestampLo = getPartitionTimestampByTimestamp(maxTimestamp);
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(partitionTimestampLo);
        updatePartitionSizeByRawIndex(indexRaw, transientRowCount);

        indexRaw += LONGS_PER_TX_ATTACHED_PARTITION;

        attachedPartitions.setPos(indexRaw + LONGS_PER_TX_ATTACHED_PARTITION);
        long newTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        initPartitionAt(indexRaw, newTimestampLo, 0L, txn - 1);
        transientRowCount = 0L;
        txPartitionCount++;
        if (extensionListener != null) {
            extensionListener.onTableExtended(newTimestampLo);
        }
    }

    public void truncate(long columnVersion, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        removeAllPartitions();
        if (!PartitionBy.isPartitioned(partitionBy)) {
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, 0L, -1L);
        }

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset(writeAreaSize);
        resetTxn(
                txMemBase,
                writeBaseOffset,
                getSymbolColumnCount(),
                ++txn,
                seqTxn,
                dataVersion,
                partitionTableVersion,
                structureVersion,
                columnVersion,
                truncateVersion
        );
        prevPartitionTableVersion = partitionTableVersion;
        storeSymbolCounts(symbolCountProviders);
        finishABHeader(writeBaseOffset, symbolColumnCount * Long.BYTES, 0, CommitMode.NOSYNC);
    }

    public boolean unsafeLoadAll() {
        super.unsafeLoadAll();
        this.baseVersion = getVersion();
        this.prevPartitionTableVersion = partitionTableVersion;
        this.txPartitionCount = 1;
        if (baseVersion >= 0) {
            this.readBaseOffset = getBaseOffset();
            this.readRecordSize = getRecordSize();
            this.prevTransientRowCount = this.transientRowCount;
            this.prevMaxTimestamp = maxTimestamp;
            this.prevMinTimestamp = minTimestamp;
            return true;
        }
        return false;
    }

    public void updateAttachedPartitionSizeByRawIndex(int partitionIndex, long partitionTimestampLo, long partitionSize, long partitionNameTxn) {
        if (partitionIndex > -1) {
            updatePartitionSizeByRawIndex(partitionIndex, partitionSize);
        } else {
            insertPartitionSizeByTimestamp(-(partitionIndex + 1), partitionTimestampLo, partitionSize, partitionNameTxn);
        }
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void updatePartitionSizeByRawIndex(int partitionIndex, long partitionTimestampLo, long rowCount) {
        updateAttachedPartitionSizeByRawIndex(partitionIndex, partitionTimestampLo, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount, long partitionNameTxn) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, partitionNameTxn);
    }

    private static long updatePartitionFlagAt(long maskedSize, boolean flag, int bitOffset) {
        if (flag) {
            maskedSize |= 1L << bitOffset;
        } else {
            maskedSize &= ~(1L << bitOffset);
        }
        return maskedSize;
    }

    private static long updatePartitionHasParquetFormat(long maskedSize, boolean isParquetFormat) {
        return updatePartitionFlagAt(maskedSize, isParquetFormat, PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET);
    }

    private static long updatePartitionIsReadOnly(long maskedSize, boolean isReadOnly) {
        return updatePartitionFlagAt(maskedSize, isReadOnly, PARTITION_MASK_READ_ONLY_BIT_OFFSET);
    }

    private int calculateWriteOffset(int areaSize) {
        boolean currentIsA = (baseVersion & 1L) == 0L;
        int currentOffset = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_A_32) : txMemBase.getInt(TX_BASE_OFFSET_B_32);
        if (TX_BASE_HEADER_SIZE + areaSize <= currentOffset) {
            return TX_BASE_HEADER_SIZE;
        }
        int currentSizeSymbols = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : txMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        int currentSizePartitions = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : txMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);
        int currentSize = calculateTxRecordSize(currentSizeSymbols, currentSizePartitions);
        return currentOffset + currentSize;
    }

    private int calculateWriteSize() {
        // If by any action data is reset and table is partitioned, clear attachedPartitions
        if (maxTimestamp == Long.MIN_VALUE && PartitionBy.isPartitioned(partitionBy)) {
            attachedPartitions.clear();
        }
        return calculateTxRecordSize(symbolColumnCount * Long.BYTES, attachedPartitions.size() * Long.BYTES);
    }

    private void commitFullRecord(int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        symbolColumnCount = symbolCountProviders.size();

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset(writeAreaSize);
        putLong(TX_OFFSET_TXN_64, ++txn);
        putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
        putLong(TX_OFFSET_FIXED_ROW_COUNT_64, fixedRowCount);
        putLong(TX_OFFSET_MIN_TIMESTAMP_64, minTimestamp);
        putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
        putLong(TX_OFFSET_STRUCT_VERSION_64, structureVersion);
        putLong(TX_OFFSET_DATA_VERSION_64, dataVersion);
        putLong(TX_OFFSET_PARTITION_TABLE_VERSION_64, partitionTableVersion);
        putLong(TX_OFFSET_COLUMN_VERSION_64, columnVersion);
        putLong(TX_OFFSET_TRUNCATE_VERSION_64, truncateVersion);
        putLong(TX_OFFSET_SEQ_TXN_64, seqTxn);
        putLagValues();
        putInt(TX_OFFSET_MAP_WRITER_COUNT_32, symbolColumnCount);
        putInt(TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnCount));

        // store symbol counts
        storeSymbolCounts(symbolCountProviders);

        // store attached partitions
        txPartitionCount = 1;
        saveAttachedPartitionsToTx(symbolColumnCount);
        finishABHeader(writeBaseOffset, symbolColumnCount * Long.BYTES, attachedPartitions.size() * Long.BYTES, commitMode);

        prevTransientRowCount = transientRowCount;
        prevMinTimestamp = minTimestamp;
        prevMaxTimestamp = maxTimestamp;

        prevRecordStructureVersion = lastRecordStructureVersion;
        lastRecordStructureVersion = recordStructureVersion;
        prevRecordBaseOffset = lastRecordBaseOffset;
        lastRecordBaseOffset = writeBaseOffset;
        prevPartitionTableVersion = partitionTableVersion;
    }

    private void finishABHeader(int areaOffset, int bytesSymbols, int bytesPartitions, int commitMode) {
        boolean currentIsA = (baseVersion & 1) == 0;

        // When current is A, write to B
        long offsetOffset = currentIsA ? TX_BASE_OFFSET_B_32 : TX_BASE_OFFSET_A_32;
        long symbolSizeOffset = currentIsA ? TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_A_32;
        long partitionsSizeOffset = currentIsA ? TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_A_32;

        txMemBase.putInt(offsetOffset, areaOffset);
        txMemBase.putInt(symbolSizeOffset, bytesSymbols);
        txMemBase.putInt(partitionsSizeOffset, bytesPartitions);

        Unsafe.getUnsafe().storeFence();
        txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

        readRecordSize = calculateTxRecordSize(bytesSymbols, bytesPartitions);
        readBaseOffset = areaOffset;

        assert readBaseOffset + readRecordSize <= txMemBase.size();
        super.switchRecord(readBaseOffset, readRecordSize);

        if (commitMode != CommitMode.NOSYNC) {
            txMemBase.sync(commitMode == CommitMode.ASYNC);
        }
    }

    private long getLong(long offset) {
        assert offset + 8 <= readRecordSize;
        return txMemBase.getLong(readBaseOffset + offset);
    }

    private void insertPartitionSizeByTimestamp(int index, long partitionTimestamp, long partitionSize, long partitionNameTxn) {
        int size = attachedPartitions.size();
        attachedPartitions.setPos(size + LONGS_PER_TX_ATTACHED_PARTITION);
        if (index < size) {
            // insert in the middle
            attachedPartitions.arrayCopy(index, index + LONGS_PER_TX_ATTACHED_PARTITION, size - index);
            partitionTableVersion++;
        } else if (extensionListener != null) {
            extensionListener.onTableExtended(partitionTimestamp);
        }
        recordStructureVersion++;
        initPartitionAt(index, partitionTimestamp, partitionSize, partitionNameTxn);
    }

    private void openTxnFile(FilesFacade ff, LPSZ path) {
        if (ff.exists(path)) {
            if (txMemBase == null) {
                txMemBase = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            } else {
                txMemBase.of(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            }
            return;
        }
        throw CairoException.critical(ff.errno()).put("Cannot append. File does not exist: ").put(path);
    }

    private void putInt(long offset, int value) {
        assert offset + Integer.BYTES <= writeAreaSize;
        txMemBase.putInt(writeBaseOffset + offset, value);
    }

    private void putLagValues() {
        putLong(TX_OFFSET_LAG_MIN_TIMESTAMP_64, lagMinTimestamp);
        putLong(TX_OFFSET_LAG_MAX_TIMESTAMP_64, lagMaxTimestamp);
        putInt(TX_OFFSET_LAG_ROW_COUNT_32, lagRowCount);
        int lagTxnRaw = lagOrdered ? lagTxnCount : -lagTxnCount;
        putInt(TX_OFFSET_LAG_TXN_COUNT_32, lagTxnRaw);
        putInt(TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnRaw));
    }

    private void putLong(long offset, long value) {
        txMemBase.putLong(writeBaseOffset + offset, value);
    }

    private void saveAttachedPartitionsToTx(int symbolColumnCount) {
        final int size = attachedPartitions.size();
        final long partitionTableOffset = getPartitionTableSizeOffset(symbolColumnCount);
        putInt(partitionTableOffset, size * Long.BYTES);
        // change partition count only when we have something to save to the partition table
        if (maxTimestamp != Long.MIN_VALUE) {
            for (int i = 0; i < size; i++) {
                putLong(getPartitionTableIndexOffset(partitionTableOffset, i), attachedPartitions.getQuick(i));
            }
        }
    }

    private void storeSymbolCounts(ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        for (int i = 0, n = symbolCountProviders.size(); i < n; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            int symCount = symbolCountProviders.getQuick(i).getSymbolCount();
            putInt(offset, symCount);
            offset += Integer.BYTES;
            putInt(offset, symCount);
        }
    }

    private void updateAttachedPartitionSizeByTimestamp(long timestamp, long partitionSize, long partitionNameTxn) {
        final long partitionTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        updateAttachedPartitionSizeByRawIndex(findAttachedPartitionRawIndexByLoTimestamp(partitionTimestampLo), partitionTimestampLo, partitionSize, partitionNameTxn);
    }

    private void updatePartitionSizeByRawIndex(int index, long partitionSize) {
        int offset = index + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);
        if ((maskedSize & PARTITION_SIZE_MASK) != partitionSize) {
            attachedPartitions.setQuick(offset, (maskedSize & PARTITION_FLAGS_MASK) | (partitionSize));
            recordStructureVersion++;
        }
    }

    private void writeTransientSymbolCount(int symbolIndex, int symCount) {
        // This updates into current record
        long recordOffset = getSymbolWriterTransientIndexOffset(symbolIndex);
        assert recordOffset + Integer.BYTES <= readRecordSize;
        txMemBase.putInt(readBaseOffset + recordOffset, symCount);
    }

    // It is possible that O3 commit will create partition just before
    // the last one, leaving last partition row count 0 when doing ic().
    // That's when the data from the last partition is moved to in-memory lag.
    // One way to detect this is to check if index of the "last" partition is not
    // last partition in the attached partition list.
    void reconcileOptimisticPartitions() {
        int lastPartitionTsIndex = attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TS_OFFSET;
        if (lastPartitionTsIndex > 0 && maxTimestamp < attachedPartitions.getQuick(lastPartitionTsIndex)) {
            int maxTimestampPartitionIndex = getPartitionIndex(getLastPartitionTimestamp());
            if (maxTimestampPartitionIndex < getPartitionCount() - 1) {
                // accumulate value, which we have to subtract
                // from fixedRowCount (total count of rows of non-active partitions)
                long rowCount = 0;
                for (int i = maxTimestampPartitionIndex, n = getPartitionCount() - 1; i < n; i++) {
                    rowCount += getPartitionSize(i);
                }
                attachedPartitions.setPos((maxTimestampPartitionIndex + 1) * LONGS_PER_TX_ATTACHED_PARTITION);
                recordStructureVersion++;

                // remove partitions
                this.fixedRowCount -= rowCount;
                this.transientRowCount = getPartitionSize(maxTimestampPartitionIndex);
            }
        }
    }

    void resetToLastPartition(long committedTransientRowCount, long newMaxTimestamp) {
        recordStructureVersion++;
        updatePartitionSizeByTimestamp(maxTimestamp, committedTransientRowCount);
        prevMaxTimestamp = newMaxTimestamp;
        maxTimestamp = prevMaxTimestamp;
        transientRowCount = committedTransientRowCount;
    }

    void resetToLastPartition(long committedTransientRowCount) {
        resetToLastPartition(committedTransientRowCount, getLong(TX_OFFSET_MAX_TIMESTAMP_64));
    }

    long unsafeCommittedFixedRowCount() {
        return getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
    }

    long unsafeCommittedTransientRowCount() {
        return getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
    }

    void updatePartitionSizeAndTxnByRawIndex(int index, long partitionSize) {
        recordStructureVersion++;
        updatePartitionSizeByRawIndex(index, partitionSize);
        attachedPartitions.set(index + PARTITION_NAME_TX_OFFSET, txn);
    }
}
