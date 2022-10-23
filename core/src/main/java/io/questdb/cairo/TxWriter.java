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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public final class TxWriter extends TxReader implements Closeable, Mutable, SymbolValueCountCollector {
    private final FilesFacade ff;
    private long prevTransientRowCount;
    private int txPartitionCount;
    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private MemoryCMARW txMemBase;
    private int readBaseOffset;
    private int writeBaseOffset;
    private int writeAreaSize;
    private long baseVersion;
    private long readRecordSize;

    private long recordStructureVersion = 0;
    private long lastRecordStructureVersion = -1;
    private long prevRecordStructureVersion = -2;
    private int lastRecordBaseOffset = -1;
    private int prevRecordBaseOffset = -2;
    private TableWriter.ExtensionListener extensionListener;

    public TxWriter(FilesFacade ff) {
        super(ff);
        this.ff = ff;
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

    public void bumpStructureVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        structureVersion++;
        commit(CommitMode.NOSYNC, denseSymbolMapWriters);
    }

    public void bumpTruncateVersion() {
        truncateVersion++;
    }

    public void cancelRow() {
        if (transientRowCount == 1 && txPartitionCount > 1) {
            // we have to undo creation of partition
            txPartitionCount--;
            fixedRowCount -= prevTransientRowCount;
            transientRowCount = prevTransientRowCount + 1; // When row cancel finishes 1 is subtracted. Add 1 to compensate.
            attachedPartitions.setPos(attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION);
            prevTransientRowCount = getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
        }

        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
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
        if (txMemBase != null) {
            // Never trim _txn file to size. Size of the file can only grow up.
            txMemBase.close(false);
        }
        recordStructureVersion = 0;
        lastRecordStructureVersion = -1;
        prevRecordStructureVersion = -2;
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
    public TxWriter ofRO(@Transient LPSZ path, int partitionBy) {
        throw new IllegalStateException();
    }

    public boolean unsafeLoadAll() {
        super.unsafeLoadAll();
        this.baseVersion = getVersion();
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

    protected long unsafeGetRawMemorySize() {
        return Math.max(super.unsafeGetRawMemorySize(), writeAreaSize + writeBaseOffset);
    }

    @Override
    public void collectValueCount(int symbolIndexInTxWriter, int count) {
        writeTransientSymbolCount(symbolIndexInTxWriter, count);
    }

    public void commit(int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {

        if (prevRecordStructureVersion == recordStructureVersion && prevRecordBaseOffset > 0) {

            // Optimisation for the case where commit appends rows to the last partition only
            // In this case all to be changed is TX_OFFSET_MAX_TIMESTAMP_64 and TX_OFFSET_TRANSIENT_ROW_COUNT_64
            writeBaseOffset = prevRecordBaseOffset;
            putLong(TX_OFFSET_TXN_64, ++txn);
            putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
            putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);

            // Store symbol counts. Unfortunately we cannot skip it in here
            storeSymbolCounts(symbolCountProviders);

            Unsafe.getUnsafe().storeFence();
            txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

            super.switchRecord(writeBaseOffset, writeAreaSize); // writeAreaSize should be between records
            this.readBaseOffset = writeBaseOffset;

            prevTransientRowCount = transientRowCount;
            prevMinTimestamp = minTimestamp;
            prevMaxTimestamp = maxTimestamp;

            prevRecordBaseOffset = lastRecordBaseOffset;
            lastRecordBaseOffset = writeBaseOffset;
        } else {
            // Slow path, record structure changed
            commitFullRecord(commitMode, symbolCountProviders);
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
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
    }

    public boolean isActivePartition(long timestamp) {
        return getPartitionTimestampLo(maxTimestamp) == timestamp;
    }

    public TxWriter ofRW(@Transient LPSZ path, int partitionBy) {
        clear();
        openTxnFile(ff, path);
        try {
            super.initRO(txMemBase, partitionBy);
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

    public void openFirstPartition(long timestamp) {
        txPartitionCount = 1;
        updateAttachedPartitionSizeByTimestamp(timestamp, 0L, txn - 1);
    }

    public void removeAttachedPartitions(long timestamp) {
        recordStructureVersion++;
        final long partitionTimestampLo = getPartitionTimestampLo(timestamp);
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        if (index > -1) {
            final int size = attachedPartitions.size();
            final int lim = size - LONGS_PER_TX_ATTACHED_PARTITION;
            if (index < lim) {
                attachedPartitions.arrayCopy(index + LONGS_PER_TX_ATTACHED_PARTITION, index, lim - index);
            }
            attachedPartitions.setPos(lim);
            partitionTableVersion++;
        } else {
            assert false;
        }
    }

    public void reset(long fixedRowCount, long transientRowCount, long maxTimestamp, int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        recordStructureVersion++;
        this.fixedRowCount = fixedRowCount;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        commit(commitMode, symbolCountProviders);
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
            this.columnVersion = newVersion;
        }
    }

    public void setExtensionListener(TableWriter.ExtensionListener extensionListener) {
        this.extensionListener = extensionListener;
    }

    public void setMinTimestamp(long timestamp) {
        recordStructureVersion++;
        minTimestamp = timestamp;
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void switchPartitions(long timestamp) {
        recordStructureVersion++;
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        long partitionTimestampLo = getPartitionTimestampLo(maxTimestamp);
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        updatePartitionSizeByIndex(index, transientRowCount);

        index += LONGS_PER_TX_ATTACHED_PARTITION;

        attachedPartitions.setPos(index + LONGS_PER_TX_ATTACHED_PARTITION);
        long newTimestampLo = getPartitionTimestampLo(timestamp);
        initPartitionAt(index, newTimestampLo, 0, txn - 1, -1);
        transientRowCount = 0;
        txPartitionCount++;
        if (extensionListener != null) {
            extensionListener.onTableExtended(newTimestampLo);
        }
    }

    public void truncate(long columnVersion) {
        recordStructureVersion++;
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txPartitionCount = 1;
        attachedPartitions.clear();
        if (!PartitionBy.isPartitioned(partitionBy)) {
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, 0, -1L, columnVersion);
        }

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset();
        resetTxn(txMemBase, writeBaseOffset, getSymbolColumnCount(), ++txn, ++dataVersion, ++partitionTableVersion, structureVersion, columnVersion, ++truncateVersion);
        finishABHeader(writeBaseOffset, symbolColumnCount * 8, 0, CommitMode.NOSYNC);
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        assert timestamp >= maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void updatePartitionSizeByIndex(int partitionIndex, long partitionTimestampLo, long rowCount) {
        updateAttachedPartitionSizeByIndex(partitionIndex, partitionTimestampLo, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount, long partitionNameTxn) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, partitionNameTxn);
    }

    void bumpPartitionTableVersion() {
        recordStructureVersion++;
        partitionTableVersion++;
    }

    private int calculateWriteOffset() {
        int areaSize = calculateTxRecordSize(symbolColumnCount * 8, attachedPartitions.size() * 8);
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
        return calculateTxRecordSize(symbolColumnCount * 8, attachedPartitions.size() * 8);
    }

    private void commitFullRecord(int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        symbolColumnCount = symbolCountProviders.size();

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset();
        putLong(TX_OFFSET_TXN_64, ++txn);
        putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
        putLong(TX_OFFSET_FIXED_ROW_COUNT_64, fixedRowCount);
        putLong(TX_OFFSET_MIN_TIMESTAMP_64, minTimestamp);
        putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
        putLong(TX_OFFSET_PARTITION_TABLE_VERSION_64, partitionTableVersion);
        putLong(TX_OFFSET_STRUCT_VERSION_64, structureVersion);
        putLong(TX_OFFSET_DATA_VERSION_64, dataVersion);
        putLong(TX_OFFSET_COLUMN_VERSION_64, columnVersion);
        putInt(TX_OFFSET_MAP_WRITER_COUNT_32, symbolColumnCount);
        putLong(TX_OFFSET_TRUNCATE_VERSION_64, truncateVersion);

        // store symbol counts
        storeSymbolCounts(symbolCountProviders);

        // store attached partitions
        txPartitionCount = 1;
        saveAttachedPartitionsToTx(symbolColumnCount);
        finishABHeader(writeBaseOffset, symbolColumnCount * 8, attachedPartitions.size() * 8, commitMode);

        prevTransientRowCount = transientRowCount;
        prevMinTimestamp = minTimestamp;
        prevMaxTimestamp = maxTimestamp;

        prevRecordStructureVersion = lastRecordStructureVersion;
        lastRecordStructureVersion = recordStructureVersion;
        prevRecordBaseOffset = lastRecordBaseOffset;
        lastRecordBaseOffset = writeBaseOffset;
    }

    private void finishABHeader(int areaOffset, int bytesSymbols, int bytesPartitions, int commitMode) {
        boolean currentIsA = (baseVersion & 1L) == 0L;

        // When current is A, write to B
        long offsetOffset = currentIsA ? TX_BASE_OFFSET_B_32 : TX_BASE_OFFSET_A_32;
        long symbolSizeOffset = currentIsA ? TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_A_32;
        long partitionsSizeOffset = currentIsA ? TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_A_32;

        txMemBase.putInt(offsetOffset, areaOffset);
        txMemBase.putInt(symbolSizeOffset, bytesSymbols);
        txMemBase.putInt(partitionsSizeOffset, bytesPartitions);

        Unsafe.getUnsafe().storeFence();
        txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

        this.readRecordSize = calculateTxRecordSize(bytesSymbols, bytesPartitions);
        this.readBaseOffset = areaOffset;
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
        initPartitionAt(index, partitionTimestamp, partitionSize, partitionNameTxn, -1);
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
        assert offset + 4 <= writeAreaSize;
        txMemBase.putInt(writeBaseOffset + offset, value);
    }

    private void putLong(long offset, long value) {
        txMemBase.putLong(writeBaseOffset + offset, value);
    }

    void resetToLastPartition(long committedTransientRowCount) {
        resetToLastPartition(committedTransientRowCount, getLong(TX_OFFSET_MAX_TIMESTAMP_64));
    }

    void resetToLastPartition(long committedTransientRowCount, long newMaxTimestamp) {
        recordStructureVersion++;
        updatePartitionSizeByTimestamp(maxTimestamp, committedTransientRowCount);
        prevMaxTimestamp = newMaxTimestamp;
        maxTimestamp = prevMaxTimestamp;
        transientRowCount = committedTransientRowCount;
    }

    private void saveAttachedPartitionsToTx(int symbolColumnCount) {
        // change partition count only when we have something to save to the partition table
        if (maxTimestamp != Long.MIN_VALUE) {
            final int size = attachedPartitions.size();
            final long partitionTableOffset = getPartitionTableSizeOffset(symbolColumnCount);
            putInt(partitionTableOffset, size * Long.BYTES);
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

    long unsafeCommittedFixedRowCount() {
        return getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
    }

    long unsafeCommittedTransientRowCount() {
        return getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
    }

    private void updateAttachedPartitionSizeByIndex(int partitionIndex, long partitionTimestampLo, long partitionSize, long partitionNameTxn) {
        if (partitionIndex > -1) {
            updatePartitionSizeByIndex(partitionIndex, partitionSize);
        } else {
            insertPartitionSizeByTimestamp(-(partitionIndex + 1), partitionTimestampLo, partitionSize, partitionNameTxn);
        }
    }

    private void updateAttachedPartitionSizeByTimestamp(long timestamp, long partitionSize, long partitionNameTxn) {
        final long partitionTimestampLo = getPartitionTimestampLo(timestamp);
        updateAttachedPartitionSizeByIndex(findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo), partitionTimestampLo, partitionSize, partitionNameTxn);
    }

    void updatePartitionColumnVersion(long partitionTimestamp) {
        final int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
        attachedPartitions.set(index + PARTITION_COLUMN_VERSION_OFFSET, columnVersion);
    }

    void updatePartitionSizeAndTxnByIndex(int index, long partitionSize) {
        recordStructureVersion++;
        attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
        attachedPartitions.set(index + PARTITION_NAME_TX_OFFSET, txn);
    }

    private void updatePartitionSizeByIndex(int index, long partitionSize) {
        if (attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET) != partitionSize) {
            recordStructureVersion++;
            attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
        }
    }

    private void writeTransientSymbolCount(int symbolIndex, int symCount) {
        // This updates into current record
        long recordOffset = getSymbolWriterTransientIndexOffset(symbolIndex);
        assert recordOffset + 4 <= readRecordSize;
        txMemBase.putInt(readBaseOffset + recordOffset, symCount);
    }
}
