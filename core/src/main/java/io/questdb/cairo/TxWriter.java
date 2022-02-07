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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32;

public final class TxWriter extends TxReader implements Closeable, Mutable, SymbolValueCountCollector {
    private final static Log LOG = LogFactory.getLog(TxWriter.class);
    private final FilesFacade ff;
    private long prevTransientRowCount;
    private int attachedPositionDirtyIndex;
    private int txPartitionCount;
    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private final OffsetWriterMemory txMem = new OffsetWriterMemory();
    private MemoryCMARW txMemBase;
    private int readBaseOffset;
    private int writeBaseOffset;
    private int writeAreaSize;
    private long baseVersion;
    private long readRecordSize;

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
        ++structureVersion;
        commit(CommitMode.NOSYNC, denseSymbolMapWriters);
    }

    public void cancelRow() {
        if (transientRowCount == 1 && txPartitionCount > 1) {
            // we have to undo creation of partition
            txPartitionCount--;
            fixedRowCount -= prevTransientRowCount;
            transientRowCount = prevTransientRowCount + 1; // When row cancel finishes 1 is subtracted. Add 1 to compensate.
            attachedPartitions.setPos(attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION);
            prevTransientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        }

        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
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
            txMemBase.close(false);
        }
    }

    @Override
    public void close() {
        try {
            // Never trim _txn file to size. Size of the file can only grow up.
            if (txMemBase != null) {
                txMemBase.close(false);
                txMemBase = null;
            }
        } finally {
            super.close();
        }
    }

    @Override
    public TxWriter ofRO(@Transient Path path, int partitionBy) {
        throw new IllegalStateException();
    }

    public void commit(int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        symbolColumnCount = symbolCountProviders.size();

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset();
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);
        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
        txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, minTimestamp);
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);
        txMem.putLong(TX_OFFSET_PARTITION_TABLE_VERSION, this.partitionTableVersion);
        txMem.putLong(TX_OFFSET_STRUCT_VERSION, structureVersion);
        txMem.putLong(TX_OFFSET_DATA_VERSION, dataVersion);
        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, symbolColumnCount);

        // store symbol counts
        storeSymbolCounts(symbolCountProviders);

        // store attached partitions
        txPartitionCount = 1;
        attachedPositionDirtyIndex = 0;
        saveAttachedPartitionsToTx(symbolColumnCount);
        finishABHeader(writeBaseOffset, symbolColumnCount * 8, attachedPartitions.size() * 8, commitMode);

        prevTransientRowCount = transientRowCount;
    }

    public void removeAttachedPartitions(long timestamp) {
        final long partitionTimestampLo = getPartitionTimestampLo(timestamp);
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        if (index > -1) {
            final int size = attachedPartitions.size();
            final int lim = size - LONGS_PER_TX_ATTACHED_PARTITION;
            if (index < lim) {
                attachedPartitions.arrayCopy(index + LONGS_PER_TX_ATTACHED_PARTITION, index, lim - index);
                attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, index);
            }
            attachedPartitions.setPos(lim);
            partitionTableVersion++;
        } else {
            assert false;
        }
    }

    @Override
    public void collectValueCount(int symbolIndexInTxWriter, int count) {
        writeTransientSymbolCount(symbolIndexInTxWriter, count);
    }

    public void reset(long fixedRowCount, long transientRowCount, long maxTimestamp, int commitMode, ObjList<SymbolMapWriter> symbolCountProviders) {
        this.fixedRowCount = fixedRowCount;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        commit(commitMode, symbolCountProviders);
    }

    public void finishPartitionSizeUpdate(long minTimestamp, long maxTimestamp) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        finishPartitionSizeUpdate();
    }

    public void finishPartitionSizeUpdate() {
        assert getPartitionCount() > 0;
        this.transientRowCount = getPartitionSize(getPartitionCount() - 1);
        this.fixedRowCount = 0;
        this.txPartitionCount = getPartitionCount();
        for (int i = 0, hi = txPartitionCount - 1; i < hi; i++) {
            this.fixedRowCount += getPartitionSize(i);
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

    public TxWriter ofRW(@Transient Path path, int partitionBy) {
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
        updateAttachedPartitionSizeByTimestamp(timestamp, 0);
    }

    public void truncate() {
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txPartitionCount = 1;
        attachedPositionDirtyIndex = 0;
        attachedPartitions.clear();

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset();
        resetTxn(txMemBase, writeBaseOffset, getSymbolColumnCount(), ++txn, ++dataVersion, ++partitionTableVersion, structureVersion);
        finishABHeader(writeBaseOffset, symbolColumnCount * 8, 0, CommitMode.NOSYNC);
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

    private int calculateWriteOffset() {
        int areaSize = calculateTxRecordSize(symbolColumnCount * 8, attachedPartitions.size() * 8);
        boolean currentIsA = baseVersion % 2 == 0;
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
        // If by any action data is reset, clear attachedPartitions
        if (maxTimestamp == Long.MIN_VALUE) {
            attachedPartitions.clear();
        }
        return calculateTxRecordSize(symbolColumnCount * 8, attachedPartitions.size() * 8);
    }

    protected long unsafeGetRawMemorySize() {
        return Math.max(super.unsafeGetRawMemorySize(), writeAreaSize + writeBaseOffset);
    }

    private void finishABHeader(int areaOffset, int bytesSymbols, int bytesPartitions, int commitMode) {
        boolean currentIsA = baseVersion % 2 == 0;

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
//        LOG.infoW().$("wrote txn record ").$(currentIsA ? "B:" : "A:").$(baseVersion).$(", offset=").$(areaOffset).$(", size=").$(readRecordSize)
//                .$(", txn=").$(txn).$(", records=").$(fixedRowCount + transientRowCount).$(", dataVersion=").$(dataVersion).$();
        this.readBaseOffset = areaOffset;

        if (commitMode != CommitMode.NOSYNC) {
            txMemBase.sync(commitMode == CommitMode.ASYNC);
        }
    }

    public void resetTimestamp() {
        prevMaxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void setMinTimestamp(long timestamp) {
        minTimestamp = timestamp;
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void switchPartitions(long timestamp) {
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        long partitionTimestampLo = getPartitionTimestampLo(maxTimestamp);
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        updatePartitionSizeByIndex(index, transientRowCount);
        attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, index);

        index += LONGS_PER_TX_ATTACHED_PARTITION;

        attachedPartitions.setPos(index + LONGS_PER_TX_ATTACHED_PARTITION);
        long newTimestampLo = getPartitionTimestampLo(timestamp);
        initPartitionAt(index, newTimestampLo, 0);
        transientRowCount = 0;
        txPartitionCount++;
    }

    private void openTxnFile(FilesFacade ff, Path path) {
        int pathLen = path.length();
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                if (txMemBase == null) {
                    txMemBase = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT);
                } else {
                    txMemBase.of(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT);
                }
                return;
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);
        } finally {
            path.trimTo(pathLen);
        }
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        assert timestamp >= maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void updatePartitionSizeByIndex(int partitionIndex, long partitionTimestampLo, long rowCount) {
        attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, updateAttachedPartitionSizeByIndex(partitionIndex, partitionTimestampLo, rowCount));
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount) {
        attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, updateAttachedPartitionSizeByTimestamp(timestamp, rowCount));
    }

    private void writeTransientSymbolCount(int symbolIndex, int symCount) {
        // This updates into current record
        long recordOffset = getSymbolWriterTransientIndexOffset(symbolIndex);
        assert recordOffset + 4 <= readRecordSize;
        txMemBase.putInt(readBaseOffset + recordOffset, symCount);
    }

    void bumpPartitionTableVersion() {
        partitionTableVersion++;
    }

    long getCommittedFixedRowCount() {
        return txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }

    long getCommittedTransientRowCount() {
        return txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
    }

    private int insertPartitionSizeByTimestamp(int index, long partitionTimestamp, long partitionSize) {
        int size = attachedPartitions.size();
        attachedPartitions.setPos(size + LONGS_PER_TX_ATTACHED_PARTITION);
        if (index < size) {
            // insert in the middle
            attachedPartitions.arrayCopy(index, index + LONGS_PER_TX_ATTACHED_PARTITION, size - index);
            partitionTableVersion++;
        }
        initPartitionAt(index, partitionTimestamp, partitionSize);
        return index;
    }

    void resetToLastPartition(long committedTransientRowCount) {
        resetToLastPartition(committedTransientRowCount, txMem.getLong(TX_OFFSET_MAX_TIMESTAMP));
    }

    void resetToLastPartition(long committedTransientRowCount, long newMaxTimestamp) {
        updatePartitionSizeByTimestamp(maxTimestamp, committedTransientRowCount);
        prevMaxTimestamp = newMaxTimestamp;
        maxTimestamp = prevMaxTimestamp;
        transientRowCount = committedTransientRowCount;
    }

    private void saveAttachedPartitionsToTx(int symbolColumnCount) {
        // change partition count only when we have something to save to the
        // partition table
        if (maxTimestamp != Long.MIN_VALUE) {
            final int size = attachedPartitions.size();
            final long partitionTableOffset = getPartitionTableSizeOffset(symbolColumnCount);
            txMem.putInt(partitionTableOffset, size * Long.BYTES);
            for (int i = attachedPositionDirtyIndex; i < size; i++) {
                txMem.putLong(getPartitionTableIndexOffset(partitionTableOffset, i), attachedPartitions.getQuick(i));
            }
            attachedPositionDirtyIndex = size;
        }
    }

    private void storeSymbolCounts(ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        for (int i = 0, n = symbolCountProviders.size(); i < n; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            int symCount = symbolCountProviders.getQuick(i).getSymbolCount();
            txMem.putInt(offset, symCount);
            offset += Integer.BYTES;
            txMem.putInt(offset, symCount);
        }
    }

    private int updateAttachedPartitionSizeByIndex(int partitionIndex, long partitionTimestampLo, long partitionSize) {
        if (partitionIndex > -1) {
            updatePartitionSizeByIndex(partitionIndex, partitionSize);
            return partitionIndex;
        }
        return insertPartitionSizeByTimestamp(-(partitionIndex + 1), partitionTimestampLo, partitionSize);
    }

    private int updateAttachedPartitionSizeByTimestamp(long timestamp, long partitionSize) {
        final long partitionTimestampLo = getPartitionTimestampLo(timestamp);
        return updateAttachedPartitionSizeByIndex(findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo), partitionTimestampLo, partitionSize);
    }

    private void updatePartitionSizeByIndex(int index, long partitionSize) {
        if (attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET) != partitionSize) {
            attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
            attachedPartitions.set(index + PARTITION_DATA_TX_OFFSET, txn);
        }
    }

    void updatePartitionSizeByIndexAndTxn(int index, long partitionSize) {
        attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
        attachedPartitions.set(index + PARTITION_NAME_TX_OFFSET, txn);
        attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, index);
    }

    private class OffsetWriterMemory {
        public int getInt(long offset) {
            assert offset + 4 <= readRecordSize;
            return txMemBase.getInt(readBaseOffset + offset);
        }

        public long getLong(long offset) {
            assert offset + 8 <= readRecordSize;
            return txMemBase.getLong(readBaseOffset + offset);
        }

        public void putInt(long offset, int value) {
            assert offset + 4 <= writeAreaSize;
            txMemBase.putInt(writeBaseOffset + offset, value);
        }

        public void putLong(long offset, long value) {
            txMemBase.putLong(writeBaseOffset + offset, value);
        }
    }
}