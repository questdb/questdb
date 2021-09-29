/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public final class TxWriter extends TxReader implements Closeable {
    protected long prevTransientRowCount;
    private int attachedPositionDirtyIndex;
    private int txPartitionCount;
    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private MemoryCMARW txMem;

    public TxWriter(FilesFacade ff, Path path, int partitionBy) {
        super(ff, path, partitionBy);
        try {
            readUnchecked();
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public void append() {
        transientRowCount++;
    }

    public void appendBlock(long timestampLo, long timestampHi, long nRowsAdded) {
        if (timestampLo < maxTimestamp) {
            throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
        }

        if (txPartitionCount == 0) {
            txPartitionCount = 1;
        }
        this.maxTimestamp = timestampHi;
        this.transientRowCount += nRowsAdded;
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

    public void bumpStructureVersion(ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_STRUCT_VERSION, ++structureVersion);

        final int count = denseSymbolMapWriters.size();
        final int oldCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, count);
        storeSymbolCounts(denseSymbolMapWriters);

        // when symbol column is removed partition table has to be moved up
        // to do that we just write partition table behind symbol writer table
        if (oldCount != count) {
            // Save full attached partition list
            attachedPositionDirtyIndex = 0;
            saveAttachedPartitionsToTx(count);
            symbolsCount = count;
        }

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
    }

    public void cancelRow() {
        if (transientRowCount == 0 && txPartitionCount > 1) {
            // we have to undo creation of partition
            txPartitionCount--;
            fixedRowCount -= prevTransientRowCount;
            transientRowCount = prevTransientRowCount;
            attachedPartitions.setPos(attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION);
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
    public void close() {
        try {
            if (txMem != null) {
                txMem.jumpTo(getTxEofOffset());
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void readUnchecked() {
        super.readUnchecked();
        this.prevTransientRowCount = this.transientRowCount;
        this.prevMaxTimestamp = maxTimestamp;
        this.prevMinTimestamp = minTimestamp;
    }

    @Override
    protected MemoryCMR openTxnFile(FilesFacade ff, Path path, int rootLen) {
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return txMem = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT);
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
    }

    public void commit(int commitMode, ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);
        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
        txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, minTimestamp);
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);
        txMem.putLong(TX_OFFSET_PARTITION_TABLE_VERSION, this.partitionTableVersion);

        // store symbol counts
        storeSymbolCounts(denseSymbolMapWriters);

        // store attached partitions
        symbolsCount = denseSymbolMapWriters.size();
        txPartitionCount = 1;
        saveAttachedPartitionsToTx(symbolsCount);

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
        if (commitMode != CommitMode.NOSYNC) {
            txMem.sync(commitMode == CommitMode.ASYNC);
        }

        prevTransientRowCount = transientRowCount;
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

    public void newBlock() {
        prevMaxTimestamp = maxTimestamp;
    }

    public void openFirstPartition(long timestamp) {
        txPartitionCount = 1;
        updateAttachedPartitionSizeByTimestamp(timestamp, 0);
    }

    public void removeAttachedPartitions(long timestamp) {
        final long partitionTimestampLo = getPartitionTimestampLo(timestamp);
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        if (index > -1) {
            int size = attachedPartitions.size();
            if (index + LONGS_PER_TX_ATTACHED_PARTITION < size) {
                attachedPartitions.arrayCopy(index + LONGS_PER_TX_ATTACHED_PARTITION, index, size - index - LONGS_PER_TX_ATTACHED_PARTITION);
                attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, index);
            }
            attachedPartitions.setPos(size - LONGS_PER_TX_ATTACHED_PARTITION);
            partitionTableVersion++;
        }
    }

    public void reset(long fixedRowCount, long transientRowCount, long maxTimestamp) {
        long txn = txMem.getLong(TX_OFFSET_TXN) + 1;
        txMem.putLong(TX_OFFSET_TXN, txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
        if (this.maxTimestamp != maxTimestamp) {
            txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);
            txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);
        }
        Unsafe.getUnsafe().storeFence();

        // txn check
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);

        this.fixedRowCount = fixedRowCount;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        this.txn = txn;
    }

    public void reset() {
        resetTxn(
                txMem,
                symbolsCount,
                txMem.getLong(TX_OFFSET_TXN) + 1,
                txMem.getLong(TX_OFFSET_DATA_VERSION) + 1,
                txMem.getLong(TX_OFFSET_PARTITION_TABLE_VERSION) + 1);
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

    public void truncate() {
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txn++;
        txPartitionCount = 1;
        attachedPositionDirtyIndex = 0;
        attachedPartitions.clear();
        resetTxn(txMem, symbolsCount, txn, ++dataVersion, ++partitionTableVersion);
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

    public void writeTransientSymbolCount(int symbolIndex, int symCount) {
        txMem.putInt(getSymbolWriterTransientIndexOffset(symbolIndex), symCount);
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

    private void saveAttachedPartitionsToTx(int symCount) {
        final int size = attachedPartitions.size();
        final long partitionTableOffset = getPartitionTableSizeOffset(symCount);
        txMem.putInt(partitionTableOffset, size * Long.BYTES);
        if (maxTimestamp != Long.MIN_VALUE) {
            for (int i = attachedPositionDirtyIndex; i < size; i++) {
                txMem.putLong(getPartitionTableIndexOffset(partitionTableOffset, i), attachedPartitions.getQuick(i));
            }
            attachedPositionDirtyIndex = size;
        }
    }

    private void storeSymbolCounts(ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            int symCount = denseSymbolMapWriters.getQuick(i).getSymbolCount();
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
}