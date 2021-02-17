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

import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public final class TransactionFile extends TransactionFileReader implements Closeable {
    private int attachedPositionDirtyIndex;
    private int txPartitionCount;
    private final long tempMem8b = Unsafe.malloc(Long.BYTES);

    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private long prevTransientRowCount;

    private ReadWriteMemory txMem;

    public TransactionFile(FilesFacade ff, Path path) {
        super(ff, path);
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

    public void appendRowNoTimestamp(long nRowsAdded) {
        transientRowCount += nRowsAdded;
    }

    public void bumpStructureVersion(ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_STRUCT_VERSION, ++structureVersion);

        final int count = denseSymbolMapWriters.size();
        final int oldCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, count);
        for (int i = 0; i < count; i++) {
            txMem.putInt(getSymbolWriterIndexOffset(i), denseSymbolMapWriters.getQuick(i).getSymbolCount());
        }

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
            popAttachedPartitions();
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
        txMem = Misc.free(txMem);
        Unsafe.free(tempMem8b, Long.BYTES);
    }

    @Override
    public void read() {
        super.read();
        this.prevTransientRowCount = this.transientRowCount;
        this.prevMaxTimestamp = maxTimestamp;
        this.prevMinTimestamp = minTimestamp;
    }

    @Override
    protected VirtualMemory openTxnFile(FilesFacade ff, Path path, int rootLen) {
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return txMem = new ReadWriteMemory(ff, path, ff.getPageSize());
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

        symbolsCount = denseSymbolMapWriters.size();
        int attachedPositionDirtyIndex = this.attachedPositionDirtyIndex;
        saveAttachedPartitionsToTx(symbolsCount);
        if (txPartitionCount > 1) {
            commitPendingPartitions(attachedPositionDirtyIndex);
            txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
            txPartitionCount = 1;
        }

        txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, minTimestamp);
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);

        // store symbol counts
        for (int i = 0; i < symbolsCount; i++) {
            int symbolCount = denseSymbolMapWriters.getQuick(i).getSymbolCount();
            txMem.putInt(getSymbolWriterIndexOffset(i), symbolCount);
        }

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
        if (commitMode != CommitMode.NOSYNC) {
            txMem.sync(0, commitMode == CommitMode.ASYNC);
        }

        prevTransientRowCount = transientRowCount;
    }

    public void freeTxMem() {
        try {
            if (txMem != null) {
                txMem.jumpTo(getTxEofOffset());
            }
        } finally {
            close();
        }
    }

    public long getLastTxSize() {
        return txPartitionCount == 1 ? transientRowCount - prevTransientRowCount : transientRowCount;
    }

    public int getTxPartitionCount() {
        return txPartitionCount;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
    }

    public void newBlock() {
        prevMaxTimestamp = maxTimestamp;
    }

    public void openFirstPartition() {
        txPartitionCount = 1;
    }

    public void removePartition(long timestamp, long nextMinTimestamp) {
        final long partitionSize = getPartitionSizeByPartitionTimestamp(timestamp);
        final long txn = txMem.getLong(TX_OFFSET_TXN) + 1;
        txMem.putLong(TX_OFFSET_TXN, txn);
        Unsafe.getUnsafe().storeFence();
        final long partitionVersion = txMem.getLong(TX_OFFSET_PARTITION_TABLE_VERSION) + 1;
        txMem.putLong(TX_OFFSET_PARTITION_TABLE_VERSION, partitionVersion);

        if (nextMinTimestamp != minTimestamp) {
            txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, nextMinTimestamp);
            minTimestamp = nextMinTimestamp;
        }

        // decrement row count
        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT) - partitionSize);

        removeAttachedPartitions(timestamp);
        saveAttachedPartitionsToTx(symbolsCount);

        Unsafe.getUnsafe().storeFence();
        // txn check
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
        fixedRowCount -= partitionSize;
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
                txMem.getLong(TX_OFFSET_DATA_VERSION) + 1);
    }

    public void resetTimestamp() {
        prevMaxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void setMinTimestamp(long firstTimestamp) {
        minTimestamp = firstTimestamp;
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void append() {
        transientRowCount++;
    }

    public void switchPartitions() {
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;

        updatePartitionSizeByTimestamp(maxTimestamp, transientRowCount);
        transientRowCount = 0;

        txPartitionCount++;
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount) {
        attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, updateAttachedPartitionSizeByTimestamp(timestamp, rowCount));
    }

    public void truncate() {
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txn++;
        txPartitionCount = 1;
        attachedPartitions.clear();
        resetTxn(txMem, symbolsCount, txn, ++dataVersion);
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void finishOutOfOrderUpdate(long minTimestamp, long maxTimestamp) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        assert attachedPartitions.size() > 0;
        this.transientRowCount = attachedPartitions.getQuick(attachedPartitions.size() - LONGS_PER_PARTITION + PARTITION_SIZE_OFFSET);
        this.fixedRowCount = 0;
        for (int i = 0, hi = attachedPartitions.size() - LONGS_PER_PARTITION; i < hi; i += LONGS_PER_PARTITION) {
            this.fixedRowCount += attachedPartitions.getQuick(i + PARTITION_SIZE_OFFSET);
        }
        txPartitionCount++;
    }

    private static long openReadWriteOrFail(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd != -1) {
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private void commitPendingPartitions(int attachedPositionDirtyIndex) {
        int size = attachedPartitions.size();
        for (int i = attachedPositionDirtyIndex; i < size - 1; i += LONGS_PER_PARTITION) {
            try {
                long partitionTimestamp = attachedPartitions.getQuick(i + PARTITION_TS_OFFSET);
                long partitionSize = attachedPartitions.getQuick(i + PARTITION_SIZE_OFFSET);

                setPathForPartition(path, partitionBy, partitionTimestamp);
                long fd = openReadWriteOrFail(ff, path.concat(ARCHIVE_FILE_NAME).$());
                try {
                    Unsafe.getUnsafe().putLong(tempMem8b, partitionSize);
                    if (ff.write(fd, tempMem8b, Long.BYTES, 0) != Long.BYTES) {
                        throw CairoException.instance(ff.errno()).put("Commit failed, file=").put(path);
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private long getTxEofOffset() {
        return getTxMemSize(symbolsCount, attachedPartitions.size());
    }

    private void popAttachedPartitions() {
        attachedPartitions.truncateTo(attachedPartitions.size() - LONGS_PER_PARTITION);
    }

    private void removeAttachedPartitions(long timestamp) {
        int index = findAttachedPartitionIndex(timestamp);
        assert index >= 0;
        int size = attachedPartitions.size();
        if (size > index + 1) {
            attachedPartitions.arrayCopy(index + LONGS_PER_PARTITION, index, size - index - LONGS_PER_PARTITION);
            attachedPositionDirtyIndex = Math.min(attachedPositionDirtyIndex, index);
        }
        attachedPartitions.truncateTo(size - LONGS_PER_PARTITION);
    }

    private void saveAttachedPartitionsToTx(int symCount) {
        int size = attachedPartitions.size();
        txMem.putInt(getPartitionTableSizeOffset(symCount), size);
        if (maxTimestamp != Long.MIN_VALUE) {
            for (int i = attachedPositionDirtyIndex; i < size; i++) {
                txMem.putLong(getPartitionTableIndexOffset(symCount, i), attachedPartitions.getQuick(i));
            }
            attachedPositionDirtyIndex = size;
        }
    }
}