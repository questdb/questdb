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

import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TransactionFile implements Closeable {

    private static final int LONGS_PER_PARTITION = 4;
    private static final int PARTITION_TS_OFFSET = 0;
    private static final int PARTITION_SIZE_OFFSET = 1;
    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private final LongList attachedPartitions = new LongList();
    private int attachedPositionDirtyIndex;
    private ReadWriteMemory txMem;
    private long fixedRowCount;
    private long txn;
    private int symbolsCount;
    private long dataVersion;
    private long structureVersion;
    private int txPartitionCount;
    private long tempMem8b = Unsafe.malloc(Long.BYTES);

    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private long prevTransientRowCount;

    private long minTimestamp;
    private long maxTimestamp;
    private long transientRowCount;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;
    private int partitionBy = -1;

    public TransactionFile(FilesFacade ff, Path path) {
        this.ff = ff;
        this.path = path;
        this.rootLen = path.length();
    }

    public void appendBlock(long timestampLo, long timestampHi, long nRowsAdded) {
        assert false : "todo";
    }

    public void appendRowNoTimestamp(long nRowsAdded) {
        transientRowCount += nRowsAdded;
    }

    public boolean attachedPartitionsContains(long ts) {
        return findAttachedPartitionIndex(getPartitionLo(ts)) >= 0;
    }

    public long attachedPartitionsSize(long ts) {
        final int index = findAttachedPartitionIndex(getPartitionLo(ts));
        if (index >= 0) {
            return attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET);
        }
        return -1;
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

    public void checkAddPartition(long minTimestamp) {
    }

    public void checkAddPartition(long partitionHi, long partitionSize) {
    }

    @Override
    public void close() {
        txMem = Misc.free(txMem);
        Unsafe.free(tempMem8b, Long.BYTES);
    }

    public void commit(int commitMode, ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);

        if (txPartitionCount > 1) {
            commitPendingPartitions();
            txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
            txPartitionCount = 1;
        }

        txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, minTimestamp);
        txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);

        // store symbol counts
        symbolsCount = denseSymbolMapWriters.size();
        for (int i = 0; i < symbolsCount; i++) {
            int symbolCount = denseSymbolMapWriters.getQuick(i).getSymbolCount();
            txMem.putInt(getSymbolWriterIndexOffset(i), symbolCount);
        }

        saveAttachedPartitionsToTx(symbolsCount);

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

    public long getFixedRowCount() {
        return fixedRowCount;
    }

    public long getLastTxSize() {
        return txPartitionCount == 1 ? transientRowCount - prevTransientRowCount : transientRowCount;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long firstTimestamp) {
        this.minTimestamp = firstTimestamp;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public long getTransientRowCount() {
        return transientRowCount;
    }

    public int getTxPartitionCount() {
        return txPartitionCount;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
    }

    public void initPartitionFloor(Timestamps.TimestampFloorMethod timestampFloorMethod, int partitionBy) {
        assert this.timestampFloorMethod == null;
        this.timestampFloorMethod = timestampFloorMethod;
        this.partitionBy = partitionBy;
    }

    public void newBlock() {
        prevMaxTimestamp = maxTimestamp;
    }

    public void openFirstPartition() {
        txPartitionCount = 1;
    }

    public void read() {
        if (this.txMem == null) {
            this.txMem = openTxnFile();
        }
        this.txn = txMem.getLong(TX_OFFSET_TXN);
        this.transientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.prevTransientRowCount = this.transientRowCount;
        this.fixedRowCount = txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
        this.minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        this.maxTimestamp = txMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        this.dataVersion = txMem.getLong(TX_OFFSET_DATA_VERSION);
        this.structureVersion = txMem.getLong(TX_OFFSET_STRUCT_VERSION);
        this.symbolsCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        this.prevMaxTimestamp = maxTimestamp;
        this.prevMinTimestamp = minTimestamp;
        loadAttachedPartitions();
    }

    public long readFixedRowCount() {
        return txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }

    public int readSymbolWriterIndexOffset(int i) {
        return txMem.getInt(getSymbolWriterIndexOffset(i));
    }

    public int readWriterCount() {
        return txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
    }

    public void removePartition(long timestamp, long partitionSize) {
        long nextMinTimestamp = minTimestamp;
        if (attachedPartitions.size() > 0 && timestamp == attachedPartitions.getQuick(0)) {
            nextMinTimestamp = attachedPartitions.size() > 5 ? attachedPartitions.getQuick(5) : Long.MAX_VALUE;
        }

        long txn = txMem.getLong(TX_OFFSET_TXN) + 1;
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

    public void startRow() {
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void switchPartitions() {
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;

        setAttachedPartitionSize(maxTimestamp, transientRowCount);
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
        attachedPartitions.clear();
        resetTxn(txMem, symbolsCount, txn, ++dataVersion);
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void updateOutOfOrder(long minTimestamp, long maxTimestamp, long transientRowCount, long fixedRowCount) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        this.fixedRowCount = fixedRowCount;
    }

    private static long openReadWriteOrFail(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd != -1) {
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private void commitPendingPartitions() {
        for (int i = attachedPositionDirtyIndex; i < attachedPartitions.size(); i += LONGS_PER_PARTITION) {
            try {
                long partitionTimestamp = attachedPartitions.getQuick(i + PARTITION_TS_OFFSET);
                long partitionSize = attachedPartitions.getQuick(i + PARTITION_SIZE_OFFSET);

                setPathForPartition(path, partitionBy, partitionTimestamp);
                long fd = openReadWriteOrFail(ff, path.concat(ARCHIVE_FILE_NAME).$());
                try {
                    Unsafe.getUnsafe().putLong(tempMem8b, partitionSize);
                    if (ff.write(fd, tempMem8b, Long.BYTES, 0) == Long.BYTES) {
                        continue;
                    } else {
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

    private int findAttachedPartitionIndex(long ts) {
        ts = getPartitionLo(ts);
        // TODO: make binary search
        for (int i = 0, size = attachedPartitions.size(); i < size; i += LONGS_PER_PARTITION) {
            if (attachedPartitions.getQuick(i) > ts) {
                return -(i + 1);
            }
            if (attachedPartitions.getQuick(i) == ts) {
                return i;
            }
        }
        return -(attachedPartitions.size() + 1);
    }

    private long getPartitionLo(long timestamp) {
        return timestampFloorMethod != null ? timestampFloorMethod.floor(timestamp) : Long.MIN_VALUE;
    }

    private long getTxEofOffset() {
        return getTxMemSize(symbolsCount, attachedPartitions.size());
    }

    private void loadAttachedPartitions() {
        int symbolWriterCount = symbolsCount;
        int partitionTableSize = txMem.getInt(getPartitionTableSizeOffset(symbolWriterCount));
        if (partitionTableSize > 0) {
            for (int i = 0; i < partitionTableSize; i++) {
                attachedPartitions.add(txMem.getLong(getPartitionTableIndexOffset(symbolWriterCount, i)));
            }
        }
    }

    private ReadWriteMemory openTxnFile() {
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return new ReadWriteMemory(ff, path, ff.getPageSize());
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
    }

    private void popAttachedPartitions() {
        attachedPartitions.truncateTo(attachedPartitions.size() - LONGS_PER_PARTITION);
    }

    private void removeAttachedPartitions(long timestamp) {
        int index = findAttachedPartitionIndex(timestamp);
        assert index >= 0;
        int size = attachedPartitions.size();
        if (size > index + 1) {
            attachedPartitions.arrayCopy(index + LONGS_PER_PARTITION, index, size - index - 1);
        }
        attachedPartitions.truncateTo(size - LONGS_PER_PARTITION);
    }

    private void saveAttachedPartitionsToTx(int symCount) {
        setAttachedPartitionSize(maxTimestamp, transientRowCount);

        int n = attachedPartitions.size();
        txMem.putInt(getPartitionTableSizeOffset(symCount), n);
        for (int i = attachedPositionDirtyIndex; i < n; i++) {
            txMem.putLong(getPartitionTableIndexOffset(symCount, i), attachedPartitions.get(i));
        }
        attachedPositionDirtyIndex = n;
    }

    private void setAttachedPartitionSize(long maxTimestamp, long partitionSize) {
        long partitionTimestamp = getPartitionLo(maxTimestamp);
        int index = findAttachedPartitionIndex(partitionTimestamp);
        if (index >= 0) {
            // Update
            attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
            attachedPositionDirtyIndex = Math.min(index, attachedPositionDirtyIndex);
        } else {
            // Insert
            int existingSize = attachedPartitions.size();
            attachedPartitions.extendAndSet(existingSize + LONGS_PER_PARTITION - 1, 0);
            index = -(index + 1);
            if (index < existingSize) {
                // Insert in the middle
                attachedPartitions.arrayCopy(index, index + LONGS_PER_PARTITION, LONGS_PER_PARTITION);
            }
            attachedPartitions.set(index + PARTITION_TS_OFFSET, partitionTimestamp);
            attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
            attachedPositionDirtyIndex = Math.min(index, attachedPositionDirtyIndex);
        }
    }
}
