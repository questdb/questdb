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
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TransactionFile implements Closeable {

    private ReadWriteMemory txMem;
    private final FilesFacade ff;
    private final Path path;
    private final int rootLen;
    private long fixedRowCount;
    private long txn;
    private int symbolsCount;
    private boolean saveAttachedPartitionList;
    private final LongHashSet attachedPartitions = new LongHashSet();
    private long dataVersion;
    private long structureVersion;
    private int txPartitionCount;

    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private long prevTransientRowCount;

    private long minTimestamp;
    private long maxTimestamp;
    private long transientRowCount;

    public TransactionFile(FilesFacade ff, Path path) {
        this.ff = ff;
        this.path = path;
        this.rootLen = path.length();
    }

    public void appendTransientRows(long nRowsAdded) {
        transientRowCount += nRowsAdded;
    }

    public boolean attachedPartitionsContains(long ts) {
        return attachedPartitions.contains(ts);
    }

    public long getPrevMaxTimestamp() {
        return prevMaxTimestamp;
    }

    public void startRow() {
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void newBlock() {
        prevMaxTimestamp = maxTimestamp;
    }

    public void resetTimestamp() {
        prevMaxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void rollbackPartition() {
        // assert false : "todo";

//        if (partitionBy != PartitionBy.NONE) {
//            if (transientRowCount != 0) {
//                if (prevMaxTimestamp > Long.MIN_VALUE) {
//                    txPartitionCount--;
//                }
//
//                transientRowCount = txPrevTransientRowCount;
//                fixedRowCount -= txPrevTransientRowCount;
//            }
//        }
        rollbackRows();
        txPartitionCount--;
        fixedRowCount -= prevTransientRowCount;
        transientRowCount = prevTransientRowCount;
    }

    public void rollbackRows() {
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void checkAddAttachedPartition(long timestamp) {
        int keyIndex = attachedPartitions.keyIndex(timestamp);
        if (keyIndex > -1) {
            attachedPartitions.addAt(keyIndex, timestamp);
            saveAttachedPartitionList = true;
        }
    }

    @Override
    public void close() {
        Misc.free(txMem);
    }

    public long getFixedRowCount() {
        return fixedRowCount;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
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

    public long getPrevTransientRowCount() {
        return prevTransientRowCount;
    }

    public void openFirstPartition() {
        txPartitionCount = 1;
    }

    public long readFixedRowCount() {
        return txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }

    public int readWriterCount() {
        return txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
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

    public int readSymbolWriterIndexOffset(int i) {
        return txMem.getInt(getSymbolWriterIndexOffset(i));
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

    public void setMinTimestamp(long firstTimestamp) {
        this.minTimestamp = firstTimestamp;
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        maxTimestamp = timestamp;
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

    public void beginCommit() {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);
    }

    public void commit(int commitMode, ObjList<SymbolMapWriter> denseSymbolMapWriters) {
        if (txPartitionCount > 1) {
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

        if (saveAttachedPartitionList) {
            saveAttachedPartitionsToTx(symbolsCount);
            saveAttachedPartitionList = false;
        }

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
        if (commitMode != CommitMode.NOSYNC) {
            txMem.sync(0, commitMode == CommitMode.ASYNC);
        }

        prevTransientRowCount = transientRowCount;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
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
            saveAttachedPartitionsToTx(count);
            symbolsCount = count;
        }

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
    }

    public long switchPartitions() {
        txPartitionCount++;
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        transientRowCount = 0;
        return prevTransientRowCount;
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

    private void saveAttachedPartitionsToTx(int symCount) {
        int n = attachedPartitions.size();
        txMem.putInt(getPartitionTableSizeOffset(symCount), n);
        for (int i = 0; i < n; i++) {
            txMem.putLong(getPartitionTableIndexOffset(symCount, i), attachedPartitions.get(i));
        }
    }

    private long getTxEofOffset() {
        return getTxMemSize(symbolsCount, attachedPartitions.size());
    }

    public void freeTxMem() {
        if (txMem != null) {
            try {
                txMem.jumpTo(getTxEofOffset());
            } finally {
                txMem.close();
            }
        }
    }
}
