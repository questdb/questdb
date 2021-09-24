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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable {
    protected static final int PARTITION_TS_OFFSET = 0;
    protected static final int PARTITION_SIZE_OFFSET = 1;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    protected static final int PARTITION_DATA_TX_OFFSET = 3;

    protected final FilesFacade ff;
    protected final int rootLen;
    protected final LongList attachedPartitions = new LongList();
    private final Timestamps.TimestampFloorMethod timestampFloorMethod;
    protected Path path;
    protected long minTimestamp;
    protected long maxTimestamp;
    protected long txn;
    protected int symbolsCount;
    protected long dataVersion;
    protected long structureVersion;
    protected long fixedRowCount;
    protected long transientRowCount;
    protected int partitionBy;
    protected long partitionTableVersion;
    protected int attachedPartitionsSize = 0;
    private MemoryMR roTxMem;

    public TxReader(FilesFacade ff, Path path, int partitionBy) {
        this.ff = ff;
        this.path = new Path(path.length() + 10);
        this.path.put(path);
        this.rootLen = path.length();
        try {
            roTxMem = openTxnFile(this.ff, this.path, rootLen);
            this.timestampFloorMethod = partitionBy != PartitionBy.NONE ? getPartitionFloor(partitionBy) : null;
            this.partitionBy = partitionBy;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public boolean attachedPartitionsContains(long ts) {
        return findAttachedPartitionIndex(ts) > -1;
    }

    @Override
    public void close() {
        roTxMem = Misc.free(roTxMem);
        path = Misc.free(path);
    }

    public long getDataVersion() {
        return dataVersion;
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

    public int getPartitionCount() {
        return attachedPartitions.size() / LONGS_PER_TX_ATTACHED_PARTITION;
    }

    public long getPartitionDataTxn(int i) {
        return getPartitionDataTxnByIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionDataTxnByIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_DATA_TX_OFFSET);
    }

    public long getPartitionNameTxn(int i) {
        return getPartitionNameTxnByIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionNameTxnByIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_NAME_TX_OFFSET);
    }

    public long getPartitionNameTxnByPartitionTimestamp(long ts) {
        final int index = findAttachedPartitionIndex(ts);
        if (index > -1) {
            return attachedPartitions.getQuick(index + PARTITION_NAME_TX_OFFSET);
        }
        return -1;
    }

    public long getPartitionSize(int i) {
        return getPartitionSizeByIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionSizeByIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET);
    }

    public long getPartitionSizeByPartitionTimestamp(long ts) {
        final int index = findAttachedPartitionIndex(ts);
        if (index > -1) {
            return attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET);
        }
        return -1;
    }

    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    public long getPartitionTimestamp(int i) {
        return attachedPartitions.getQuick(i * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TS_OFFSET);
    }

    public long getRowCount() {
        return transientRowCount + fixedRowCount;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public long getTransientRowCount() {
        return transientRowCount;
    }

    public long getTxEofOffset() {
        return getTxMemSize(symbolsCount, attachedPartitions.size());
    }

    public long getTxn() {
        return txn;
    }

    public long readFixedRowCount() {
        return roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }

    public int readSymbolCount(int symbolIndex) {
        return roTxMem.getInt(getSymbolWriterIndexOffset(symbolIndex));
    }

    public void readSymbolCounts(IntList symbolCountSnapshot) {
        int symbolMapCount = roTxMem.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT);
        if (symbolMapCount > 0) {
            // No need to call setSize here, file mapped beyond symbol section already
            // while reading attached partitions
            for (int i = 0; i < symbolMapCount; i++) {
                symbolCountSnapshot.add(roTxMem.getInt(TableUtils.getSymbolWriterIndexOffset(i)));
            }
        }
    }

    public int readSymbolWriterIndexOffset(int i) {
        return roTxMem.getInt(getSymbolWriterIndexOffset(i));
    }

    public long readTxn() {
        return roTxMem.getLong(TX_OFFSET_TXN);
    }

    public long readTxnCheck() {
        return roTxMem.getLong(TableUtils.TX_OFFSET_TXN_CHECK);
    }

    public void readUnchecked() {
        this.txn = roTxMem.getLong(TX_OFFSET_TXN);
        this.transientRowCount = roTxMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.fixedRowCount = roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
        this.minTimestamp = roTxMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        this.maxTimestamp = roTxMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        this.dataVersion = roTxMem.getLong(TX_OFFSET_DATA_VERSION);
        this.structureVersion = roTxMem.getLong(TX_OFFSET_STRUCT_VERSION);
        final long prevSymbolCount = this.symbolsCount;
        this.symbolsCount = roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        if (prevSymbolCount != symbolsCount) {
            roTxMem.growToFileSize();
        }
        final long prevPartitionTableVersion = this.partitionTableVersion;
        this.partitionTableVersion = roTxMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);
        loadAttachedPartitions(prevPartitionTableVersion);
    }

    public int readWriterCount() {
        return roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
    }

    private void copyAttachedPartitionsFromTx(int txAttachedPartitionsSize, int max) {
        roTxMem.extend(getPartitionTableIndexOffset(symbolsCount, txAttachedPartitionsSize));
        attachedPartitions.setPos(txAttachedPartitionsSize);
        for (int i = max; i < txAttachedPartitionsSize; i++) {
            attachedPartitions.setQuick(i, roTxMem.getLong(getPartitionTableIndexOffset(symbolsCount, i)));
        }
        attachedPartitionsSize = txAttachedPartitionsSize;
    }

    protected int findAttachedPartitionIndex(long ts) {
        return findAttachedPartitionIndexByLoTimestamp(getPartitionTimestampLo(ts));
    }

    int findAttachedPartitionIndexByLoTimestamp(long ts) {
        // Start from the end, usually it will be last partition searched / appended
        int hi = attachedPartitions.size() - LONGS_PER_TX_ATTACHED_PARTITION;
        if (hi > -1) {
            long last = attachedPartitions.getQuick(hi);
            if (last < ts) {
                return -(hi + LONGS_PER_TX_ATTACHED_PARTITION + 1);
            }
            if (last == ts) {
                return hi;
            }
        }
        return attachedPartitions.binarySearchBlock(0, attachedPartitions.size(), LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts);
    }

    long getPartitionTimestampLo(long timestamp) {
        return timestampFloorMethod != null && timestamp != Numbers.LONG_NaN ? timestampFloorMethod.floor(timestamp) : Long.MIN_VALUE;
    }

    protected void initPartitionAt(int index, long partitionTimestampLo, long partitionSize) {
        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestampLo);
        attachedPartitions.setQuick(index + PARTITION_SIZE_OFFSET, partitionSize);
        attachedPartitions.setQuick(index + PARTITION_NAME_TX_OFFSET, -1);
        attachedPartitions.setQuick(index + PARTITION_DATA_TX_OFFSET, txn);
    }

    private void loadAttachedPartitions(long prevPartitionTableVersion) {
        if (partitionBy != PartitionBy.NONE) {
            int txAttachedPartitionsSize = roTxMem.getInt(getPartitionTableSizeOffset(symbolsCount)) / Long.BYTES;
            if (txAttachedPartitionsSize > 0) {
                if (prevPartitionTableVersion != partitionTableVersion) {
                    attachedPartitions.clear();
                    copyAttachedPartitionsFromTx(txAttachedPartitionsSize, 0);
                } else {
                    if (attachedPartitionsSize < txAttachedPartitionsSize) {
                        copyAttachedPartitionsFromTx(txAttachedPartitionsSize, Math.max(attachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION, 0));
                    }
                }
                attachedPartitions.setQuick(txAttachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_SIZE_OFFSET, transientRowCount);
            } else {
                attachedPartitionsSize = 0;
                attachedPartitions.clear();
            }
        } else {
            // Add transient row count as the only partition in attached partitions list
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, Long.MIN_VALUE, transientRowCount);
        }
    }

    protected MemoryMR openTxnFile(FilesFacade ff, Path path, int rootLen) {
        try {
            if (this.ff.exists(this.path.concat(TXN_FILE_NAME).$())) {
                return Vm.getMRInstance(ff, path, ff.length(path), MemoryTag.MMAP_DEFAULT);
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(this.path);
        } finally {
            this.path.trimTo(rootLen);
        }
    }

    void readRowCounts() {
        this.transientRowCount = roTxMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.fixedRowCount = roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }
}
