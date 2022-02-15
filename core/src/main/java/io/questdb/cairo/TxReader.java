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
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable, Mutable {
    protected static final int PARTITION_TS_OFFSET = 0;
    protected static final int PARTITION_SIZE_OFFSET = 1;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    protected static final int PARTITION_DATA_TX_OFFSET = 3;
    protected final LongList attachedPartitions = new LongList();
    private final IntList symbolCountSnapshot = new IntList();
    private final FilesFacade ff;
    protected long minTimestamp;
    protected long maxTimestamp;
    protected long txn;
    protected int symbolColumnCount;
    protected long dataVersion;
    protected long structureVersion;
    protected long fixedRowCount;
    protected long transientRowCount;
    protected int partitionBy;
    protected long partitionTableVersion;
    protected int attachedPartitionsSize = 0;
    private PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private MemoryMR roTxMem;

    public TxReader(FilesFacade ff) {
        this.ff = ff;
    }

    public boolean attachedPartitionsContains(long ts) {
        return findAttachedPartitionIndex(ts) > -1;
    }

    @Override
    public void clear() {
        close();
        partitionTableVersion = 0;
        attachedPartitionsSize = 0;
        attachedPartitions.clear();
    }

    @Override
    public void close() {
        roTxMem = Misc.free(roTxMem);
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

    public int getSymbolColumnCount() {
        return symbolColumnCount;
    }

    public int getSymbolValueCount(int i) {
        return symbolCountSnapshot.get(i);
    }

    public long getTransientRowCount() {
        return transientRowCount;
    }

    public long getTxEofOffset() {
        return getTxMemSize(symbolColumnCount, attachedPartitions.size());
    }

    public long getTxn() {
        return txn;
    }

    public TxReader ofRO(@Transient Path path, int partitionBy) {
        clear();
        int tableRootLen = path.length();
        try {
            roTxMem = openTxnFile(ff, path);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionBy = partitionBy;
        } catch (Throwable e) {
            close();
            path.trimTo(tableRootLen);
            throw e;
        }
        return this;
    }

    public void unsafeLoadAll(int symbolColumnCount, int partitionSegmentSize, boolean forceClean) {
        this.txn = roTxMem.getLong(TX_OFFSET_TXN);
        this.transientRowCount = roTxMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.fixedRowCount = roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
        this.minTimestamp = roTxMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        this.maxTimestamp = roTxMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        this.dataVersion = roTxMem.getLong(TX_OFFSET_DATA_VERSION);
        this.structureVersion = roTxMem.getLong(TX_OFFSET_STRUCT_VERSION);
        final long prevPartitionTableVersion = this.partitionTableVersion;
        this.partitionTableVersion = roTxMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);
        this.symbolColumnCount = symbolColumnCount;

        unsafeLoadSymbolCounts(symbolColumnCount);
        unsafeLoadPartitions(prevPartitionTableVersion, partitionSegmentSize, forceClean);
    }

    /**
     * Load variable length area sized from the file, e.g. Symbol Column Count and Partitions Size
     * to fail fast reload if they are not clean values.
     *
     * @param symbolColumnCount symbol count is used to calculate offset of partition table in file.
     * @return size partition table in bytes
     */
    public int unsafeReadPartitionSegmentSize(int symbolColumnCount) {
        roTxMem.extend(getPartitionTableSizeOffset(symbolColumnCount) + 4);
        return roTxMem.getInt(getPartitionTableSizeOffset(symbolColumnCount));
    }

    public int unsafeReadSymbolColumnCount() {
        return roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
    }

    public long unsafeReadPartitionTableVersion() {
        return roTxMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);
    }

    private int findAttachedPartitionIndex(long ts) {
        return findAttachedPartitionIndexByLoTimestamp(getPartitionTimestampLo(ts));
    }

    int findAttachedPartitionIndexByLoTimestamp(long ts) {
        // Start from the end, usually it will be last partition searched / appended
        return attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts, BinarySearch.SCAN_UP);
    }

    protected long getPartitionTimestampLo(long timestamp) {
        return partitionFloorMethod != null && timestamp != Numbers.LONG_NaN ? partitionFloorMethod.floor(timestamp) : Long.MIN_VALUE;
    }

    protected void initPartitionAt(int index, long partitionTimestampLo, long partitionSize) {
        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestampLo);
        attachedPartitions.setQuick(index + PARTITION_SIZE_OFFSET, partitionSize);
        attachedPartitions.setQuick(index + PARTITION_NAME_TX_OFFSET, -1);
        attachedPartitions.setQuick(index + PARTITION_DATA_TX_OFFSET, txn);
    }

    protected MemoryMR openTxnFile(FilesFacade ff, Path path) {
        int pathLen = path.length();
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return Vm.getMRInstance(ff, path, ff.length(path), MemoryTag.MMAP_DEFAULT);
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);
        } finally {
            path.trimTo(pathLen);
        }
    }

    protected long unsafeGetRawMemory() {
        return roTxMem.getPageAddress(0);
    }

    private void unsafeLoadPartitions(long prevPartitionTableVersion, int partitionTableSize, boolean forceClean) {
        if (PartitionBy.isPartitioned(partitionBy)) {
            int txAttachedPartitionsSize = partitionTableSize / Long.BYTES;
            if (txAttachedPartitionsSize > 0) {
                if (prevPartitionTableVersion != partitionTableVersion || forceClean) {
                    attachedPartitions.clear();
                    unsafeLoadPartitions0(txAttachedPartitionsSize, 0);
                } else {
                    if (attachedPartitionsSize < txAttachedPartitionsSize) {
                        unsafeLoadPartitions0(
                                txAttachedPartitionsSize,
                                Math.max(attachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION, 0)
                        );
                    }
                }
                attachedPartitions.setQuick(
                        txAttachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_SIZE_OFFSET,
                        transientRowCount
                );
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

    private void unsafeLoadPartitions0(int txAttachedPartitionsSize, int max) {
        roTxMem.extend(getPartitionTableIndexOffset(symbolColumnCount, txAttachedPartitionsSize));
        attachedPartitions.setPos(txAttachedPartitionsSize);
        for (int i = max; i < txAttachedPartitionsSize; i++) {
            attachedPartitions.setQuick(i, roTxMem.getLong(getPartitionTableIndexOffset(symbolColumnCount, i)));
        }
        attachedPartitionsSize = txAttachedPartitionsSize;
    }

    private void unsafeLoadSymbolCounts(int symbolMapCount) {
        this.symbolCountSnapshot.clear();
        // No need to call setSize here, file mapped beyond symbol section already
        // while reading attached partition count
        for (int i = 0; i < symbolMapCount; i++) {
            symbolCountSnapshot.add(roTxMem.getInt(TableUtils.getSymbolWriterIndexOffset(i)));
        }
    }

    protected long unsafeReadFixedRowCount() {
        return roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
    }

    protected int unsafeReadSymbolCount(int symbolIndex) {
        return roTxMem.getInt(getSymbolWriterIndexOffset(symbolIndex));
    }

    protected int unsafeReadSymbolWriterIndexOffset(int denseSymbolIndex) {
        return roTxMem.getInt(getSymbolWriterIndexOffset(denseSymbolIndex));
    }

    long unsafeReadTxn() {
        return roTxMem.getLong(TX_OFFSET_TXN);
    }

    long unsafeReadTxnCheck() {
        return roTxMem.getLong(TableUtils.TX_OFFSET_TXN_CHECK);
    }
}
