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

import io.questdb.cairo.vm.Mappable;
import io.questdb.cairo.vm.MappedReadOnlyMemory;
import io.questdb.cairo.vm.SinglePageMappedReadOnlyPageMemory;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable {
    protected static final int PARTITION_TS_OFFSET = 0;
    protected static final int PARTITION_SIZE_OFFSET = 1;
    protected static final int PARTITION_TX_OFFSET = 2;

    protected final FilesFacade ff;
    protected final int rootLen;
    protected final LongList attachedPartitions = new LongList();
    protected Path path;
    protected long minTimestamp;
    protected long maxTimestamp;
    protected long txn;
    protected int symbolsCount;
    protected long dataVersion;
    protected long structureVersion;
    protected long fixedRowCount;
    protected long transientRowCount;
    protected int partitionBy = -1;
    protected long partitionTableVersion;
    private MappedReadOnlyMemory roTxMem;
    private Timestamps.TimestampFloorMethod timestampFloorMethod;

    public TxReader(FilesFacade ff, Path path) {
        this.ff = ff;
        this.path = new Path(path.length() + 10);
        this.path.put(path);
        this.rootLen = path.length();
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

    public long getPartitionSize(int i) {
        return attachedPartitions.getQuick(i * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_SIZE_OFFSET);
    }

    public long getPartitionSizeByPartitionTimestamp(long ts) {
        final int index = findAttachedPartitionIndex(getPartitionTimestampLo(ts));
        if (index > -1) {
            return attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET);
        }
        return -1;
    }

    public long getPartitionTxn(int i) {
        return attachedPartitions.getQuick(i * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TX_OFFSET);
    }

    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    public long getPartitionTimestamp(int i) {
        return attachedPartitions.getQuick(i * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TS_OFFSET);
    }

    public int getPartitionsCount() {
        return attachedPartitions.size() / LONGS_PER_TX_ATTACHED_PARTITION;
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

    public void initPartitionBy(int partitionBy) {
        this.timestampFloorMethod = partitionBy != PartitionBy.NONE ? getPartitionFloor(partitionBy) : null;
        this.partitionBy = partitionBy;
    }

    public void open() {
        assert this.roTxMem == null;
        roTxMem = (MappedReadOnlyMemory) openTxnFile(ff, path, rootLen);
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
            // No need to call grow here, file mapped beyond symbol section already
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
        this.symbolsCount = roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        this.partitionTableVersion = roTxMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION);
        loadAttachedPartitions(this.maxTimestamp, this.transientRowCount);
    }

    public int readWriterCount() {
        return roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
    }

    protected int findAttachedPartitionIndex(long ts) {
        ts = getPartitionTimestampLo(ts);
        return findAttachedPartitionIndexByLoTimestamp(ts);
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

        int blockHint = 2;
        //noinspection ConstantConditions
        assert (1 << blockHint) == LONGS_PER_TX_ATTACHED_PARTITION;
        return attachedPartitions.binarySearchBlock(0, hi, blockHint, ts);
    }

    long getPartitionTimestampLo(long timestamp) {
        return timestampFloorMethod != null ? timestampFloorMethod.floor(timestamp) : Long.MIN_VALUE;
    }

    private int insertPartitionSizeByTimestamp(int index, long partitionTimestamp, long partitionSize) {
        // Insert
        int size = attachedPartitions.size();
        attachedPartitions.extendAndSet(size + LONGS_PER_TX_ATTACHED_PARTITION - 1, 0);
        index = -(index + 1);
        if (index < size) {
            // Insert in the middle
            attachedPartitions.arrayCopy(index, index + LONGS_PER_TX_ATTACHED_PARTITION, size - index);
            partitionTableVersion++;
        }

        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestamp);
        attachedPartitions.setQuick(index + PARTITION_SIZE_OFFSET, partitionSize);
        // Out of order transaction which added this partition
        attachedPartitions.setQuick(index + PARTITION_TX_OFFSET, (index < size) ? txn + 1 : 0);
        return index;
    }

    private void loadAttachedPartitions(long maxTimestamp, long transientRowCount) {
        attachedPartitions.clear();
        if (partitionBy != PartitionBy.NONE) {
            roTxMem.grow(getPartitionTableIndexOffset(symbolsCount, 0));
            int partitionTableSize = roTxMem.getInt(getPartitionTableSizeOffset(symbolsCount)) / Long.BYTES;
            if (partitionTableSize > 0) {
                roTxMem.grow(getPartitionTableIndexOffset(symbolsCount, partitionTableSize));
                for (int i = 0; i < partitionTableSize; i++) {
                    attachedPartitions.add(roTxMem.getLong(getPartitionTableIndexOffset(symbolsCount, i)));
                }
            }

            if (maxTimestamp != Long.MIN_VALUE) {
                updateAttachedPartitionSizeByTimestamp(maxTimestamp, transientRowCount);
            }
        } else {
            roTxMem.grow(getPartitionTableIndexOffset(symbolsCount, 0));
            updateAttachedPartitionSizeByTimestamp(0L, transientRowCount);
        }
    }

    protected Mappable openTxnFile(FilesFacade ff, Path path, int rootLen) {
        try {
            if (this.ff.exists(this.path.concat(TXN_FILE_NAME).$())) {
                return new SinglePageMappedReadOnlyPageMemory(ff, path, this.ff.getPageSize(), getPartitionTableIndexOffset(0, 0));
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(this.path);
        } finally {
            this.path.trimTo(rootLen);
        }
    }

    protected int updateAttachedPartitionSizeByTimestamp(long timestamp, long partitionSize) {
        // todo: asses the opportunity to not round timestamp down if possible
        //    but instead perhaps maintain already rounded timestamp
        long partitionTimestampLo = getPartitionTimestampLo(timestamp);
//        assert partitionTimestampLo == timestamp;
        int index = findAttachedPartitionIndexByLoTimestamp(partitionTimestampLo);
        if (index > -1) {
            // Update
            updatePartitionSizeByIndex(index, partitionSize);
            return index;
        }

        return insertPartitionSizeByTimestamp(index, partitionTimestampLo, partitionSize);
    }

    private void updatePartitionSizeByIndex(int index, long partitionSize) {
        if (attachedPartitions.getQuick(index + PARTITION_SIZE_OFFSET) != partitionSize) {
            attachedPartitions.set(index + PARTITION_SIZE_OFFSET, partitionSize);
        }
    }
}
