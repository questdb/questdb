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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(TxReader.class);
    protected static final int PARTITION_TS_OFFSET = 0;
    protected static final int PARTITION_SIZE_OFFSET = 1;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    protected static final int PARTITION_DATA_TX_OFFSET = 3;
    protected final LongList attachedPartitions = new LongList();
    private final IntList symbolCountSnapshot = new IntList();
    private final FilesFacade ff;
    private final OffsetMemory roTxMem = new OffsetMemory();
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
    private MemoryMR roTxMemBase;
    private int baseOffset;
    private long size;
    private long version;
    private int symbolsSize;
    private int partitionSegmentSize;
    protected long columnVersion;

    public TxReader(FilesFacade ff) {
        this.ff = ff;
    }

    public boolean attachedPartitionsContains(long ts) {
        return findAttachedPartitionIndex(ts) > -1;
    }

    @Override
    public void clear() {
        clearData();
        if (roTxMemBase != null) {
            roTxMemBase.close();
        }
    }

    @Override
    public void close() {
        roTxMemBase = Misc.free(roTxMemBase);
        clear();
    }

    public int getBaseOffset() {
        return baseOffset;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public long getRecordSize() {
        return size;
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

    public long getColumnVersion() {
        return columnVersion;
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

    public long getTxn() {
        return txn;
    }

    public long getVersion() {
        return version;
    }

    public boolean unsafeLoadAll() {
        if (unsafeLoadBaseOffset()) {
            this.txn = version;
            if (txn != roTxMem.getLong(TX_OFFSET_TXN_64)) {
                return false;
            }

            this.transientRowCount = roTxMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
            this.fixedRowCount = roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
            this.minTimestamp = roTxMem.getLong(TX_OFFSET_MIN_TIMESTAMP_64);
            this.maxTimestamp = roTxMem.getLong(TX_OFFSET_MAX_TIMESTAMP_64);
            this.dataVersion = roTxMem.getLong(TX_OFFSET_DATA_VERSION_64);
            this.structureVersion = roTxMem.getLong(TX_OFFSET_STRUCT_VERSION_64);
            final long prevPartitionTableVersion = this.partitionTableVersion;
            this.partitionTableVersion = roTxMem.getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION_64);
            this.columnVersion = unsafeReadColumnVersion();
            this.symbolColumnCount = this.symbolsSize / 8;
            unsafeLoadSymbolCounts(symbolColumnCount);

            unsafeLoadPartitions(
                    prevPartitionTableVersion,
                    partitionSegmentSize
            );

            Unsafe.getUnsafe().loadFence();
            if (version == unsafeReadVersion()) {
//                LOG.infoW().$("read txn record ").$(version).$(", offset=").$(baseOffset).$(", size=").$(size)
//                        .$(", txn=").$(txn).$(", records=").$(fixedRowCount + transientRowCount).$(", dataVersion=").$(dataVersion).$();
                return true;
            }

        }

        clearData();
        return false;
    }

    public long unsafeReadColumnVersion() {
        return roTxMem.getLong(TX_OFFSET_COLUMN_VERSION_64);
    }

    public long getLastPartitionTimestamp() {
        if (PartitionBy.isPartitioned(partitionBy)) {
            return getPartitionTimestampLo(maxTimestamp);
        }
        return Long.MIN_VALUE;
    }

    public void initRO(MemoryMR txnFile, int partitionBy) {
        roTxMemBase = txnFile;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        this.partitionBy = partitionBy;
    }

    public TxReader ofRO(@Transient Path path, int partitionBy) {
        clear();
        int tableRootLen = path.length();
        try {
            openTxnFile(ff, path);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionBy = partitionBy;
        } catch (Throwable e) {
            close();
            path.trimTo(tableRootLen);
            throw e;
        }
        return this;
    }

    public boolean unsafeLoadBaseOffset() {
        this.version = unsafeReadVersion();
        Unsafe.getUnsafe().loadFence();

        boolean isA = version % 2 == 0;
        this.baseOffset = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_B_32);
        this.symbolsSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        this.partitionSegmentSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);

        // Before extending file, check that values read are not dirty
        Unsafe.getUnsafe().loadFence();
        if (unsafeReadVersion() != version) {
            return false;
        }

        this.size = calculateTxRecordSize(symbolsSize, partitionSegmentSize);
        if (this.size + this.baseOffset > roTxMemBase.size()) {
            roTxMemBase.extend(this.size + this.baseOffset);
        }
        return true;
    }

    public int unsafeReadSymbolColumnCount() {
        return roTxMem.getInt(TX_OFFSET_MAP_WRITER_COUNT_32);
    }

    public int unsafeReadSymbolCount(int symbolIndex) {
        return roTxMem.getInt(TableUtils.getSymbolWriterIndexOffset(symbolIndex));
    }

    public int unsafeReadSymbolTransientCount(int symbolIndex) {
        return roTxMem.getInt(getSymbolWriterTransientIndexOffset(symbolIndex));
    }

    public long unsafeReadVersion() {
        return roTxMemBase.getLong(TX_BASE_OFFSET_VERSION_64);
    }

    static int calculateTxRecordSize(int bytesSymbols, int bytesPartitions) {
        return TX_RECORD_HEADER_SIZE + 4 + bytesSymbols + 4 + bytesPartitions;
    }

    private void clearData() {
        baseOffset = 0;
        size = 0;
        partitionTableVersion = -1;
        attachedPartitionsSize = -1;
        attachedPartitions.clear();
        version = -1;
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

    private void openTxnFile(FilesFacade ff, Path path) {
        int pathLen = path.length();
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                if (roTxMemBase == null) {
                    roTxMemBase = Vm.getMRInstance(ff, path, ff.length(path), MemoryTag.MMAP_DEFAULT);
                } else {
                    roTxMemBase.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.MMAP_DEFAULT);
                }
                return;
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);
        } finally {
            path.trimTo(pathLen);
        }
    }

    protected long unsafeGetRawMemory() {
        return roTxMemBase.getPageAddress(0);
    }

    protected long unsafeGetRawMemorySize() {
        return this.size + this.baseOffset;
    }

    private void unsafeLoadPartitions(long prevPartitionTableVersion, int partitionTableSize) {
        if (PartitionBy.isPartitioned(partitionBy)) {
            int txAttachedPartitionsSize = partitionTableSize / Long.BYTES;
            if (txAttachedPartitionsSize > 0) {
                if (prevPartitionTableVersion != partitionTableVersion) {
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
        return roTxMem.getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
    }

    /**
     * Load variable length area sized from the file, e.g. Symbol Column Count and Partitions Size
     * to fail fast reload if they are not clean values.
     *
     * @param symbolColumnCount symbol count is used to calculate offset of partition table in file.
     * @return size partition table in bytes
     */
    private int unsafeReadPartitionSegmentSize(int symbolColumnCount) {
        long readOffset = getPartitionTableSizeOffset(symbolColumnCount);
        if (symbolColumnCount < 0 || readOffset + 4 > size) {
            return -1;
        }
        return roTxMem.getInt(readOffset);
    }

    protected int unsafeReadSymbolWriterIndexOffset(int denseSymbolIndex) {
        return roTxMem.getInt(getSymbolWriterIndexOffset(denseSymbolIndex));
    }

    private class OffsetMemory {
        public int getInt(long readOffset) {
            assert readOffset + 4 <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
            return roTxMemBase.getInt(baseOffset + readOffset);
        }

        public long getLong(long readOffset) {
            assert readOffset + 8 <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
            return roTxMemBase.getLong(baseOffset + readOffset);
        }

        public long getPageAddress(int pageIndex) {
            return roTxMemBase.getPageAddress(pageIndex + (int) (baseOffset / roTxMemBase.getPageSize()));
        }
    }
}
