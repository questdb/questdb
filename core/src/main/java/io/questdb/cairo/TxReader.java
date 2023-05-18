/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable, Mutable {
    public static final long PARTITION_FLAGS_MASK = 0x7FFFF00000000000L;
    public static final long PARTITION_SIZE_MASK = 0x80000FFFFFFFFFFFL;
    protected static final long DEFAULT_PARTITION_TIMESTAMP = 0L;
    protected static final int NONE_COL_STRUCTURE_VERSION = Integer.MIN_VALUE;
    protected static final int PARTITION_COLUMN_VERSION_OFFSET = 3;
    protected static final int PARTITION_MASKED_SIZE_OFFSET = 1;
    protected static final int PARTITION_MASK_READ_ONLY_BIT_OFFSET = 62;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    // partition size's highest possible value is 0xFFFFFFFFFFFL (15 Tera Rows):
    //
    // | reserved | read-only | available bits | partition size |
    // +----------+-----------+----------------+----------------+
    // |  1 bit   |  1 bit    |  18 bits       |      44 bits   |
    //
    // when read-only bit is set, the partition is read only.
    // we reserve the highest bit to allow negative values to
    // have meaning (in future). For instance the table reader uses
    // a negative size value to mean that the partition is not open.
    protected static final int PARTITION_TS_OFFSET = 0;
    protected final LongList attachedPartitions = new LongList();
    protected final FilesFacade ff;
    private final IntList symbolCountSnapshot = new IntList();
    protected int attachedPartitionsSize = 0;
    protected long columnVersion;
    protected long dataVersion;
    protected long fixedRowCount;
    protected long lagMaxTimestamp;
    protected long lagMinTimestamp;
    protected boolean lagOrdered;
    protected int lagRowCount;
    protected int lagTxnCount;
    protected long maxTimestamp;
    protected long minTimestamp;
    protected int partitionBy;
    protected long partitionTableVersion;
    protected long seqTxn;
    protected long structureVersion;
    protected int symbolColumnCount;
    protected long transientRowCount;
    protected long truncateVersion;
    protected long txn;
    private int baseOffset;
    private PartitionBy.PartitionCeilMethod partitionCeilMethod;
    private PartitionBy.PartitionFloorMethod partitionFloorMethod;
    private int partitionSegmentSize;
    private MemoryMR roTxMemBase;
    private long size;
    private int symbolsSize;
    private long version;

    public TxReader(FilesFacade ff) {
        this.ff = ff;
    }

    public boolean attachedPartitionsContains(long ts) {
        return findAttachedPartitionRawIndexByLoTimestamp(ts) > -1L;
    }

    public long ceilPartitionTimestamp(long timestamp) {
        return partitionCeilMethod.ceil(timestamp);
    }

    @Override
    public void clear() {
        clearData();
        Misc.free(roTxMemBase);
    }

    @Override
    public void close() {
        roTxMemBase = Misc.free(roTxMemBase);
        clear();
    }

    public void dumpTo(MemoryW mem) {
        mem.putLong(TX_BASE_OFFSET_VERSION_64, version);
        boolean isA = (version & 1L) == 0L;
        final int baseOffset = TX_BASE_HEADER_SIZE;
        mem.putInt(isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32, baseOffset);
        mem.putInt(isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32, symbolsSize);
        mem.putInt(isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32, partitionSegmentSize);

        mem.putLong(baseOffset + TX_OFFSET_TXN_64, txn);
        mem.putLong(baseOffset + TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
        mem.putLong(baseOffset + TX_OFFSET_FIXED_ROW_COUNT_64, fixedRowCount);
        mem.putLong(baseOffset + TX_OFFSET_MIN_TIMESTAMP_64, minTimestamp);
        mem.putLong(baseOffset + TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
        mem.putLong(baseOffset + TX_OFFSET_STRUCT_VERSION_64, structureVersion);
        mem.putLong(baseOffset + TX_OFFSET_DATA_VERSION_64, dataVersion);
        mem.putLong(baseOffset + TX_OFFSET_PARTITION_TABLE_VERSION_64, partitionTableVersion);
        mem.putLong(baseOffset + TX_OFFSET_COLUMN_VERSION_64, columnVersion);
        mem.putLong(baseOffset + TX_OFFSET_TRUNCATE_VERSION_64, truncateVersion);
        mem.putLong(baseOffset + TX_OFFSET_SEQ_TXN_64, seqTxn);
        mem.putInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32, lagRowCount);
        mem.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, lagMinTimestamp);
        mem.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, lagMaxTimestamp);
        mem.putInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32, lagOrdered ? lagTxnCount : -lagTxnCount);
        mem.putInt(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32, symbolColumnCount);

        int symbolMapCount = symbolCountSnapshot.size();
        for (int i = 0; i < symbolMapCount; i++) {
            long offset = TableUtils.getSymbolWriterIndexOffset(i);
            int symCount = symbolCountSnapshot.getQuick(i);
            mem.putInt(baseOffset + offset, symCount);
            offset += Integer.BYTES;
            mem.putInt(baseOffset + offset, symCount);
        }

        final int size = attachedPartitions.size();
        final long partitionTableOffset = TableUtils.getPartitionTableSizeOffset(symbolMapCount);
        mem.putInt(baseOffset + partitionTableOffset, size * Long.BYTES);
        for (int i = 0; i < size; i++) {
            long offset = TableUtils.getPartitionTableIndexOffset(partitionTableOffset, i);
            mem.putLong(baseOffset + offset, attachedPartitions.getQuick(i));
        }
    }

    public int findAttachedPartitionIndexByLoTimestamp(long ts) {
        // Start from the end, usually it will be last partition searched / appended
        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts, BinarySearch.SCAN_UP);
        if (index < 0) {
            return -((-index - 1) / LONGS_PER_TX_ATTACHED_PARTITION) - 1;
        }
        return index / LONGS_PER_TX_ATTACHED_PARTITION;
    }

    public int getBaseOffset() {
        return baseOffset;
    }

    public int getColumnStructureVersion() {
        int columnStructureVersion = Numbers.decodeHighInt(structureVersion);
        // if columnStructureVersion == 0 then the version is the same as metadata version
        // if columnStructureVersion == NONE_COL_STRUCTURE_VERSION then column structure version is 0.
        return columnStructureVersion == 0 ? getMetadataVersion() :
                columnStructureVersion == NONE_COL_STRUCTURE_VERSION ? 0 : columnStructureVersion;
    }

    public long getColumnVersion() {
        return columnVersion;
    }

    public long getDataVersion() {
        return dataVersion;
    }

    public long getFixedRowCount() {
        return fixedRowCount;
    }

    public long getLagMaxTimestamp() {
        return lagMaxTimestamp;
    }

    public long getLagMinTimestamp() {
        return lagMinTimestamp;
    }

    public int getLagRowCount() {
        return lagRowCount;
    }

    public int getLagTxnCount() {
        return lagTxnCount;
    }

    public long getLastPartitionTimestamp() {
        if (PartitionBy.isPartitioned(partitionBy)) {
            return getPartitionTimestampByTimestamp(maxTimestamp);
        }
        return DEFAULT_PARTITION_TIMESTAMP;
    }

    public long getLogicalPartitionTimestamp(long timestamp) {
        return getPartitionFloor(timestamp);
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public int getMetadataVersion() {
        return Numbers.decodeLowInt(structureVersion);
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getNextPartitionTimestamp(long timestamp) {
        if (partitionBy == PartitionBy.NONE) {
            return Long.MAX_VALUE;
        }

        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, timestamp, BinarySearch.SCAN_UP);
        if (index < 0) {
            index = -index - 1;
        } else {
            index += LONGS_PER_TX_ATTACHED_PARTITION;
        }
        int nextIndex = index + PARTITION_TS_OFFSET;
        if (nextIndex < attachedPartitions.size()) {
            long nextPartitionTs = attachedPartitions.getQuick(nextIndex);
            if (partitionFloorMethod.floor(timestamp) == partitionFloorMethod.floor(nextPartitionTs)) {
                return nextPartitionTs;
            }
        }
        return partitionCeilMethod.ceil(timestamp);
    }

    public long getPartitionColumnVersion(int i) {
        return getPartitionColumnVersionByIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionColumnVersionByIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_COLUMN_VERSION_OFFSET);
    }

    public int getPartitionCount() {
        return attachedPartitions.size() / LONGS_PER_TX_ATTACHED_PARTITION;
    }

    public int getPartitionIndex(long ts) {
        int index = findAttachedPartitionRawIndexByLoTimestamp(getPartitionTimestampByTimestamp(ts));
        if (index > -1) {
            return index / LONGS_PER_TX_ATTACHED_PARTITION;
        }
        return -1;
    }

    public long getPartitionNameTxn(int i) {
        return getPartitionNameTxnByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionNameTxnByPartitionTimestamp(long ts) {
        return getPartitionNameTxnByPartitionTimestamp(ts, -1);
    }

    public long getPartitionNameTxnByPartitionTimestamp(long ts, long defaultValue) {
        final int index = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (index > -1) {
            return attachedPartitions.getQuick(index + PARTITION_NAME_TX_OFFSET);
        }
        return defaultValue;
    }

    public long getPartitionNameTxnByRawIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_NAME_TX_OFFSET);
    }

    public long getPartitionSize(int i) {
        return getPartitionSizeByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionSizeByPartitionTimestamp(long ts) {
        final int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1) {
            return attachedPartitions.getQuick(indexRaw + PARTITION_MASKED_SIZE_OFFSET) & PARTITION_SIZE_MASK;
        }
        return -1;
    }

    public long getPartitionSizeByRawIndex(int index) {
        return attachedPartitions.getQuick(index + PARTITION_MASKED_SIZE_OFFSET) & PARTITION_SIZE_MASK;
    }

    public long getPartitionTableVersion() {
        return partitionTableVersion;
    }

    public long getPartitionTimestampByIndex(int i) {
        return attachedPartitions.getQuick(i * LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_TS_OFFSET);
    }

    public long getPartitionTimestampByTimestamp(long timestamp) {
        int indexRaw = findAttachedPartitionRawIndex(timestamp);
        if (indexRaw > -1) {
            return attachedPartitions.getQuick(indexRaw + PARTITION_TS_OFFSET);
        }
        return getPartitionFloor(timestamp);
    }

    public long getRecordSize() {
        return size;
    }

    public long getRowCount() {
        return transientRowCount + fixedRowCount;
    }

    public long getSeqTxn() {
        return seqTxn;
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

    public long getTruncateVersion() {
        return truncateVersion;
    }

    public long getTxn() {
        return txn;
    }

    public long getVersion() {
        return version;
    }

    public void initRO(MemoryMR txnFile, int partitionBy) {
        this.roTxMemBase = txnFile;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
        this.partitionCeilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
        this.partitionBy = partitionBy;
    }

    public boolean isLagOrdered() {
        return lagOrdered;
    }

    public boolean isPartitionReadOnly(int i) {
        return isPartitionReadOnlyByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public boolean isPartitionReadOnlyByPartitionTimestamp(long ts) {
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1) {
            return isPartitionReadOnlyByRawIndex(indexRaw);
        }
        return false;
    }

    public TxReader ofRO(@Transient LPSZ path, int partitionBy) {
        clear();
        try {
            openTxnFile(ff, path);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionBy = partitionBy;
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
    }

    public boolean unsafeLoadAll() {
        if (unsafeLoadBaseOffset()) {
            txn = version;
            if (txn != getLong(TX_OFFSET_TXN_64)) {
                return false;
            }

            transientRowCount = getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
            fixedRowCount = getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
            minTimestamp = getLong(TX_OFFSET_MIN_TIMESTAMP_64);
            maxTimestamp = getLong(TX_OFFSET_MAX_TIMESTAMP_64);
            dataVersion = getLong(TX_OFFSET_DATA_VERSION_64);
            structureVersion = getLong(TX_OFFSET_STRUCT_VERSION_64);
            final long prevPartitionTableVersion = partitionTableVersion;
            partitionTableVersion = getLong(TableUtils.TX_OFFSET_PARTITION_TABLE_VERSION_64);
            final long prevColumnVersion = this.columnVersion;
            columnVersion = unsafeReadColumnVersion();
            truncateVersion = getLong(TableUtils.TX_OFFSET_TRUNCATE_VERSION_64);
            seqTxn = getLong(TX_OFFSET_SEQ_TXN_64);
            symbolColumnCount = symbolsSize / Long.BYTES;
            lagRowCount = getInt(TX_OFFSET_LAG_ROW_COUNT_32);
            lagMinTimestamp = getLong(TX_OFFSET_LAG_MIN_TIMESTAMP_64);
            lagMaxTimestamp = getLong(TX_OFFSET_LAG_MAX_TIMESTAMP_64);
            int lagTxnCountRaw = getInt(TX_OFFSET_LAG_TXN_COUNT_32);
            lagTxnCount = Math.abs(lagTxnCountRaw);
            lagOrdered = lagTxnCountRaw > -1;
            unsafeLoadSymbolCounts(symbolColumnCount);
            unsafeLoadPartitions(prevPartitionTableVersion, prevColumnVersion, partitionSegmentSize);
            Unsafe.getUnsafe().loadFence();
            if (version == unsafeReadVersion()) {
                return true;
            }
        }

        clearData();
        return false;
    }

    public boolean unsafeLoadBaseOffset() {
        version = unsafeReadVersion();
        Unsafe.getUnsafe().loadFence();

        boolean isA = (version & 1L) == 0L;
        baseOffset = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_B_32);
        symbolsSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        partitionSegmentSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);

        // Before extending file, check that values read are not dirty
        Unsafe.getUnsafe().loadFence();
        if (unsafeReadVersion() != version) {
            return false;
        }

        size = calculateTxRecordSize(symbolsSize, partitionSegmentSize);
        if (size + baseOffset > roTxMemBase.size()) {
            roTxMemBase.extend(size + baseOffset);
        }
        return true;
    }

    public long unsafeReadColumnVersion() {
        return getLong(TX_OFFSET_COLUMN_VERSION_64);
    }

    public int unsafeReadSymbolColumnCount() {
        return getInt(TX_OFFSET_MAP_WRITER_COUNT_32);
    }

    public int unsafeReadSymbolCount(int symbolIndex) {
        return getInt(TableUtils.getSymbolWriterIndexOffset(symbolIndex));
    }

    public int unsafeReadSymbolTransientCount(int symbolIndex) {
        return getInt(getSymbolWriterTransientIndexOffset(symbolIndex));
    }

    public long unsafeReadVersion() {
        return roTxMemBase.getLong(TX_BASE_OFFSET_VERSION_64);
    }

    private void clearData() {
        baseOffset = 0;
        size = 0L;
        partitionTableVersion = -1L;
        attachedPartitionsSize = -1;
        attachedPartitions.clear();
        version = -1L;
        txn = -1L;
        seqTxn = -1L;
    }

    private int getInt(long readOffset) {
        assert readOffset + Integer.BYTES <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
        return roTxMemBase.getInt(baseOffset + readOffset);
    }

    private long getLong(long readOffset) {
        assert readOffset + Long.BYTES <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
        return roTxMemBase.getLong(baseOffset + readOffset);
    }

    private long getPartitionFloor(long timestamp) {
        return partitionFloorMethod != null ? (timestamp != Long.MIN_VALUE ? partitionFloorMethod.floor(timestamp) : Long.MIN_VALUE) : DEFAULT_PARTITION_TIMESTAMP;
    }

    private boolean isPartitionReadOnlyByRawIndex(int indexRaw) {
        long maskedSize = attachedPartitions.getQuick(indexRaw + PARTITION_MASKED_SIZE_OFFSET);
        return ((maskedSize >>> PARTITION_MASK_READ_ONLY_BIT_OFFSET) & 1) == 1;
    }

    private void openTxnFile(FilesFacade ff, LPSZ path) {
        if (ff.exists(path)) {
            if (roTxMemBase == null) {
                roTxMemBase = Vm.getMRInstance(ff, path, ff.length(path), MemoryTag.MMAP_DEFAULT);
            } else {
                roTxMemBase.of(ff, path, ff.getPageSize(), ff.length(path), MemoryTag.MMAP_DEFAULT);
            }
            return;
        }
        throw CairoException.critical(ff.errno()).put("Cannot append. File does not exist: ").put(path);
    }

    private void unsafeLoadPartitions(long prevPartitionTableVersion, long prevColumnVersion, int partitionTableSize) {
        if (PartitionBy.isPartitioned(partitionBy)) {
            int txAttachedPartitionsSize = partitionTableSize / Long.BYTES;
            if (txAttachedPartitionsSize > 0) {
                if (prevPartitionTableVersion != partitionTableVersion || prevColumnVersion != columnVersion) {
                    attachedPartitions.clear();
                    unsafeLoadPartitions0(0, txAttachedPartitionsSize);
                } else {
                    if (attachedPartitionsSize < txAttachedPartitionsSize) {
                        unsafeLoadPartitions0(
                                Math.max(attachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION, 0),
                                txAttachedPartitionsSize
                        );
                    }
                }
                int offset = txAttachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION + PARTITION_MASKED_SIZE_OFFSET;
                long mask = attachedPartitions.getQuick(offset) & PARTITION_FLAGS_MASK;
                attachedPartitions.setQuick(offset, mask | (transientRowCount & PARTITION_SIZE_MASK)); // preserve mask
                attachedPartitions.setPos(txAttachedPartitionsSize);
            } else {
                attachedPartitionsSize = 0;
                attachedPartitions.clear();
            }
        } else {
            // Add transient row count as the only partition in attached partitions list
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, transientRowCount, -1L, columnVersion);
        }
    }

    private void unsafeLoadPartitions0(int lo, int hi) {
        attachedPartitions.setPos(hi);
        final long baseOffset = getPartitionTableSizeOffset(symbolColumnCount) + Integer.BYTES;
        for (int i = lo; i < hi; i++) {
            attachedPartitions.setQuick(i, getLong(baseOffset + 8L * i));
        }
        attachedPartitionsSize = hi;
    }

    private void unsafeLoadSymbolCounts(int symbolMapCount) {
        symbolCountSnapshot.clear();
        // No need to call setSize here, file mapped beyond symbol section already
        // while reading attached partition count
        for (int i = 0; i < symbolMapCount; i++) {
            symbolCountSnapshot.add(getInt(TableUtils.getSymbolWriterIndexOffset(i)));
        }
    }

    protected int findAttachedPartitionRawIndex(long ts) {
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1L) {
            return indexRaw;
        }

        int prevIndexRaw = -indexRaw - 1 - LONGS_PER_TX_ATTACHED_PARTITION;
        if (prevIndexRaw < 0) {
            return -1;
        }
        long prevPartitionTimestamp = attachedPartitions.getQuick(prevIndexRaw + PARTITION_TS_OFFSET);
        if (getPartitionFloor(prevPartitionTimestamp) == getPartitionFloor(ts)) {
            return prevIndexRaw;
        }
        // Not found.
        return -1;
    }

    int findAttachedPartitionRawIndexByLoTimestamp(long ts) {
        // Start from the end, usually it will be last partition searched / appended
        return attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts, BinarySearch.SCAN_UP);
    }

    protected void initPartitionAt(int index, long partitionTimestampLo, long partitionSize, long partitionNameTxn, long columnVersion) {
        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestampLo);
        attachedPartitions.setQuick(index + PARTITION_MASKED_SIZE_OFFSET, partitionSize & PARTITION_SIZE_MASK);
        attachedPartitions.setQuick(index + PARTITION_NAME_TX_OFFSET, partitionNameTxn);
        attachedPartitions.setQuick(index + PARTITION_COLUMN_VERSION_OFFSET, columnVersion);
    }

    protected void switchRecord(int readBaseOffset, long readRecordSize) {
        baseOffset = readBaseOffset;
        size = readRecordSize;
    }

    protected long unsafeReadFixedRowCount() {
        return getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
    }

    protected int unsafeReadSymbolWriterIndexOffset(int denseSymbolIndex) {
        return getInt(TableUtils.getSymbolWriterIndexOffset(denseSymbolIndex));
    }
}
