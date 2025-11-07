/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public class TxReader implements Closeable, Mutable {
    public static final long DEFAULT_PARTITION_TIMESTAMP = 0L;
    public static final long PARTITION_FLAGS_MASK = 0x7FFFF00000000000L;
    public static final long PARTITION_SIZE_MASK = 0x80000FFFFFFFFFFFL;
    protected static final int NONE_COL_STRUCTURE_VERSION = Integer.MIN_VALUE;
    protected static final int PARTITION_MASKED_SIZE_OFFSET = 1;
    protected static final int PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET = 61;
    protected static final int PARTITION_MASK_READ_ONLY_BIT_OFFSET = 62;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    protected static final int PARTITION_PARQUET_FILE_SIZE_OFFSET = 3;
    // partition size's highest possible value is 0xFFFFFFFFFFFL (15 Tera Rows):
    //
    // | reserved | read-only | parquet format | available bits | partition size |
    // +----------+-----------+----------------+----------------+----------------+
    // |  1 bit   |  1 bit    |  1 bit         |  17 bits       |      44 bits   |
    //
    // when read-only bit is set, the partition is read only.
    // we reserve the highest bit to allow negative values to
    // have meaning (in future). For instance the table reader uses
    // a negative size value to mean that the partition is not open.
    // parquet format bit is used to indicate that partition was converted to parquet format
    // The last long in partition is the parquet file size.
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
    protected int timestampType;
    protected long transientRowCount;
    protected long truncateVersion;
    protected long txn;
    private int baseOffset;
    private TimestampDriver.TimestampCeilMethod partitionCeilMethod;
    private TimestampDriver.TimestampFloorMethod partitionFloorMethod;
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

    public void dumpRawTxPartitionInfo(LongList container) {
        container.add(attachedPartitions);
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
        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts, Vect.BIN_SEARCH_SCAN_UP);
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

    public long getCurrentPartitionMaxTimestamp(long timestamp) {
        return getNextPartitionTimestamp(timestamp) - 1;
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

    public long getNextExistingPartitionTimestamp(long timestamp) {
        if (partitionBy == PartitionBy.NONE) {
            return Long.MAX_VALUE;
        }

        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index < 0) {
            index = -index - 1;
        } else {
            index += LONGS_PER_TX_ATTACHED_PARTITION;
        }
        int nextIndex = index + PARTITION_TS_OFFSET;
        if (nextIndex < attachedPartitions.size()) {
            return attachedPartitions.get(nextIndex);
        }
        return Long.MAX_VALUE;
    }

    public long getNextLogicalPartitionTimestamp(long timestamp) {
        if (partitionCeilMethod != null) {
            return partitionCeilMethod.ceil(timestamp);
        }
        assert partitionBy == PartitionBy.NONE;
        return Long.MAX_VALUE;
    }

    public long getNextPartitionTimestamp(long timestamp) {
        if (partitionBy == PartitionBy.NONE) {
            return Long.MAX_VALUE;
        }

        int index = attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
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

    public int getPartitionCount() {
        return attachedPartitions.size() / LONGS_PER_TX_ATTACHED_PARTITION;
    }

    public long getPartitionFloor(long timestamp) {
        return partitionFloorMethod != null
                ? (timestamp != Long.MIN_VALUE ? partitionFloorMethod.floor(timestamp) : Long.MIN_VALUE)
                : DEFAULT_PARTITION_TIMESTAMP;
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
        return getPartitionNameTxnByRawIndex(attachedPartitions, index);
    }

    public long getPartitionParquetFileSize(int partitionIndex) {
        final long fileSize = getPartitionParquetFileSizeByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION);
        assert fileSize > 0 || !isPartitionParquet(partitionIndex);
        return fileSize;
    }

    public long getPartitionRowCountByTimestamp(long ts) {
        final int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1) {
            return attachedPartitions.getQuick(indexRaw + PARTITION_MASKED_SIZE_OFFSET) & PARTITION_SIZE_MASK;
        }
        return -1;
    }

    public long getPartitionSize(int i) {
        return getPartitionSizeByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public long getPartitionSizeByRawIndex(int index) {
        return getPartitionSizeByRawIndex(attachedPartitions, index);
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

    public int getTimestampType() {
        return timestampType;
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

    public void initPartitionBy(int timestampType, int partitionBy) {
        this.timestampType = timestampType;
        this.partitionBy = partitionBy;
        this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(timestampType, partitionBy);
        this.partitionCeilMethod = PartitionBy.getPartitionCeilMethod(timestampType, partitionBy);

        if (!PartitionBy.isPartitioned(partitionBy)) {
            // Add transient row count as the only partition in attached partitions list
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, transientRowCount, -1L);
        }
    }

    public void initRO(MemoryMR txnFile) {
        this.roTxMemBase = txnFile;
    }

    public boolean isLagOrdered() {
        return lagOrdered;
    }

    public boolean isPartitionParquet(int i) {
        return isPartitionParquetByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public boolean isPartitionParquetByRawIndex(int indexRaw) {
        return checkPartitionOptionBit(indexRaw, PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET);
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

    public boolean isPartitionReadOnlyByRawIndex(int indexRaw) {
        return checkPartitionOptionBit(indexRaw, PARTITION_MASK_READ_ONLY_BIT_OFFSET);
    }

    /**
     * Copies all _txn values from the given reader.
     */
    public void loadAllFrom(TxReader srcReader) {
        // The following three fields are temporary fields used when reading mmapped _txn file,
        // so they're not mandatory to copy. Yet, we include them for the sake of copy completeness.
        this.baseOffset = srcReader.baseOffset;
        this.size = srcReader.size;
        this.attachedPartitionsSize = srcReader.attachedPartitionsSize;

        this.partitionBy = srcReader.partitionBy;
        this.partitionFloorMethod = srcReader.partitionFloorMethod;
        this.partitionCeilMethod = srcReader.partitionCeilMethod;

        this.version = srcReader.version;
        this.symbolsSize = srcReader.symbolsSize;
        this.partitionSegmentSize = srcReader.partitionSegmentSize;

        this.txn = srcReader.txn;
        this.transientRowCount = srcReader.transientRowCount;
        this.fixedRowCount = srcReader.fixedRowCount;
        this.minTimestamp = srcReader.minTimestamp;
        this.maxTimestamp = srcReader.maxTimestamp;
        this.structureVersion = srcReader.structureVersion;
        this.dataVersion = srcReader.dataVersion;
        this.partitionTableVersion = srcReader.partitionTableVersion;
        this.columnVersion = srcReader.columnVersion;
        this.truncateVersion = srcReader.truncateVersion;
        this.seqTxn = srcReader.seqTxn;
        this.lagRowCount = srcReader.lagRowCount;
        this.lagMinTimestamp = srcReader.lagMinTimestamp;
        this.lagMaxTimestamp = srcReader.lagMaxTimestamp;
        this.lagOrdered = srcReader.lagOrdered;
        this.lagTxnCount = srcReader.lagTxnCount;
        this.symbolColumnCount = srcReader.symbolColumnCount;

        symbolCountSnapshot.clear();
        symbolCountSnapshot.addAll(srcReader.symbolCountSnapshot);

        attachedPartitions.clear();
        attachedPartitions.addAll(srcReader.attachedPartitions);
    }

    public TxReader ofRO(@Transient LPSZ path, int timestampType, int partitionBy) {
        clear();
        try {
            openTxnFile(ff, path);
            initPartitionBy(timestampType, partitionBy);
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
    }

    @Override
    public String toString() {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        StringSink sink = new StringSink();
        sink.put("{");
        sink.put("txn: ").put(txn);
        sink.put(", attachedPartitions: [");
        for (int i = 0; i < attachedPartitions.size(); i += LONGS_PER_TX_ATTACHED_PARTITION) {
            long timestamp = getPartitionTimestampByIndex(i / LONGS_PER_TX_ATTACHED_PARTITION);
            long rowCount = attachedPartitions.getQuick(i + PARTITION_MASKED_SIZE_OFFSET) & PARTITION_SIZE_MASK;

            if (i / LONGS_PER_TX_ATTACHED_PARTITION == getPartitionCount()) {
                rowCount = transientRowCount;
            }

            long nameTxn = getPartitionNameTxnByRawIndex(i);
            long parquetSize = getPartitionParquetFileSizeByRawIndex(i);

            if (i > 0) {
                sink.put(",");
            }
            sink.put("\n{ts: '");

            timestampDriver.append(sink, timestamp);
            sink.put("', rowCount: ").put(rowCount);
            sink.put(", nameTxn: ").put(nameTxn);
            if (isPartitionParquet(i / LONGS_PER_TX_ATTACHED_PARTITION)) {
                sink.put(", parquetSize: ").put(parquetSize);
            }
            if (isPartitionReadOnlyByRawIndex(i)) {
                sink.put(", readOnly=true");
            }
            sink.put("}");
        }
        sink.put("\n], transientRowCount: ").put(transientRowCount);
        sink.put(", fixedRowCount: ").put(fixedRowCount);
        sink.put(", minTimestamp: '");
        timestampDriver.append(sink, minTimestamp);
        sink.put("', maxTimestamp: '");
        timestampDriver.append(sink, maxTimestamp);
        sink.put("', dataVersion: ").put(dataVersion);
        sink.put(", structureVersion: ").put(structureVersion);
        sink.put(", partitionTableVersion: ").put(partitionTableVersion);
        sink.put(", columnVersion: ").put(columnVersion);
        sink.put(", truncateVersion: ").put(truncateVersion);
        sink.put(", seqTxn: ").put(seqTxn);
        sink.put(", symbolColumnCount: ").put(symbolColumnCount);
        sink.put(", lagRowCount: ").put(lagRowCount);
        sink.put(", lagMinTimestamp: '");
        timestampDriver.append(sink, lagMinTimestamp);
        sink.put("', lagMaxTimestamp: '");
        timestampDriver.append(sink, lagMaxTimestamp);
        sink.put("', lagTxnCount: ").put(lagTxnCount);
        sink.put(", lagOrdered: ").put(lagOrdered);
        sink.put("}");
        return sink.toString();
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

        boolean isA = (version & 1) == 0;
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

    public long unsafeLoadRowCount() {
        if (unsafeLoadBaseOffset()) {
            txn = version;
            if (txn != getLong(TX_OFFSET_TXN_64)) {
                return -1;
            }

            final long prevPartitionTableVersion = partitionTableVersion;
            final long prevColumnVersion = this.columnVersion;
            unsafeLoadPartitions(prevPartitionTableVersion, prevColumnVersion, partitionSegmentSize);
            transientRowCount = getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
            fixedRowCount = getLong(TX_OFFSET_FIXED_ROW_COUNT_64);

            Unsafe.getUnsafe().loadFence();
            if (version == unsafeReadVersion()) {
                return getRowCount();
            }
        }

        clearData();
        return -1;
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

    private boolean checkPartitionOptionBit(int indexRaw, int bitOffset) {
        long maskedSize = attachedPartitions.getQuick(indexRaw + PARTITION_MASKED_SIZE_OFFSET);
        return ((maskedSize >>> bitOffset) & 1) == 1;
    }

    private int getInt(long readOffset) {
        assert readOffset + Integer.BYTES <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
        return roTxMemBase.getInt(baseOffset + readOffset);
    }

    private long getLong(long readOffset) {
        assert readOffset + Long.BYTES <= size : "offset " + readOffset + ", size " + size + ", txn=" + txn;
        return roTxMemBase.getLong(baseOffset + readOffset);
    }

    private long getPartitionParquetFileSizeByRawIndex(int partitionRawIndex) {
        return attachedPartitions.getQuick(partitionRawIndex + PARTITION_PARQUET_FILE_SIZE_OFFSET);
    }

    private void openTxnFile(FilesFacade ff, LPSZ path) {
        long len = ff.length(path);
        // we check for length rather than file existence to account for possible delay
        // in FS updating its catalog under pressure. Logically, the size of TXN file
        // can never be less than the header. But async nature of FS updates have to be
        // accounted for.
        if (len >= TableUtils.TX_BASE_HEADER_SIZE) {
            // This method is called from constructor, and it's possible that
            // the code will run concurrently with table truncation. For that reason,
            // we must not rely on the file size but only assume that header is present.
            // The reload method will extend the file if needed.
            long size = TableUtils.TX_BASE_HEADER_SIZE;
            if (roTxMemBase == null) {
                roTxMemBase = Vm.getCMRInstance(ff, path, size, MemoryTag.MMAP_DEFAULT);
            } else {
                roTxMemBase.of(ff, path, ff.getPageSize(), size, MemoryTag.MMAP_DEFAULT);
            }
            return;
        }
        throw CairoException.fileNotFound()
                .put("could not open txn file [path=").put(path)
                .put(", len=").put(len)
                .put(", errno=").put(ff.errno())
                .put(']');
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
            // If partitionBy is NONE, we have no partitions, but we still need to
            // have a single partition with transient row count.
            attachedPartitions.setPos(LONGS_PER_TX_ATTACHED_PARTITION);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, transientRowCount, -1L);
            attachedPartitionsSize = 1;
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

    static int findPartitionRawIndex(LongList attachedPartitions, long partitionTimestamp) {
        return attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
    }

    static long getPartitionNameTxnByRawIndex(LongList attachedPartitions, int index) {
        return attachedPartitions.getQuick(index + PARTITION_NAME_TX_OFFSET);
    }

    static long getPartitionSizeByRawIndex(LongList attachedPartitions, int index) {
        return attachedPartitions.getQuick(index + PARTITION_MASKED_SIZE_OFFSET) & PARTITION_SIZE_MASK;
    }

    void clearData() {
        baseOffset = 0;
        size = 0;
        partitionTableVersion = -1;
        attachedPartitionsSize = -1;
        attachedPartitions.clear();
        version = -1;
        txn = -1;
        seqTxn = -1;
    }

    protected int findAttachedPartitionRawIndex(long timestamp) {
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(timestamp);
        if (indexRaw > -1L) {
            return indexRaw;
        }

        int prevIndexRaw = -indexRaw - 1 - LONGS_PER_TX_ATTACHED_PARTITION;
        if (prevIndexRaw < 0) {
            return -1;
        }
        long prevPartitionTimestamp = attachedPartitions.getQuick(prevIndexRaw + PARTITION_TS_OFFSET);
        if (getPartitionFloor(prevPartitionTimestamp) == getPartitionFloor(timestamp)) {
            return prevIndexRaw;
        }
        // Not found.
        return -1;
    }

    int findAttachedPartitionRawIndexByLoTimestamp(long ts) {
        // Start from the end, usually it will be last partition searched / appended
        return attachedPartitions.binarySearchBlock(LONGS_PER_TX_ATTACHED_PARTITION_MSB, ts, Vect.BIN_SEARCH_SCAN_UP);
    }

    protected void initPartitionAt(int index, long partitionTimestampLo, long partitionSize, long partitionNameTxn) {
        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestampLo);
        attachedPartitions.setQuick(index + PARTITION_MASKED_SIZE_OFFSET, partitionSize & PARTITION_SIZE_MASK);
        attachedPartitions.setQuick(index + PARTITION_NAME_TX_OFFSET, partitionNameTxn);
        attachedPartitions.setQuick(index + PARTITION_PARQUET_FILE_SIZE_OFFSET, -1L);
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
