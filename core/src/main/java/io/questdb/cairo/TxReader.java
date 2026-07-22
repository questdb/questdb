/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
    // Flag in the high byte of the offset-3 partition-version word: a remote copy of the
    // partition's parquet data exists. Set on parquet partitions and on native partitions
    // uploaded while native (upload-while-native keeps the format bit 0).
    public static final long PARTITION_REMOTE_BIT = 1L << 63;
    // Flag in the high byte of the offset-3 word: the value was written as a native seqTxn
    // stamp by a stamp-aware binary. Released binaries stored a parquet file size in this slot
    // for parquet_generated native partitions; without this bit such a legacy word is
    // indistinguishable from a seqTxn, so the native read quarantines it (returns -1) instead
    // of trusting it. Meaningful for native-format words only; a parquet word holds the file
    // size and is valid without it.
    public static final long PARTITION_SEQ_TXN_VALID_BIT = 1L << 62;
    public static final long PARTITION_SIZE_MASK = 0x80000FFFFFFFFFFFL;
    public static final int PARTITION_SQUASH_COUNTER_MAX = 0xFFFF;
    public static final long PARTITION_VERSION_FLAGS_MASK = 0xFFL << 56;
    public static final long PARTITION_VERSION_VALUE_MASK = ~PARTITION_VERSION_FLAGS_MASK;
    protected static final int NONE_COL_STRUCTURE_VERSION = Integer.MIN_VALUE;
    protected static final int PARTITION_MASKED_SIZE_OFFSET = 1;
    protected static final int PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET = 61;
    protected static final int PARTITION_MASK_PARQUET_GENERATED_BIT_OFFSET = 60;
    protected static final int PARTITION_MASK_READ_ONLY_BIT_OFFSET = 62;
    protected static final int PARTITION_NAME_TX_OFFSET = 2;
    protected static final int PARTITION_SQUASH_COUNTER_BIT_OFFSET = 44;
    protected static final long PARTITION_SQUASH_COUNTER_MASK = 0xFFFFL << PARTITION_SQUASH_COUNTER_BIT_OFFSET;
    // partition size's highest possible value is 0xFFFFFFFFFFFL (15 Tera Rows):
    //
    // | reserved | read-only | parquet format | parquet generated | squash counter | partition size |
    // +----------+-----------+----------------+-------------------+----------------+----------------+
    // |  1 bit   |  1 bit    |  1 bit         |  1 bit            |   16 bit       |      44 bits   |
    //
    // when read-only bit is set, the partition is read only.
    // we reserve the highest bit to allow negative values to
    // have meaning (in future). For instance the table reader uses
    // a negative size value to mean that the partition is not open.
    // the parquet format bit indicates that the partition has been converted to parquet format
    // the parquet generated bit indicates that a parquet file has been generated for the partition
    // the last long (PARTITION_VERSION_OFFSET) holds, for a parquet partition the parquet file
    // size, for a native one its last-modifying seqTxn:
    //
    // |  remote  |  valid   | reserved | value (parquet file size / native seqTxn) |
    // +----------+----------+----------+-------------------------------------------+
    // |  1 bit   |  1 bit   |  6 bits  |                 56 bits                   |
    //
    // remote is cleared by the value writes that supersede the bytes (setPartitionParquetFileSize, setPartitionSeqTxn),
    // so it can't outlive them; setPartitionFormat preserves it, leaving REMOTE to caller discipline on a format flip.
    // valid (PARTITION_SEQ_TXN_VALID_BIT) accompanies every positive native seqTxn stamp; a native word without it is
    // untrusted and reads as -1 (quarantines the released-binary file-size poison). A 0 stamp writes the cleared word,
    // so "valid with value 0" is unrepresentable. setPartitionFormat sets it flipping to native, clears it flipping to
    // parquet (a parquet word is valid without it).
    // legacy: a cleared slot reads as 0L (written today) or -1L (older binaries), both folded by isPartitionOffset3Cleared().
    protected static final int PARTITION_TS_OFFSET = 0;
    protected static final int PARTITION_VERSION_OFFSET = 3;
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
            long value = attachedPartitions.getQuick(i);
            // A native partition's generated data.parquet only duplicates its native columns and is not
            // part of a snapshot, so a checkpoint must not record the partition as parquet_generated --
            // else the restored _txn claims a data.parquet that was never backed up. The masked-size
            // word carries both the format and generated bits.
            if (i % LONGS_PER_TX_ATTACHED_PARTITION == PARTITION_MASKED_SIZE_OFFSET) {
                final int partitionIndex = i / LONGS_PER_TX_ATTACHED_PARTITION;
                if (!isPartitionParquet(partitionIndex) && isPartitionParquetGenerated(partitionIndex)) {
                    value &= ~(1L << PARTITION_MASK_PARQUET_GENERATED_BIT_OFFSET);
                }
            } else if (i % LONGS_PER_TX_ATTACHED_PARTITION == PARTITION_VERSION_OFFSET) {
                // A native offset-3 word without the VALID bit is untrusted (a released binary
                // stored a parquet file size there): scrub it to the cleared sentinel so a backup
                // never carries the ambiguous word. The whole word goes, including bit 63 -- a
                // native slot cannot legitimately be REMOTE without a valid stamp. Parquet words
                // hold the file size and are valid without the bit; leave them. The cleared 0L/-1L
                // sentinels scrub to the canonical 0L, a no-op in meaning.
                final int partitionIndex = i / LONGS_PER_TX_ATTACHED_PARTITION;
                if (!isPartitionParquet(partitionIndex)
                        && (isPartitionOffset3Cleared(value) || (value & PARTITION_SEQ_TXN_VALID_BIT) == 0)) {
                    value = 0L;
                }
            }
            long offset = TableUtils.getPartitionTableIndexOffset(partitionTableOffset, i);
            mem.putLong(baseOffset + offset, value);
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

    public int getFirstNativePartitionIndex() {
        for (int i = 0, n = getPartitionCount(); i < n; i++) {
            if (!isPartitionParquet(i)) {
                return i;
            }
        }
        return -1;
    }

    public int getFirstNativePartitionWithoutParquetGenerated() {
        for (int i = 0, n = getPartitionCount(); i < n; i++) {
            if (!isPartitionParquetGenerated(i) && !isPartitionParquet(i)) {
                return i;
            }
        }
        return -1;
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

    /**
     * Returns a native partition's last-modifying seqTxn from the offset-3 word
     * (flag bits masked off), or -1 when the version is unknown or untrusted. Native-only:
     * for a parquet partition offset 3 holds the file size, read it via the parquet accessor.
     * <p>
     * A word without {@link #PARTITION_SEQ_TXN_VALID_BIT} is quarantined to -1: released
     * binaries stored a parquet file size in this slot for {@code parquet_generated} native
     * partitions, and after an upgrade such a word is indistinguishable from a seqTxn by value
     * alone. Quarantined slots heal with a real stamp on the next write or at parquet
     * generation; callers needing a version sooner must stamp one themselves.
     * <p>
     * Contract: this is a monotonic-safe version hint, NOT a deterministic identity. It is
     * always {@code >=} the highest seqTxn that actually wrote the partition, and it strictly
     * increases whenever the partition's bytes change. It is NOT identical across instances
     * applying the same WAL: block grouping, WAL-lag carry, and partition squashing all
     * over-approximate it upward (never below). Only compare it where an over-estimate is
     * harmless.
     */
    public long getNativePartitionSeqTxn(int partitionIndex) {
        assert !isPartitionParquet(partitionIndex);
        final int rawIndex = partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION;
        // getPartitionOffset3 folds the cleared 0L/-1L sentinels to 0 before the bit test;
        // the legacy all-ones word has bit 62 set and must not read as a valid stamp.
        if ((getPartitionOffset3(rawIndex) & PARTITION_SEQ_TXN_VALID_BIT) == 0) {
            return -1L;
        }
        final long seqTxn = getPartitionVersionByRawIndex(rawIndex);
        // a positive stamp is guaranteed by the writer; fold a corrupt non-positive word
        return seqTxn > 0 ? seqTxn : -1L;
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
        assert isPartitionParquet(partitionIndex) : "parquet file size read on a native partition";
        final long fileSize = getPartitionVersionByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION);
        assert fileSize > 0;
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

    public int getPartitionSquashCount(int i) {
        return getPartitionSquashCountByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
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

    /**
     * Returns the partition-version value from the offset-3 word (flag bits masked off): the
     * parquet file size for a parquet-format partition, or the last-modifying seqTxn for a native
     * one. Distinct from {@link #getPartitionTableVersion()}, which versions the partition list.
     */
    public long getPartitionVersion(int partitionIndex) {
        return getPartitionVersionByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION);
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

    public boolean hasParquetPartitions() {
        for (int i = 0, n = attachedPartitions.size(); i < n; i += LONGS_PER_TX_ATTACHED_PARTITION) {
            if (isPartitionParquetByRawIndex(i)) {
                return true;
            }
        }
        return false;
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

    public boolean isPartitionParquetByPartitionTimestamp(long ts) {
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1) {
            return isPartitionParquetByRawIndex(indexRaw);
        }
        return false;
    }

    public boolean isPartitionParquetByRawIndex(int indexRaw) {
        return checkPartitionOptionBit(indexRaw, PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET);
    }

    public boolean isPartitionParquetGenerated(int i) {
        return isPartitionParquetGeneratedByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
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

    public boolean isPartitionRemote(int i) {
        return isPartitionRemoteByRawIndex(i * LONGS_PER_TX_ATTACHED_PARTITION);
    }

    public boolean isPartitionRemoteByPartitionTimestamp(long ts) {
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(ts);
        if (indexRaw > -1) {
            return isPartitionRemoteByRawIndex(indexRaw);
        }
        return false;
    }

    public boolean isPartitionRemoteByRawIndex(int indexRaw) {
        return (getPartitionOffset3(indexRaw) & PARTITION_REMOTE_BIT) != 0;
    }

    public boolean isPartitionRemotelyServed(int i) {
        return isPartitionParquet(i) && !isPartitionParquetGenerated(i) && isPartitionRemote(i);
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
            long parquetSize = getPartitionVersionByRawIndex(i);

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
            Unsafe.loadFence();
            if (version == unsafeReadVersion()) {
                return true;
            }
        }

        clearData();
        return false;
    }

    public boolean unsafeLoadBaseOffset() {
        version = unsafeReadVersion();
        Unsafe.loadFence();

        boolean isA = (version & 1) == 0;
        baseOffset = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_B_32);
        symbolsSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        partitionSegmentSize = isA ? roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : roTxMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);

        // Before extending file, check that values read are not dirty
        Unsafe.loadFence();
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

            Unsafe.loadFence();
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

    private long getPartitionVersionByRawIndex(int partitionRawIndex) {
        final long word = attachedPartitions.getQuick(partitionRawIndex + PARTITION_VERSION_OFFSET);
        return isPartitionOffset3Cleared(word) ? -1L : (word & PARTITION_VERSION_VALUE_MASK);
    }

    private boolean isPartitionParquetGeneratedByRawIndex(int indexRaw) {
        return checkPartitionOptionBit(indexRaw, PARTITION_MASK_PARQUET_GENERATED_BIT_OFFSET);
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
                final int lastPartitionRawIndex = txAttachedPartitionsSize - LONGS_PER_TX_ATTACHED_PARTITION;
                final long partitionTableOffset = getPartitionTableSizeOffset(symbolColumnCount) + Integer.BYTES;
                // WAL appends can update the active partition's offset-1 flags and offset-3 word
                // without changing partitionTableVersion, so refresh them alongside the transient row count.
                attachedPartitions.setQuick(
                        lastPartitionRawIndex + PARTITION_VERSION_OFFSET,
                        getLong(partitionTableOffset + (lastPartitionRawIndex + PARTITION_VERSION_OFFSET) * Long.BYTES)
                );
                final int sizeOffset = lastPartitionRawIndex + PARTITION_MASKED_SIZE_OFFSET;
                final long mask = getLong(partitionTableOffset + 8L * sizeOffset) & PARTITION_FLAGS_MASK;
                attachedPartitions.setQuick(sizeOffset, mask | (transientRowCount & PARTITION_SIZE_MASK)); // preserve mask
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

    protected static boolean isPartitionOffset3Cleared(long word) {
        return word == -1L || word == 0L;
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

    protected long getPartitionOffset3(int rawIndex) {
        final long word = attachedPartitions.getQuick(rawIndex + PARTITION_VERSION_OFFSET);
        return isPartitionOffset3Cleared(word) ? 0L : word;
    }

    int getPartitionSquashCountByRawIndex(int indexRaw) {
        long partitionSizeMasked = attachedPartitions.getQuick(indexRaw + PARTITION_MASKED_SIZE_OFFSET);
        return (int) ((partitionSizeMasked >>> PARTITION_SQUASH_COUNTER_BIT_OFFSET) & PARTITION_SQUASH_COUNTER_MAX);
    }

    protected void initPartitionAt(int index, long partitionTimestampLo, long partitionSize, long partitionNameTxn) {
        attachedPartitions.setQuick(index + PARTITION_TS_OFFSET, partitionTimestampLo);
        attachedPartitions.setQuick(index + PARTITION_MASKED_SIZE_OFFSET, partitionSize & PARTITION_SIZE_MASK);
        attachedPartitions.setQuick(index + PARTITION_NAME_TX_OFFSET, partitionNameTxn);
        attachedPartitions.setQuick(index + PARTITION_VERSION_OFFSET, 0L);
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
