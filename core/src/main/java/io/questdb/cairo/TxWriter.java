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
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

public final class TxWriter extends TxReader implements Closeable, Mutable, SymbolValueCountCollector {
    private final CairoConfiguration configuration;
    private long baseVersion;
    private TableWriter.ExtensionListener extensionListener;
    private int lastRecordBaseOffset = -1;
    private long lastRecordStructureVersion = -1;
    private long lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
    private long prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
    private long prevMaxTimestamp;
    private long prevMinTimestamp;
    private long prevPartitionTableVersion = -1;
    private int prevRecordBaseOffset = -2;
    private long prevRecordStructureVersion = -2;
    private long prevTransientRowCount;
    private int readBaseOffset;
    private long readRecordSize;
    private long recordStructureVersion = 0;
    private MemoryCMARW txMemBase;
    private int txPartitionCount;
    private int writeAreaSize;
    private int writeBaseOffset;

    public TxWriter(FilesFacade ff, CairoConfiguration configuration) {
        super(ff);
        this.configuration = configuration;
    }

    public void append() {
        transientRowCount++;
    }

    public void beginPartitionSizeUpdate() {
        if (maxTimestamp != Long.MIN_VALUE) {
            // Last partition size is usually not stored in attached partitions list
            // but in transientRowCount only.
            // To resolve transientRowCount after out of order partition update
            // let's store it in attached partitions list
            // before out of order partition update happens
            //
            // Plan 4b Task 1 fix: cellKey-aware. This used to call the plain updatePartitionSizeByTimestamp
            // overload, which hardcodes cellKey 0 -- correct for a plain table (every partition's cellKey
            // is always 0) but wrong for composite: it resolves "the day containing maxTimestamp,
            // cellKey 0", which is NOT necessarily the array's actual last entry whenever that day has
            // 2+ cells and cellKey 0 is not the highest-cellKey one sharing it (e.g. maxTimestamp's row
            // is in cellKey 1, but cellKey 0 also has a row somewhere earlier that same day). Reproduced
            // directly (Plan 4b Task 1 report): this wrote transientRowCount -- which belongs to the
            // array's true last entry -- into a DIFFERENT, unrelated cell's own independently-tracked
            // size slot, silently corrupting it; the next full scan of that cell then read a phantom row
            // with zeroed/garbage column data, which went on to silently overflow a native sort buffer by
            // one entry (glibc "malloc(): invalid size (unsorted)" once that corruption was later
            // detected by an unrelated allocation). Resolve the target cell directly from the array's own
            // last entry (whatever cellKey it actually is) instead of re-deriving one via a cellKey-blind
            // timestamp lookup that always assumes 0. For a plain table getPartitionCellKey always
            // returns 0 (guarded on the plain stride), so this is byte-identical there.
            int lastIndex = getPartitionCount() - 1;
            int cellKey = lastIndex > -1 ? getPartitionCellKey(lastIndex) : 0;
            // Use the last entry's OWN timestamp, not maxTimestamp. Taking the cellKey from the last
            // ENTRY while taking the timestamp from maxTimestamp pairs two values that need not
            // describe the same partition on a composite table: the last entry is the highest
            // (ts, cellKey), and its cellKey belongs to ITS day and ITS cell, while maxTimestamp is
            // merely the largest data timestamp. When the pair named no existing partition the lookup
            // missed and updateAttachedPartitionSizeByRawIndex INSERTED ON MISS -- creating a phantom
            // _txn entry carrying nameTxn = txn-1 for a cell whose directory was never written.
            //
            // Byte-identical for a plain table: there the last entry IS the partition holding
            // maxTimestamp, so both expressions floor to the same value.
            long lastTimestamp = lastIndex > -1 ? getPartitionTimestampByIndex(lastIndex) : maxTimestamp;
            recordStructureVersion++;
            updateAttachedPartitionSizeByTimestamp(lastTimestamp, cellKey, transientRowCount, txn - 1);
        }
    }

    public void bumpColumnStructureVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        structureVersion = Numbers.encodeLowHighInts(getMetadataVersion(), getColumnStructureVersion() + 1);
        commit(denseSymbolMapWriters);
    }

    public void bumpMetadataAndColumnStructureVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        structureVersion = Numbers.decodeHighInt(structureVersion) != 0 ? Numbers.encodeLowHighInts(getMetadataVersion() + 1, getColumnStructureVersion() + 1) : structureVersion + 1;
        commit(denseSymbolMapWriters);
    }

    public void bumpMetadataVersion(ObjList<? extends SymbolCountProvider> denseSymbolMapWriters) {
        recordStructureVersion++;
        int colStoreVersion = getColumnStructureVersion();
        if (colStoreVersion == 0) {
            colStoreVersion = NONE_COL_STRUCTURE_VERSION;
        }
        structureVersion = Numbers.encodeLowHighInts(getMetadataVersion() + 1, colStoreVersion);
        commit(denseSymbolMapWriters);
    }

    public void bumpPartitionTableVersion() {
        recordStructureVersion++;
        partitionTableVersion++;
    }

    public void bumpTruncateVersion() {
        truncateVersion++;
    }

    public void cancelRow() {
        boolean allRowsCancelled = transientRowCount <= 1 && fixedRowCount == 0;
        if (transientRowCount == 1 && txPartitionCount > 1) {
            // we have to undo creation of partition
            txPartitionCount--;
            lastSealedPartitionMaxTimestamp = prevLastSealedPartitionMaxTimestamp;
            fixedRowCount -= prevTransientRowCount;
            transientRowCount = prevTransientRowCount + 1; // When row cancel finishes 1 is subtracted. Add 1 to compensate.
            attachedPartitions.setPos(attachedPartitions.size() - longsPerAttachedPartition);
            prevTransientRowCount = getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
        }

        if (allRowsCancelled) {
            maxTimestamp = Long.MIN_VALUE;
            minTimestamp = Long.MAX_VALUE;
            prevMinTimestamp = minTimestamp;
            prevMaxTimestamp = maxTimestamp;
        } else {
            maxTimestamp = prevMaxTimestamp;
            minTimestamp = prevMinTimestamp;
        }

        recordStructureVersion++;
    }

    public long cancelToMaxTimestamp() {
        return prevMaxTimestamp;
    }

    public long cancelToTransientRowCount() {
        return prevTransientRowCount;
    }

    @Override
    public void clear() {
        clearData();
        if (txMemBase != null) {
            // Never trim _txn file to size. Size of the file can only grow up.
            txMemBase.close(false);
        }
        recordStructureVersion = 0L;
        lastRecordStructureVersion = -1L;
        prevRecordStructureVersion = -2L;
        lastRecordBaseOffset = -1;
        prevRecordBaseOffset = -2;
        lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void close() {
        try {
            clear();
            txMemBase = null;
        } finally {
            super.close();
        }
    }

    @Override
    public void collectValueCount(int symbolIndexInTxWriter, int count) {
        writeTransientSymbolCount(symbolIndexInTxWriter, count);
    }

    public void commit(ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        if (prevRecordStructureVersion == recordStructureVersion && prevRecordBaseOffset > 0) {
            // Optimisation for the case where commit appends rows to the last partition only
            // In this case all to be changed is TX_OFFSET_MAX_TIMESTAMP_64 and TX_OFFSET_TRANSIENT_ROW_COUNT_64
            writeBaseOffset = prevRecordBaseOffset;
            putLong(TX_OFFSET_TXN_64, ++txn);
            putLong(TX_OFFSET_SEQ_TXN_64, seqTxn);
            putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
            putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
            putLagValues();

            // Store symbol counts. Unfortunately we cannot skip it in here
            storeSymbolCounts(symbolCountProviders);

            Unsafe.storeFence();
            txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

            super.switchRecord(writeBaseOffset, writeAreaSize); // writeAreaSize should be between records
            readBaseOffset = writeBaseOffset;

            prevTransientRowCount = transientRowCount;
            prevMinTimestamp = minTimestamp;
            prevMaxTimestamp = maxTimestamp;

            prevRecordBaseOffset = lastRecordBaseOffset;
            lastRecordBaseOffset = writeBaseOffset;
            prevPartitionTableVersion = partitionTableVersion;
            int commitMode = configuration.getCommitMode();
            if (commitMode != CommitMode.NOSYNC) {
                txMemBase.sync(commitMode == CommitMode.ASYNC);
            }
        } else {
            // Slow path, record structure changed
            commitFullRecord(configuration.getCommitMode(), symbolCountProviders);
        }
    }

    public void finishPartitionSizeUpdate(long minTimestamp, long maxTimestamp) {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        finishPartitionSizeUpdate();
    }

    public void finishPartitionSizeUpdate() {
        recordStructureVersion++;
        int numPartitions = getPartitionCount();
        transientRowCount = numPartitions > 0 ? getPartitionSize(numPartitions - 1) : 0L;
        fixedRowCount = 0L;
        txPartitionCount = getPartitionCount();
        for (int i = 0, hi = txPartitionCount - 1; i < hi; i++) {
            fixedRowCount += getPartitionSize(i);
        }
    }

    public int getAppendedPartitionCount() {
        return txPartitionCount;
    }

    public long getLastTxSize() {
        return txPartitionCount == 1 ? transientRowCount - prevTransientRowCount : transientRowCount;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount || prevPartitionTableVersion != partitionTableVersion;
    }

    public boolean incrementPartitionSquashCounter(int partitionIndex) {
        final int partitionRawIndex = partitionIndex * longsPerAttachedPartition;
        int partitionSquashCounter = getPartitionSquashCountByRawIndex(partitionRawIndex);
        if (partitionSquashCounter == PARTITION_SQUASH_COUNTER_MAX) {
            // This means 16bit unsigned value is overflown.
            // Return false so that the caller can fall back to an alternative way to track squashes.
            return false;
        }
        setPartitionSquashCounterByRawIndex(partitionRawIndex, (short) (partitionSquashCounter + 1));
        // Bump versions to make sure that incremental txn update will save the change
        // and incremental txn read will read it
        recordStructureVersion++;
        partitionTableVersion++;
        return true;
    }

    /**
     * Plan 3 Task 4: {@code (ts, cellKey)}-resolving counterpart of {@link #incrementPartitionSquashCounter(int)}.
     * Resolves the exact cell via {@link TxReader#findAttachedPartitionRawIndexBy}, then delegates --
     * production has no timestamp-resolving squash-counter caller today (the sole caller,
     * {@code TableWriter.squashSplitPartitions}, already tracks the ordinal partition index itself), so
     * this exists purely so a test can target one cell's squash counter without doing raw-index/stride
     * arithmetic by hand.
     */
    public boolean incrementPartitionSquashCounter(long timestamp, int cellKey) {
        int indexRaw = findAttachedPartitionRawIndexBy(timestamp, cellKey);
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        return incrementPartitionSquashCounter(indexRaw / longsPerAttachedPartition);
    }

    /**
     * Plan 4b Task 1 fix (follow-up): cellKey-aware, mirroring {@link #beginPartitionSizeUpdate()}'s
     * identical fix (same commit, a few lines above). This used to hardcode cellKey 0 when resolving
     * which entry to zero -- correct for a plain table (every partition's cellKey is always 0) but wrong
     * for a composite table whenever the day containing {@code timestamp} has 2+ cells and cellKey 0 is
     * not the array's actual last entry: it would zero an unrelated, already-correct sibling cell's size
     * instead of the genuinely-open last partition's (the exact bug {@link #beginPartitionSizeUpdate()}'s
     * own doc describes). That cell-blindness is why this call was, for a while, gated off entirely for
     * composite tables at the {@code TableWriter#initLastPartition} call site -- an over-broad fix that
     * also silently suppressed the safe, common case (a single-cell-per-day composite table, or one where
     * the array's last entry genuinely IS cellKey 0), regressing the pre-existing "a full TableWriter
     * reopen always leaves the still-open last partition's persisted size slot at 0" invariant (
     * {@code transientRowCount}, not this slot, is the writer's source of truth for it going forward) --
     * see {@code CompositeTxCellTest#testReopenAfterCompositeBlindLoad}. Root cause fixed properly
     * instead: resolve the target cell from the array's own last entry (whatever cellKey it actually is),
     * exactly like {@link #beginPartitionSizeUpdate()} does, which makes this safe to call unconditionally
     * again for plain, dormant-composite, AND real composite tables alike. For a plain table
     * {@code getPartitionCellKey} always returns 0 (guarded on the plain stride), so this is
     * byte-identical there.
     */
    public void initLastPartition(long timestamp) {
        txPartitionCount = 1;
        int lastIndex = getPartitionCount() - 1;
        int cellKey = lastIndex > -1 ? getPartitionCellKey(lastIndex) : 0;
        updateAttachedPartitionSizeByTimestamp(timestamp, cellKey, 0L, txn - 1);
    }

    public void insertPartition(int index, long partitionTimestamp, long size, long nameTxn) {
        // Real writes only ever produce cellKey 0 today -- composite routing (which cell a row lands
        // in) is Plan 4.
        insertPartitionSizeByTimestamp(index * longsPerAttachedPartition, partitionTimestamp, size, nameTxn, 0);
    }

    /**
     * Test-only: synthesizes a partition at an explicit (timestamp, cellKey), appended at the tail of
     * the attached-partitions list. The real write path only ever produces cellKey 0 today (composite
     * routing is Plan 4); this lets tests build multi-cell scenarios directly -- e.g. two distinct cells
     * at the same timestamp -- without waiting for real routing to exist. Callers are responsible for
     * appending in the order they want the persisted record to end up in: this does a raw tail append,
     * not a (timestamp, cellKey)-aware ordered insert (that lookup/insert logic is Task 3).
     */
    public void appendPartitionForTest(long partitionTimestamp, long size, long nameTxn, int cellKey) {
        insertPartitionSizeByTimestamp(attachedPartitions.size(), partitionTimestamp, size, nameTxn, cellKey);
    }

    /**
     * Test-only: the real (timestamp, cellKey)-ordered insert (Plan 3 Task 3). Unlike {@link
     * #appendPartitionForTest}, which always raw-tail-appends, this computes the correct sorted
     * position via {@link TxReader#findAttachedPartitionRawIndexBy} and physically inserts there,
     * shifting later entries right -- so a test can grow a multi-cell attached-partitions list one
     * insert at a time, in any order, and assert the resulting (ts, cellKey) layout. Production
     * mutators still only ever pass cellKey 0 (real cellKey write-routing is Plan 4); this seam exists
     * purely to exercise the ordering logic ahead of that.
     */
    public void insertPartitionForTest(long partitionTimestamp, long size, long nameTxn, int cellKey) {
        int indexRaw = findAttachedPartitionRawIndexBy(partitionTimestamp, cellKey);
        if (indexRaw > -1) {
            throw CairoException.nonCritical().put("partition (ts, cellKey) already exists");
        }
        insertPartitionSizeByTimestamp(-(indexRaw + 1), partitionTimestamp, size, nameTxn, cellKey);
    }

    public boolean isInsideExistingPartition(long timestamp) {
        int index = attachedPartitions.binarySearchBlock(attachedPartitionsShl, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1 && index < attachedPartitions.size()) {
            return true;
        }

        int prevPartition = (-index - 1) - longsPerAttachedPartition;
        if (prevPartition > -1) {
            long prevPartitionTs = attachedPartitions.getQuick(prevPartition + PARTITION_TS_OFFSET);
            return getPartitionFloor(prevPartitionTs) == getPartitionFloor(timestamp);
        }
        return false;
    }

    @Override
    public TxWriter ofRO(@Transient LPSZ path, int timestampType, int partitionBy) {
        throw new IllegalStateException();
    }

    public TxWriter ofRW(@Transient LPSZ path) {
        clear();
        openTxnFile(ff, path);
        try {
            super.initRO(txMemBase);
            unsafeLoadAll();
        } catch (Throwable e) {
            if (txMemBase != null) {
                // Do not truncate in case the file cannot be read
                txMemBase.close(false);
                txMemBase = null;
            }
            super.close();
            throw e;
        }
        return this;
    }

    public TxWriter ofRW(@Transient LPSZ path, int timestampType, int partitionBy) {
        TxWriter t = ofRW(path);
        t.initPartitionBy(timestampType, partitionBy);
        return t;
    }

    public void removeAllPartitions() {
        maxTimestamp = Long.MIN_VALUE;
        minTimestamp = Long.MAX_VALUE;
        lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        attachedPartitions.clear();
        recordStructureVersion++;
        truncateVersion++;
        partitionTableVersion++;
        dataVersion++;
    }

    public int removeAttachedPartitions(long timestamp) {
        return removeAttachedPartitions(timestamp, 0);
    }

    /**
     * Plan 3 Task 4: {@code (ts, cellKey)}-resolving counterpart of {@link #removeAttachedPartitions(long)},
     * which is now a thin {@code cellKey = 0} delegate to this method (byte-identical for plain and
     * dormant-composite tables, mirroring Task 3's {@code findAttachedPartitionRawIndexByLoTimestamp}).
     * Resolves the exact same-ts cell to remove via {@link TxReader#findAttachedPartitionRawIndexBy}
     * rather than just the first same-ts entry, so removing one cell cannot delete a sibling cell at the
     * same timestamp instead.
     */
    public int removeAttachedPartitions(long timestamp, int cellKey) {
        recordStructureVersion++;
        final long partitionTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        int indexRaw = findAttachedPartitionRawIndexBy(partitionTimestampLo, cellKey);
        if (indexRaw > -1) {
            final int size = attachedPartitions.size();
            final int lim = size - longsPerAttachedPartition;
            if (indexRaw < lim) {
                attachedPartitions.arrayCopy(indexRaw + longsPerAttachedPartition, indexRaw, lim - indexRaw);
            }
            attachedPartitions.setPos(lim);
            partitionTableVersion++;
            return indexRaw / longsPerAttachedPartition;
        } else {
            assert false;
            return -1;
        }
    }

    public void reset(
            long fixedRowCount,
            long transientRowCount,
            long maxTimestamp,
            ObjList<? extends SymbolCountProvider> symbolCountProviders
    ) {
        recordStructureVersion++;
        this.fixedRowCount = fixedRowCount;
        this.maxTimestamp = maxTimestamp;
        this.transientRowCount = transientRowCount;
        commit(symbolCountProviders);
    }

    public void resetLagAppliedRows() {
        txMemBase.putInt(readBaseOffset + TX_OFFSET_LAG_TXN_COUNT_32, 0);
        txMemBase.putInt(readBaseOffset + TX_OFFSET_LAG_ROW_COUNT_32, 0);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, Long.MAX_VALUE);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, Long.MIN_VALUE);
        txMemBase.putLong(readBaseOffset + TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0));
    }

    public void resetLagValuesUnsafe() {
        txMemBase.putLong(readBaseOffset + TX_OFFSET_SEQ_TXN_64, 0);
        txMemBase.putInt(readBaseOffset + TX_OFFSET_CHECKSUM_32, 0);
        resetLagAppliedRows();
    }

    public void resetStructureVersionUnsafe() {
        txMemBase.putLong(readBaseOffset + TX_OFFSET_STRUCT_VERSION_64, 0);
    }

    public void resetTimestamp() {
        recordStructureVersion++;
        lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevMaxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        maxTimestamp = prevMaxTimestamp;
        minTimestamp = prevMinTimestamp;
    }

    public void setColumnVersion(long newVersion) {
        if (columnVersion != newVersion) {
            recordStructureVersion++;
            columnVersion = newVersion;
        }
    }

    public void setExtensionListener(TableWriter.ExtensionListener extensionListener) {
        this.extensionListener = extensionListener;
    }

    public void setLagMaxTimestamp(long timestamp) {
        lagMaxTimestamp = timestamp;
    }

    public void setLagMinTimestamp(long timestamp) {
        lagMinTimestamp = timestamp;
    }

    public void setLagOrdered(boolean ordered) {
        lagOrdered = ordered;
    }

    public void setLagRowCount(int rowCount) {
        lagRowCount = rowCount;
    }

    public void setLagTxnCount(int txnCount) {
        lagTxnCount = txnCount;
    }

    public void setMaxTimestamp(long timestamp) {
        this.maxTimestamp = timestamp;
    }

    public void setMinTimestamp(long timestamp) {
        recordStructureVersion++;
        minTimestamp = timestamp;
        if (prevMinTimestamp == Long.MAX_VALUE) {
            prevMinTimestamp = minTimestamp;
        }
    }

    public void setPartitionNative(long timestamp, long seqTxn) {
        setPartitionFormat(timestamp, false, seqTxn);
    }

    public void setPartitionParquet(long timestamp, long fileLength) {
        setPartitionFormat(timestamp, true, fileLength);
    }

    /**
     * Marks exactly one {@code (ts, cellKey)} record parquet. See
     * {@link #setPartitionFormatByRawIndex(int, boolean, long)} for why a timestamp is not enough.
     */
    public void setPartitionParquetByRawIndex(int indexRaw, long fileLength) {
        setPartitionFormatByRawIndex(indexRaw, true, fileLength);
    }

    /**
     * Marks exactly one {@code (ts, cellKey)} record native again.
     */
    public void setPartitionNativeByRawIndex(int indexRaw, long seqTxn) {
        setPartitionFormatByRawIndex(indexRaw, false, seqTxn);
    }

    public void setPartitionParquetFileSize(int partitionIndex, long size) {
        setPartitionParquetFileSizeByRawIndex(partitionIndex * longsPerAttachedPartition, size);
    }

    public void setPartitionParquetFileSizeByRawIndex(int indexRaw, long size) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        long flags = getPartitionOffset3(indexRaw) & PARTITION_VERSION_FLAGS_MASK & ~PARTITION_REMOTE_BIT;
        attachedPartitions.setQuick(indexRaw + PARTITION_VERSION_OFFSET, (size & PARTITION_VERSION_VALUE_MASK) | flags);
    }

    public void setPartitionParquetGenerated(int partitionIndex, boolean parquetGenerated) {
        int indexRaw = partitionIndex * longsPerAttachedPartition;
        setPartitionParquetGeneratedByRawIndex(indexRaw, parquetGenerated);
    }

    public void setPartitionParquetGenerated(long timestamp, boolean parquetGenerated) {
        setPartitionParquetGeneratedByRawIndex(findAttachedPartitionRawIndex(timestamp), parquetGenerated);
    }

    public void setPartitionParquetGeneratedByRawIndex(int indexRaw, boolean parquetGenerated) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        int offset = indexRaw + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);
        attachedPartitions.setQuick(offset, updatePartitionHasParquetGenerated(maskedSize, parquetGenerated));
    }

    public void setPartitionReadOnly(int partitionIndex, boolean isReadOnly) {
        setPartitionReadOnlyByRawIndex(partitionIndex * longsPerAttachedPartition, isReadOnly);
    }

    public void setPartitionReadOnlyByRawIndex(int indexRaw, boolean isReadOnly) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        int offset = indexRaw + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);
        attachedPartitions.setQuick(offset, updatePartitionIsReadOnly(maskedSize, isReadOnly));
    }

    public void setPartitionReadOnlyByTimestamp(long timestamp, boolean isReadOnly) {
        setPartitionReadOnlyByRawIndex(findAttachedPartitionRawIndex(timestamp), isReadOnly);
    }

    public void setPartitionRemote(int partitionIndex, boolean isRemote) {
        setPartitionRemoteByRawIndex(partitionIndex * longsPerAttachedPartition, isRemote);
    }

    public void setPartitionRemoteByRawIndex(int indexRaw, boolean isRemote) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        final long word = getPartitionOffset3(indexRaw);
        final long updated = isRemote
                ? word | PARTITION_REMOTE_BIT
                : word & ~PARTITION_REMOTE_BIT;
        attachedPartitions.setQuick(indexRaw + PARTITION_VERSION_OFFSET, updated);
    }

    public void setPartitionRemoteByTimestamp(long timestamp, boolean isRemote) {
        setPartitionRemoteByRawIndex(findAttachedPartitionRawIndex(timestamp), isRemote);
    }

    public void setPartitionSeqTxn(int partitionIndex, long seqTxn) {
        setPartitionSeqTxnByRawIndex(partitionIndex * longsPerAttachedPartition, seqTxn);
    }

    /**
     * Stamps a native partition's last-modifying seqTxn into the offset-3 word, preserving
     * reserved flag bits. Clears REMOTE and parquet_generated: the stamp records a data
     * change, so no remote or generated copy matches the bytes anymore. A positive stamp
     * carries {@link TxReader#PARTITION_SEQ_TXN_VALID_BIT}, marking the word a trusted seqTxn
     * (untrusted legacy words read as -1). The non-WAL path stamps 0, the cleared word, which
     * reads back as the -1 "no version" sentinel.
     */
    public void setPartitionSeqTxnByRawIndex(int indexRaw, long seqTxn) {
        setPartitionParquetGeneratedByRawIndex(indexRaw, false);
        long flags = getPartitionOffset3(indexRaw) & PARTITION_VERSION_FLAGS_MASK & ~(PARTITION_REMOTE_BIT | PARTITION_SEQ_TXN_VALID_BIT);
        final long valid = seqTxn > 0 ? PARTITION_SEQ_TXN_VALID_BIT : 0L;
        attachedPartitions.setQuick(indexRaw + PARTITION_VERSION_OFFSET, (seqTxn & PARTITION_VERSION_VALUE_MASK) | flags | valid);
    }

    public void setSeqTxn(long seqTxn) {
        this.seqTxn = seqTxn;
    }

    public void switchPartitions(long timestamp) {
        recordStructureVersion++;
        prevLastSealedPartitionMaxTimestamp = lastSealedPartitionMaxTimestamp;
        lastSealedPartitionMaxTimestamp = maxTimestamp;
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        long partitionTimestampLo = getPartitionTimestampByTimestamp(maxTimestamp);
        int indexRaw = findAttachedPartitionRawIndexByLoTimestamp(partitionTimestampLo);
        updatePartitionSizeByRawIndex(indexRaw, transientRowCount);

        indexRaw += longsPerAttachedPartition;

        attachedPartitions.setPos(indexRaw + longsPerAttachedPartition);
        long newTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        // Real writes only ever produce cellKey 0 today -- composite routing (which cell a row lands
        // in) is Plan 4.
        initPartitionAt(indexRaw, newTimestampLo, 0L, txn - 1, 0);
        transientRowCount = 0L;
        txPartitionCount++;
        if (extensionListener != null) {
            extensionListener.onTableExtended(newTimestampLo);
        }
    }

    public void truncate(long columnVersion, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        removeAllPartitions();
        if (!PartitionBy.isPartitioned(partitionBy)) {
            attachedPartitions.setPos(longsPerAttachedPartition);
            initPartitionAt(0, DEFAULT_PARTITION_TIMESTAMP, 0L, -1L, 0);
        }

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset(writeAreaSize);
        resetTxn(
                txMemBase,
                writeBaseOffset,
                getSymbolColumnCount(),
                ++txn,
                seqTxn,
                dataVersion,
                partitionTableVersion,
                structureVersion,
                columnVersion,
                truncateVersion
        );
        prevPartitionTableVersion = partitionTableVersion;
        storeSymbolCounts(symbolCountProviders);
        finishABHeader(writeBaseOffset, symbolColumnCount * Long.BYTES, 0, CommitMode.NOSYNC);
    }

    public boolean unsafeLoadAll() {
        super.unsafeLoadAll();
        this.baseVersion = getVersion();
        this.prevPartitionTableVersion = partitionTableVersion;
        this.txPartitionCount = 1;
        this.lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        this.prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        if (baseVersion >= 0) {
            this.readBaseOffset = getBaseOffset();
            this.readRecordSize = getRecordSize();
            this.prevTransientRowCount = this.transientRowCount;
            this.prevMaxTimestamp = maxTimestamp;
            this.prevMinTimestamp = minTimestamp;
            return true;
        }
        return false;
    }

    /**
     * Reopen-ordering fix (Plan 3 Task 2). TableWriter's constructor must open this txWriter via the
     * 1-arg {@link #ofRW(LPSZ)} before table metadata -- and therefore composite-ness -- is known (see
     * the constructor's comment above its {@code setComposite} call), so the very first
     * attached-partitions load always runs at the plain (4-long) stride. That's harmless for a plain
     * table (the stride was already correct) and for a fresh table (nothing persisted yet to misread).
     * For an ALREADY-PARTITIONED composite table, though, {@link TxReader#unsafeLoadPartitions} mis-folds
     * the live transientRowCount into slot 5 of the last partition's record (reserved, should stay 0)
     * instead of the real masked-size slot 1, because it used the wrong (still-plain) stride at fold
     * time -- and initPartitionBy() does not reload attachedPartitions for an already-partitioned table,
     * so that corruption sticks. (The last partition's masked-size slot 1 itself is NOT user-visibly
     * wrong afterward -- TableWriter's own configureAppendPosition()/initLastPartition() unconditionally
     * resets it to 0 later regardless, by design, since a writer always re-derives the open last
     * partition's size from transientRowCount. The lasting, real defect is stale non-zero garbage left
     * behind in reserved slot 5, which a future task giving that slot real meaning would silently
     * misread.)
     * <p>
     * Call this once, right after {@code setComposite(true)} has learned the table is actually
     * composite; a no-op otherwise (guarded on the stride setComposite just set, so calling it
     * unconditionally for every table is safe). Forces a full re-copy of the raw attached-partitions
     * region at the now-correct stride -- healing slot 5 back to its true on-disk value of 0 -- and
     * re-runs the transient-row-count fold at the correct slot-1 offset.
     */
    void reloadAttachedPartitionsAfterComposite() {
        if (longsPerAttachedPartition == LONGS_PER_TX_ATTACHED_PARTITION_COMPOSITE) {
            // attachedPartitionsSize is the "high water mark" unsafeLoadPartitions() uses to decide
            // whether it needs to (re)copy raw longs from the file; forcing it below the real region
            // size makes the next unsafeLoadAll() redo that copy (undoing the blind load's stride-4
            // slot-5 corruption) as well as the transient-row-count fold, both now at the correct stride.
            attachedPartitionsSize = -1;
            unsafeLoadAll();
        }
    }

    public void updateAttachedPartitionSizeByRawIndex(int partitionIndex, long partitionTimestampLo, long partitionSize, long partitionNameTxn) {
        // Plain wrapper: every production caller resolves partitionIndex from a plain/dormant-composite
        // (cellKey 0 always) context -- composite routing (which cell a row lands in) is Plan 4.
        updateAttachedPartitionSizeByRawIndex(partitionIndex, partitionTimestampLo, partitionSize, partitionNameTxn, 0);
    }

    /**
     * Plan 3 Task 4: {@code cellKey}-aware counterpart of {@link #updateAttachedPartitionSizeByRawIndex(int, long, long, long)},
     * which is now a thin {@code cellKey = 0} delegate to this method. {@code partitionIndex} is still a
     * RAW index (or its negative insertion-point encoding) already resolved by the caller, so the
     * update-in-place branch needs no cellKey (a raw index already identifies one exact cell); only the
     * insert-on-miss branch needs it, to insert the new partition at the caller's actual cellKey instead
     * of always hardcoding 0.
     */
    public void updateAttachedPartitionSizeByRawIndex(int partitionIndex, long partitionTimestampLo, long partitionSize, long partitionNameTxn, int cellKey) {
        if (partitionIndex > -1) {
            updatePartitionSizeByRawIndex(partitionIndex, partitionSize);
        } else {
            insertPartitionSizeByTimestamp(-(partitionIndex + 1), partitionTimestampLo, partitionSize, partitionNameTxn, cellKey);
        }
    }

    public void updateMaxTimestamp(long timestamp) {
        prevMaxTimestamp = maxTimestamp;
        maxTimestamp = timestamp;
    }

    public void updatePartitionSizeAndTxnByRawIndex(int index, long partitionSize) {
        recordStructureVersion++;
        updatePartitionSizeByRawIndex(index, partitionSize);
        // New partition version is written, reset the squash counter.
        setPartitionSquashCounterByRawIndex(index, (short) 0);
        attachedPartitions.set(index + PARTITION_NAME_TX_OFFSET, txn);
    }

    public void updatePartitionSizeByRawIndex(int partitionIndex, long partitionTimestampLo, long rowCount) {
        updateAttachedPartitionSizeByRawIndex(partitionIndex, partitionTimestampLo, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, txn - 1);
    }

    public void updatePartitionSizeByTimestamp(long timestamp, long rowCount, long partitionNameTxn) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, rowCount, partitionNameTxn);
    }

    /**
     * Plan 3 Task 4: {@code cellKey}-aware counterpart of {@link #updatePartitionSizeByTimestamp(long, long)}.
     * The plain overload is a thin {@code cellKey = 0} delegate to {@link #updateAttachedPartitionSizeByTimestamp(long, long, long)}
     * (unchanged); this one resolves via {@code (ts, cellKey)} instead, so a size update aimed at one
     * cell cannot silently land on a different cell at the same timestamp.
     * <p>
     * Named {@code updatePartitionSizeByCell} rather than an {@code updatePartitionSizeByTimestamp}
     * overload on purpose: a same-arity {@code (long, int, long)} overload of that name previously
     * collided with the pre-existing {@code updatePartitionSizeByTimestamp(long, long, long)}
     * (rowCount, partitionNameTxn) overload, differing only by the primitive type of the middle
     * parameter. Per JLS 15.12.2.5 "most specific method," a 3-arg call whose middle argument is
     * statically {@code int} (e.g. an int literal, as in {@code TableWriter.processWalCommit}'s
     * {@code updatePartitionSizeByTimestamp(o3TimestampMin, 0, txWriter.getTxn() - 1)}) silently
     * rebound from the intended (rowCount, partitionNameTxn) overload to this one -- turning
     * "create the artificial empty-table partition with 0 rows" into "...with getTxn()-1 rows"
     * whenever {@code getTxn() != 1}. Renaming removes the collision outright rather than patching
     * the one call site, so no future caller can rediscover the same landmine.
     */
    public void updatePartitionSizeByCell(long timestamp, int cellKey, long rowCount) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, cellKey, rowCount, txn - 1);
    }

    /**
     * As {@link #updatePartitionSizeByCell(long, int, long)}, but with an explicit partition name-txn.
     * ATTACH needs {@code -1} here: a composite day CONTAINER carries no {@code .nameTxn} suffix, because
     * composite versions are per CELL inside the container. Taking the default {@code txn - 1} would name
     * a directory that does not exist.
     */
    public void updatePartitionSizeByCell(long timestamp, int cellKey, long rowCount, long partitionNameTxn) {
        recordStructureVersion++;
        updateAttachedPartitionSizeByTimestamp(timestamp, cellKey, rowCount, partitionNameTxn);
    }

    private static long updatePartitionFlagAt(long maskedSize, boolean flag, int bitOffset) {
        if (flag) {
            maskedSize |= 1L << bitOffset;
        } else {
            maskedSize &= ~(1L << bitOffset);
        }
        return maskedSize;
    }

    private static long updatePartitionHasParquetFormat(long maskedSize, boolean isParquetFormat) {
        return updatePartitionFlagAt(maskedSize, isParquetFormat, PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET);
    }

    private static long updatePartitionHasParquetGenerated(long maskedSize, boolean parquetGenerated) {
        return updatePartitionFlagAt(maskedSize, parquetGenerated, PARTITION_MASK_PARQUET_GENERATED_BIT_OFFSET);
    }

    private static long updatePartitionIsReadOnly(long maskedSize, boolean isReadOnly) {
        return updatePartitionFlagAt(maskedSize, isReadOnly, PARTITION_MASK_READ_ONLY_BIT_OFFSET);
    }

    private int calculateWriteOffset(int areaSize) {
        boolean currentIsA = (baseVersion & 1L) == 0L;
        int currentOffset = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_A_32) : txMemBase.getInt(TX_BASE_OFFSET_B_32);
        if (TX_BASE_HEADER_SIZE + areaSize <= currentOffset) {
            return TX_BASE_HEADER_SIZE;
        }
        int currentSizeSymbols = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_A_32) : txMemBase.getInt(TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
        int currentSizePartitions = currentIsA ? txMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_A_32) : txMemBase.getInt(TX_BASE_OFFSET_PARTITIONS_SIZE_B_32);
        int currentSize = calculateTxRecordSize(currentSizeSymbols, currentSizePartitions);
        return currentOffset + currentSize;
    }

    private int calculateWriteSize() {
        // If by any action data is reset and table is partitioned, clear attachedPartitions
        if (maxTimestamp == Long.MIN_VALUE && PartitionBy.isPartitioned(partitionBy)) {
            attachedPartitions.clear();
        }
        return calculateTxRecordSize(symbolColumnCount * Long.BYTES, attachedPartitions.size() * Long.BYTES);
    }

    private void commitFullRecord(int commitMode, ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        symbolColumnCount = symbolCountProviders.size();

        writeAreaSize = calculateWriteSize();
        writeBaseOffset = calculateWriteOffset(writeAreaSize);
        putLong(TX_OFFSET_TXN_64, ++txn);
        putLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64, transientRowCount);
        putLong(TX_OFFSET_FIXED_ROW_COUNT_64, fixedRowCount);
        putLong(TX_OFFSET_MIN_TIMESTAMP_64, minTimestamp);
        putLong(TX_OFFSET_MAX_TIMESTAMP_64, maxTimestamp);
        putLong(TX_OFFSET_STRUCT_VERSION_64, structureVersion);
        putLong(TX_OFFSET_DATA_VERSION_64, dataVersion);
        putLong(TX_OFFSET_PARTITION_TABLE_VERSION_64, partitionTableVersion);
        putLong(TX_OFFSET_COLUMN_VERSION_64, columnVersion);
        putLong(TX_OFFSET_TRUNCATE_VERSION_64, truncateVersion);
        putLong(TX_OFFSET_SEQ_TXN_64, seqTxn);
        putLagValues();
        putInt(TX_OFFSET_MAP_WRITER_COUNT_32, symbolColumnCount);
        putInt(TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnCount));

        // store symbol counts
        storeSymbolCounts(symbolCountProviders);

        // store attached partitions
        txPartitionCount = 1;
        saveAttachedPartitionsToTx(symbolColumnCount);
        finishABHeader(writeBaseOffset, symbolColumnCount * Long.BYTES, attachedPartitions.size() * Long.BYTES, commitMode);

        prevTransientRowCount = transientRowCount;
        prevMinTimestamp = minTimestamp;
        prevMaxTimestamp = maxTimestamp;
        lastSealedPartitionMaxTimestamp = Long.MIN_VALUE;
        prevLastSealedPartitionMaxTimestamp = Long.MIN_VALUE;

        prevRecordStructureVersion = lastRecordStructureVersion;
        lastRecordStructureVersion = recordStructureVersion;
        prevRecordBaseOffset = lastRecordBaseOffset;
        lastRecordBaseOffset = writeBaseOffset;
        prevPartitionTableVersion = partitionTableVersion;
    }

    private void finishABHeader(int areaOffset, int bytesSymbols, int bytesPartitions, int commitMode) {
        boolean currentIsA = (baseVersion & 1) == 0;

        // When current is A, write to B
        long offsetOffset = currentIsA ? TX_BASE_OFFSET_B_32 : TX_BASE_OFFSET_A_32;
        long symbolSizeOffset = currentIsA ? TX_BASE_OFFSET_SYMBOLS_SIZE_B_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_A_32;
        long partitionsSizeOffset = currentIsA ? TX_BASE_OFFSET_PARTITIONS_SIZE_B_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_A_32;

        txMemBase.putInt(offsetOffset, areaOffset);
        txMemBase.putInt(symbolSizeOffset, bytesSymbols);
        txMemBase.putInt(partitionsSizeOffset, bytesPartitions);
        // Plan 3b Task 1: self-describing partition-stride marker -- a GLOBAL property (not part of
        // either A/B section), so it is written at the same fixed offset on every commit regardless of
        // currentIsA, derived from this writer's own (already-correct, metadata-derived) stride.
        txMemBase.putInt(TX_BASE_OFFSET_PARTITION_STRIDE_32, partitionStrideMarker(longsPerAttachedPartition));

        Unsafe.storeFence();
        txMemBase.putLong(TX_BASE_OFFSET_VERSION_64, ++baseVersion);

        readRecordSize = calculateTxRecordSize(bytesSymbols, bytesPartitions);
        readBaseOffset = areaOffset;

        assert readBaseOffset + readRecordSize <= txMemBase.size();
        super.switchRecord(readBaseOffset, readRecordSize);

        if (commitMode != CommitMode.NOSYNC) {
            txMemBase.sync(commitMode == CommitMode.ASYNC);
        }
    }

    private long getLong(long offset) {
        assert offset + 8 <= readRecordSize;
        return txMemBase.getLong(readBaseOffset + offset);
    }

    private void insertPartitionSizeByTimestamp(int index, long partitionTimestamp, long partitionSize, long partitionNameTxn, int cellKey) {
        int size = attachedPartitions.size();
        attachedPartitions.setPos(size + longsPerAttachedPartition);
        if (index < size) {
            // insert in the middle
            attachedPartitions.arrayCopy(index, index + longsPerAttachedPartition, size - index);
            partitionTableVersion++;
        } else if (extensionListener != null) {
            extensionListener.onTableExtended(partitionTimestamp);
        }
        recordStructureVersion++;
        initPartitionAt(index, partitionTimestamp, partitionSize, partitionNameTxn, cellKey);
    }

    private void openTxnFile(FilesFacade ff, LPSZ path) {
        if (ff.exists(path)) {
            if (txMemBase == null) {
                txMemBase = Vm.getSmallCMARWInstance(ff, path, MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            } else {
                txMemBase.of(ff, path, ff.getPageSize(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
            }
            return;
        }
        throw CairoException.critical(ff.errno()).put("Cannot append. File does not exist: ").put(path);
    }

    private void putInt(long offset, int value) {
        assert offset + Integer.BYTES <= writeAreaSize;
        txMemBase.putInt(writeBaseOffset + offset, value);
    }

    private void putLagValues() {
        putLong(TX_OFFSET_LAG_MIN_TIMESTAMP_64, lagMinTimestamp);
        putLong(TX_OFFSET_LAG_MAX_TIMESTAMP_64, lagMaxTimestamp);
        putInt(TX_OFFSET_LAG_ROW_COUNT_32, lagRowCount);
        int lagTxnRaw = lagOrdered ? lagTxnCount : -lagTxnCount;
        putInt(TX_OFFSET_LAG_TXN_COUNT_32, lagTxnRaw);
        putInt(TX_OFFSET_CHECKSUM_32, calculateTxnLagChecksum(txn, seqTxn, lagRowCount, lagMinTimestamp, lagMaxTimestamp, lagTxnRaw));
    }

    private void putLong(long offset, long value) {
        txMemBase.putLong(writeBaseOffset + offset, value);
    }

    private void saveAttachedPartitionsToTx(int symbolColumnCount) {
        final int size = attachedPartitions.size();
        final long partitionTableOffset = getPartitionTableSizeOffset(symbolColumnCount);
        putInt(partitionTableOffset, size * Long.BYTES);
        // change partition count only when we have something to save to the partition table
        if (maxTimestamp != Long.MIN_VALUE) {
            for (int i = 0; i < size; i++) {
                putLong(getPartitionTableIndexOffset(partitionTableOffset, i), attachedPartitions.getQuick(i));
            }
        }
    }

    private void setPartitionFormat(long timestamp, boolean isParquetFormat, long version) {
        int indexRaw = findAttachedPartitionRawIndex(timestamp);
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        setPartitionFormatByRawIndex(indexRaw, isParquetFormat, version);
    }

    /**
     * Raw-index counterpart of {@link #setPartitionFormat(long, boolean, long)}, for per-cell parquet on
     * a composite table.
     * <p>
     * The timestamp-keyed form resolves through {@code findAttachedPartitionRawIndex}, which answers for
     * cellKey 0, so on a composite day it would flip the format of the FIRST cell no matter which one
     * was meant. A raw index names exactly one {@code (ts, cellKey)} record -- and a cell IS a partition
     * record, which is what lets a day hold a mix of native and parquet cells.
     * <p>
     * Behaviour-preserving: the body below is the original one, unchanged, and {@code cellKey == 0}
     * resolves to the same raw index the timestamp-keyed form would have found.
     */
    public void setPartitionFormatByRawIndex(int indexRaw, boolean isParquetFormat, long version) {
        if (indexRaw < 0) {
            throw CairoException.nonCritical().put("bad partition index -1");
        }
        int offset = indexRaw + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);

        maskedSize = updatePartitionHasParquetFormat(maskedSize, isParquetFormat);

        attachedPartitions.setQuick(offset, maskedSize);

        long flags = getPartitionOffset3(indexRaw) & PARTITION_VERSION_FLAGS_MASK & ~PARTITION_SEQ_TXN_VALID_BIT;
        if (!isParquetFormat && version > 0) {
            flags |= PARTITION_SEQ_TXN_VALID_BIT;
        }
        attachedPartitions.setQuick(indexRaw + PARTITION_VERSION_OFFSET, (version & PARTITION_VERSION_VALUE_MASK) | flags);
    }

    private void setPartitionSquashCounterByRawIndex(int partitionRawIndex, short partitionSquashCounter) {
        int rawIndex = partitionRawIndex + PARTITION_MASKED_SIZE_OFFSET;
        long partitionSizeMasked = attachedPartitions.getQuick(rawIndex);
        // Clear the existing squash counter bits
        partitionSizeMasked &= ~PARTITION_SQUASH_COUNTER_MASK;
        // Set the new squash counter value
        partitionSizeMasked |= ((long) (partitionSquashCounter & PARTITION_SQUASH_COUNTER_MAX) << PARTITION_SQUASH_COUNTER_BIT_OFFSET);
        attachedPartitions.setQuick(rawIndex, partitionSizeMasked);
    }

    private void storeSymbolCounts(ObjList<? extends SymbolCountProvider> symbolCountProviders) {
        for (int i = 0, n = symbolCountProviders.size(); i < n; i++) {
            long offset = getSymbolWriterIndexOffset(i);
            int symCount = symbolCountProviders.getQuick(i).getSymbolCount();
            putInt(offset, symCount);
            offset += Integer.BYTES;
            putInt(offset, symCount);
        }
    }

    private void updateAttachedPartitionSizeByTimestamp(long timestamp, long partitionSize, long partitionNameTxn) {
        // Plain wrapper: byte-identical to calling the cellKey-aware overload below with cellKey 0.
        updateAttachedPartitionSizeByTimestamp(timestamp, 0, partitionSize, partitionNameTxn);
    }

    private void updateAttachedPartitionSizeByTimestamp(long timestamp, int cellKey, long partitionSize, long partitionNameTxn) {
        final long partitionTimestampLo = getPartitionTimestampByTimestamp(timestamp);
        int indexRaw = findAttachedPartitionRawIndexBy(partitionTimestampLo, cellKey);
        updateAttachedPartitionSizeByRawIndex(indexRaw, partitionTimestampLo, partitionSize, partitionNameTxn, cellKey);
    }

    private void updatePartitionSizeByRawIndex(int index, long partitionSize) {
        int offset = index + PARTITION_MASKED_SIZE_OFFSET;
        long maskedSize = attachedPartitions.getQuick(offset);
        if ((maskedSize & PARTITION_SIZE_MASK) != partitionSize) {
            attachedPartitions.setQuick(offset, (maskedSize & PARTITION_FLAGS_MASK) | (partitionSize));
            recordStructureVersion++;
        }
    }

    private void writeTransientSymbolCount(int symbolIndex, int symCount) {
        // This updates into current record
        long recordOffset = getSymbolWriterTransientIndexOffset(symbolIndex);
        assert recordOffset + Integer.BYTES <= readRecordSize;
        txMemBase.putInt(readBaseOffset + recordOffset, symCount);
    }

    // It is possible that O3 commit will create partition just before
    // the last one, leaving last partition row count 0 when doing ic().
    // That's when the data from the last partition is moved to in-memory lag.
    // One way to detect this is to check if index of the "last" partition is not
    // last partition in the attached partition list.
    void reconcileOptimisticPartitions() {
        int lastPartitionTsIndex = attachedPartitions.size() - longsPerAttachedPartition + PARTITION_TS_OFFSET;
        if (lastPartitionTsIndex > 0 && maxTimestamp < attachedPartitions.getQuick(lastPartitionTsIndex)) {
            int maxTimestampPartitionIndex = getPartitionIndex(getLastPartitionTimestamp());
            if (maxTimestampPartitionIndex < getPartitionCount() - 1) {
                // accumulate value, which we have to subtract
                // from fixedRowCount (total count of rows of non-active partitions)
                long rowCount = 0;
                for (int i = maxTimestampPartitionIndex, n = getPartitionCount() - 1; i < n; i++) {
                    rowCount += getPartitionSize(i);
                }
                attachedPartitions.setPos((maxTimestampPartitionIndex + 1) * longsPerAttachedPartition);
                recordStructureVersion++;

                // remove partitions
                this.fixedRowCount -= rowCount;
                this.transientRowCount = getPartitionSize(maxTimestampPartitionIndex);
            }
        }
    }

    void resetToLastPartition(long committedTransientRowCount, long newMaxTimestamp) {
        recordStructureVersion++;
        updatePartitionSizeByTimestamp(maxTimestamp, committedTransientRowCount);
        prevMaxTimestamp = newMaxTimestamp;
        maxTimestamp = prevMaxTimestamp;
        transientRowCount = committedTransientRowCount;
    }

    void resetToLastPartition(long committedTransientRowCount) {
        resetToLastPartition(
                committedTransientRowCount,
                Math.max(getLong(TX_OFFSET_MAX_TIMESTAMP_64), lastSealedPartitionMaxTimestamp)
        );
    }

    long unsafeCommittedFixedRowCount() {
        return getLong(TX_OFFSET_FIXED_ROW_COUNT_64);
    }

    long unsafeCommittedTransientRowCount() {
        return getLong(TX_OFFSET_TRANSIENT_ROW_COUNT_64);
    }
}
