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
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.LPSZ;

public class ColumnVersionWriter extends ColumnVersionReader {
    private final CairoConfiguration configuration;
    private final MemoryCMARW mem;
    private final boolean partitioned;
    private boolean hasChanges;
    private long size;
    private long version;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(CairoConfiguration configuration, LPSZ fileName, boolean partitioned) {
        final FilesFacade ff = configuration.getFilesFacade();
        this.mem = Vm.getCMARWInstance(ff, fileName, ff.getPageSize(), 0, MemoryTag.MMAP_TABLE_READER, CairoConfiguration.O_NONE);
        this.configuration = configuration;
        this.partitioned = partitioned;
        this.size = this.mem.size();
        super.ofRO(mem);
        if (this.size > 0) {
            this.version = super.readUnsafe();
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        mem.close(false);
    }

    public void commit() {
        if (!hasChanges) {
            return;
        }
        doCommit();
        hasChanges = false;
    }

    public void copyColumnVersions(long srcTimestamp, long dstTimestamp) {
        int index = copyColumnVersions(srcTimestamp, dstTimestamp, cachedColumnVersionList);
        if (index > -1) {
            for (int n = cachedColumnVersionList.size(); index < n; index += BLOCK_SIZE) {
                if (cachedColumnVersionList.get(index) == srcTimestamp) {
                    cachedColumnVersionList.setQuick(index, dstTimestamp);
                } else {
                    break;
                }
            }
        }
    }

    public long getOffsetA() {
        return mem.getLong(OFFSET_OFFSET_A_64);
    }

    public long getOffsetB() {
        return mem.getLong(OFFSET_OFFSET_B_64);
    }

    @Override
    public long getVersion() {
        return version;
    }

    public boolean hasChanges() {
        return hasChanges;
    }

    public void overrideColumnVersions(long partitionTimestamp, ColumnVersionReader src) {
        copyColumnVersions(partitionTimestamp, partitionTimestamp, src.cachedColumnVersionList);
    }

    @Override
    public long readUnsafe() {
        this.hasChanges = false;
        return this.version = super.readUnsafe();
    }

    public void removeColumnTop(long partitionTimestamp, int columnIndex) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex >= 0) {
            cachedColumnVersionList.setQuick(recordIndex + COLUMN_TOP_OFFSET, 0);
            hasChanges = true;
        }
    }

    /**
     * Plan 4b Task 2: {@code cellKey}-aware counterpart of {@link #removeColumnTop(long, int)}, used by
     * {@code TableWriter#updateO3ColumnTops}'s partition-split branch so a split of one cell can never
     * zero out a DIFFERENT cell's column top sharing the same timestamp. {@code cellKey == 0} is
     * byte-identical to the 2-arg overload.
     */
    public void removeColumnTop(long partitionTimestamp, int cellKey, int columnIndex) {
        int recordIndex = getRecordIndex(partitionTimestamp, cellKey, columnIndex);
        if (recordIndex >= 0) {
            cachedColumnVersionList.setQuick(recordIndex + COLUMN_TOP_OFFSET, 0);
            hasChanges = true;
        }
    }

    public void removePartition(long partitionTimestamp) {
        removePartition(partitionTimestamp, 0);
    }

    /**
     * Plan 3 Task 5: {@code cellKey}-aware counterpart of {@link #removePartition(long)}. Within the
     * {@code partitionTimestamp} block, records are ordered by the packed {@code (cellKey,
     * columnIndex)} key -- cellKey occupies the HIGH bits, so every row belonging to one cell forms a
     * single contiguous sub-run inside that ts block. This removes exactly that sub-run, not the whole
     * timestamp block.
     * <p>
     * The plain/dormant {@link #removePartition(long)} overload delegates here with {@code cellKey =
     * 0}; for a plain (or dormant) table every row at a timestamp has cellKey 0, so it removes the
     * identical row set as the pre-composite-partitioning code -- the byte-identity guarantee for the
     * remove path.
     */
    public void removePartition(long partitionTimestamp, int cellKey) {
        int from = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (from > -1) {
            final int sz = cachedColumnVersionList.size();

            // Find the start of this cellKey's contiguous sub-run (first record whose cellKey >= target).
            int start = from;
            while (start < sz
                    && cachedColumnVersionList.getQuick(start) == partitionTimestamp
                    && unpackCellKey(cachedColumnVersionList.getQuick(start + COLUMN_INDEX_OFFSET)) < cellKey) {
                start += BLOCK_SIZE;
            }

            // Find the end of the sub-run (first record after start whose cellKey != target).
            int end = start;
            while (end < sz
                    && cachedColumnVersionList.getQuick(end) == partitionTimestamp
                    && unpackCellKey(cachedColumnVersionList.getQuick(end + COLUMN_INDEX_OFFSET)) == cellKey) {
                end += BLOCK_SIZE;
            }

            if (end > start) {
                cachedColumnVersionList.removeIndexBlock(start, end - start);
                hasChanges = true;
            }
        }
    }

    /**
     * Removes every row at {@code partitionTimestamp} regardless of cellKey -- i.e. the whole
     * timestamp block, which is what the public {@link #removePartition(long)} overload did before
     * Plan 3 Task 5 narrowed it to a single cell. Used internally by {@link #copyColumnVersions(long,
     * long)} and {@link #squashPartition(long, long)}, both of which relocate or discard an entire
     * timestamp's record family -- potentially spanning multiple cells -- as one atomic unit, not a
     * single cell's rows. Deliberately NOT delegated to {@link #removePartition(long, int)} for any
     * fixed cellKey: doing so would silently leave other cells' stale records behind once multi-cell
     * routing activates (Plan 4), even though it is a no-op difference today (Plan 3 dormancy: only
     * cellKey 0 ever exists in production).
     */
    private void removeAllCellsAtTimestamp(long partitionTimestamp) {
        int from = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (from > -1) {
            int to = cachedColumnVersionList.binarySearchBlock(from, BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_DOWN);
            int len = to - from + BLOCK_SIZE;
            cachedColumnVersionList.removeIndexBlock(from, len);
            hasChanges = true;
        }
    }

    public void replaceInitialPartitionRecords(long lastPartitionTimestamp, long transientRowCount) {
        // Remove all default partitions that point beyond the last partition
        // Replace them as if the column was added to the last partition
        for (int i = 0, n = cachedColumnVersionList.size(); i < n; i += BLOCK_SIZE) {
            long partitionTimestamp = cachedColumnVersionList.getQuick(i);
            long initialPartitionTimestamp = cachedColumnVersionList.get(i + TIMESTAMP_ADDED_PARTITION_OFFSET);
            int columnIndex = (int) cachedColumnVersionList.get(i + COLUMN_INDEX_OFFSET);
            long columnNameTxn = cachedColumnVersionList.getQuick(i + COLUMN_NAME_TXN_OFFSET);

            if (partitionTimestamp == COL_TOP_DEFAULT_PARTITION) {
                if (initialPartitionTimestamp > lastPartitionTimestamp) {
                    // There is an initial partition record that point beyond the last partition.
                    // This can happen as a resul of partition drop.
                    // Move the initial partition record to the last partition, as if column was added there.
                    cachedColumnVersionList.set(i + TIMESTAMP_ADDED_PARTITION_OFFSET, lastPartitionTimestamp);
                    // Because column we not really added there, put the column top to the value
                    // of the last partition row count (e.g. transientRowCount)
                    // Add the column top if there is no explicit record for this column, if there is one
                    // keep the existing value.
                    int recordIndex = getRecordIndex(lastPartitionTimestamp, columnIndex);
                    if (recordIndex < 0) {
                        upsert(lastPartitionTimestamp, columnIndex, columnNameTxn, transientRowCount);
                    }
                }
            } else {
                break;
            }
        }
    }

    /*
     * Rollback to previous version that is stored before the current version in the file.
     * This function cannot be called multiple times in a row without commit() in between because
     * only one previous version is stored.
     */
    public void rollback() {
        mem.putLong(OFFSET_VERSION_64, version - 1);
        // load version and other fields from mem
        readUnsafe();
    }

    public void squashPartition(long targetPartitionTimestamp, long sourcePartitionTimestamp) {
        // Discards the entire source partition (every cell, not just cellKey 0) -- see
        // removeAllCellsAtTimestamp's javadoc.
        removeAllCellsAtTimestamp(sourcePartitionTimestamp);
        // Remove all default partitions that point to the targetPartitionTimestamp
        for (int i = 0, n = cachedColumnVersionList.size(); i < n; i += BLOCK_SIZE) {
            long partitionTimestamp = cachedColumnVersionList.getQuick(i);
            long defaultPartitionTimestamp = cachedColumnVersionList.get(i + TIMESTAMP_ADDED_PARTITION_OFFSET);

            if (partitionTimestamp == COL_TOP_DEFAULT_PARTITION) {
                if (defaultPartitionTimestamp == sourcePartitionTimestamp) {
                    // replace with target block
                    cachedColumnVersionList.set(i + TIMESTAMP_ADDED_PARTITION_OFFSET, targetPartitionTimestamp);
                }
            } else {
                break;
            }
        }
    }

    public void truncate() {
        if (cachedColumnVersionList.size() > 0) {

            int from = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, SYMBOL_TABLE_VERSION_PARTITION + 1, Vect.BIN_SEARCH_SCAN_UP);
            if (from < 0) {
                from = -from - 1;
            }

            if (partitioned) {
                // Remove all partitions after SYMBOL_TABLE_VERSION_PARTITION
                if (from < cachedColumnVersionList.size()) {
                    cachedColumnVersionList.setPos(from);
                }
                // Keep default column version but reset the added timestamp to min
                for (int i = 0, n = cachedColumnVersionList.size(); i < n; i += BLOCK_SIZE) {
                    cachedColumnVersionList.setQuick(i + TIMESTAMP_ADDED_PARTITION_OFFSET, SYMBOL_TABLE_VERSION_PARTITION);
                }
            } else {
                // We have to keep all the column name txns because the files are truncated but not re-created.
                // But we want to remove all the column tops.
                // The column name txn can be added when the column is added via alter table or when column is updated.
                // When ALTER table add column is executed it creates a record in the NaN partition with the column name txn
                // and a record in 0 (default) partition with the column top.
                // When the column is changed using UPDATE SQL, the column name txn is only set in 0 (default) partition.
                // These 2 scenarios are test covered in TruncateTest.

                // Result action is to remove all column tops and keep all column name txns.
                for (int i = from; i < cachedColumnVersionList.size(); i += BLOCK_SIZE) {
                    cachedColumnVersionList.setQuick(i + COLUMN_TOP_OFFSET, 0);
                }
            }

            hasChanges = true;
            commit();
        }
    }

    /**
     * Adds or updates column version entry in the cached list. Entries from the cache are committed to disk via
     * commit() call. In cache and on disk entries are maintained in ascending chronological order of partition
     * timestamps and ascending column index order within each timestamp.
     *
     * @param timestamp   partition timestamp
     * @param columnIndex column index
     * @param txn         column file txn name
     * @param columnTop   column top
     */
    public void upsert(long timestamp, int columnIndex, long txn, long columnTop) {
        upsert(timestamp, 0, columnIndex, txn, columnTop);
    }

    /**
     * Plan 3 Task 5: {@code cellKey}-aware counterpart of {@link #upsert(long, int, long, long)}.
     * Finds or inserts by the FULL packed {@code (cellKey, columnIndex)} key, and inserts in packed
     * order so {@link #getRecordIndex(long, int, int)}'s binary-search-then-scan invariant continues
     * to hold. For a plain table ({@code cellKey == 0}) the packed key equals the bare column index,
     * so insertion order -- and the on-disk bytes -- are unchanged from the pre-composite-partitioning
     * layout.
     *
     * @param timestamp   partition timestamp
     * @param cellKey     dense per-(timePartition, dimension-tuple) cell ordinal; 0 for plain/dormant
     * @param columnIndex column index
     * @param txn         column file txn name
     * @param columnTop   column top
     */
    public void upsert(long timestamp, int cellKey, int columnIndex, long txn, long columnTop) {
        final long packedColumnIndex = packColIndex(cellKey, columnIndex);
        final int sz = cachedColumnVersionList.size();
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            // brute force columns for this timestamp
            while (index < sz && cachedColumnVersionList.getQuick(index) == timestamp) {
                final long thisPacked = cachedColumnVersionList.getQuick(index + COLUMN_INDEX_OFFSET);

                if (thisPacked == packedColumnIndex) {
                    if (txn > -1) {
                        cachedColumnVersionList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
                    }
                    cachedColumnVersionList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
                    insert = false;
                    break;
                }

                if (thisPacked > packedColumnIndex) {
                    break;
                }

                index += BLOCK_SIZE;
            }
        } else {
            index = -index - 1;
        }

        if (insert) {
            if (index < sz) {
                cachedColumnVersionList.insert(index, BLOCK_SIZE);
            } else {
                cachedColumnVersionList.setPos(Math.max(index + BLOCK_SIZE, sz + BLOCK_SIZE));
            }
            cachedColumnVersionList.setQuick(index, timestamp);
            cachedColumnVersionList.setQuick(index + COLUMN_INDEX_OFFSET, packedColumnIndex);
            cachedColumnVersionList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
            cachedColumnVersionList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
        }
        hasChanges = true;
    }

    public void upsertColumnTop(long partitionTimestamp, int columnIndex, long colTop) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex > -1L) {
            cachedColumnVersionList.setQuick(recordIndex + COLUMN_TOP_OFFSET, colTop);
            hasChanges = true;
        } else {
            // This is a 0 column top record we need to store it
            // to mark that the column is written in O3 even before the partition the column was originally added
            int defaultRecordIndex = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
            if (defaultRecordIndex >= 0) {
                long columnNameTxn = cachedColumnVersionList.getQuick(defaultRecordIndex + COLUMN_NAME_TXN_OFFSET);
                long defaultPartitionTimestamp = cachedColumnVersionList.getQuick(defaultRecordIndex + TIMESTAMP_ADDED_PARTITION_OFFSET);
                // Do not add 0 column top if the default partition
                if (defaultPartitionTimestamp > partitionTimestamp || colTop > 0) {
                    upsert(partitionTimestamp, columnIndex, columnNameTxn, colTop);
                }
            } else if (colTop > 0) {
                // Store non-zero column tops only, zero is default
                // for columns added on the table creation
                upsert(partitionTimestamp, columnIndex, -1L, colTop);
            }
        }
    }

    /**
     * Plan 4b Task 2 (opus review Critical): {@code cellKey}-aware counterpart of {@link
     * #upsertColumnTop(long, int, long)}. Root cause of the Critical: {@code
     * TableWriter#updateO3ColumnTops} -- the post-O3-commit consumer that turns each dispatched cell's
     * own {@code colTopSinkAddr} scratch value into a durable column-version record -- called the
     * cellKey-BLIND 3-arg overload, whose {@code getRecordIndex(partitionTimestamp, columnIndex)}
     * resolves cellKey 0 implicitly. For a composite table, that silently upserted (or, on the
     * find-existing-record branch, directly IN-PLACE OVERWROTE) whichever cell's record happens to be
     * packed at cellKey 0 -- clobbering a DIFFERENT, untouched sibling cell's legitimate column top with
     * the cell actually being appended/merged this commit's own value, whenever the extended/merged cell
     * itself was NOT cellKey 0. Reproduced directly: extending a non-zero-cellKey cell after {@code ADD
     * COLUMN} corrupted a cellKey-0 sibling's column-version record, making its {@code <col>.d} file
     * unreadable on the next query. {@code cellKey == 0} is byte-identical to the 3-arg overload.
     */
    public void upsertColumnTop(long partitionTimestamp, int cellKey, int columnIndex, long colTop) {
        int recordIndex = getRecordIndex(partitionTimestamp, cellKey, columnIndex);
        if (recordIndex > -1L) {
            cachedColumnVersionList.setQuick(recordIndex + COLUMN_TOP_OFFSET, colTop);
            hasChanges = true;
        } else {
            // This is a 0 column top record we need to store it
            // to mark that the column is written in O3 even before the partition the column was originally added
            int defaultRecordIndex = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
            if (defaultRecordIndex >= 0) {
                long columnNameTxn = cachedColumnVersionList.getQuick(defaultRecordIndex + COLUMN_NAME_TXN_OFFSET);
                long defaultPartitionTimestamp = cachedColumnVersionList.getQuick(defaultRecordIndex + TIMESTAMP_ADDED_PARTITION_OFFSET);
                // Do not add 0 column top if the default partition
                if (defaultPartitionTimestamp > partitionTimestamp || colTop > 0) {
                    upsert(partitionTimestamp, cellKey, columnIndex, columnNameTxn, colTop);
                }
            } else if (colTop > 0) {
                // Store non-zero column tops only, zero is default
                // for columns added on the table creation
                upsert(partitionTimestamp, cellKey, columnIndex, -1L, colTop);
            }
        }
    }

    public void upsertDefaultTxnName(int columnIndex, long columnNameTxn, long partitionTimestamp) {
        // When table is partitioned, use columnTop place to store the timestamp of the partition where the column added
        upsert(COL_TOP_DEFAULT_PARTITION, columnIndex, columnNameTxn, partitionTimestamp);
    }

    public void upsertSymbolTableTxnName(int columnIndex, long columnNameTxn) {
        // Store the column name txn for the symbol-table, this txn can increase independently of column name txn in
        // partitions, when symbol capacity is changed; columnTop is unused here and set to 0.
        upsert(SYMBOL_TABLE_VERSION_PARTITION, columnIndex, columnNameTxn, 0);
    }

    private void bumpFileSize(long size) {
        mem.setSize(size);
        this.size = size;
    }

    private long calculateSize(int entryCount) {
        // calculate the area size required to store the versions
        // we're assuming that 'columnVersions' contains 4 longs per entry
        // We're storing 4 longs per entry in the file
        return (long) entryCount * BLOCK_SIZE_BYTES;
    }

    private long calculateWriteOffset(long areaSize) {
        boolean currentIsA = isCurrentA();
        long currentOffset = currentIsA ? getOffsetA() : getOffsetB();
        currentOffset = Math.max(currentOffset, HEADER_SIZE);
        if (HEADER_SIZE + areaSize <= currentOffset) {
            return HEADER_SIZE;
        }
        long currentSize = currentIsA ? getSizeA() : getSizeB();
        return currentOffset + currentSize;
    }

    private int copyColumnVersions(long srcTimestamp, long dstTimestamp, LongList srcColumnVersionList) {
        int srcIndex = srcColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, srcTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (srcIndex < 0) { // source does not have partition information
            return -1;
        }

        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, dstTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1L) {
            // Wipe out all the information about this partition (every cell, not just cellKey 0) to
            // replace with the new one -- see removeAllCellsAtTimestamp's javadoc.
            removeAllCellsAtTimestamp(dstTimestamp);
            index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, dstTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        }

        if (index < 0) { // the cache does not contain this partition
            index = -index - 1;
            int srcEnd = srcColumnVersionList.binarySearchBlock(srcIndex, BLOCK_SIZE_MSB, srcTimestamp, Vect.BIN_SEARCH_SCAN_DOWN);
            cachedColumnVersionList.insertFromSource(index, srcColumnVersionList, srcIndex, srcEnd + BLOCK_SIZE);
        } else {
            throw CairoException.critical(0).put("invalid Column Version state ").put(dstTimestamp).put(" column version state, cannot update partition information");
        }
        hasChanges = true;
        return index;
    }

    private void doCommit() {
        int entryCount = cachedColumnVersionList.size() / BLOCK_SIZE;
        long areaSize = calculateSize(entryCount);
        long writeOffset = calculateWriteOffset(areaSize);
        bumpFileSize(writeOffset + areaSize);
        store(entryCount, writeOffset);
        if (isCurrentA()) {
            updateB(writeOffset, areaSize);
        } else {
            updateA(writeOffset, areaSize);
        }

        Unsafe.storeFence();
        storeNewVersion();

        final int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            mem.sync(commitMode == CommitMode.ASYNC);
        }
    }

    private long getSizeA() {
        return mem.getLong(OFFSET_SIZE_A_64);
    }

    private long getSizeB() {
        return mem.getLong(OFFSET_SIZE_B_64);
    }

    private boolean isCurrentA() {
        return (version & 1L) == 0L;
    }

    private void store(int entryCount, long offset) {
        for (int i = 0; i < entryCount; i++) {
            int x = i * BLOCK_SIZE;
            mem.putLong(offset, cachedColumnVersionList.getQuick(x));
            mem.putLong(offset + 8, cachedColumnVersionList.getQuick(x + COLUMN_INDEX_OFFSET));
            mem.putLong(offset + 16, cachedColumnVersionList.getQuick(x + COLUMN_NAME_TXN_OFFSET));
            mem.putLong(offset + 24, cachedColumnVersionList.getQuick(x + COLUMN_TOP_OFFSET));
            offset += BLOCK_SIZE * 8;
        }
    }

    private void storeNewVersion() {
        mem.putLong(OFFSET_VERSION_64, ++version);
    }

    private void updateA(long aOffset, long aSize) {
        mem.putLong(OFFSET_OFFSET_A_64, aOffset);
        mem.putLong(OFFSET_SIZE_A_64, aSize);
    }

    private void updateB(long bOffset, long bSize) {
        mem.putLong(OFFSET_OFFSET_B_64, bOffset);
        mem.putLong(OFFSET_SIZE_B_64, bSize);
    }

    static {
        //noinspection ConstantValue
        assert HEADER_SIZE == TableUtils.COLUMN_VERSION_FILE_HEADER_SIZE;
    }
}
