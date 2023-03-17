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
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class ColumnVersionWriter extends ColumnVersionReader {
    private final MemoryCMARW mem;
    private boolean hasChanges;
    private long size;
    private long version;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = Vm.getCMARWInstance(ff, fileName, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_READER, CairoConfiguration.O_NONE);
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

    public void copyPartition(long partitionTimestamp, ColumnVersionReader source) {
        LongList src = source.cachedList;
        LongList dest = this.cachedList;

        int srcIndex = src.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_UP);
        if (srcIndex < 0) { // source does not have partition information
            return;
        }

        int index = dest.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_UP);
        if (index > -1L) {
            // Wipe out all the information about this partition to replace with the new one.
            removePartition(partitionTimestamp);
            index = dest.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_UP);
        }

        if (index < 0) { // the cache does not contain this partition
            index = -index - 1;
            int srcEnd = src.binarySearchBlock(srcIndex, BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_DOWN);
            dest.insertFromSource(index, src, srcIndex, srcEnd + BLOCK_SIZE);
        } else {
            throw CairoException.critical(0)
                    .put("invalid Column Version state ")
                    .ts(partitionTimestamp)
                    .put(" column version state, cannot update partition information");
        }
        hasChanges = true;
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

    public void removeColumnTop(long partitionTimestamp, int columnIndex) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex >= 0) {
            cachedList.setQuick(recordIndex + COLUMN_TOP_OFFSET, 0);
            hasChanges = true;
        }
    }

    public void removePartition(long partitionTimestamp) {
        int from = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_UP);
        if (from > -1) {
            int to = cachedList.binarySearchBlock(from, BLOCK_SIZE_MSB, partitionTimestamp, BinarySearch.SCAN_DOWN);
            int len = to - from + BLOCK_SIZE;
            cachedList.removeIndexBlock(from, len);
            hasChanges = true;
        }
    }

    public void truncate(boolean isPartitioned) {
        if (cachedList.size() > 0) {

            if (isPartitioned) {
                int from = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, COL_TOP_DEFAULT_PARTITION + 1, BinarySearch.SCAN_UP);
                if (from < 0) {
                    from = -from - 1;
                }
                // Remove all partitions after COL_TOP_DEFAULT_PARTITION
                if (from < cachedList.size()) {
                    cachedList.setPos(from);
                }
                // Keep default column version but reset the added timestamp to min
                for (int i = 0, n = cachedList.size(); i < n; i += BLOCK_SIZE) {
                    cachedList.setQuick(i + TIMESTAMP_ADDED_PARTITION_OFFSET, COL_TOP_DEFAULT_PARTITION);
                }
            } else {
                //reset column tops
                for (int i = 3, n = cachedList.size(); i < n; i += 4) {
                    cachedList.setQuick(i, 0);
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
        final int sz = cachedList.size();
        int index = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, BinarySearch.SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            // brute force columns for this timestamp
            while (index < sz && cachedList.getQuick(index) == timestamp) {
                final long thisIndex = cachedList.getQuick(index + COLUMN_INDEX_OFFSET);

                if (thisIndex == columnIndex) {
                    if (txn > -1) {
                        cachedList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
                    }
                    cachedList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
                    insert = false;
                    break;
                }

                if (thisIndex > columnIndex) {
                    break;
                }

                index += BLOCK_SIZE;
            }
        } else {
            index = -index - 1;
        }

        if (insert) {
            if (index < sz) {
                cachedList.insert(index, BLOCK_SIZE);
            } else {
                cachedList.setPos(Math.max(index + BLOCK_SIZE, sz + BLOCK_SIZE));
            }
            cachedList.setQuick(index, timestamp);
            cachedList.setQuick(index + COLUMN_INDEX_OFFSET, columnIndex);
            cachedList.setQuick(index + COLUMN_NAME_TXN_OFFSET, txn);
            cachedList.setQuick(index + COLUMN_TOP_OFFSET, columnTop);
        }
        hasChanges = true;
    }

    public void upsertColumnTop(long partitionTimestamp, int columnIndex, long colTop) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex > -1L) {
            cachedList.setQuick(recordIndex + COLUMN_TOP_OFFSET, colTop);
            hasChanges = true;
        } else {
            // This is a 0 column top record we need to store it
            // to mark that the column is written in O3 even before the partition the column was originally added
            int defaultRecordIndex = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
            if (defaultRecordIndex >= 0) {
                long columnNameTxn = cachedList.getQuick(defaultRecordIndex + COLUMN_NAME_TXN_OFFSET);
                long defaultPartitionTimestamp = cachedList.getQuick(defaultRecordIndex + TIMESTAMP_ADDED_PARTITION_OFFSET);
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

    public void upsertDefaultTxnName(int columnIndex, long columnNameTxn, long partitionTimestamp) {
        // When table is partitioned, use columnTop place to store the timestamp of the partition where the column added
        upsert(COL_TOP_DEFAULT_PARTITION, columnIndex, columnNameTxn, partitionTimestamp);
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

    private void doCommit() {
        int entryCount = cachedList.size() / BLOCK_SIZE;
        long areaSize = calculateSize(entryCount);
        long writeOffset = calculateWriteOffset(areaSize);
        bumpFileSize(writeOffset + areaSize);
        store(entryCount, writeOffset);
        if (isCurrentA()) {
            updateB(writeOffset, areaSize);
        } else {
            updateA(writeOffset, areaSize);
        }

        Unsafe.getUnsafe().storeFence();
        storeNewVersion();
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
            mem.putLong(offset, cachedList.getQuick(x));
            mem.putLong(offset + 8, cachedList.getQuick(x + COLUMN_INDEX_OFFSET));
            mem.putLong(offset + 16, cachedList.getQuick(x + COLUMN_NAME_TXN_OFFSET));
            mem.putLong(offset + 24, cachedList.getQuick(x + COLUMN_TOP_OFFSET));
            offset += BLOCK_SIZE * 8;
        }
    }

    private void storeNewVersion() {
        mem.putLong(OFFSET_VERSION_64, ++this.version);
    }

    private void updateA(long aOffset, long aSize) {
        mem.putLong(OFFSET_OFFSET_A_64, aOffset);
        mem.putLong(OFFSET_SIZE_A_64, aSize);
    }

    private void updateB(long bOffset, long bSize) {
        mem.putLong(OFFSET_OFFSET_B_64, bOffset);
        mem.putLong(OFFSET_SIZE_B_64, bSize);
    }

    @Override
    long readUnsafe() {
        this.hasChanges = false;
        return this.version = super.readUnsafe();
    }

    static {
        assert HEADER_SIZE == TableUtils.COLUMN_VERSION_FILE_HEADER_SIZE;
    }
}
