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
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;

public class ColumnVersionWriter extends ColumnVersionReader {
    private final MemoryCMARW mem;
    private long version;
    private long size;
    private boolean hasChanges;

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
    public void close() {
        mem.close(false);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getVersion() {
        return version;
    }

    public void commit() {
        if (!hasChanges) {
            return;
        }
        doCommit();
        hasChanges = false;
    }

    public long getOffsetA() {
        return mem.getLong(OFFSET_OFFSET_A_64);
    }

    public long getOffsetB() {
        return mem.getLong(OFFSET_OFFSET_B_64);
    }

    public boolean hasChanges() {
        return hasChanges;
    }

    public void removeColumnTop(long partitionTimestamp, int columnIndex) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex >= 0) {
            getCachedList().setQuick(recordIndex + 3, 0);
            hasChanges = true;
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
        LongList cachedList = getCachedList();
        final int sz = cachedList.size();
        int index = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, BinarySearch.SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            // brute force columns for this timestamp
            while (index < sz && cachedList.getQuick(index) == timestamp) {
                final long thisIndex = cachedList.getQuick(index + 1);

                if (thisIndex == columnIndex) {
                    if (txn > -1) {
                        cachedList.setQuick(index + 2, txn);
                    }
                    cachedList.setQuick(index + 3, columnTop);
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
            cachedList.setQuick(index + 1, columnIndex);
            cachedList.setQuick(index + 2, txn);
            cachedList.setQuick(index + 3, columnTop);
        }
        hasChanges = true;
    }

    public void upsertColumnTop(long partitionTimestamp, int columnIndex, long colTop) {
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        LongList cachedList = getCachedList();
        if (recordIndex > -1L) {
            cachedList.setQuick(recordIndex + 3, colTop);
            hasChanges = true;
        } else {
            // This is a 0 column top record we need to store it
            // to mark that the column is written in O3 even before the partition the column was originally added
            int defaultRecordIndex = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
            if (defaultRecordIndex >= 0) {
                long columnNameTxn = cachedList.getQuick(defaultRecordIndex + 2);
                long defaultPartitionTimestamp = cachedList.getQuick(defaultRecordIndex + 3);
                // Do not add 0 column top if the default parttion
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
        LongList cachedList = getCachedList();
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
        LongList cachedList = getCachedList();
        for (int i = 0; i < entryCount; i++) {
            int x = i * BLOCK_SIZE;
            mem.putLong(offset, cachedList.getQuick(x));
            mem.putLong(offset + 8, cachedList.getQuick(x + 1));
            mem.putLong(offset + 16, cachedList.getQuick(x + 2));
            mem.putLong(offset + 24, cachedList.getQuick(x + 3));
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

    static {
        assert HEADER_SIZE == TableUtils.COLUMN_VERSION_FILE_HEADER_SIZE;
    }
}
