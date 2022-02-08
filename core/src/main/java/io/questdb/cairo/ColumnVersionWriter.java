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
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public class ColumnVersionWriter implements Closeable {
    public static final int OFFSET_VERSION_64 = 0;
    public static final int OFFSET_OFFSET_A_64 = OFFSET_VERSION_64 + 8;
    public static final int OFFSET_SIZE_A_64 = OFFSET_OFFSET_A_64 + 8;
    public static final int OFFSET_OFFSET_B_64 = OFFSET_SIZE_A_64 + 8;
    public static final int OFFSET_SIZE_B_64 = OFFSET_OFFSET_B_64 + 8;
    public static final int HEADER_SIZE = OFFSET_SIZE_B_64 + 8;
    public static final int BLOCK_SIZE = 8;
    public static final int BLOCK_SIZE_BYTES = BLOCK_SIZE * Long.BYTES;
    public static final int BLOCK_SIZE_MSB = Numbers.msb(BLOCK_SIZE);
    private final MemoryMARW mem;
    private final LongList cachedList = new LongList();
    private long version;
    private long size;
    private long transientOffset;
    private long transientSize;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = Vm.getCMARWInstance(ff, fileName, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_READER);
        this.size = this.mem.size();
        this.version = this.size < OFFSET_VERSION_64 + 8 ? 0 : mem.getLong(OFFSET_VERSION_64);
    }

    @Override
    public void close() {
        mem.close(false);
    }

    public void commit() {
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

    public long getOffset() {
        return transientOffset;
    }

    public long getOffsetA() {
        return mem.getLong(OFFSET_OFFSET_A_64);
    }

    public long getOffsetB() {
        return mem.getLong(OFFSET_OFFSET_B_64);
    }

    public long getSize() {
        return transientSize;
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
        if (HEADER_SIZE + areaSize <= currentOffset) {
            return HEADER_SIZE;
        }
        long currentSize = currentIsA ? getSizeA() : getSizeB();
        return currentOffset + currentSize;
    }

    /**
     * Adds or updates column version entry in the cached list. Entries from the cache are committed to disk via
     * commit() call. In cache and on disk entries are maintained in ascending chronological order of partition
     * timestamps and ascending column index order within each timestamp.
     *
     * @param timestamp   partition timestamp
     * @param columnIndex column index
     * @param txn         column version.
     */
    public void upsert(long timestamp, int columnIndex, long txn) {
        final int sz = cachedList.size();
        int index = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, BinarySearch.SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            // brute force columns for this timestamp
            while (index < sz && cachedList.getQuick(index) == timestamp) {
                final long thisIndex = cachedList.getQuick(index + 1);

                if (thisIndex == columnIndex) {
                    cachedList.setQuick(index + 2, txn);
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
        }
    }

    private void bumpFileSize(long size) {
        mem.setSize(size);
        this.size = size;
    }

    private long getSizeA() {
        return mem.getLong(OFFSET_SIZE_A_64);
    }

    private long getSizeB() {
        return mem.getLong(OFFSET_SIZE_B_64);
    }

    private boolean isCurrentA() {
        return version % 2 == 0;
    }

    LongList getCachedList() {
        return cachedList;
    }

    private void store(int entryCount, long offset) {
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
        this.transientOffset = aOffset;
        this.transientSize = aSize;
    }

    private void updateB(long bOffset, long bSize) {
        mem.putLong(OFFSET_OFFSET_B_64, bOffset);
        mem.putLong(OFFSET_SIZE_B_64, bSize);
        this.transientOffset = bOffset;
        this.transientSize = bSize;
    }
}
