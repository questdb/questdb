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
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public class ColumnVersionWriter implements Closeable {
    public static final int OFFSET_AREA = 0;
    public static final int OFFSET_OFFSET_A = 8;
    public static final int OFFSET_SIZE_A = 16;
    public static final int OFFSET_OFFSET_B = 24;
    public static final int OFFSET_SIZE_B = 32;
    public static final int BLOCK_SIZE = 4;
    public static final int BLOCK_SIZE_BYTES = BLOCK_SIZE * Long.BYTES;
    public static final int BLOCK_SIZE_MSB = Numbers.msb(BLOCK_SIZE);
    private final MemoryMARW mem;
    private final LongList cachedList = new LongList();
    private long size;
    private long transientOffset;
    private long transientSize;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = Vm.getCMARWInstance(ff, fileName, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_READER, CairoConfiguration.O_NONE);
        this.size = size;
    }

    /**
     * Adds or updates column version entry in the cached list. Entries from the cache are committed to disk via
     * commit() call. In cache and on disk entries are maintained in ascending chronological order of partition
     * timestamps and ascending column index order within each timestamp.
     *
     * @param timestamp     partition timestamp
     * @param columnIndex   column index
     * @param columnVersion column version.
     */
    public void upsert(long timestamp, int columnIndex, long columnVersion) {
        final int sz = cachedList.size();
        int index = cachedList.binarySearchBlock(BLOCK_SIZE_MSB, timestamp, BinarySearch.SCAN_UP);
        boolean insert = true;
        if (index > -1) {
            // brute force columns for this timestamp
            while (index < sz && cachedList.getQuick(index) == timestamp) {
                final long thisIndex = cachedList.getQuick(index + 1);

                if (thisIndex == columnIndex) {
                    cachedList.setQuick(index + 2, columnVersion);
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
            cachedList.setQuick(index + 2, columnVersion);
        }
    }

    @Override
    public void close() {
        mem.close();
    }

    public void commit() {
        // + 8 is the A/B switch at top of the file, it is 8 bytes to keep data aligned
        // + 8 is the offset of A area
        // + 8 is the length of A area
        // + 8 is the offset of B area
        // + 8 is the length of B area
        final long headerSize = 8 + 8 + 8 + 8 + 8;
        // calculate the area size required to store the versions
        // we're assuming that 'columnVersions' contains 4 longs per entry
        final int entryCount = cachedList.size() / BLOCK_SIZE;
        // We're storing 4 longs per entry in the file
        final long areaSize = entryCount * 4 * 8L;

        if (size == 0) {
            // This is initial write, include header size into the resize
            bumpFileSize(headerSize + areaSize);
            // writing A group
            store(entryCount, headerSize);
            // We update transient offset and size here
            // the important values are for area 'A'.
            // This is the reason 'B' is updated first so that
            // 'A' overwrites transient values
            updateB(headerSize + areaSize, 0);
            updateA(headerSize, areaSize);
            switchToA();
            this.size = areaSize + headerSize;
        } else {
            long aOffset = getOffsetA();
            final long aSize = getSizeA();
            long bOffset = getOffsetB();
            final long bSize = getSizeB();

            if (isB()) {
                // we have to write 'A' area, which may not be big enough

                // is area 'A' above 'B' ?
                if (aOffset < bOffset) {
                    if (aSize <= bOffset - headerSize) {
                        aOffset = headerSize;
                    } else {
                        aOffset = bOffset + bSize;
                        bumpFileSize(aOffset + areaSize);
                    }
                }
                store(entryCount, aOffset);
                // update offsets of 'A'
                updateA(aOffset, areaSize);
                // switch to 'A'
                switchToA();
            } else {
                // current is 'A'
                // check if 'A' wound up below 'B'
                if (aOffset > bOffset) {
                    // check if 'B' bits between top and 'A'
                    if (areaSize <= aOffset - headerSize) {
                        bOffset = headerSize;
                    } else {
                        // 'B' does not fit between top and 'A'
                        bOffset = aOffset + aSize;
                        bumpFileSize(bOffset + areaSize);
                    }
                } else {
                    // check if file is big enough
                    if (bSize < areaSize) {
                        bumpFileSize(bOffset + areaSize);
                    }
                }
                // if 'B' is last we just overwrite it
                store(entryCount, bOffset);
                updateB(bOffset, areaSize);
                switchToB();
            }
        }
    }

    public long getOffset() {
        return transientOffset;
    }

    public long getOffsetA() {
        return mem.getLong(OFFSET_OFFSET_A);
    }

    public long getOffsetB() {
        return mem.getLong(OFFSET_OFFSET_B);
    }

    public long getSize() {
        return transientSize;
    }

    public long getSizeA() {
        return mem.getLong(OFFSET_SIZE_A);
    }

    public long getSizeB() {
        return mem.getLong(OFFSET_SIZE_B);
    }

    public boolean isB() {
        return (char) mem.getByte(OFFSET_AREA) == 'B';
    }

    private void bumpFileSize(long size) {
        mem.setSize(size);
        this.size = size;
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

    private void switchToA() {
        mem.putLong(OFFSET_AREA, (byte) 'A');
    }

    private void switchToB() {
        mem.putLong(OFFSET_AREA, (byte) 'B');
    }

    private void updateA(long aOffset, long aSize) {
        mem.putLong(OFFSET_OFFSET_A, aOffset);
        mem.putLong(OFFSET_SIZE_A, aSize);
        this.transientOffset = aOffset;
        this.transientSize = aSize;
    }

    private void updateB(long bOffset, long bSize) {
        mem.putLong(OFFSET_OFFSET_B, bOffset);
        mem.putLong(OFFSET_SIZE_B, bSize);
        this.transientOffset = bOffset;
        this.transientSize = bSize;
    }
}
