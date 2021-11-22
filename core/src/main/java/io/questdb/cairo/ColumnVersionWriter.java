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

import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;

import java.io.Closeable;

public class ColumnVersionWriter implements Closeable {
    public static final int OFFSET_AREA = 0;
    public static final int OFFSET_OFFSET_A = 8;
    public static final int OFFSET_SIZE_A = 16;
    public static final int OFFSET_OFFSET_B = 24;
    public static final int OFFSET_SIZE_B = 32;
    private final MemoryMARW mem;
    private long size;
    private long transientOffset;
    private long transientSize;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = new MemoryCMARWImpl(ff, fileName, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_READER);
        this.size = size;
    }

    @Override
    public void close() {
        mem.close();
    }

    public void commit(LongList columnVersions) {
        // + 8 is the A/B switch at top of the file, it is 8 bytes to keep data aligned
        // + 8 is the offset of A area
        // + 8 is the length of A area
        // + 8 is the offset of B area
        // + 8 is the length of B area
        final long headerSize = 8 + 8 + 8 + 8 + 8;
        // calculate the area size required to store the versions
        // we're assuming that 'columnVersions' contains 4 longs per entry
        final int entryCount = columnVersions.size() / 4;
        // We're storing 3 longs per entry in the file
        final long areaSize = entryCount * 3 * 8L;

        if (size == 0) {
            // This is initial write, include header size into the resize
            bumpFileSize(headerSize + areaSize);
            // writing A group
            store(columnVersions, entryCount, headerSize);
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
                store(columnVersions, entryCount, aOffset);
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
                store(columnVersions, entryCount, bOffset);
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

    private void store(LongList columnVersions, int entryCount, long offset) {
        for (int i = 0; i < entryCount; i++) {
            int x = i * 4;
            mem.putLong(offset, columnVersions.getQuick(x));
            mem.putLong(offset + 8, columnVersions.getQuick(x + 1));
            mem.putLong(offset + 16, columnVersions.getQuick(x + 2));
            offset += 24;
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
