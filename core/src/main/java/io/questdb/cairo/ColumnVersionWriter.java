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

public class ColumnVersionWriter {
    private final MemoryMARW mem;
    private long size;

    // size should be read from the transaction file
    // it can be zero when there are no columns deviating from the main
    // data branch
    public ColumnVersionWriter(FilesFacade ff, LPSZ fileName, long size) {
        this.mem = new MemoryCMARWImpl(ff, fileName, ff.getPageSize(), size, MemoryTag.MMAP_TABLE_READER);
        this.size = size;
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
            mem.setSize(areaSize + headerSize);
            // writing A group
            store(columnVersions, entryCount, headerSize);
            mem.putLong(0, (byte) 'A');
            mem.putLong(8, headerSize);
            mem.putLong(16, areaSize);
            mem.putLong(32, 0); // 'B' offset, 0 - no 'B' area
            mem.putLong(40, 0); // 'B' length, 0 - no 'B' area
            this.size = areaSize + headerSize;
        } else {
            char current = (char) mem.getByte(0);
            if (current == 'B') {
                // we have to write 'A' area, which may not be big enough
                long aOffset = mem.getLong(8);
                long aSize = mem.getLong(16);

                // is area 'A' at top of file ?
                if (aOffset == headerSize) {
                    if (aSize > areaSize) {
                        // happy days, 'A' is at start of the file, and it is large enough
                        store(columnVersions, entryCount, aOffset);
                    } else {
                        // 'A' is at the start of the file, and it is small
                        // we need to relocate i
                    }
                }

            }
        }
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
}
