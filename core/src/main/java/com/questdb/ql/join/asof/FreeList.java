/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.join.asof;

import com.questdb.std.IntList;
import com.questdb.std.LongList;

public class FreeList {
    private final IntList sizes = new IntList();
    private final LongList offsets = new LongList();
    private int maxSize = -1;
    private long totalSize = 0;

    public void add(long offset, int size) {
        int index = sizes.binarySearch(size);

        if (index < 0) {
            index = -(index + 1);
        }

        sizes.add(index, size);
        offsets.add(index, offset);

        if (size > maxSize) {
            maxSize = size;
        }

        totalSize += size;
    }

    public void clear() {
        this.totalSize = 0;
        this.maxSize = -1;
        sizes.clear();
        offsets.clear();
    }

    public long findAndRemove(int size) {
        if (size > maxSize) {
            return -1;
        }

        int index = sizes.binarySearch(size);
        if (index < 0) {
            index = -(index + 1);

            if (index == sizes.size()) {
                if (index > 0) {
                    maxSize = sizes.getQuick(index - 1);
                } else {
                    maxSize = -1;
                }
                return -1;

            }
        }
        long offset = offsets.getQuick(index);
        offsets.removeIndex(index);
        totalSize -= sizes.getQuick(index);
        sizes.removeIndex(index);
        return offset;
    }

    public long getTotalSize() {
        return totalSize;
    }

    @Override
    public String toString() {
        return "FreeList{" +
                "sizes=" + sizes +
                ", offsets=" + offsets +
                ", maxSize=" + maxSize +
                '}';
    }
}
