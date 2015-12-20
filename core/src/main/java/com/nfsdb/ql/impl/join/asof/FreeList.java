/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.collections.IntList;
import com.nfsdb.collections.LongList;

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
