/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.join.asof;

import com.nfsdb.std.IntList;
import com.nfsdb.std.LongList;

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
