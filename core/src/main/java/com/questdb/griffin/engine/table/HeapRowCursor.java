/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin.engine.table;

import com.questdb.common.RowCursor;
import com.questdb.std.IntLongPriorityQueue;
import com.questdb.std.Unsafe;

class HeapRowCursor implements RowCursor {
    private final IntLongPriorityQueue heap;
    private RowCursor[] cursors;
    private int nCursors;

    public HeapRowCursor(int nCursors) {
        this.heap = new IntLongPriorityQueue(nCursors);
        this.nCursors = nCursors;
    }

    @Override
    public boolean hasNext() {
        return heap.hasNext();
    }

    @Override
    public long next() {
        int idx = heap.popIndex();
        RowCursor cursor = Unsafe.arrayGet(cursors, idx);
        return cursor.hasNext() ? heap.popAndReplace(idx, cursor.next()) : heap.popValue();
    }

    public void of(RowCursor[] cursors) {
        assert nCursors == cursors.length;
        this.cursors = cursors;
        this.heap.clear();
        for (int i = 0; i < nCursors; i++) {
            RowCursor cursor = Unsafe.arrayGet(cursors, i);
            if (cursor.hasNext()) {
                heap.add(i, cursor.next());
            }
        }
    }
}
