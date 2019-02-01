/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.cairo.sql.RowCursor;
import com.questdb.std.IntLongPriorityQueue;
import com.questdb.std.ObjList;

class HeapRowCursor implements RowCursor {
    private final IntLongPriorityQueue heap;
    private ObjList<RowCursor> cursors;

    public HeapRowCursor() {
        this.heap = new IntLongPriorityQueue();
    }

    @Override
    public boolean hasNext() {
        return heap.hasNext();
    }

    @Override
    public long next() {
        int idx = heap.popIndex();
        RowCursor cursor = cursors.getQuick(idx);
        return cursor.hasNext() ? heap.popAndReplace(idx, cursor.next()) : heap.popValue();
    }

    public void of(ObjList<RowCursor> cursors) {
        int nCursors = cursors.size();
        this.cursors = cursors;
        this.heap.clear();
        for (int i = 0; i < nCursors; i++) {
            final RowCursor cursor = cursors.getQuick(i);
            if (cursor.hasNext()) {
                heap.add(i, cursor.next());
            }
        }
    }
}
