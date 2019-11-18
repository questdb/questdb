/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.IntLongPriorityQueue;
import io.questdb.std.ObjList;

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
