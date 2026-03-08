/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.std.IntLongSortedList;
import io.questdb.std.ObjList;

/**
 * Returns rows from current page frame in table (physical) order:
 * - fetches first record index per cursor into priority queue
 * - then returns record with the smallest index and adds next record
 * from related cursor into queue until all cursors are exhausted.
 */
class HeapRowCursor implements RowCursor {
    private final IntLongSortedList list;
    private ObjList<RowCursor> cursors;

    public HeapRowCursor() {
        this.list = new IntLongSortedList();
    }

    @Override
    public boolean hasNext() {
        return list.hasNext();
    }

    @Override
    public long next() {
        int idx = list.peekIndex();
        RowCursor cursor = cursors.getQuick(idx);
        return cursor.hasNext() ? list.pollAndReplace(idx, cursor.next()) : list.pollValue();
    }

    public void of(ObjList<RowCursor> cursors, int activeCursors) {
        this.cursors = cursors;
        this.list.clear();
        for (int i = 0; i < activeCursors; i++) {
            final RowCursor cursor = cursors.getQuick(i);
            if (cursor.hasNext()) {
                list.add(i, cursor.next());
            }
        }
    }
}
