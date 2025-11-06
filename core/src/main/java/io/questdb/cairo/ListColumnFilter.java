/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.IntList;

public final class ListColumnFilter extends IntList implements ColumnFilter, Plannable {

    public ListColumnFilter() {
    }

    public ListColumnFilter(int capacity) {
        super(capacity);
    }

    public ListColumnFilter copy() {
        ListColumnFilter copy = new ListColumnFilter(size());
        copy.addAll(this);
        return copy;
    }

    @Override
    public int getColumnCount() {
        return size();
    }

    @Override
    public int getColumnIndex(int position) {
        return getQuick(position);
    }

    @Override
    public void toPlan(PlanSink sink) {
        for (int i = 0, n = size(); i < n; i++) {
            int colIdx = get(i);
            int col = (colIdx > 0 ? colIdx : -colIdx) - 1;
            if (i > 0) {
                sink.val(", ");
            }
            sink.putBaseColumnName(col);
            if (colIdx < 0) {
                sink.val(" ").val("desc");
            }
        }
    }
}
