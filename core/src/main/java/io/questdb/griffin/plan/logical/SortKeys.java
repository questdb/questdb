/*+*****************************************************************************
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

package io.questdb.griffin.plan.logical;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * An order over column ids, each key ascending or descending; empty when no order is requested.
 */
public final class SortKeys implements Mutable {
    private final IntList columnIds = new IntList();
    private final ObjList<SortDirection> directions = new ObjList<>();

    @Override
    public void clear() {
        columnIds.clear();
        directions.clear();
    }

    public void copyFrom(SortKeys keys) {
        clear();
        columnIds.addAll(keys.columnIds);
        directions.addAll(keys.directions);
    }

    public IntList getColumnIds() {
        return columnIds;
    }

    public ObjList<SortDirection> getDirections() {
        return directions;
    }

    public boolean isEmpty() {
        return columnIds.size() == 0;
    }

    /**
     * Copies the keys of the sort, or clears the keys when the sort is null.
     */
    public void of(SortPlan sort) {
        clear();
        if (sort != null) {
            columnIds.addAll(sort.getColumnIds());
            directions.addAll(sort.getDirections());
        }
    }

    public int size() {
        return columnIds.size();
    }
}
