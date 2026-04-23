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

package io.questdb.test.griffin.fuzz;

import io.questdb.std.ObjList;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;

/**
 * Generated table description: name, column list, and the designated
 * timestamp column name. The timestamp column is always present and is
 * also included as the last entry in {@link #getColumns()}.
 */
public final class FuzzTable {
    private final ObjList<FuzzColumn> columns;
    private final String name;
    private final String tsColumnName;

    public FuzzTable(String name, ObjList<FuzzColumn> columns, String tsColumnName) {
        this.name = name;
        this.columns = columns;
        this.tsColumnName = tsColumnName;
    }

    public FuzzColumn getColumn(int index) {
        return columns.getQuick(index);
    }

    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Returns a list of non-timestamp columns whose type kind satisfies the
     * given predicate. Useful for picking group-by keys, aggregate targets,
     * join keys, etc.
     */
    public ObjList<FuzzColumn> getColumnsByKind(KindPredicate pred) {
        ObjList<FuzzColumn> out = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            FuzzColumn c = columns.getQuick(i);
            if (c.getName().equals(tsColumnName)) {
                continue;
            }
            if (pred.test(c.getType())) {
                out.add(c);
            }
        }
        return out;
    }

    public ObjList<FuzzColumn> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    public String getTsColumnName() {
        return tsColumnName;
    }

    @FunctionalInterface
    public interface KindPredicate {
        boolean test(FuzzColumnType type);
    }
}
