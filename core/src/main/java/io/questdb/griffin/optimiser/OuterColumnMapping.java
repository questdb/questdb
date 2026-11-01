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

package io.questdb.griffin.optimiser;

import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

/**
 * The stack of mappings from outer columns to the columns that satisfy them, which {@link Decorrelation} grows as it
 * rewrites a body block by block. A block's mappings lie above the {@link #mark} taken when the block started.
 */
final class OuterColumnMapping implements Mutable {
    private final IntList columnIds;
    private final IntList outerIds;

    /**
     * Borrows {@code outerIds} and {@code columnIds} as the stack's storage.
     */
    OuterColumnMapping(IntList outerIds, IntList columnIds) {
        this.outerIds = outerIds;
        this.columnIds = columnIds;
    }

    @Override
    public void clear() {
        outerIds.clear();
        columnIds.clear();
    }

    int columnIdAt(int index) {
        return columnIds.getQuick(index);
    }

    /**
     * The column the mapping between {@code lo} and {@code hi} maps the outer column to, or -1.
     */
    int mappedColumn(int outerId, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (outerIds.getQuick(i) == outerId) {
                return columnIds.getQuick(i);
            }
        }
        return -1;
    }

    /**
     * The column the mapping above {@code base} maps the outer column to, or -1.
     */
    int mappedColumn(int outerId, int base) {
        return mappedColumn(outerId, base, outerIds.size());
    }

    /**
     * The index of the mapping between {@code lo} and {@code hi} that maps an outer column to the column, or -1.
     */
    int mappedIndex(int columnId, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (columnIds.getQuick(i) == columnId) {
                return i;
            }
        }
        return -1;
    }

    int mark() {
        return outerIds.size();
    }

    int outerIdAt(int index) {
        return outerIds.getQuick(index);
    }

    void push(int outerId, int columnId) {
        outerIds.add(outerId);
        columnIds.add(columnId);
    }

    /**
     * Renames the mapped columns above {@code base} that {@code columnIds} holds.
     */
    void remapColumns(int base, IntIntHashMap columnIds) {
        for (int i = base, n = this.columnIds.size(); i < n; i++) {
            final int remappedId = columnIds.get(this.columnIds.getQuick(i));
            if (remappedId > -1) {
                this.columnIds.setQuick(i, remappedId);
            }
        }
    }

    void removeAt(int index) {
        outerIds.removeIndex(index);
        columnIds.removeIndex(index);
    }

    void reset(int mark) {
        outerIds.setPos(mark);
        columnIds.setPos(mark);
    }

    void setColumnIdAt(int index, int columnId) {
        columnIds.setQuick(index, columnId);
    }
}
