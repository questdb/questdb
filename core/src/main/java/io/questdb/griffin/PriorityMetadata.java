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

package io.questdb.griffin;

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;

/**
 * This class supports generation of VirtualRecordCursorFactory in allowing functions
 * reference previously used function of the same projection. It contains projection columns
 * but also references the base table metadata as the delegate. It facilitates the
 * priority system in case there is name collision between projection aliases and the
 * base table columns. Such collisions are resolved by preferring the base table. As is the case with
 * other major databases.
 */
public class PriorityMetadata extends AbstractRecordMetadata {
    private final RecordMetadata baseMetadata;
    private final int virtualColumnReservedSlots;

    public PriorityMetadata(int virtualColumnReservedSlots, RecordMetadata baseMetadata) {
        this.virtualColumnReservedSlots = virtualColumnReservedSlots;
        // hold on to the base metadata, in case this is a join metadata, and it is able to
        // resolve column names containing table aliases.
        this.baseMetadata = baseMetadata;
    }

    public void add(TableColumnMetadata m) {
        int keyIndex = columnNameIndexMap.keyIndex(m.getColumnName());
        if (keyIndex < 0) {
            throw CairoException.duplicateColumn(m.getColumnName());
        }
        int pos = columnMetadata.size();
        columnMetadata.add(m);
        columnNameIndexMap.putAt(keyIndex, m.getColumnName(), pos);
    }

    public int getBaseColumnIndex(int index) {
        if (index < virtualColumnReservedSlots) {
            return -1;
        }
        return index - virtualColumnReservedSlots;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        int index = baseMetadata.getColumnIndexQuiet(columnName, lo, hi);
        if (index == -1) {
            int keyIndex = columnNameIndexMap.keyIndex(columnName, lo, hi);
            if (keyIndex < 0) {
                return columnNameIndexMap.valueAt(keyIndex);
            }
            return -1;
        }
        return index + virtualColumnReservedSlots;
    }

    @Override
    public TableColumnMetadata getColumnMetadata(int index) {
        if (index < virtualColumnReservedSlots) {
            return columnMetadata.getQuick(index);
        }
        return baseMetadata.getColumnMetadata(index - virtualColumnReservedSlots);
    }
}
