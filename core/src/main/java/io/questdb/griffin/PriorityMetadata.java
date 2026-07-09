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

    public int getVirtualColumnReservedSlots() {
        return virtualColumnReservedSlots;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        int index = baseMetadata.getColumnIndexQuiet(columnName, lo, hi);
        if (index == -1) {
            int keyIndex = columnNameIndexMap.keyIndex(columnName, lo, hi);
            if (keyIndex < 0) {
                return columnNameIndexMap.valueAt(keyIndex);
            }
            // The base splits a composed name on the dot, so this metadata reports splitsOnDot and
            // SqlUtil.getColumnIndexQuiet skips its quote-strip retry for a DOTTED protected alias to
            // avoid mis-splitting it against the base. But this metadata's own projection columns store a
            // content-dotted name clean (a.b) and match it verbatim, so retry the LOCAL map with the
            // protective quotes stripped - only for a dotted interior (an operator token has no dot and
            // is already handled by SqlUtil's retry, which must keep preferring the base). Checking only
            // the local map here cannot mis-split against the base.
            if (SqlUtil.quoteProtectedInteriorDot(columnName, lo, hi) > -1) {
                keyIndex = columnNameIndexMap.keyIndex(columnName, lo + 1, hi - 1);
                if (keyIndex < 0) {
                    return columnNameIndexMap.valueAt(keyIndex);
                }
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

    @Override
    public boolean splitsOnDot() {
        // getColumnIndexQuiet delegates the ranged lookup to the base, so a wrapped join splits on
        // the dot too; forward the flag so the compiler's quote-strip retry skips it (as it does for
        // a bare join) instead of mis-splitting a dotted alias into an unrelated table.column.
        return baseMetadata.splitsOnDot();
    }
}
