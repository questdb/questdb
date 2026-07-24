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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

/**
 * An INT column reference that keeps the referenced column's wide half.
 * <p>
 * {@link IntColumn} overrides only {@link #getInt(Record)} and inherits
 * {@code getLong() = Numbers.intToLong(getInt())}, so it re-wraps an INT expression whose
 * 64-bit read carries more than 32 bits - {@code a::LONG} over {@code SELECT i + j AS a}
 * would return the wrapped value while {@code (i + j)::LONG} returns the wide one. That
 * also made the value an {@code INSERT ... SELECT} stored depend on the plan shape, since
 * an elided projection hands the arithmetic function to the row copier while a surviving
 * one hands it this column reference.
 * <p>
 * This variant reads the record at the width the caller asks for, so the alias is
 * transparent. It is emitted only where the referenced column is known to be
 * function-backed - {@link ColumnTypes#isColumnIntWidthStable(int)} answers false - which
 * is exactly the condition under which {@link Record#getLong(int)} is legal on an INT
 * column. Over a stored INT column that read would take 8 bytes off a 4-byte slot, so
 * {@link IntColumn} stays in charge there.
 * <p>
 * Because it is a proxy, it must report the REFERENCED expression's row stability rather
 * than its own: a caller that reads both widths of one row - {@code nullif}, {@code coalesce},
 * the {@code IN} split key - decides between one long-width read and two INT-width reads on
 * that answer, and two reads of a non-deterministic expression are two different draws.
 * {@link ColumnTypes#isColumnRowStable(int)} supplies it, defaulting to the conservative false.
 *
 * @see io.questdb.cairo.sql.Function#isIntWidthStable()
 * @see io.questdb.cairo.sql.Function#isRowStable()
 */
public class IntWideColumn extends IntFunction implements ColumnFunction {
    // Both fields are final and the class holds no per-cursor state, so low indexes are served
    // from a pool exactly as IntColumn does - one pool per row-stability answer.
    private static final ObjList<IntWideColumn> ROW_STABLE_COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private static final ObjList<IntWideColumn> ROW_UNSTABLE_COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private final int columnIndex;
    private final boolean isRowStable;

    private IntWideColumn(int columnIndex, boolean isRowStable) {
        this.columnIndex = columnIndex;
        this.isRowStable = isRowStable;
    }

    public static IntWideColumn newInstance(int columnIndex, boolean isRowStable) {
        if (columnIndex < STATIC_COLUMN_COUNT) {
            return isRowStable
                    ? ROW_STABLE_COLUMNS.getQuick(columnIndex)
                    : ROW_UNSTABLE_COLUMNS.getQuick(columnIndex);
        }
        return new IntWideColumn(columnIndex, isRowStable);
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public int getInt(Record rec) {
        return rec.getInt(columnIndex);
    }

    @Override
    public long getLong(Record rec) {
        return rec.getLong(columnIndex);
    }

    @Override
    public boolean isIntWidthStable() {
        return false;
    }

    @Override
    public boolean isRowStable() {
        return isRowStable;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    static {
        ROW_STABLE_COLUMNS.setPos(STATIC_COLUMN_COUNT);
        ROW_UNSTABLE_COLUMNS.setPos(STATIC_COLUMN_COUNT);
        for (int i = 0; i < STATIC_COLUMN_COUNT; i++) {
            ROW_STABLE_COLUMNS.setQuick(i, new IntWideColumn(i, true));
            ROW_UNSTABLE_COLUMNS.setQuick(i, new IntWideColumn(i, false));
        }
    }
}
