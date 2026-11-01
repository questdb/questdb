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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.LongList;

/**
 * Spliced-column record of a constant-false WINDOW JOIN whose aggregates include a function with a
 * non-NULL empty value, such as count() or isOrdered(). For each spliced column, {@code emptyValueTypes}
 * holds the type tag of its empty value and {@code emptyValues} the value's raw bits.
 * {@link #getBool(int)}, {@link #getDouble(int)} and {@link #getLong(int)} report the empty value of a
 * spliced column whose tag is BOOLEAN, DOUBLE or LONG respectively. A column tagged
 * {@link ColumnType#UNDEFINED}, a getter that does not match the tag, and every other getter keep
 * the NULL behaviour of {@link ExtraNullColumnRecord}.
 */
public class ExtraEmptyValueColumnRecord extends ExtraNullColumnRecord {
    private final int columnSplit;
    private final IntList emptyValueTypes;
    private final LongList emptyValues;

    public ExtraEmptyValueColumnRecord(int columnSplit, LongList emptyValues, IntList emptyValueTypes) {
        super(columnSplit);
        this.columnSplit = columnSplit;
        this.emptyValues = emptyValues;
        this.emptyValueTypes = emptyValueTypes;
    }

    @Override
    public boolean getBool(int col) {
        if (isEmptyValueOfType(col, ColumnType.BOOLEAN)) {
            return emptyValues.getQuick(col - columnSplit) != 0;
        }
        return super.getBool(col);
    }

    @Override
    public double getDouble(int col) {
        if (isEmptyValueOfType(col, ColumnType.DOUBLE)) {
            return Double.longBitsToDouble(emptyValues.getQuick(col - columnSplit));
        }
        return super.getDouble(col);
    }

    @Override
    public long getLong(int col) {
        if (isEmptyValueOfType(col, ColumnType.LONG)) {
            return emptyValues.getQuick(col - columnSplit);
        }
        return super.getLong(col);
    }

    private boolean isEmptyValueOfType(int col, short typeTag) {
        return col >= columnSplit && emptyValueTypes.getQuick(col - columnSplit) == typeTag;
    }
}
