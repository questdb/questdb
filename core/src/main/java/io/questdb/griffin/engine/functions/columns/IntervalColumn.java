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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.Interval;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

public class IntervalColumn extends IntervalFunction implements Function, FunctionExtension {
    private static final ObjList<IntervalColumn> COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private final int columnIndex;

    private IntervalColumn(int columnIndex, int columnType) {
        super(columnType);
        this.columnIndex = columnIndex;
    }

    public static IntervalColumn newInstance(int columnIndex, int columnType) {
        if (columnIndex < STATIC_COLUMN_COUNT) {
            IntervalColumn column = COLUMNS.getQuick(columnIndex);
            column.setType(columnType);
            return column;
        }
        return new IntervalColumn(columnIndex, columnType);
    }

    @Override
    public FunctionExtension extendedOps() {
        return this;
    }

    @Override
    public int getArrayLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return rec.getInterval(columnIndex);
    }

    @Override
    public Record getRecord(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.putColumnName(columnIndex);
    }

    static {
        COLUMNS.setPos(STATIC_COLUMN_COUNT);
        for (int i = 0; i < STATIC_COLUMN_COUNT; i++) {
            COLUMNS.setQuick(i, new IntervalColumn(i, ColumnType.INTERVAL_RAW));
        }
    }
}
