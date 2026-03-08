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

package io.questdb.griffin.engine.functions.columns;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.Long128Function;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

public class Long128Column extends Long128Function implements ColumnFunction {
    private static final ObjList<Long128Column> COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private final int columnIndex;

    private Long128Column(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public static Long128Column newInstance(int columnIndex) {
        if (columnIndex < STATIC_COLUMN_COUNT) {
            return COLUMNS.getQuick(columnIndex);
        }
        return new Long128Column(columnIndex);
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getLong128Hi(Record rec) {
        return rec.getLong128Hi(columnIndex);
    }

    @Override
    public long getLong128Lo(Record rec) {
        return rec.getLong128Lo(columnIndex);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    static {
        COLUMNS.setPos(STATIC_COLUMN_COUNT);
        for (int i = 0; i < STATIC_COLUMN_COUNT; i++) {
            COLUMNS.setQuick(i, new Long128Column(i));
        }
    }
}
