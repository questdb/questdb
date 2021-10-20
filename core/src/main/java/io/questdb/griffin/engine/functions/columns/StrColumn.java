/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

public class StrColumn extends StrFunction implements ScalarFunction {
    private static final ObjList<StrColumn> COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private final int columnIndex;

    public StrColumn(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public static StrColumn newInstance(int columnIndex) {
        if (columnIndex < STATIC_COLUMN_COUNT) {
            return COLUMNS.getQuick(columnIndex);
        }
        return new StrColumn(columnIndex);
    }

    @Override
    public CharSequence getStr(Record rec) {
        return rec.getStr(columnIndex);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return rec.getStrB(columnIndex);
    }

    @Override
    public void getStr(Record rec, CharSink sink) {
        rec.getStr(columnIndex, sink);
    }

    @Override
    public int getStrLen(Record rec) {
        return rec.getStrLen(columnIndex);
    }

    static {
        COLUMNS.setPos(STATIC_COLUMN_COUNT);
        for (int i = 0; i < STATIC_COLUMN_COUNT; i++) {
            COLUMNS.setQuick(i, new StrColumn(i));
        }
    }
}
