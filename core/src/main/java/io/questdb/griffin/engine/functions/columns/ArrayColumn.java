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

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Record;

public class ArrayColumn extends ArrayFunction implements ColumnFunction {
    private final int columnIndex;

    public ArrayColumn(int columnIndex, int columnType) {
        this.columnIndex = columnIndex;
        this.type = columnType;
    }

    public static ArrayColumn newInstance(int columnIndex, int columnType) {
        return new ArrayColumn(columnIndex, columnType);
    }

    @Override
    public ArrayView getArray(Record rec) {
        return rec.getArray(columnIndex, type);
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }
}
