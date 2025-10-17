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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.ObjList;

import static io.questdb.griffin.engine.functions.columns.ColumnUtils.STATIC_COLUMN_COUNT;

public class TimestampColumn extends TimestampFunction implements ColumnFunction {
    private static final ObjList<TimestampColumn> COLUMNS = new ObjList<>(STATIC_COLUMN_COUNT);
    private final int columnIndex;

    private TimestampColumn(int columnIndex, int timestampType) {
        super(timestampType);
        this.columnIndex = columnIndex;
    }

    public static TimestampColumn newInstance(int columnIndex, int timestampType) {
        if (columnIndex < STATIC_COLUMN_COUNT) {
            TimestampColumn column = COLUMNS.getQuick(columnIndex);
            column.setType(timestampType);
        }
        return new TimestampColumn(columnIndex, timestampType);
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    @Override
    public long getTimestamp(Record rec) {
        return rec.getTimestamp(columnIndex);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    static {
        COLUMNS.setPos(STATIC_COLUMN_COUNT);
        for (int i = 0; i < STATIC_COLUMN_COUNT; i++) {
            COLUMNS.setQuick(i, new TimestampColumn(i, ColumnType.TIMESTAMP_MICRO));
        }
    }
}
