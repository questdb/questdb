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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.std.str.CharSink;

public interface RecordMetadata extends ColumnTypes {

    int COLUMN_NOT_FOUND = -1;

    int getColumnCount();

    int getColumnType(int index);

    default int getColumnIndex(CharSequence columnName) {
        int index = getColumnIndexQuiet(columnName);
        if (index == -1) {
            throw InvalidColumnException.INSTANCE;
        }
        return index;
    }

    default int getColumnIndexQuiet(CharSequence columnName) {
        return getColumnIndexQuiet(columnName, 0, columnName.length());
    }

    int getColumnIndexQuiet(CharSequence columnName, int lo, int hi);

    String getColumnName(int columnIndex);

    default int getColumnType(CharSequence columnName) {
        return getColumnType(getColumnIndex(columnName));
    }

    int getIndexValueBlockCapacity(int columnIndex);

    default int getIndexValueBlockCapacity(CharSequence columnName) {
        return getIndexValueBlockCapacity(getColumnIndex(columnName));
    }

    int getTimestampIndex();

    default boolean isSymbolTableStatic(CharSequence columnName) {
        return isSymbolTableStatic(getColumnIndex(columnName));
    }

    boolean isColumnIndexed(int columnIndex);

    boolean isSymbolTableStatic(int columnIndex);

    default void toJson(CharSink sink) {
        sink.put('{');
        sink.putQuoted("columnCount").put(':').put(getColumnCount());
        sink.put(',');
        sink.putQuoted("columns").put(':');
        sink.put('[');
        for (int i = 0, n = getColumnCount(); i < n; i++) {
            final int type = getColumnType(i);
            if (i > 0) {
                sink.put(',');
            }
            sink.put('{');
            sink.putQuoted("index").put(':').put(i).put(',');
            sink.putQuoted("name").put(':').putQuoted(getColumnName(i)).put(',');
            sink.putQuoted("type").put(':').putQuoted(ColumnType.nameOf(type));
            if (isColumnIndexed(i)) {
                sink.put(',').putQuoted("indexed").put(":true");
                sink.put(',').putQuoted("indexValueBlockCapacity").put(':').put(getIndexValueBlockCapacity(i));
            }
            sink.put('}');
        }
        sink.put(']');
        sink.put(',').putQuoted("timestampIndex").put(':').put(getTimestampIndex());
        sink.put('}');
    }
}
