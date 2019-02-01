/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.sql;

import com.questdb.cairo.ColumnType;
import com.questdb.cairo.ColumnTypes;
import com.questdb.std.str.CharSink;

public interface RecordMetadata extends ColumnTypes {

    int getColumnCount();

    int getColumnType(int index);

    default int getColumnIndex(CharSequence columnName) {
        int index = getColumnIndexQuiet(columnName);
        if (index == -1) {
            throw InvalidColumnException.INSTANCE;
        }
        return index;
    }

    int getColumnIndexQuiet(CharSequence columnName);

    CharSequence getColumnName(int columnIndex);

    default int getColumnType(CharSequence columnName) {
        return getColumnType(getColumnIndex(columnName));
    }

    int getIndexValueBlockCapacity(int columnIndex);

    default int getIndexValueBlockCapacity(CharSequence columnName) {
        return getIndexValueBlockCapacity(getColumnIndex(columnName));
    }

    int getTimestampIndex();

    boolean isColumnIndexed(int columnIndex);

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
