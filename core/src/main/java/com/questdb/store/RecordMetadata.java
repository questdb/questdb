/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.store;

import com.questdb.std.str.CharSink;

public interface RecordMetadata {

    String getAlias();

    void setAlias(String alias);

    RecordColumnMetadata getColumn(CharSequence name);

    int getColumnCount();

    /**
     * Finds index of column by given name. If name is invalid a JournalRuntimeException is thrown.
     *
     * @param name column name
     * @return column index between 0 and getColumnCount()-1
     */
    int getColumnIndex(CharSequence name);

    int getColumnIndexQuiet(CharSequence name);

    String getColumnName(int index);

    /**
     * Finds column metadata by column index. Method does not perform boundary checks, relying on caller to do so.
     *
     * @param index must be &gt;=0 and &lt; getColumnCount()
     * @return column metadata
     */
    RecordColumnMetadata getColumnQuick(int index);

    int getTimestampIndex();

    default void toJson(CharSink sink) {
        sink.put('{');
        sink.putQuoted("columnCount").put(':').put(getColumnCount());
        sink.put(',');
        sink.putQuoted("columns").put(':');
        sink.put('[');
        for (int i = 0, n = getColumnCount(); i < n; i++) {
            RecordColumnMetadata m = getColumnQuick(i);
            if (i > 0) {
                sink.put(',');
            }
            sink.put('{');
            sink.putQuoted("index").put(':').put(i).put(',');
            sink.putQuoted("name").put(':').putQuoted(m.getName()).put(',');
            sink.putQuoted("type").put(':').putQuoted(ColumnType.nameOf(m.getType()));
            if (m.isIndexed()) {
                sink.put(',').putQuoted("indexed").put(":true");
            }
            sink.put('}');
        }
        sink.put(']');
        sink.put('}');
    }
}
