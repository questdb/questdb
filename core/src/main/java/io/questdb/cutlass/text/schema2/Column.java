/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.text.schema2;

import io.questdb.cairo.ColumnType;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;

public class Column implements Mutable, Sinkable {
    private final ObjList<TypeAdapter> formats = new ObjList<>();
    private int columnType;
    private boolean fileColumnIgnore;
    private int fileColumnIndex;
    private CharSequence fileColumnName;
    private CharSequence tableColumnName;

    // if CSV has fewer columns than the table, the schema must have explicit "insert NULL" attribute for those columns
    private boolean tableInsertNull;

    public Column(CharSequence fileColumnName, int fileColumnIndex, boolean fileColumnIgnore, int columnType, CharSequence tableColumnName, boolean tableInsertNull) {
        this.columnType = columnType;
        this.fileColumnIndex = fileColumnIndex;
        this.fileColumnName = fileColumnName;
        this.fileColumnIgnore = fileColumnIgnore;
        this.tableColumnName = tableColumnName;
        this.tableInsertNull = tableInsertNull;
    }

    public void addAllFormats(ObjList<TypeAdapter> formats) {
        this.formats.addAll(formats);
    }

    public void addFormat(TypeAdapter format) {
        formats.add(format);
    }

    @Override
    public void clear() {
        fileColumnIndex = -1;
        tableColumnName = null;
        fileColumnName = null;
        columnType = -1;
        formats.clear();
    }

    public int getColumnType() {
        return columnType;
    }

    public int getFileColumnIndex() {
        return fileColumnIndex;
    }

    public CharSequence getFileColumnName() {
        return fileColumnName;
    }

    public TypeAdapter getFormat(int formatIndex) {
        return formats.getQuick(formatIndex);
    }

    public int getFormatCount() {
        return formats.size();
    }

    public CharSequence getTableColumnName() {
        return tableColumnName;
    }

    public CharSequence getTableOrFileColumnName() {
        return tableColumnName != null ? tableColumnName : fileColumnName;
    }

    public boolean isFileColumnIgnore() {
        return fileColumnIgnore;
    }

    // does it make sense to require users to map columns not present in the file ?
    // they'd be set to default values anyway
    public boolean isTableInsertNull() {
        return tableInsertNull;
    }

    @Override
    public void toSink(CharSinkBase<?> sink) {
        sink.putAscii('{');
        sink.putAscii("\"file_column_name\":\"").put(fileColumnName).putAscii("\",");
        sink.putAscii("\"file_column_index\":").put(fileColumnIndex).putAscii(',');
        sink.putAscii("\"file_column_ignore\":").put(fileColumnIgnore).putAscii(',');
        sink.putAscii("\"column_type\":\"").put(ColumnType.nameOf(columnType)).putAscii("\",");
        sink.putAscii("\"table_column_name\":\"").put(tableColumnName).putAscii("\",");
        sink.putAscii("\"insert_null\":\"").put(tableInsertNull).putAscii("\",");
        sink.putAscii("\"formats\":").putAscii('[');
        for (int i = 0, n = formats.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(',');
            }
            TypeAdapter format = this.formats.getQuick(i);
            format.toSink(sink);
        }
        sink.putAscii(']');
        sink.putAscii('}');
    }
}
