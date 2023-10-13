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
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class Column implements Mutable, Sinkable {
    private final ObjList<TypeAdapter> formats = new ObjList<>();
    private int columnType;
    private int fileColumnIndex;
    private CharSequence fileColumnName;
    private CharSequence tableColumnName;

    public Column(CharSequence fileColumnName, int fileColumnIndex, int columnType, CharSequence tableColumnName) {
        this.columnType = columnType;
        this.fileColumnIndex = fileColumnIndex;
        this.fileColumnName = fileColumnName;
        this.tableColumnName = tableColumnName;
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

    public TypeAdapter getFormat(int formatIndex) {
        return formats.getQuick(formatIndex);
    }

    public int getFormatCount() {
        return formats.size();
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.put("\"file_column_name\":\"").put(fileColumnName).put("\",");
        sink.put("\"file_column_index\":").put(fileColumnIndex).put(',');
        sink.put("\"column_type\":\"").put(ColumnType.nameOf(columnType)).put("\",");
        sink.put("\"table_column_name\":\"").put(tableColumnName).put("\",");
        sink.put("\"formats\":").put('[');
        for (int i = 0, n = formats.size(); i < n; i++) {
            if (i > 0) {
                sink.put(',');
            }
            TypeAdapter format = this.formats.getQuick(i);
            format.toSink(sink);
        }
        sink.put(']');
        sink.put('}');
    }
}
