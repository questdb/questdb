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
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.Nullable;

public class SchemaV2 implements Mutable, Sinkable {
    public static final int FORMATS_ACTION_ADD = 1;
    public static final int FORMATS_ACTION_REPLACE = 2;
    // todo: schema could pool columns
    private final ObjList<Column> columnList = new ObjList<>();
    private final IntObjHashMap<ObjList<TypeAdapter>> formats = new IntObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<Column> nameToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private int formatsAction = FORMATS_ACTION_ADD;

    // legacy v1 schema interaction
    public void addColumn(CharSequence columnName, int columnType, TypeAdapter format) {
        int index = nameToColumnMap.keyIndex(columnName);
        if (index > -1) {
            final String columnNameStr = Chars.toString(columnName);
            // new column name
            final Column column = new Column(
                    columnNameStr,
                    -1,
                    columnType,
                    null
            );
            column.addFormat(format);
            this.columnList.add(column);
            this.nameToColumnMap.putAt(index, columnNameStr, column);
        }
    }

    public void addColumn(
            CharSequence fileColumnName,
            int fileColumnIndex,
            int columnType,
            CharSequence tableColumnName,
            @Transient ObjList<TypeAdapter> formats
    ) {
        int index = nameToColumnMap.keyIndex(fileColumnName);
        if (index > -1) {
            final String fileColumnNameStr = Chars.toString(fileColumnName);
            // new column name
            final Column column = new Column(
                    fileColumnNameStr,
                    fileColumnIndex,
                    columnType,
                    Chars.toString(tableColumnName)
            );
            column.addAllFormats(formats);
            this.columnList.add(column);
            this.nameToColumnMap.putAt(index, fileColumnNameStr, column);
        }
    }

    public void addFormats(int columnType, @Transient ObjList<TypeAdapter> formats) {
        int keyIndex = this.formats.keyIndex(columnType);
        ObjList<TypeAdapter> embeddedFormatList;
        if (keyIndex > -1) {
            embeddedFormatList = new ObjList<>();
            this.formats.putAt(keyIndex, columnType, embeddedFormatList);
        } else {
            embeddedFormatList = this.formats.valueAt(keyIndex);
        }
        embeddedFormatList.addAll(formats);
    }

    @Override
    public void clear() {
        columnList.clear();
        nameToColumnMap.clear();
        // formats are keyed on column type, we could reduce allocation by
        // reusing lists for each type
        for (int i = 0, n = ColumnType.MAX; i < n; i++) {
            ObjList<TypeAdapter> formats = this.formats.get(i);
            if (formats != null) {
                formats.clear();
            }
        }
        formatsAction = FORMATS_ACTION_ADD;
    }

    public @Nullable TypeAdapter findFirstFormat(CharSequence columnName) {
        final Column column = nameToColumnMap.get(columnName);
        if (column != null && column.getFormatCount() > 0) {
            return column.getFormat(0);
        }
        return null;
    }

    public Column getColumn(int columnIndex) {
        return columnList.getQuick(columnIndex);
    }

    public int getColumnCount() {
        return columnList.size();
    }

    public int getFormatsAction() {
        return formatsAction;
    }

    public void setFormatsAction(int formatsAction) {
        this.formatsAction = formatsAction;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.put("\"columns\":").put('[');
        for (int i = 0, n = columnList.size(); i < n; i++) {
            columnList.getQuick(i).toSink(sink);
        }
        sink.put(']').put(',');
        sink.putQuoted("formats").put(": {");
        int formatCount = 0;
        for (int i = 0; i < ColumnType.MAX; i++) {
            ObjList<TypeAdapter> formats = this.formats.get(i);
            if (formats != null && formats.size() > 0) {
                if (formatCount > 0) {
                    sink.put(',');
                }
                formatCount++;
                sink.putQuoted(ColumnType.nameOf(i)).put(": [");
                for (int j = 0, n = formats.size(); j < n; j++) {
                    if (j > 0) {
                        sink.put(',');
                    }
                    formats.getQuick(j).toSink(sink);
                }
                sink.put(']');
            }
        }
        sink.put('}').put(',');
        sink.putQuoted("formats_action").put(':').putQuoted(formatsAction == FORMATS_ACTION_ADD ? "ADD" : (formatsAction == FORMATS_ACTION_REPLACE ? "REPLACE" : "UNKNOWN"));
        sink.put('}');
    }
}
