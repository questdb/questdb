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
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.TextException;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.Nullable;

public class SchemaV2 implements Mutable, Sinkable {
    public static final int FORMATS_ACTION_ADD = 1;
    public static final int FORMATS_ACTION_REPLACE = 2;
    // todo: schema could pool columns
    private final ObjList<Column> columnList = new ObjList<>();
    private final ObjectPool<Column> columnPool;
    private final IntObjHashMap<Column> fileColumnIndexToColumnMap = new IntObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<Column> fileColumnNameToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private final IntObjHashMap<ObjList<TypeAdapter>> formats = new IntObjHashMap<>();
    private final LowerCaseCharSequenceObjHashMap<Column> tableColumnNameToColumnMap = new LowerCaseCharSequenceObjHashMap<>();
    private int formatsAction = FORMATS_ACTION_ADD;

    public SchemaV2(TextConfiguration textConfiguration) {
        columnPool = new ObjectPool<>(Column::new, textConfiguration.getSchemaColumnPoolCapacity());
    }

    // legacy v1 schema interaction
    public void addColumn(CharSequence columnName, int columnType, TypeAdapter format) throws TextException {
        int index = fileColumnNameToColumnMap.keyIndex(columnName);
        if (index > -1) {
            final String columnNameStr = Chars.toString(columnName);
            // new column name
            final Column column = columnPool.next().of(
                    columnNameStr,
                    -1,
                    false,
                    columnType,
                    null
            );
            column.addFormat(format);
            this.columnList.add(column);
            this.fileColumnNameToColumnMap.putAt(index, columnNameStr, column);
        } else {
            throw TextException.$("duplicate schema column [name=").put(columnName).put(']');
        }
    }

    public void addColumn(
            CharSequence fileColumnName,
            int fileColumnIndex,
            boolean fileColumnIgnore,
            int columnType,
            CharSequence tableColumnName,
            @Transient ObjList<TypeAdapter> formats
    ) throws JsonException {
        final int fileColumnNameMapIndex;
        if (fileColumnName != null) {
            fileColumnNameMapIndex = fileColumnNameToColumnMap.keyIndex(fileColumnName);
            if (fileColumnNameMapIndex < 0) {
                throw TextException.$("schema error, duplicate file column name [file_column_name=").put(fileColumnName).put(']');
            }
        } else {
            // file column name is not provided, will not add it to the map
            fileColumnNameMapIndex = -1;
        }

        final int fileColumnIndexMapIndex;
        if (fileColumnIndex != -1) {
            // file column is referenced by index
            fileColumnIndexMapIndex = fileColumnIndexToColumnMap.keyIndex(fileColumnIndex);
            if (fileColumnIndexMapIndex < 0) {
                throw TextException.$("schema error, duplicate file column index [file_column_index=").put(fileColumnIndex).put(']');
            }
        } else {
            // file column index is not provided, will not add it to the map
            fileColumnIndexMapIndex = -1;
        }

        final int tableColumnNameMapIndex;
        if (tableColumnName != null) {
            tableColumnNameMapIndex = tableColumnNameToColumnMap.keyIndex(tableColumnName);
            if (tableColumnNameMapIndex < 0) {
                throw TextException.$("schema error, duplicate table column name [table_column_name=").put(tableColumnName).put(']');
            }
        } else {
            tableColumnNameMapIndex = -1;
        }

        final String fileColumnNameStr = Chars.toString(fileColumnName);
        final String tableColumnNameStr = Chars.toString(tableColumnName);
        // new column name
        final Column column = columnPool.next().of(
                fileColumnNameStr,
                fileColumnIndex,
                fileColumnIgnore,
                columnType,
                tableColumnNameStr
        );
        column.addAllFormats(formats);
        this.columnList.add(column);

        if (fileColumnNameMapIndex != -1) {
            this.fileColumnNameToColumnMap.putAt(fileColumnNameMapIndex, fileColumnNameStr, column);
        }

        if (fileColumnIndexMapIndex != -1) {
            this.fileColumnIndexToColumnMap.putAt(fileColumnIndexMapIndex, fileColumnIndex, column);
        }

        if (tableColumnNameMapIndex != -1) {
            this.tableColumnNameToColumnMap.putAt(tableColumnNameMapIndex, tableColumnNameStr, column);
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
        fileColumnNameToColumnMap.clear();
        fileColumnIndexToColumnMap.clear();
        tableColumnNameToColumnMap.clear();

        // formats are keyed on column type, we could reduce allocation by
        // reusing lists for each type
        for (int i = 0, n = ColumnType.NULL; i < n; i++) {
            ObjList<TypeAdapter> formats = this.formats.get(i);
            if (formats != null) {
                formats.clear();
            }
        }
        formatsAction = FORMATS_ACTION_ADD;
    }

    public @Nullable TypeAdapter findFirstFormat(CharSequence columnName) {
        final Column column = fileColumnNameToColumnMap.get(columnName);
        if (column != null && column.getFormatCount() > 0) {
            return column.getFormat(0);
        }
        return null;
    }

    public @Nullable TypeAdapter findFirstFormat(int columnIndex) {
        final Column column = fileColumnIndexToColumnMap.get(columnIndex);
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

    public ObjList<Column> getColumnList() {
        return columnList;
    }

    public IntObjHashMap<Column> getFileColumnIndexToColumnMap() {
        return fileColumnIndexToColumnMap;
    }

    public LowerCaseCharSequenceObjHashMap<Column> getFileColumnNameToColumnMap() {
        return fileColumnNameToColumnMap;
    }

    public int getFormatsAction() {
        return formatsAction;
    }

    public LowerCaseCharSequenceObjHashMap<Column> getTableColumnNameToColumnMap() {
        return tableColumnNameToColumnMap;
    }

    public void setFormatsAction(int formatsAction) {
        this.formatsAction = formatsAction;
    }

    @Override
    public void toSink(CharSink<?> sink) {
        sink.putAscii('{');
        sink.putAscii("\"columns\":").putAscii('[');
        for (int i = 0, n = columnList.size(); i < n; i++) {
            columnList.getQuick(i).toSink(sink);
        }
        sink.putAscii(']').putAscii(',');
        sink.putAsciiQuoted("formats").putAscii(": {");
        int formatCount = 0;
        for (int i = 0; i < ColumnType.NULL; i++) {
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
                sink.putAscii(']');
            }
        }
        sink.putAscii('}').putAscii(',');
        sink.putAsciiQuoted("formats_action").putAscii(':').putAsciiQuoted(formatsAction == FORMATS_ACTION_ADD ? "ADD" : (formatsAction == FORMATS_ACTION_REPLACE ? "REPLACE" : "UNKNOWN"));
        sink.putAscii('}');
    }
}
