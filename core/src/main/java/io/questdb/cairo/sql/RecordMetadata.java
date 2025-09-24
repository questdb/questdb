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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.str.Utf16Sink;

/**
 * Retrieve metadata of a table record (row) such as the column count, the type of column by numeric index,
 * the name of a column by numeric index, the storage block capacity of
 * indexed symbols, and the numeric index of a designated timestamp column.
 * <p>
 * Types are defined in {@link io.questdb.cairo.ColumnType}
 */
public interface RecordMetadata extends ColumnTypes, Plannable {

    int COLUMN_NOT_FOUND = -1;

    /**
     * @return the number of columns in a record
     */
    int getColumnCount();

    /**
     * Gets the numeric index of a column by name
     *
     * @param columnName name of the column
     * @return numeric index of the column
     */
    default int getColumnIndex(CharSequence columnName) {
        final int columnIndex = getColumnIndexQuiet(columnName);
        if (columnIndex > -1) {
            return columnIndex;
        }
        throw InvalidColumnException.INSTANCE;
    }

    /**
     * Performs the same function as {@link #getColumnIndex(CharSequence)}
     * but will not throw an exception if the column does not exist.
     *
     * @param columnName name of the column
     * @return index of the column
     */
    default int getColumnIndexQuiet(CharSequence columnName) {
        return getColumnIndexQuiet(columnName, 0, columnName.length());
    }

    /**
     * Gets the numeric index of a column by name.
     * Will not throw an exception if the column does not exist.
     *
     * @param columnName name of the column
     * @param lo         the low boundary index of the columnName chars, inclusive
     * @param hi         the hi boundary index of the columnName chars, exclusive
     * @return index of the column
     */
    int getColumnIndexQuiet(CharSequence columnName, int lo, int hi);

    TableColumnMetadata getColumnMetadata(int columnIndex);

    /**
     * Retrieves column name.
     *
     * @param columnIndex numeric index of the column
     * @return name of the column
     */
    String getColumnName(int columnIndex);

    /**
     * Return the type of column by index. Returns an integer defined
     * in {@link io.questdb.cairo.ColumnType}
     *
     * @param columnIndex numeric index of a column
     * @return integer value indicating the type of the column
     */
    int getColumnType(int columnIndex);

    /**
     * Return the type of column by name
     *
     * @param columnName name of the column
     * @return the type of the column
     */
    default int getColumnType(CharSequence columnName) {
        return getColumnType(getColumnIndex(columnName));
    }

    /**
     * The returned value defines how many row IDs to store in a single storage block on disk
     * for an indexed column. Fewer blocks used to store row IDs achieves better performance.
     *
     * @param columnIndex numeric index of the column
     * @return number of row IDs per block
     */
    int getIndexValueBlockCapacity(int columnIndex);

    /**
     * The returned value defines how many row IDs to store in a single storage block on disk
     * for an indexed column. Fewer blocks used to store row IDs achieves better performance.
     *
     * @param columnName name of the column
     * @return number of row IDs per block
     */
    default int getIndexValueBlockCapacity(CharSequence columnName) {
        return getIndexValueBlockCapacity(getColumnIndex(columnName));
    }

    /**
     * Access column metadata, i.e. getMetadata(1).getTimestampIndex()
     *
     * @param columnIndex numeric index of the column
     * @return TableReaderMetadata
     */
    default RecordMetadata getMetadata(int columnIndex) {
        return null;
    }

    /**
     * @return the numeric index of the designated timestamp column.
     */
    int getTimestampIndex();

    default int getTimestampType() {
        int timestampIndex = getTimestampIndex();
        if (timestampIndex < 0) {
            return ColumnType.NULL;
        }
        return getColumnType(timestampIndex);
    }

    /**
     * Writing index for the column
     *
     * @param columnIndex column index
     * @return writing index
     */
    int getWriterIndex(int columnIndex);

    /**
     * @param columnIndex column index
     * @return true if the column with the given column index is present, otherwise false.
     */
    boolean hasColumn(int columnIndex);

    /**
     * @param columnIndex numeric index of the column
     * @return true if column is indexed, otherwise false.
     */
    boolean isColumnIndexed(int columnIndex);

    /**
     * @param columnIndex numeric index of the column
     * @return true if column is part of deduplication key used in inserts.
     */
    boolean isDedupKey(int columnIndex);

    /**
     * @param columnName name of the column
     * @return true if symbol table is static, otherwise false.
     */
    default boolean isSymbolTableStatic(CharSequence columnName) {
        return isSymbolTableStatic(getColumnIndex(columnName));
    }

    /**
     * @param columnIndex numeric index of the column
     * @return true if symbol table is static, otherwise false.
     */
    boolean isSymbolTableStatic(int columnIndex);

    /**
     * @return true if the record is from WAL enabled table, otherwise false.
     */
    default boolean isWalEnabled() {
        return false;
    }

    /**
     * Create a JSON object with record metadata
     *
     * @param sink a character sink to write to
     */
    default void toJson(Utf16Sink sink) {
        sink.put('{');
        sink.putAsciiQuoted("columnCount").put(':').put(getColumnCount());
        sink.put(',');
        sink.putAsciiQuoted("columns").put(':');
        sink.put('[');
        for (int i = 0, n = getColumnCount(); i < n; i++) {
            final int type = getColumnType(i);
            if (i > 0) {
                sink.putAscii(',');
            }
            sink.put('{');
            sink.putAsciiQuoted("index").putAscii(':').put(i).putAscii(',');
            sink.putAsciiQuoted("name").putAscii(':').putQuoted(getColumnName(i)).putAscii(',');
            sink.putAsciiQuoted("type").putAscii(':').putQuoted(ColumnType.nameOf(type));
            if (isColumnIndexed(i)) {
                sink.putAscii(',').putAsciiQuoted("indexed").putAscii(":true");
                sink.putAscii(',').putAsciiQuoted("indexValueBlockCapacity").putAscii(':').put(getIndexValueBlockCapacity(i));
            }
            sink.putAscii('}');
        }
        sink.putAscii(']');
        sink.putAscii(',').putAsciiQuoted("timestampIndex").putAscii(':').put(getTimestampIndex());
        sink.putAscii('}');
    }

    @Override
    default void toPlan(PlanSink sink) {
        for (int i = 0, n = getColumnCount(); i < n; i++) {
            if (i > 0) {
                sink.val(',');
            }
            sink.val(getColumnName(i));
        }
    }
}
