/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

/**
 * Retrieve metadata of a table record (row) such as the column count, the type of column by numeric index,
 * the name of a column by numeric index, the storage block capacity of
 * indexed symbols, and the numeric index of a designated timestamp column.
 *
 * Types are defined in {@link io.questdb.cairo.ColumnType}
 */
public interface RecordMetadata extends ColumnTypes {

    int COLUMN_NOT_FOUND = -1;

    /**
     * @return the number of columns in a record
     */
    int getColumnCount();

    /**
     * Return the type of column by index. Returns an integer defined
     * in {@link io.questdb.cairo.ColumnType}
     *
     * @param columnIndex numeric index of a column
     * @return integer value indicating the type of the column
     */
    int getColumnType(int columnIndex);

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
     * @param lo the low boundary index of the columnName chars, inclusive
     * @param hi the hi boundary index of the columnName chars, exclusive
     * @return index of the column
     */
    int getColumnIndexQuiet(CharSequence columnName, int lo, int hi);

    /** Retrieves column name.
     *
     * @param columnIndex numeric index of the column
     * @return name of the column
     */
    String getColumnName(int columnIndex);

    /**
     * Retrieves column hash. Hash augments the name to ensure when column is removed and
     * then added with the same name the clients do not perceive this event as no-change.
     *
     * @param columnIndex numeric index of the column
     * @return hash value
     */
    long getColumnHash(int columnIndex);

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
     * How many row IDs to store in a single storage block on disk for an indexed column.
     * Fewer blocks used to store row IDs achieves better performance.
     *
     * @param columnIndex numeric index of the column
     * @return number of row IDs per block
     */
    int getIndexValueBlockCapacity(int columnIndex);

    /**
     * How many row IDs to store in a single storage block on disk for an indexed column.
     * Fewer blocks used to store row IDs achieves better performance.
     *
     * @param columnName name of the column
     * @return number of row IDs per block
     */
    default int getIndexValueBlockCapacity(CharSequence columnName) {
        return getIndexValueBlockCapacity(getColumnIndex(columnName));
    }

    /**
     * @return the numeric index of the designated timestamp column.
     */
    int getTimestampIndex();

    /**
     * Writing index for the column
     *
     * @param columnIndex column index
     * @return writing index
     */
    int getWriterIndex(int columnIndex);

    /**
     * @param columnName name of the column
     * @return true if symbol table is static, otherwise false.
     */
    default boolean isSymbolTableStatic(CharSequence columnName) {
        return isSymbolTableStatic(getColumnIndex(columnName));
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
     * @param columnIndex numeric index of the column
     * @return true if column is indexed, otherwise false.
     */
    boolean isColumnIndexed(int columnIndex);

    /**
     * @param columnIndex numeric index of the column
     * @return true if symbol table is static, otherwise false.
     */
    boolean isSymbolTableStatic(int columnIndex);

    default boolean isWalEnabled() {
        return false;
    }

    /**
     * Create a JSON object with record metadata
     *
     * @param sink a character sink to write to
     */
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
