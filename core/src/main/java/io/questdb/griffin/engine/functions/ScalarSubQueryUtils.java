/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;

/**
 * Helpers shared by the cursor-comparison factories (for example {@code col > (select ...)}),
 * where the right-hand operand is a scalar sub-query cursor expected to yield at most one row.
 */
public final class ScalarSubQueryUtils {

    private ScalarSubQueryUtils() {
    }

    /**
     * Enforces that a scalar sub-query cursor holds no further rows once its single value has
     * been consumed. Must be called only after the first row has already been read and its value
     * extracted into a stable (non-flyweight) field, because this advances the cursor by one row.
     *
     * @param cursor   the sub-query cursor, positioned on its first (already-read) row
     * @param position the parse position of the sub-query, used for the error marker
     * @throws SqlException if the cursor yields a second row
     */
    public static void assertNoMoreRows(RecordCursor cursor, int position) throws SqlException {
        if (cursor.hasNext()) {
            throw SqlException.$(position, "scalar sub-query returned more than one row");
        }
    }

    /**
     * Validates that a scalar sub-query factory provides exactly one column.
     *
     * @param factory  the sub-query cursor factory
     * @param position the parse position of the sub-query, used for the error marker
     * @return the factory metadata, for further column-type validation
     * @throws SqlException if the sub-query selects more than one column
     */
    public static RecordMetadata assertSingleColumn(RecordCursorFactory factory, int position) throws SqlException {
        final RecordMetadata metadata = factory.getMetadata();
        if (metadata.getColumnCount() != 1) {
            throw SqlException.$(position, "select must provide exactly one column");
        }
        return metadata;
    }

    /**
     * Reads the single-column scalar of a sub-query record widened to {@code double}. Typed integer
     * NULLs map to {@link Double#NaN} so the comparison follows QuestDB's sentinel-null convention
     * instead of comparing against a raw sentinel value: strict comparisons match no rows, while the
     * negated inclusive forms ({@code >=}, {@code <=}) match rows whose left operand is also null
     * (null equals null).
     *
     * @param record        the sub-query record positioned on the scalar row
     * @param columnTypeTag the {@link ColumnType} tag of the cursor column
     * @return the scalar as a double, or {@link Double#NaN} for null values
     */
    public static double readDoubleValue(Record record, int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.DOUBLE -> record.getDouble(0);
            case ColumnType.FLOAT -> record.getFloat(0);
            case ColumnType.LONG -> {
                final long l = record.getLong(0);
                yield l == Numbers.LONG_NULL ? Double.NaN : l;
            }
            case ColumnType.INT -> {
                final int i = record.getInt(0);
                yield i == Numbers.INT_NULL ? Double.NaN : i;
            }
            case ColumnType.SHORT -> record.getShort(0);
            case ColumnType.BYTE -> record.getByte(0);
            default -> Double.NaN;
        };
    }

    /**
     * Reads the single-column scalar of a sub-query record as an {@code int}. Only narrow integer
     * cursor columns are expected here; anything else (including the NULL type) yields
     * {@link Numbers#INT_NULL}, which follows QuestDB's sentinel-null convention: strict comparisons
     * match no rows, while the negated inclusive forms ({@code >=}, {@code <=}) match rows whose
     * left operand is also null (null equals null).
     *
     * @param record        the sub-query record positioned on the scalar row
     * @param columnTypeTag the {@link ColumnType} tag of the cursor column
     * @return the scalar as an int, or {@link Numbers#INT_NULL} for null values
     */
    public static int readIntValue(Record record, int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.BYTE -> record.getByte(0);
            case ColumnType.SHORT -> record.getShort(0);
            case ColumnType.INT -> record.getInt(0);
            default -> Numbers.INT_NULL;
        };
    }

    /**
     * Reads the single-column scalar of a sub-query record widened to {@code long}. An INT_NULL
     * cursor scalar maps to {@link Numbers#LONG_NULL} (via {@link Numbers#intToLong}) so it follows
     * QuestDB's sentinel-null convention rather than behaving like {@code Integer.MIN_VALUE}: strict
     * comparisons match no rows, while the negated inclusive forms ({@code >=}, {@code <=}) match
     * rows whose left operand is also null (null equals null).
     *
     * @param record        the sub-query record positioned on the scalar row
     * @param columnTypeTag the {@link ColumnType} tag of the cursor column
     * @return the scalar as a long, or {@link Numbers#LONG_NULL} for null values
     */
    public static long readLongValue(Record record, int columnTypeTag) {
        return switch (columnTypeTag) {
            case ColumnType.BYTE -> record.getByte(0);
            case ColumnType.SHORT -> record.getShort(0);
            case ColumnType.INT -> Numbers.intToLong(record.getInt(0));
            case ColumnType.LONG -> record.getLong(0);
            default -> Numbers.LONG_NULL;
        };
    }
}
