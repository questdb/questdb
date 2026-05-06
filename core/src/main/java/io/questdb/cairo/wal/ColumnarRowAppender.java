/*+*****************************************************************************
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

package io.questdb.cairo.wal;

import io.questdb.cutlass.line.tcp.ConnectionSymbolCache;
import io.questdb.cutlass.qwp.protocol.QwpArrayColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpBooleanColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpDecimalColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpFixedWidthColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpGeoHashColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpStringColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpSymbolColumnCursor;
import io.questdb.cutlass.qwp.protocol.QwpTimestampColumnCursor;

/**
 * Interface for bulk column-oriented row appending to WAL.
 * <p>
 * This interface provides an alternative to the row-by-row {@link io.questdb.cairo.TableWriter.Row} API,
 * allowing entire columns to be written at once. This is particularly efficient when the source
 * data is already in columnar format (like QWP v1).
 * <p>
 * <b>Lifecycle:</b>
 * <ol>
 *   <li>Call {@link #beginColumnarWrite(int)} with the row count</li>
 *   <li>Write any present columns using the appropriate putXxxColumn method</li>
 *   <li>Call {@link #endColumnarWrite(long, long, boolean)} to finalize</li>
 * </ol>
 * <p>
 * Regular table columns may be omitted for a batch. Omitted columns are filled with NULL values
 * for all rows when {@link #endColumnarWrite(long, long, boolean)} is called. Columns that are
 * written must provide values for the full batch size, encoding per-row NULLs in the column data
 * itself (for example via a null bitmap). The designated timestamp column is special: it must be
 * written explicitly for the batch, unless the caller uses a separate server-assigned timestamp path.
 * <p>
 * <b>Error handling:</b> If any error occurs during column writes, call {@link #cancelColumnarWrite()}
 * to rollback the partial write.
 * <p>
 * <b>Performance benefits:</b>
 * <ul>
 *   <li>Fixed-width columns with no nulls: direct memcpy from wire format</li>
 *   <li>Better cache locality: process entire column before moving to next</li>
 *   <li>Reduced per-row overhead: no virtual method calls per row</li>
 * </ul>
 */
public interface ColumnarRowAppender {

    /**
     * Begins a columnar write operation.
     * <p>
     * Must be called before any putXxxColumn methods. After this call,
     * the caller may write any subset of regular columns and then call either
     * {@link #endColumnarWrite(long, long, boolean)} or {@link #cancelColumnarWrite()}.
     * Regular columns omitted from the batch are NULL-filled on finalize. The designated timestamp
     * column remains mandatory unless the caller uses a separate server-assigned timestamp path.
     *
     * @param rowCount number of rows to be written
     */
    void beginColumnarWrite(int rowCount);

    /**
     * Cancels the current columnar write operation.
     * <p>
     * Rolls back any partially written column data. Must be called if an error
     * occurs after {@link #beginColumnarWrite(int)} but before
     * {@link #endColumnarWrite(long, long, boolean)}.
     */
    void cancelColumnarWrite();

    /**
     * Completes the columnar write operation.
     * <p>
     * Finalizes all written columns, NULL-fills any omitted regular columns for the batch, and
     * updates internal state. This method expects each written column to cover the full batch size.
     *
     * @param minTimestamp the minimum timestamp in the written rows
     * @param maxTimestamp the maximum timestamp in the written rows
     * @param outOfOrder   whether rows are out of order
     */
    void endColumnarWrite(long minTimestamp, long maxTimestamp, boolean outOfOrder);

    /**
     * Writes an array column (double or long arrays, 1-N dimensions).
     * <p>
     * Handles multi-dimensional arrays by building the array structure from the cursor
     * and using ArrayTypeDriver for storage.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the array column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (encoded array type)
     * @throws QwpParseException if cursor iteration fails
     */
    void putArrayColumn(int columnIndex, QwpArrayColumnCursor cursor,
                        int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a BOOLEAN column.
     * <p>
     * Boolean values are bit-packed in QWP v1 wire format but stored as bytes in WAL.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the boolean column cursor
     * @param rowCount    total number of rows
     */
    void putBooleanColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount);

    /**
     * Writes a BOOLEAN column cursor to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the boolean column cursor
     * @param rowCount    total number of rows
     */
    void putBooleanToStringColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount);

    /**
     * Writes a BOOLEAN column cursor to a numeric column (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE).
     * <p>
     * Converts true to 1, false to 0, null to the type's null sentinel.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the boolean column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type
     */
    void putBooleanToNumericColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount, int columnType);

    /**
     * Writes a BOOLEAN column cursor to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the boolean column cursor
     * @param rowCount    total number of rows
     */
    void putBooleanToVarcharColumn(int columnIndex, QwpBooleanColumnCursor cursor, int rowCount);

    /**
     * Writes a CHAR column from string data.
     * <p>
     * Extracts the first character from each string value and writes it as a 2-byte char.
     * For single ASCII bytes, the byte is cast directly to char. For multi-byte UTF-8,
     * the first codepoint is decoded.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putCharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a DECIMAL128 column with optional scale conversion.
     * <p>
     * If the wire scale differs from the column scale, values are rescaled using Decimal256.rescale().
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     * @throws QwpParseException if cursor iteration fails
     */
    void putDecimal128Column(int columnIndex, QwpDecimalColumnCursor cursor,
                             int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a DECIMAL256 column with optional scale conversion.
     * <p>
     * If the wire scale differs from the column scale, values are rescaled using Decimal256.rescale().
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     * @throws QwpParseException if cursor iteration fails
     */
    void putDecimal256Column(int columnIndex, QwpDecimalColumnCursor cursor,
                             int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a DECIMAL64 column with optional scale conversion.
     * <p>
     * If the wire scale differs from the column scale, values are rescaled using Decimal256.rescale().
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     * @throws QwpParseException if cursor iteration fails
     */
    void putDecimal64Column(int columnIndex, QwpDecimalColumnCursor cursor,
                            int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a wire DECIMAL64/128/256 column to a DECIMAL8, DECIMAL16, or DECIMAL32 column.
     * <p>
     * Loads the wire decimal value into Decimal256, rescales to the target column's scale,
     * and writes the narrowed result.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     * @throws QwpParseException if cursor iteration fails
     */
    void putDecimalToSmallDecimalColumn(int columnIndex, QwpDecimalColumnCursor cursor,
                                        int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a DECIMAL column cursor to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     */
    void putDecimalToStringColumn(int columnIndex, QwpDecimalColumnCursor cursor, int rowCount);

    /**
     * Writes a DECIMAL column cursor to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the decimal column cursor
     * @param rowCount    total number of rows
     */
    void putDecimalToVarcharColumn(int columnIndex, QwpDecimalColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width column with potential nulls.
     * <p>
     * Handles sparse-to-dense null expansion: the wire format has packed non-null values
     * with a null bitmap, while WAL storage has null sentinels inline.
     * <p>
     * <b>Fast path:</b> When nullBitmapAddress is 0 (no nulls), performs direct memcpy.
     *
     * @param columnIndex       the column index in the table
     * @param valuesAddress     address of packed non-null values
     * @param valueCount        number of non-null values
     * @param valueSize         size of each value in bytes
     * @param nullBitmapAddress address of null bitmap (0 if column is not nullable or has no nulls)
     * @param rowCount          total number of rows (including nulls)
     */
    void putFixedColumn(int columnIndex, long valuesAddress, int valueCount,
                        int valueSize, long nullBitmapAddress, int rowCount);

    /**
     * Writes a fixed-width column with non-numeric wire types (UUID, LONG256, DATE, TIMESTAMP, CHAR)
     * to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param ilpType     the QWP wire type code
     */
    void putFixedOtherToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, byte ilpType);

    /**
     * Writes a fixed-width column with non-numeric wire types (UUID, LONG256, DATE, TIMESTAMP, CHAR)
     * to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param ilpType     the QWP wire type code
     */
    void putFixedOtherToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount, byte ilpType);

    /**
     * Writes a fixed-width integer column to a DECIMAL128 column with scale conversion.
     * <p>
     * Reads integer values from the cursor, treats them as unscaled decimals with scale=0,
     * and rescales to the target column's scale.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putFixedToDecimal128Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                    int rowCount, int columnType);

    /**
     * Writes a fixed-width integer column to a DECIMAL256 column with scale conversion.
     * <p>
     * Reads integer values from the cursor, treats them as unscaled decimals with scale=0,
     * and rescales to the target column's scale.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putFixedToDecimal256Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                    int rowCount, int columnType);

    /**
     * Writes a fixed-width integer column to a DECIMAL64 column with scale conversion.
     * <p>
     * Reads integer values from the cursor, treats them as unscaled decimals with scale=0,
     * and rescales to the target column's scale.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putFixedToDecimal64Column(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                   int rowCount, int columnType);

    /**
     * Writes a fixed-width integer column to a DECIMAL8, DECIMAL16, or DECIMAL32 column.
     * <p>
     * Reads integer values from the cursor, treats them as unscaled decimals with scale=0,
     * rescales to the target column's scale, and writes the narrowed result.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putFixedToSmallDecimalColumn(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                      int rowCount, int columnType);

    /**
     * Writes a fixed-width integer column to a STRING column.
     * <p>
     * Reads integer values from the cursor and converts them to their string representation.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     */
    void putFixedToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width integer column to a SYMBOL column.
     * <p>
     * Converts the integer value to its string representation and resolves it
     * as a symbol value through the symbol table.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     */
    void putFixedToSymbolColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width integer column to a VARCHAR column.
     * <p>
     * Reads integer values from the cursor and converts them to their string representation.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     */
    void putFixedToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width float/double column to any DECIMAL column with scale conversion.
     * <p>
     * Reads floating-point values from the cursor via getDouble() and converts to
     * the target decimal type using Decimal256.fromDouble().
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor (FLOAT or DOUBLE wire type)
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putFloatToDecimalColumn(int columnIndex, QwpFixedWidthColumnCursor cursor,
                                 int rowCount, int columnType);

    /**
     * Writes a fixed-width float/double column to a numeric column with value conversion.
     * <p>
     * Handles float→integer (with whole-number and range check),
     * float widening (FLOAT→DOUBLE), and float narrowing (DOUBLE→FLOAT).
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor (FLOAT or DOUBLE wire type)
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type
     */
    void putFloatToNumericColumn(
            int columnIndex,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType
    );

    /**
     * Writes a fixed-width float/double column to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor (FLOAT or DOUBLE wire type)
     * @param rowCount    total number of rows
     */
    void putFloatToStringColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width float/double column to a SYMBOL column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor (FLOAT or DOUBLE wire type)
     * @param rowCount    total number of rows
     */
    void putFloatToSymbolColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a fixed-width float/double column to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor (FLOAT or DOUBLE wire type)
     * @param rowCount    total number of rows
     */
    void putFloatToVarcharColumn(int columnIndex, QwpFixedWidthColumnCursor cursor, int rowCount);

    /**
     * Writes a GeoHash column.
     * <p>
     * Handles the variable-width GeoHash wire format (1-8 bytes based on precision)
     * and writes to the appropriate storage size based on column type (GEOBYTE/SHORT/INT/LONG).
     *
     * @param columnIndex the column index in the table
     * @param cursor      the GeoHash column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (GEOBYTE, GEOSHORT, GEOINT, or GEOLONG)
     * @throws QwpParseException if cursor iteration fails
     */
    void putGeoHashColumn(int columnIndex, QwpGeoHashColumnCursor cursor,
                          int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a GeoHash column cursor to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the GeoHash column cursor
     * @param rowCount    total number of rows
     */
    void putGeoHashToStringColumn(int columnIndex, QwpGeoHashColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a GeoHash column cursor to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the GeoHash column cursor
     * @param rowCount    total number of rows
     */
    void putGeoHashToVarcharColumn(int columnIndex, QwpGeoHashColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a fixed-width integer column to a numeric column with value conversion.
     * <p>
     * Handles integer→integer widening (e.g., INT→LONG) and integer→float/double
     * cross-family coercion. Values are read via getLong() and cast to the target type.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the fixed-width column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type
     */
    void putIntegerToNumericColumn(
            int columnIndex,
            QwpFixedWidthColumnCursor cursor,
            int rowCount,
            int columnType
    );

    /**
     * Writes a STRING column.
     * <p>
     * STRING uses a different storage format than VARCHAR (legacy format with offset pointers).
     * Values are converted from UTF-8 to UTF-16 for storage.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putStringColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a BOOLEAN column.
     * <p>
     * Parses string values as booleans: "true"/"1" → true, "false"/"0" → false (case-insensitive).
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putStringToBooleanColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a DECIMAL column with scale conversion.
     * <p>
     * Parses string values as BigDecimal and converts to the target decimal type.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (includes scale metadata)
     */
    void putStringToDecimalColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a GeoHash column.
     * <p>
     * Parses string values as geohash strings and truncates to the target precision.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (GEOBYTE, GEOSHORT, GEOINT, or GEOLONG)
     */
    void putStringToGeoHashColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a LONG256 column.
     * <p>
     * Parses string values as hex-encoded Long256 values.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putStringToLong256Column(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a numeric column (BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE).
     * <p>
     * Parses string values using the appropriate numeric parser for the target type.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type
     */
    void putStringToNumericColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a SYMBOL column.
     * <p>
     * Resolves string values through the symbol table.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putStringToSymbolColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a TIMESTAMP column.
     * <p>
     * Parses string values as ISO-8601 timestamps.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     * @param columnType  the target QuestDB column type (TIMESTAMP or TIMESTAMP_NANO)
     */
    void putStringToTimestampColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount, int columnType) throws QwpParseException;

    /**
     * Writes a STRING column cursor to a UUID column.
     * <p>
     * Parses string values as UUID strings (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putStringToUuidColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;

    /**
     * Writes a SYMBOL column.
     * <p>
     * Symbol values are resolved through the symbol table. Throws
     * {@link io.questdb.cairo.CairoException} if symbol resolution fails.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the symbol column cursor
     * @param rowCount    total number of rows
     */
    void putSymbolColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount);

    /**
     * Writes a SYMBOL column with optional caching support.
     * <p>
     * When symbolCache is provided and cursor is in delta mode, avoids
     * string allocation on cache hits by mapping clientSymbolId → tableSymbolId.
     * <p>
     * Cache behavior:
     * <ul>
     *   <li>Cache hit: writes cached tableSymbolId, skips string allocation, records hit</li>
     *   <li>Cache miss + committed symbol: resolves, caches, writes, records miss</li>
     *   <li>Cache miss + uncommitted symbol: resolves, DON'T cache (unstable), writes, records miss</li>
     *   <li>Watermark changed: clears column cache before processing</li>
     * </ul>
     *
     * @param columnIndex        the column index in the table
     * @param cursor             the symbol column cursor
     * @param rowCount           total number of rows
     * @param symbolCache        connection-level symbol cache (null to disable caching)
     * @param tableId            table ID for per-column cache lookup
     * @param initialSymbolCount committed symbol count for stability check
     */
    void putSymbolColumn(
            int columnIndex,
            QwpSymbolColumnCursor cursor,
            int rowCount,
            ConnectionSymbolCache symbolCache,
            long tableId,
            int initialSymbolCount
    );

    /**
     * Writes a SYMBOL column cursor to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the symbol column cursor
     * @param rowCount    total number of rows
     */
    void putSymbolToStringColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount);

    /**
     * Writes a SYMBOL column cursor to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the symbol column cursor
     * @param rowCount    total number of rows
     */
    void putSymbolToVarcharColumn(int columnIndex, QwpSymbolColumnCursor cursor, int rowCount);

    /**
     * Writes the designated timestamp column.
     * <p>
     * The designated timestamp has special handling: in WAL format, it stores 128-bit
     * entries with (timestamp, rowId) pairs for O3 (out-of-order) processing.
     * <p>
     * When {@code serverTimestampMicros} is not {@link io.questdb.std.Numbers#LONG_NULL},
     * null designated timestamp rows use this value instead of storing NULL (atNow semantics).
     *
     * @param columnIndex           the column index in the table
     * @param valuesAddress         address of timestamp values
     * @param valueCount            number of non-null values
     * @param nullBitmapAddress     address of null bitmap (0 if not nullable)
     * @param rowCount              total number of rows
     * @param serverTimestampMicros server timestamp to use for null rows, or Numbers.LONG_NULL
     */
    void putTimestampColumn(int columnIndex, long valuesAddress, int valueCount,
                            long nullBitmapAddress, int rowCount,
                            long serverTimestampMicros);

    /**
     * Writes a TIMESTAMP column with precision conversion.
     * <p>
     * Handles conversion between microseconds and nanoseconds based on ILP wire format
     * and target column type:
     * <ul>
     *   <li>If ilpType is TYPE_TIMESTAMP_NANOS and columnType is TIMESTAMP (micros): divide by 1000</li>
     *   <li>If ilpType is TYPE_TIMESTAMP and columnType is TIMESTAMP_NANO: multiply by 1000</li>
     * </ul>
     * <p>
     * This method handles both direct-access and Gorilla-encoded timestamp cursors.
     *
     * @param columnIndex     the column index in the table
     * @param cursor          the timestamp column cursor
     * @param rowCount        total number of rows
     * @param ilpType         the QWP wire type (TYPE_TIMESTAMP or TYPE_TIMESTAMP_NANOS)
     * @param columnType      the target QuestDB column type
     * @param isDesignated    whether this is the designated timestamp column
     * @param serverTimestamp server timestamp to use for null designated rows, or Numbers.LONG_NULL
     * @throws QwpParseException if cursor iteration fails
     */
    void putTimestampColumnWithConversion(
            int columnIndex,
            QwpTimestampColumnCursor cursor,
            int rowCount,
            byte ilpType,
            int columnType,
            boolean isDesignated,
            long serverTimestamp
    ) throws QwpParseException;

    /**
     * Writes a TIMESTAMP column cursor to a STRING column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the timestamp column cursor
     * @param rowCount    total number of rows
     * @param ilpType     the QWP wire type (for precision: micros vs nanos)
     */
    void putTimestampToStringColumn(int columnIndex, QwpTimestampColumnCursor cursor, int rowCount, byte ilpType);

    /**
     * Writes a TIMESTAMP column cursor to a VARCHAR column.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the timestamp column cursor
     * @param rowCount    total number of rows
     * @param ilpType     the QWP wire type (for precision: micros vs nanos)
     */
    void putTimestampToVarcharColumn(int columnIndex, QwpTimestampColumnCursor cursor, int rowCount, byte ilpType);

    /**
     * Writes a VARCHAR column.
     * <p>
     * Variable-length columns cannot be memcpy'd and must be processed value-by-value,
     * but the cursor provides zero-allocation access via DirectUtf8Sequence.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the string column cursor
     * @param rowCount    total number of rows
     */
    void putVarcharColumn(int columnIndex, QwpStringColumnCursor cursor, int rowCount) throws QwpParseException;
}
