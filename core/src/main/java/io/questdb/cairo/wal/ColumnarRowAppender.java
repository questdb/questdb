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

package io.questdb.cairo.wal;

import io.questdb.cutlass.ilpv4.protocol.IlpV4BooleanColumnCursor;
import io.questdb.cutlass.ilpv4.protocol.IlpV4StringColumnCursor;
import io.questdb.cutlass.ilpv4.protocol.IlpV4SymbolColumnCursor;

/**
 * Interface for bulk column-oriented row appending to WAL.
 * <p>
 * This interface provides an alternative to the row-by-row {@link io.questdb.cairo.TableWriter.Row} API,
 * allowing entire columns to be written at once. This is particularly efficient when the source
 * data is already in columnar format (like ILP v4).
 * <p>
 * <b>Lifecycle:</b>
 * <ol>
 *   <li>Call {@link #beginColumnarWrite(int)} with the row count</li>
 *   <li>Write each column using the appropriate putXxxColumn method</li>
 *   <li>Call {@link #endColumnarWrite(long, long, boolean)} to finalize</li>
 * </ol>
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
     * the caller must write all columns and then call either
     * {@link #endColumnarWrite(long, long, boolean)} or {@link #cancelColumnarWrite()}.
     *
     * @param rowCount number of rows to be written
     */
    void beginColumnarWrite(int rowCount);

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
     * Writes the designated timestamp column.
     * <p>
     * The designated timestamp has special handling: in WAL format, it stores 128-bit
     * entries with (timestamp, rowId) pairs for O3 (out-of-order) processing.
     *
     * @param columnIndex       the column index in the table
     * @param valuesAddress     address of timestamp values
     * @param valueCount        number of non-null values
     * @param nullBitmapAddress address of null bitmap (0 if not nullable)
     * @param rowCount          total number of rows
     * @param startRowId        starting row ID for this batch
     */
    void putTimestampColumn(int columnIndex, long valuesAddress, int valueCount,
                            long nullBitmapAddress, int rowCount, long startRowId);

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
    void putVarcharColumn(int columnIndex, IlpV4StringColumnCursor cursor, int rowCount);

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
    void putStringColumn(int columnIndex, IlpV4StringColumnCursor cursor, int rowCount);

    /**
     * Writes a SYMBOL column.
     * <p>
     * Symbol values need to be resolved through the symbol table. Returns false
     * if symbol resolution fails (e.g., new symbol that can't be created).
     *
     * @param columnIndex the column index in the table
     * @param cursor      the symbol column cursor
     * @param rowCount    total number of rows
     * @return true if all symbols were written successfully, false if resolution failed
     */
    boolean putSymbolColumn(int columnIndex, IlpV4SymbolColumnCursor cursor, int rowCount);

    /**
     * Writes a BOOLEAN column.
     * <p>
     * Boolean values are bit-packed in ILP v4 wire format but stored as bytes in WAL.
     *
     * @param columnIndex the column index in the table
     * @param cursor      the boolean column cursor
     * @param rowCount    total number of rows
     */
    void putBooleanColumn(int columnIndex, IlpV4BooleanColumnCursor cursor, int rowCount);

    /**
     * Marks a column as all-null for the current batch.
     *
     * @param columnIndex the column index in the table
     * @param columnType  the column type (needed to write correct null sentinel)
     * @param rowCount    total number of rows
     */
    void putNullColumn(int columnIndex, int columnType, int rowCount);

    /**
     * Completes the columnar write operation.
     * <p>
     * Finalizes all written columns and updates internal state.
     *
     * @param minTimestamp the minimum timestamp in the written rows
     * @param maxTimestamp the maximum timestamp in the written rows
     * @param outOfOrder   whether rows are out of order
     */
    void endColumnarWrite(long minTimestamp, long maxTimestamp, boolean outOfOrder);

    /**
     * Cancels the current columnar write operation.
     * <p>
     * Rolls back any partially written column data. Must be called if an error
     * occurs after {@link #beginColumnarWrite(int)} but before
     * {@link #endColumnarWrite(long, long, boolean)}.
     */
    void cancelColumnarWrite();
}
