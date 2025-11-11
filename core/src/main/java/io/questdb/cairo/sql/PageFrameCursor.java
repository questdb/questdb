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

import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * A cursor interface for iterating over page frames in QuestDB tables.
 * <p>
 * PageFrameCursor provides efficient access to table data organized in page frames,
 * supporting both full data access and lightweight metadata-only operations for
 * scenarios like skip/limit queries. It extends QuietCloseable for resource management
 * and SymbolTableSource for symbol table access.
 * <p>
 * The cursor supports:
 * <ul>
 * <li>Sequential iteration through page frames with {@link #next()}</li>
 * <li>Skip-aware iteration with {@link #next(long)} for efficient limit queries</li>
 * <li>Size calculation and metadata access</li>
 * <li>Column index mapping for query execution</li>
 * <li>Symbol table access for symbol columns</li>
 * </ul>
 */
public interface PageFrameCursor extends QuietCloseable, SymbolTableSource {

    /**
     * Calculates the number of rows left in the cursor across all page frames, and updates the provided counter.
     * <p>
     * This method provides an efficient way to determine the number of remaining rows without having to
     * iterate through all page frames. The implementation should update the counter with the calculated
     * size information.
     * <p>
     * Note: This method is only guaranteed to work correctly if {@link #supportsSizeCalculation()}
     * returns {@code true}.
     *
     * @param counter the counter object to be updated with size information
     * @see #supportsSizeCalculation()
     * @see #size()
     */
    void calculateSize(RecordCursor.Counter counter);

    /**
     * Returns local (query) to table reader index mapping.
     * Used to map local column indexes to indexes from the Parquet file.
     * Such mapping requires knowing the corresponding table reader indexes.
     */
    IntList getColumnIndexes();

    /**
     * Returns the number of rows remaining in the current interval that have not yet been
     * processed by this cursor.
     * <p>
     * This method provides an efficient count of unprocessed rows in the interval currently
     * being scanned, without requiring iteration through the data. It is primarily used for
     * size calculation operations in conjunction with {@link #calculateSize(RecordCursor.Counter)},
     * allowing quick determination of the total number of rows available in the cursor.
     * <p>
     * The returned value represents rows in the current interval only. For cursors spanning
     * multiple intervals and/or partitions, this count does not include rows from intervals/partitions
     * that have not yet been visited.
     *
     * @return the number of unprocessed rows remaining in the current partition
     * @see #calculateSize(RecordCursor.Counter)
     * @see #size()
     */
    long getRemainingRowsInInterval();

    /**
     * Returns the symbol table for the specified column index.
     * <p>
     * This method provides access to symbol tables for symbol columns, allowing efficient
     * lookup and resolution of symbol values. The column index should correspond to the
     * local query column index.
     *
     * @param columnIndex the local column index for which to retrieve the symbol table
     * @return the symbol table for the specified column, or {@code null} if the column is not a symbol column
     * @throws IllegalArgumentException if the column index is invalid
     */
    @Override
    StaticSymbolTable getSymbolTable(int columnIndex);

    /**
     * Returns true if the cursor belongs to an external parquet file, false in case of table partition files.
     */
    boolean isExternal();

    /**
     * Fetches the next page frame without skipping any rows.
     * <p>
     * This is a convenience method equivalent to calling {@link #next(long)} with a skip target of 0.
     *
     * @return the next page frame, or {@code null} if no more frames are available
     * @see #next(long)
     */
    default PageFrame next() {
        return next(0);
    }

    /**
     * Fetches the next page frame taking into account the skip target. The skip target is the
     * number of rows the outer code is looking to skip. This typically occurs in "limit" queries
     * where not all rows of the dataset are fetched.
     * <p>
     * When the next frame size is under the skip target, the implementor is required to set at least these attributes
     * on the fetched frame:
     * <ul>
     * <li>partition lo</li>
     * <li>partition hi</li>
     * <li>partition index</li>
     * </ul>
     * <p>
     * This is a lightweight frame, which should be calculated purely from metadata, e.g. to avoid lifting any of the
     * table's data.
     * <p>
     * When partition size is over the skip target, even partially - a fully populated object must be produced.
     *
     * @param skipTarget the number of rows user wants to skip over in a limit SQL query
     * @return either a fully populated or lightweight frame, or {@code null} if no more frames are available
     */
    @Nullable
    PageFrame next(long skipTarget);

    /**
     * @return number of rows in all page frames
     */
    long size();

    /**
     * @return true if cursor supports fast size calculation,
     * i.e. {@link #calculateSize(RecordCursor.Counter)} is properly implemented.
     */
    boolean supportsSizeCalculation();

    /**
     * Returns the cursor to the beginning of the page frame.
     */
    void toTop();
}
