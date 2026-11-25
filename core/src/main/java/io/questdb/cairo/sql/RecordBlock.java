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

package io.questdb.cairo.sql;

/**
 * A run of a cursor's next rows, readable column by column. See
 * {@link RecordCursor#peekRecordBlock(int)}.
 * <p>
 * A column with a non-zero {@link #getColumnAddress(int) address} holds the block's rows in
 * memory, the value of row {@code r} at {@code address + r * stride}, in the column type's
 * fixed-size storage layout: the same bits the {@link Record} getter of that type returns. A block
 * that gathers its rows from scattered positions, such as a filter's selected rows, lists them
 * instead: when the column's {@link #getColumnRowIndexesAddress(int) row indexes} are non-zero, the
 * value of row {@code r} is at {@code address + rowIndex(r) * stride}, where {@code rowIndex(r)} is
 * the r-th long of that list. A
 * SYMBOL column holds the symbol keys, resolved by the cursor's
 * {@link RecordCursor#getSymbolTable(int) symbol table}; a BOOLEAN one, one byte per row, true
 * when it is 1, as {@link Record#getBool(int)} reads it. A
 * column with address 0 (any variable-size column, and any column the producer does not lay out
 * this way) is read through {@link #getRecordAt(int)}.
 * <p>
 * A block is valid until the cursor moves: the next {@code hasNext()},
 * {@code peekRecordBlock()}, {@code skipRecordBlock()}, {@code toTop()} or {@code close()}.
 */
public interface RecordBlock {

    /**
     * @return the address of row 0's value of the column, or 0 when the column is read through
     * {@link #getRecordAt(int)} only
     */
    long getColumnAddress(int columnIndex);

    /**
     * @return the bytes between the values of consecutive rows of a column with a non-zero
     * address
     */
    long getColumnStride(int columnIndex);

    /**
     * The row indexes a column's values are gathered by, see {@link #getRowIndexesAddress()}: the
     * block's, unless the column's values are laid out otherwise, such as computed for the block's
     * rows, one after another.
     *
     * @return the address of the first row's index, or 0 when row {@code r}'s value is at position
     * {@code r}
     */
    default long getColumnRowIndexesAddress(int columnIndex) {
        return getRowIndexesAddress();
    }

    /**
     * The block's row indexes, for a block that gathers its rows: one long per row, in the block's
     * row order, each the position of the row's values in every column with a non-zero address, in
     * units of that column's stride. The list is valid as long as the block is.
     *
     * @return the address of the first row's index, or 0 when the block's rows are consecutive, row
     * {@code r} at position {@code r}
     */
    default long getRowIndexesAddress() {
        return 0;
    }

    /**
     * Positions a record at a row of the block and returns it. The record may be the cursor's own
     * {@link RecordCursor#getRecord()}, which is undefined after a block's use until the next
     * {@code hasNext()}. The next call repositions it.
     *
     * @param row the row, 0-based, below {@link #getRowCount()}
     */
    Record getRecordAt(int row);

    /**
     * @return the rows in the block, at least 1
     */
    int getRowCount();
}
