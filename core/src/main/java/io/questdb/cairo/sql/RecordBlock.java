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
 * fixed-size storage layout: the same bits the {@link Record} getter of that type returns. A
 * SYMBOL column holds the symbol keys, resolved by the cursor's
 * {@link RecordCursor#getSymbolTable(int) symbol table}; a BOOLEAN one, one byte per row. A
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
