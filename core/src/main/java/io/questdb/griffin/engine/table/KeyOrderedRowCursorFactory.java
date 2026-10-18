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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;

/**
 * A row cursor factory whose cursors emit the rows of a page frame key by key, in the key order a query requires, and
 * the rows of each key in its index direction. The record cursor reads such a factory key by key over all page frames,
 * so the keys stay ordered when the scan reads more than one page frame.
 */
interface KeyOrderedRowCursorFactory extends RowCursorFactory {

    /**
     * The cursor over the rows in the page frame of the keys at {@code keyLo}, inclusive, to {@code keyHi}, exclusive,
     * in key order.
     */
    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi);

    /**
     * The cursor over the rows in the page frame of the keys at {@code keyLo}, inclusive, to {@code keyHi}, exclusive,
     * in key order, where the rows of the key at {@code keyLo} start at the row {@code firstRow} of the partition in the
     * index direction. Rows keep their numbering from the start of the page frame.
     */
    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi, long firstRow);

    /**
     * The direction the cursors read the rows of each key in, {@link io.questdb.cairo.idx.IndexReader#DIR_FORWARD} or
     * {@link io.questdb.cairo.idx.IndexReader#DIR_BACKWARD}.
     */
    int getIndexDirection();

    /**
     * The number of keys, known once the cursors are prepared.
     */
    int getKeyCount();

    /**
     * The row the cursor of the key at {@code keyIndex} returns after it skips {@code rowIndex} of the rows
     * {@code rowLo} to {@code rowHi}, inclusive, of the partition of the page frame. A count of those rows from
     * {@link #getRowCount} must have shown that the key has more than {@code rowIndex} of them.
     */
    long getRow(PageFrame pageFrame, int keyIndex, long rowLo, long rowHi, long rowIndex);

    /**
     * The number of rows of the keys at {@code keyLo}, inclusive, to {@code keyHi}, exclusive, among the rows
     * {@code rowLo} to {@code rowHi}, inclusive, of the partition of the page frame, or any number above
     * {@code maxCount} once the count passes it, or -1 when the factory cannot tell without reading the rows. The
     * count reads the index outside those rows only on the shorter way to them from an end of each posting list, and
     * stops after {@code maxCount} rows.
     */
    long getRowCount(PageFrame pageFrame, int keyLo, int keyHi, long rowLo, long rowHi, long maxCount);
}
