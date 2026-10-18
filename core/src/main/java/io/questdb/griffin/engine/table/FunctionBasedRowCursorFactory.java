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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;

public interface FunctionBasedRowCursorFactory extends RowCursorFactory {

    /**
     * The cursor over the rows of the page frame from the row {@code firstRow} of the partition on, in the index
     * direction, numbered from the start of the page frame. A count of the rows of the page frame from
     * {@link #getRowCount} must have succeeded.
     */
    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, long firstRow);

    Function getFunction();

    /**
     * The row the cursor returns after it skips {@code rowIndex} of the rows {@code rowLo} to {@code rowHi}, inclusive,
     * of the partition of the page frame. A count of those rows from {@link #getRowCount} must have shown that the key
     * has more than {@code rowIndex} of them.
     */
    long getRow(PageFrame pageFrame, long rowLo, long rowHi, long rowIndex);

    /**
     * The number of rows the cursor returns among the rows {@code rowLo} to {@code rowHi}, inclusive, of the partition
     * of the page frame, or any number above {@code maxCount} once the count passes it, or -1 when the factory cannot
     * tell without reading the rows.
     */
    long getRowCount(PageFrame pageFrame, long rowLo, long rowHi, long maxCount);
}
