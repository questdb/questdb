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
 * A row cursor factory whose cursors emit the rows of a page frame key by key, in the key order a query requires. The
 * record cursor reads such a factory key by key over all page frames, so the keys stay ordered when the scan reads
 * more than one page frame.
 */
interface KeyOrderedRowCursorFactory extends RowCursorFactory {

    /**
     * The cursor over the rows in the page frame of the keys at {@code keyLo}, inclusive, to {@code keyHi}, exclusive,
     * in key order.
     */
    RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi);

    /**
     * The number of keys, known once the cursors are prepared.
     */
    int getKeyCount();

    /**
     * The number of rows of the keys at {@code keyLo}, inclusive, to {@code keyHi}, exclusive, among the rows
     * {@code rowLo} to {@code rowHi}, inclusive, of the partition of the page frame, or -1 when the factory cannot tell
     * without reading the rows.
     */
    long getRowCount(PageFrame pageFrame, int keyLo, int keyHi, long rowLo, long rowHi);
}
