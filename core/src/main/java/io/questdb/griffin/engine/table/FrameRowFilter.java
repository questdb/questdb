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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.std.DirectLongList;

/**
 * A filter that may select a native frame's rows column-wise, in one pass over the columns it reads,
 * as its {@code getBool()} would select them row by row. The parallel filter offers it each frame
 * before its row loop.
 */
public interface FrameRowFilter {

    /**
     * Appends to {@code rows} the indexes of the frame's rows, from 0 to {@code frameRowCount}, that
     * {@code getBool()} keeps, in ascending order; or appends nothing and returns false when it cannot
     * for this frame, which the row loop then filters.
     *
     * @param frameMemory   the frame
     * @param frameRowCount the frame's rows
     * @param rows          the selected rows, appended to
     * @return whether the frame's rows were selected
     */
    boolean filterFrame(PageFrameMemory frameMemory, long frameRowCount, DirectLongList rows);
}
