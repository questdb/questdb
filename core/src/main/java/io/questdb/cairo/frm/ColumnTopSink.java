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

package io.questdb.cairo.frm;

/**
 * Where a writable {@link Frame} reports a column's new top, instead of writing straight into the {@code
 * ColumnVersionWriter} every worker thread shares and which is not thread safe.
 */
public interface ColumnTopSink {

    /**
     * Applies everything {@link #setColumnTop} staged, and is the ONLY place a thread-safe implementation may touch
     * shared, structurally mutable state.
     */
    default void commitColumnTops() {
    }

    /**
     * Whether {@link #setColumnTop} may run concurrently, one thread per DISTINCT column index, once {@link
     * #ofColumnCount} has sized this sink.
     */
    default boolean isThreadSafe() {
        return false;
    }

    /**
     * Sizes this sink for a frame of {@code columnCount} columns and drops whatever a previous frame left in it.
     */
    default void ofColumnCount(int columnCount) {
    }

    /**
     * Reports one column's top.
     */
    void setColumnTop(int columnIndex, long columnTop);
}
