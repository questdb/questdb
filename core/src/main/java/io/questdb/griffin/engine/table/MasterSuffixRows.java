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

import io.questdb.std.DirectLongList;

/**
 * The master rows of a join that a negative LIMIT over the join reads through the join's parallel master filter. The
 * LIMIT has the filter's cursor return older master rows under the open join until the join emits as many rows as it
 * selects, then return only the master rows those rows join.
 */
public final class MasterSuffixRows {
    private final long maxRowCount;
    private AsyncFilteredNegativeLimitRecordCursor lastRows;

    public MasterSuffixRows(long maxRowCount) {
        this.maxRowCount = maxRowCount;
    }

    /**
     * The master row the filter's cursor returned last.
     */
    public long getRowId() {
        return lastRows.getRowId();
    }

    /**
     * True when the filter's cursor returns the last {@code -lo} master rows in this execution and the join read them,
     * so the LIMIT can read older ones and keep up to {@code -lo} of them. A join that read none, over an empty slave,
     * emits no rows whichever rows its master holds.
     */
    public boolean isReadingLastRows(long lo) {
        return isWithinFilter(lo) && lastRows != null && lastRows.isOpen() && lastRows.isRead();
    }

    /**
     * Has the filter's cursor return, from its next toTop(), master rows before the ones it returned. Returns false,
     * changing nothing, when no older master row may pass the filter.
     */
    public boolean readOlderRows() {
        return lastRows.readOlderRows();
    }

    /**
     * Has the filter's cursor return, from its next toTop(), the listed master rows, which the list holds newest first.
     */
    public void readRows(DirectLongList rowIds) {
        lastRows.readRows(rowIds);
    }

    /**
     * True when the LIMIT count is negative and within the most rows the filter holds.
     */
    boolean isWithinFilter(long count) {
        return count < 0 && count >= -maxRowCount;
    }

    /**
     * The filter's cursor for a negative count.
     */
    void of(AsyncFilteredNegativeLimitRecordCursor lastRows) {
        this.lastRows = lastRows;
    }
}
