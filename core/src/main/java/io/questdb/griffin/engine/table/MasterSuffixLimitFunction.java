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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.DirectLongList;
import io.questdb.std.Numbers;

/**
 * The LIMIT count the parallel filter of a join's master applies, which the negative LIMIT over the join controls: the
 * LIMIT's own count when it is negative and within the most rows the filter holds, else NULL, all master rows. The
 * LIMIT has the filter's cursor return older master rows under the open join until the join emits as many rows as it
 * selects, then return only the master rows those rows join.
 */
public final class MasterSuffixLimitFunction extends LongFunction implements UnaryFunction {
    private final Function count;
    private final long maxRowCount;
    private AsyncFilteredNegativeLimitRecordCursor lastRows;

    public MasterSuffixLimitFunction(Function count, long maxRowCount) {
        this.count = count;
        this.maxRowCount = maxRowCount;
    }

    @Override
    public Function getArg() {
        return count;
    }

    @Override
    public long getLong(Record rec) {
        final long value = count.getLong(rec);
        return value < 0 && value >= -maxRowCount ? value : Numbers.LONG_NULL;
    }

    /**
     * The master row the filter's cursor returned last.
     */
    public long getRowId() {
        return lastRows.getRowId();
    }

    /**
     * True when the filter's cursor returns the last {@code -lo} master rows in this execution, so the LIMIT can read
     * older ones and keep up to {@code -lo} of them.
     */
    public boolean isReadingLastRows(long lo) {
        return lo < 0 && lo >= -maxRowCount && lastRows != null && lastRows.isOpen();
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
     * The filter's cursor for a negative count.
     */
    void of(AsyncFilteredNegativeLimitRecordCursor lastRows) {
        this.lastRows = lastRows;
    }
}
