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
import io.questdb.std.Numbers;

/**
 * The LIMIT count the parallel filter of a join's master applies, which the negative LIMIT over the join controls: the
 * LIMIT's own count when it is negative and within the most rows the filter holds, else NULL, all master rows. The
 * LIMIT grows the count through the filter's cursor, which reads the longer suffix again under the open join, and
 * past the most rows the filter holds, has the filter read all master rows under a new cursor over the join.
 */
public final class MasterSuffixLimitFunction extends LongFunction implements UnaryFunction {
    private static final long ROW_COUNT_GROWTH = 64;
    private final Function count;
    private final long maxRowCount;
    private boolean isReadingAllRows;
    private AsyncFilteredNegativeLimitRecordCursor lastRows;

    public MasterSuffixLimitFunction(Function count, long maxRowCount) {
        this.count = count;
        this.maxRowCount = maxRowCount;
    }

    public void clearReadingAllRows() {
        isReadingAllRows = false;
    }

    @Override
    public Function getArg() {
        return count;
    }

    @Override
    public long getLong(Record rec) {
        if (isReadingAllRows) {
            return Numbers.LONG_NULL;
        }
        final long value = count.getLong(rec);
        return value < 0 && value >= -maxRowCount ? value : Numbers.LONG_NULL;
    }

    /**
     * True when the filter returned as many of the last master rows as it was asked for, and older master rows may pass
     * it. The cursor of a filter the join did not read, or that read all master rows, reports none.
     */
    public boolean hasMoreRows() {
        return lastRows.hasOlderRows();
    }

    /**
     * Rewinds the filter's cursor to read {@link #ROW_COUNT_GROWTH} times as many of the last master rows, up to the
     * most it holds. Returns false, rewinding nothing, when the filter already read that many.
     */
    public boolean readMoreRows() {
        final long rowLimit = lastRows.getRowLimit();
        if (rowLimit >= maxRowCount) {
            return false;
        }
        lastRows.readLastRows(Math.min(rowLimit * ROW_COUNT_GROWTH, maxRowCount));
        return true;
    }

    public void setReadingAllRows() {
        isReadingAllRows = true;
    }

    /**
     * The filter's cursor for a negative count.
     */
    void of(AsyncFilteredNegativeLimitRecordCursor lastRows) {
        this.lastRows = lastRows;
    }
}
