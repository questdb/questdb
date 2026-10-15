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
 * The LIMIT count the parallel filter of a join's master applies, which the negative LIMIT over the join sets: a
 * negative count reads the last master rows, NULL all of them. Until the LIMIT sets a count, the filter applies the
 * count of the LIMIT itself, which EXPLAIN shows, and reads all master rows when that count is not negative.
 */
public final class MasterSuffixLimitFunction extends LongFunction implements UnaryFunction {
    private final Function count;
    private boolean hasRowCount;
    private AsyncFilteredNegativeLimitRecordCursor lastRows;
    private long rowCount;

    public MasterSuffixLimitFunction(Function count) {
        this.count = count;
    }

    public void clearRowCount() {
        hasRowCount = false;
    }

    @Override
    public Function getArg() {
        return count;
    }

    @Override
    public long getLong(Record rec) {
        if (hasRowCount) {
            return rowCount;
        }
        final long value = count.getLong(rec);
        return value < 0 ? value : Numbers.LONG_NULL;
    }

    /**
     * True when the filter, asked for the last {@code -rowCount} rows, returned all of them, so the master may hold more.
     * The cursor of a filter the join did not read reports no rows.
     */
    public boolean hasMoreRows(long rowCount) {
        return rowCount < 0 && rowCount != Numbers.LONG_NULL && lastRows.size() >= -rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
        hasRowCount = true;
    }

    /**
     * The filter's cursor for a negative count.
     */
    void of(AsyncFilteredNegativeLimitRecordCursor lastRows) {
        this.lastRows = lastRows;
    }
}
