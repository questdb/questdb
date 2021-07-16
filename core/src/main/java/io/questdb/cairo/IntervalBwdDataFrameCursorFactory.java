/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.Misc;

public class IntervalBwdDataFrameCursorFactory extends AbstractDataFrameCursorFactory {
    private final IntervalBwdDataFrameCursor cursor;
    private final RuntimeIntrinsicIntervalModel intervals;

    public IntervalBwdDataFrameCursorFactory(
            CairoEngine engine,
            String tableName,
            long tableVersion,
            RuntimeIntrinsicIntervalModel intervals,
            int timestampIndex
    ) {
        super(engine, tableName, tableVersion);
        this.cursor = new IntervalBwdDataFrameCursor(intervals, timestampIndex);
        this.intervals = intervals;
    }

    @Override
    public DataFrameCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        cursor.of(getReader(executionContext.getCairoSecurityContext()), executionContext);
        return cursor;
    }

    @Override
    public void close() {
        Misc.free(intervals);
    }

}
