/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.Interval;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDayIntervalFunction extends IntervalFunction {
    protected final Interval interval = new Interval();

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return interval;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        final long now = executionContext.getNow();
        final long start = intervalStart(now);
        final long end = intervalEnd(start);
        interval.of(start, end);
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    protected long intervalEnd(long start) {
        return start + Timestamps.DAY_MICROS - 1;
    }

    protected long intervalStart(long now) {
        return Timestamps.floorDD(Timestamps.addDays(now, shiftFromToday()));
    }

    protected abstract int shiftFromToday();
}
