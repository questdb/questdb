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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import org.jetbrains.annotations.NotNull;

/**
 * Converts interval endpoint values from one timestamp precision to another
 * (e.g., microsecond to nanosecond) during UNION / UNION ALL / set operations.
 */
public class CastIntervalToIntervalFunction extends IntervalFunction implements CastFunction {
    private final Function arg;
    private final TimestampDriver driver;
    private final int fromTimestampType;
    private final Interval interval = new Interval();

    public CastIntervalToIntervalFunction(Function arg, int toIntervalType) {
        super(toIntervalType);
        this.arg = arg;
        this.fromTimestampType = IntervalUtils.getTimestampTypeByIntervalType(arg.getType());
        this.driver = IntervalUtils.getTimestampDriverByIntervalType(toIntervalType);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        Interval src = arg.getInterval(rec);
        if (Interval.NULL.equals(src)) {
            return Interval.NULL;
        }
        long lo = driver.from(src.getLo(), fromTimestampType);
        long hi = driver.from(src.getHi(), fromTimestampType);
        return interval.of(lo, hi);
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(arg).val("::interval");
    }
}

