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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.Interval;
import org.jetbrains.annotations.NotNull;

class OffsetIntervalFunctionFromRules extends IntervalFunction implements UnaryFunction {
    private final Interval interval = new Interval();
    private final Function intervalFunction;
    private final int multiplier;
    private final TimeZoneRules rules;

    public OffsetIntervalFunctionFromRules(Function intervalFunction, TimeZoneRules rules, int multiplier) {
        this.intervalFunction = intervalFunction;
        this.rules = rules;
        this.multiplier = multiplier;
    }

    @Override
    public Function getArg() {
        return intervalFunction;
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        long timestampLo = intervalFunction.getInterval(rec).getLo();
        long timestampHi = intervalFunction.getInterval(rec).getHi();
        interval.of(timestampLo + multiplier * rules.getOffset(timestampLo), timestampHi + multiplier * rules.getOffset(timestampHi));
        return interval;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("to_utc(").val(intervalFunction).val(',').val(multiplier).val(')');
    }
}