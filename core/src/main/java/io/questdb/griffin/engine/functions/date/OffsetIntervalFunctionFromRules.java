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

class OffsetIntervalFunctionFromRules extends IntervalFunction implements UnaryFunction {
    private final Function interval;
    private final int multiplier;
    private final TimeZoneRules rules;

    public OffsetIntervalFunctionFromRules(Function interval, TimeZoneRules rules, int multiplier) {
        this.interval = interval;
        this.rules = rules;
        this.multiplier = multiplier;
    }

    @Override
    public Function getArg() {
        return interval;
    }

    @Override
    public Interval getInterval(Record rec) {
        final long utcLo = interval.getInterval(rec).getLo();
        final long utcHi = interval.getInterval(rec).getHi();
        return new Interval((utcLo + multiplier * rules.getOffset(utcLo)), (utcHi + multiplier * rules.getOffset(utcHi)));
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("to_utc(").val(interval).val(',').val(multiplier).val(')');
    }
}