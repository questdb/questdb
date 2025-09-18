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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;

public abstract class AbstractDayIntervalWithTimezoneFunction extends AbstractDayIntervalFunction implements UnaryFunction {
    protected final Function tzFunc;

    public AbstractDayIntervalWithTimezoneFunction(int intervalType, Function tzFunc) {
        super(intervalType);
        this.tzFunc = tzFunc;
    }

    @Override
    public Function getArg() {
        return tzFunc;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        UnaryFunction.super.init(symbolTableSource, executionContext);
        intervalType = executionContext.getIntervalFunctionType();
        this.timestampDriver = IntervalUtils.getTimestampDriverByIntervalType(intervalType);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isRuntimeConstant() {
        return UnaryFunction.super.isRuntimeConstant();
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    protected Interval calculateInterval(long now, CharSequence tz) {
        if (tz == null) {
            // no timezone, default to UTC
            final long start = timestampDriver.startOfDay(now, shiftFromToday());
            final long end = timestampDriver.endOfDay(start);
            return interval.of(start, end);
        }

        try {
            final long l = Dates.parseOffset(tz);
            if (l != Long.MIN_VALUE) {
                // the timezone is in numeric offset format
                final long offset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                final long nowWithTz = now + offset;
                final long startWithTz = timestampDriver.startOfDay(nowWithTz, shiftFromToday());
                final long endWithTz = timestampDriver.endOfDay(startWithTz);
                return interval.of(startWithTz - offset, endWithTz - offset);
            }

            // the timezone is a timezone name string
            final TimeZoneRules tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                    Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, tz.length())),
                    timestampDriver.getTZRuleResolution()
            );
            final long offset = tzRules.getOffset(now);
            final long nowWithTz = now + offset;
            // calculate date start and end with tz
            long startWithTz = timestampDriver.startOfDay(nowWithTz, shiftFromToday());
            long endWithTz = timestampDriver.endOfDay(startWithTz);
            return interval.of(timestampDriver.toUTC(startWithTz, tzRules), timestampDriver.toUTC(endWithTz, tzRules));
        } catch (NumericException e) {
            return interval.of(Interval.NULL.getLo(), Interval.NULL.getHi());
        }
    }
}
