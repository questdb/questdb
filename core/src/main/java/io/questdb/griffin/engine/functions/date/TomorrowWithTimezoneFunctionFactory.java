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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;

public class TomorrowWithTimezoneFunctionFactory implements FunctionFactory {
    private static final String SIGNATURE = "tomorrow(S)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function tzFunc = args.getQuick(0);
        if (tzFunc.isConstant() || tzFunc.isRuntimeConstant()) {
            return new RuntimeConstFunc(tzFunc);
        }
        return new Func(tzFunc);
    }

    private static class Func extends IntervalFunction implements UnaryFunction {
        private final Interval interval = new Interval();
        private final Function tzFunc;
        private long now;

        public Func(Function tzFunc) {
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getArg() {
            return tzFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            long nowWithTz = now;
            final CharSequence tz = tzFunc.getStrA(rec);
            if (tz != null) {
                try {
                    nowWithTz = Timestamps.toTimezone(nowWithTz, TimestampFormatUtils.EN_LOCALE, tz);
                } catch (NumericException e) {
                    return Interval.NULL;
                }
            }
            final long tomorrowStart = Timestamps.floorDD(Timestamps.addDays(nowWithTz, 1));
            final long tomorrowEnd = Timestamps.floorDD(Timestamps.addDays(nowWithTz, 2)) - 1;
            return interval.of(tomorrowStart, tomorrowEnd);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            now = executionContext.getNow();
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isReadThreadSafe() {
            return UnaryFunction.super.isReadThreadSafe();
        }

        @Override
        public boolean isRuntimeConstant() {
            return UnaryFunction.super.isRuntimeConstant();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }

    private static class RuntimeConstFunc extends IntervalFunction implements UnaryFunction {
        private final Interval interval = new Interval();
        private final Function tzFunc;

        public RuntimeConstFunc(Function tzFunc) {
            this.tzFunc = tzFunc;
        }

        @Override
        public Function getArg() {
            return tzFunc;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            return interval;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            long now = executionContext.getNow();
            final CharSequence tz = tzFunc.getStrA(null);
            if (tz != null) {
                try {
                    now = Timestamps.toTimezone(now, TimestampFormatUtils.EN_LOCALE, tz);
                } catch (NumericException e) {
                    interval.of(Interval.NULL.getLo(), Interval.NULL.getHi());
                    return;
                }
            }
            final long tomorrowStart = Timestamps.floorDD(Timestamps.addDays(now, 1));
            final long tomorrowEnd = Timestamps.floorDD(Timestamps.addDays(now, 2)) - 1;
            interval.of(tomorrowStart, tomorrowEnd);
        }

        @Override
        public boolean isConstant() {
            return false;
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }
}
