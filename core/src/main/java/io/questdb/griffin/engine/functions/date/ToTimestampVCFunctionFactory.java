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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;

public class ToTimestampVCFunctionFactory implements FunctionFactory {
    private static final String NAME = "to_timestamp";

    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.TIMESTAMP_MICRO;
    }

    @Override
    public String getSignature() {
        return "to_timestamp(Ss)";
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        return isDeferrable(args, argPositions, ColumnType.TIMESTAMP_MICRO);
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        final CharSequence pattern = pattern(args.getQuick(1), argPositions);
        if (arg.isConstant()) {
            return evaluateConstant(arg, pattern, configuration.getDefaultDateLocale(), ColumnType.TIMESTAMP_MICRO);
        } else {
            return new Func(arg, pattern, configuration.getDefaultDateLocale(), ColumnType.TIMESTAMP_MICRO, NAME);
        }
    }

    protected static ConstantFunction evaluateConstant(Function arg, CharSequence pattern, DateLocale locale, int timestampType) {
        CharSequence value = arg.getStrA(null);
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        try {
            if (value != null) {
                DateFormat timestampFormat = driver.getTimestampDateFormatFactory().get(pattern);
                return new TimestampConstant(timestampFormat.parse(value, locale), timestampType);
            }
        } catch (NumericException ignore) {
        }

        return driver.getTimestampConstantNull();
    }

    /**
     * Whether a call with a non-constant value builds a parsing function, after the errors its construction raises
     * for the pattern.
     */
    static boolean isDeferrable(ObjList<Function> args, IntList argPositions, int timestampType) throws SqlException {
        ColumnType.getTimestampDriver(timestampType).getTimestampDateFormatFactory().get(pattern(args.getQuick(1), argPositions));
        return !args.getQuick(0).isConstant();
    }

    /**
     * The pattern a constant pattern argument spells; raises the error for a NULL pattern.
     */
    static CharSequence pattern(Function patternFunc, IntList argPositions) throws SqlException {
        final CharSequence pattern = patternFunc.getStrA(null);
        if (pattern == null) {
            throw SqlException.$(argPositions.getQuick(1), "pattern is required");
        }
        return pattern;
    }

    protected static final class Func extends TimestampFunction implements UnaryFunction {

        private final Function arg;
        private final DateLocale locale;
        private final String name;
        private final DateFormat timestampFormat;

        public Func(Function arg, CharSequence pattern, DateLocale locale, int timestampType, String name) {
            super(timestampType);
            this.arg = arg;
            this.timestampFormat = timestampDriver.getTimestampDateFormatFactory().get(pattern);
            this.locale = locale;
            this.name = name;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getTimestamp(Record rec) {
            CharSequence value = arg.getStrA(rec);
            try {
                if (value != null) {
                    return timestampFormat.parse(value, locale);
                }
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("(").val(arg).val(')');
        }
    }
}
