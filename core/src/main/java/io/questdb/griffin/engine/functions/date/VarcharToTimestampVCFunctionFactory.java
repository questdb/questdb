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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.Utf8Sequence;

public final class VarcharToTimestampVCFunctionFactory extends ToTimestampVCFunctionFactory {
    private final static String NAME = "to_timestamp";

    @Override
    public String getSignature() {
        return "to_timestamp(Øs)";
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
        final CharSequence pattern = args.getQuick(1).getStrA(null);
        if (pattern == null) {
            throw SqlException.$(argPositions.getQuick(1), "pattern is required");
        }
        DateLocale defaultDateLocale = configuration.getDefaultDateLocale();
        if (arg.isConstant()) {
            return evaluateConstant(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_MICRO);
        } else {
            if ("en".equals(defaultDateLocale.getName()) || (defaultDateLocale.getName() != null && defaultDateLocale.getName().startsWith("en-"))) {
                return new ToAsciiTimestampFunc(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_MICRO, NAME);
            }
            return new Func(arg, pattern, defaultDateLocale, ColumnType.TIMESTAMP_MICRO, NAME);
        }
    }

    protected static final class ToAsciiTimestampFunc extends TimestampFunction implements UnaryFunction {

        private final Function arg;
        private final DateLocale locale;
        private final String name;
        private final DateFormat timestampFormat;

        public ToAsciiTimestampFunc(Function arg, CharSequence pattern, DateLocale locale, int timestampType, String name) {
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
            Utf8Sequence value = arg.getVarcharA(rec);
            try {
                if (value != null && value.isAscii()) {
                    return timestampFormat.parse(value.asAsciiCharSequence(), locale);
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
