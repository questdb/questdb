/*******************************************************************************
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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.str.Utf8Sequence;

public class VarcharToDateFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "to_date(Øs)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function arg = args.getQuick(0);
        final CharSequence pattern = args.getQuick(1).getStrA(null);
        if (pattern == null) {
            throw SqlException.$(argPositions.getQuick(1), "pattern is required");
        }
        DateLocale defaultDateLocale = configuration.getDefaultDateLocale();
        if ("en".equals(defaultDateLocale.getName()) || (defaultDateLocale.getName() != null && defaultDateLocale.getName().startsWith("en-"))) {
            return new ToAsciiDateFunction(arg, DateFormatFactory.INSTANCE.get(pattern), defaultDateLocale, pattern);
        }
        return new ToDateFunction(arg, DateFormatFactory.INSTANCE.get(pattern), defaultDateLocale, pattern);
    }

    private static final class ToAsciiDateFunction extends DateFunction implements UnaryFunction {

        private final Function arg;
        private final DateFormat dateFormat;
        private final DateLocale locale;
        private final CharSequence pattern;

        public ToAsciiDateFunction(Function arg, DateFormat dateFormat, DateLocale locale, CharSequence pattern) {
            this.arg = arg;
            this.dateFormat = dateFormat;
            this.locale = locale;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDate(Record rec) {
            Utf8Sequence value = arg.getVarcharA(rec);
            try {
                if (value != null && value.isAscii()) {
                    return dateFormat.parse(value.asAsciiCharSequence(), locale);
                }
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("to_date(").val(arg).val(',').val(pattern).val(')');
        }
    }

    private static final class ToDateFunction extends DateFunction implements UnaryFunction {

        private final Function arg;
        private final DateFormat dateFormat;
        private final DateLocale locale;
        private final CharSequence pattern;

        public ToDateFunction(Function arg, DateFormat dateFormat, DateLocale locale, CharSequence pattern) {
            this.arg = arg;
            this.dateFormat = dateFormat;
            this.locale = locale;
            this.pattern = pattern;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDate(Record rec) {
            CharSequence value = arg.getStrA(rec);
            try {
                return dateFormat.parse(value, locale);
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("to_date(").val(arg).val(',').val(pattern).val(')');
        }
    }
}
