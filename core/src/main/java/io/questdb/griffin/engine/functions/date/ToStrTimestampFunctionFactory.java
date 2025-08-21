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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.Nullable;

public class ToStrTimestampFunctionFactory implements FunctionFactory {

    private static final ThreadLocal<StringSink> tlSink = ThreadLocal.withInitial(StringSink::new);

    @Override
    public String getSignature() {
        return "to_str(Ns)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function fmt = args.getQuick(1);
        CharSequence format = fmt.getStrA(null);
        if (format == null) {
            throw SqlException.$(argPositions.getQuick(1), "format must not be null");
        }
        Function var = args.getQuick(0);
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(var.getType()), ColumnType.TIMESTAMP_MICRO));
        DateFormat timestampFormat = driver.getTimestampDateFormatFactory().get(fmt.getStrA(null));
        if (var.isConstant()) {
            long value = var.getTimestamp(null);
            if (value == Numbers.LONG_NULL) {
                return StrConstant.NULL;
            }

            StringSink sink = tlSink.get();
            sink.clear();
            timestampFormat.format(value, configuration.getDefaultDateLocale(), "Z", sink);
            return new StrConstant(sink);
        }

        return new ToCharDateFFunc(args.getQuick(0), timestampFormat, configuration.getDefaultDateLocale());
    }

    private static class ToCharDateFFunc extends StrFunction implements UnaryFunction {
        final Function arg;
        final DateFormat format;
        final DateLocale locale;
        final StringSink sinkA;
        final StringSink sinkB;

        public ToCharDateFFunc(Function arg, DateFormat format, DateLocale timestampLocale) {
            this.arg = arg;
            this.format = format;
            locale = timestampLocale;
            sinkA = new StringSink();
            sinkB = new StringSink();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return toSink(rec, sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return toSink(rec, sinkB);
        }

        @Override
        public int getStrLen(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value != Numbers.LONG_NULL) {
                sinkA.clear();
                toSink(value, sinkA);
                return sinkA.length();
            }
            return -1;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("to_str(").val(arg).val(')');
        }

        @Nullable
        private CharSequence toSink(Record rec, StringSink sink) {
            final long value = arg.getTimestamp(rec);
            if (value != Numbers.LONG_NULL) {
                sink.clear();
                toSink(value, sink);
                return sink;
            }
            return null;
        }

        private void toSink(long value, Utf16Sink sink) {
            format.format(value, locale, "Z", sink);
        }
    }
}
