/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.str;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.griffin.engine.functions.constants.NullConstant;
import com.questdb.griffin.engine.functions.constants.StrConstant;
import com.questdb.std.Numbers;
import com.questdb.std.ObjList;
import com.questdb.std.microtime.DateFormat;
import com.questdb.std.microtime.DateFormatCompiler;
import com.questdb.std.microtime.DateLocale;
import com.questdb.std.microtime.DateLocaleFactory;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class ToCharTimestampFunctionFactory implements FunctionFactory {

    private static final ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);
    private static final ThreadLocal<StringSink> tlSink = ThreadLocal.withInitial(StringSink::new);

    @Override
    public String getSignature() {
        return "to_char(Ns)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        Function fmt = args.getQuick(1);
        CharSequence format = fmt.getStr(null);
        if (format == null) {
            throw SqlException.$(fmt.getPosition(), "format must not be null");
        }

        DateFormat dateFormat = tlCompiler.get().compile(fmt.getStr(null));
        Function var = args.getQuick(0);
        if (var.isConstant()) {
            long value = var.getTimestamp(null);
            if (value == Numbers.LONG_NaN) {
                return new NullConstant(position);
            }

            StringSink sink = tlSink.get();
            sink.clear();
            dateFormat.format(value, DateLocaleFactory.INSTANCE.getDefaultDateLocale(), "Z", sink);
            return new StrConstant(position, sink);
        }

        return new ToCharDateFFunc(position, args.getQuick(0), dateFormat);
    }

    private static class ToCharDateFFunc extends StrFunction implements UnaryFunction {
        final Function arg;
        final DateFormat format;
        final DateLocale locale;
        final StringSink sink1;
        final StringSink sink2;

        public ToCharDateFFunc(int position, Function arg, DateFormat format) {
            super(position);
            this.arg = arg;
            this.format = format;
            locale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
            sink1 = new StringSink();
            sink2 = new StringSink();
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return toSink(rec, sink1);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return toSink(rec, sink2);
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NaN) {
                return;
            }
            toSink(value, sink);
        }

        @Override
        public int getStrLen(Record rec) {
            long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NaN) {
                return -1;
            }
            sink1.clear();
            toSink(value, sink1);
            return sink1.length();
        }

        @Nullable
        private CharSequence toSink(Record rec, StringSink sink) {
            final long value = arg.getTimestamp(rec);
            if (value == Numbers.LONG_NaN) {
                return null;
            }
            sink.clear();
            toSink(value, sink);
            return sink;
        }

        private void toSink(long value, CharSink sink) {
            format.format(value, locale, "Z", sink);
        }
    }
}
