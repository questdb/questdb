/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.functions.StrFunction;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatCompiler;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

public class ToCharDateVCFunctionFactory implements FunctionFactory {

    private static final ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);

    @Override
    public String getSignature() {
        return "to_char(Ms)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new ToCharDateVCFFunc(position, args.getQuick(0), args.getQuick(1));
    }

    private static class ToCharDateVCFFunc extends StrFunction {
        final Function date;
        final DateFormat format;
        final DateLocale locale;
        final StringSink sink1;
        final StringSink sink2;

        public ToCharDateVCFFunc(int position, Function date, Function fmt) {
            super(position);
            this.date = date;
            format = tlCompiler.get().compile(fmt.getStr(null));
            locale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
            sink1 = new StringSink();
            sink2 = new StringSink();
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
            format.format(date.getDate(rec), locale, "Z", sink);
        }

        private CharSequence toSink(Record record, StringSink sink) {
            sink.clear();
            getStr(record, sink);
            return sink;
        }
    }
}
