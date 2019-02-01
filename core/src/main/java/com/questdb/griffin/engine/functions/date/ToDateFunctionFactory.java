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

package com.questdb.griffin.engine.functions.date;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.DateFunction;
import com.questdb.griffin.engine.functions.UnaryFunction;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatCompiler;
import com.questdb.std.time.DateLocale;
import com.questdb.std.time.DateLocaleFactory;

public class ToDateFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<DateFormatCompiler> tlCompiler = ThreadLocal.withInitial(DateFormatCompiler::new);

    @Override
    public String getSignature() {
        return "to_date(Ss)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        final Function arg = args.getQuick(0);
        final CharSequence pattern = args.getQuick(1).getStr(null);
        if (pattern == null) {
            throw SqlException.$(args.getQuick(1).getPosition(), "pattern is required");
        }
        return new Func(position, arg, tlCompiler.get().compile(pattern), DateLocaleFactory.INSTANCE.getDefaultDateLocale());
    }

    private static final class Func extends DateFunction implements UnaryFunction {

        private final Function arg;
        private final DateFormat dateFormat;
        private final DateLocale locale;

        public Func(int position, Function arg, DateFormat dateFormat, DateLocale locale) {
            super(position);
            this.arg = arg;
            this.dateFormat = dateFormat;
            this.locale = locale;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getDate(Record rec) {
            CharSequence value = arg.getStr(rec);
            try {
                if (value != null) {
                    return dateFormat.parse(value, locale);
                }
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NaN;
        }
    }
}
