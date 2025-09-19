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
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.Utf8Sequence;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class VarcharToPgDateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_pg_date(Ø)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        return new ToPgDateFunction(arg);
    }

    public static final class ToPgDateFunction extends DateFunction implements UnaryFunction {

        private final Function arg;

        public ToPgDateFunction(Function arg) {
            this.arg = arg;
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
                    return DateFormatUtils.PG_DATE_FORMAT.parse(value.asAsciiCharSequence(), EN_LOCALE);
                }
            } catch (NumericException ignore) {
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public String getName() {
            return "to_pg_date";
        }
    }
}
