/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class SizePrettyFunctionFactory implements FunctionFactory {

    public static final String SYMBOL = "size_pretty";

    // _, Kilo, Mega, Giga, Tera, Peta, Exa, Zetta (this last is out of range for a long)
    private static final char[] SCALE = {' ', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'};

    public static CharSequence toSizePretty(StringSink sink, long size) {
        int z = Numbers.msb(size) / 10;
        long scale = 1L << z * 10; // 1024 times z (z is index in SCALE)
        float value = (float) size / scale;
        Numbers.append(sink, value, 1);
        sink.put(' ').put(SCALE[z]).put('B');
        return sink;
    }

    @Override
    public String getSignature() {
        return SYMBOL + "(L)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPos, CairoConfiguration config, SqlExecutionContext context) {
        return new SizePretty(args.getQuick(0));
    }

    private static class SizePretty extends StrFunction implements UnaryFunction {
        private final StringSink sink = new StringSink();
        private final Function size;

        private SizePretty(Function size) {
            this.size = size;
        }

        @Override
        public Function getArg() {
            return size;
        }

        @Override
        public String getName() {
            return SYMBOL;
        }

        @Override
        public CharSequence getStr(Record rec) {
            long s = size.getLong(rec);
            sink.clear();
            return s != Long.MIN_VALUE ? toSizePretty(sink, s).toString() : null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }
    }
}
