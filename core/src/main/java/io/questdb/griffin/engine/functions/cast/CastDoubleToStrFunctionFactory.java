/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class CastDoubleToStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Ds)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function intFunc = args.getQuick(0);
        if (intFunc.isConstant()) {
            final StringSink sink = Misc.getThreadLocalBuilder();
            sink.put(intFunc.getDouble(null), configuration.getDoubleToStrCastScale());
            return new StrConstant(Chars.toString(sink));
        }
        return new Func(args.getQuick(0), configuration.getDoubleToStrCastScale());
    }

    private static class Func extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final int scale;

        public Func(Function arg, int scale) {
            this.arg = arg;
            this.scale = scale;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStr(Record rec) {
            final double value = arg.getDouble(rec);
            if (Double.isNaN(value)) {
                return null;
            }
            sinkA.clear();
            sinkA.put(value, scale);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            final double value = arg.getDouble(rec);
            if (Double.isNaN(value)) {
                return null;
            }
            sinkB.clear();
            sinkB.put(value, scale);
            return sinkB;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            final double value = arg.getDouble(rec);
            if (Double.isNaN(value)) {
                return;
            }
            sink.put(value, scale);
        }
    }
}
