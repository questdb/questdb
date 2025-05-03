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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.UnicodeParser;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

public class UniStrFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "unistr(S)";
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
        final int argPosition = argPositions.getQuick(0);
        if (arg.isConstant()) {
            CharSequence input = arg.getStrA(null);
            if (input == null) {
                return NullConstant.NULL;
            }
            StringSink sink = new StringSink();
            UnicodeParser.parse(input, argPosition, sink);
            return new ConstUniStrFunction(Chars.toString(sink));
        } else if (arg.isRuntimeConstant()) {
            return new RuntimeConstUniStrFunction(arg, argPosition);
        }
        return new UniStrFunction(arg, argPosition);
    }

    public static class ConstUniStrFunction extends StrFunction {
        private final String value;

        public ConstUniStrFunction(String value) {
            this.value = value;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return value;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return value;
        }

        @Override
        public int getStrLen(Record rec) {
            return value.length();
        }
    }

    public static class RuntimeConstUniStrFunction extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final int argPosition;
        private final StringSink sink = new StringSink();
        private StringSink result;

        public RuntimeConstUniStrFunction(Function arg, int argPosition) {
            this.arg = arg;
            this.argPosition = argPosition;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return result;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return result;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            CharSequence input = arg.getStrA(null);
            if (input == null) {
                result = null;
            } else {
                sink.clear();
                result = sink;
                UnicodeParser.parse(input, argPosition, sink);
            }
        }
    }

    public static class UniStrFunction extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final int argPosition;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public UniStrFunction(Function arg, int argPosition) {
            this.arg = arg;
            this.argPosition = argPosition;
        }

        @Override
        public Function getArg() {
            return null;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return sinkTheArg(arg.getStrA(rec), sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return sinkTheArg(arg.getStrB(rec), sinkB);
        }

        private CharSequence sinkTheArg(CharSequence input, StringSink sink) {
            if (input == null) {
                return null;
            }

            sink.clear();
            UnicodeParser.parse(input, argPosition, sink);
            return sink;
        }
    }
}
