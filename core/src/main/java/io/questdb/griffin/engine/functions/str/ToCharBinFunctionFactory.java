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

package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public class ToCharBinFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_char(U)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new ToCharBinFunc(args.getQuick(0));
    }

    private static class ToCharBinFunc extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ToCharBinFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "to_char";
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
            BinarySequence sequence = arg.getBin(rec);
            if (sequence == null) {
                return -1;
            }

            int len = (int) sequence.length();
            // number of lines
            int incompleteLine = len % 16;
            int count = len / 16 * 57;
            if (incompleteLine > 0) {
                count += incompleteLine * 2 + incompleteLine + 8;
            } else {
                count--; // subtract extra line end we took into account
            }
            return count;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private CharSequence toSink(Record rec, StringSink sink) {
            final BinarySequence sequence = arg.getBin(rec);
            if (sequence == null) {
                return null;
            }
            sink.clear();
            Chars.toSink(sequence, sink);
            return sink;
        }
    }
}
