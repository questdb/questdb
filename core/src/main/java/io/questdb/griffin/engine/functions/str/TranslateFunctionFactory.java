/*+*****************************************************************************
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Postgres-compatible translate(): any character in the input string that matches a
 * character in the {@code from} set is replaced by the corresponding character in the
 * {@code to} set (matched by position). If {@code from} is longer than {@code to}, the
 * extra {@code from} characters have no replacement and are removed from the result.
 * Returns NULL if any argument is NULL.
 */
public class TranslateFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "translate(SSS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new TranslateFunc(args.getQuick(0), args.getQuick(1), args.getQuick(2));
    }

    private static class TranslateFunc extends StrFunction implements TernaryFunction {
        private final Function fromFunc;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private final Function toFunc;
        private final Function valueFunc;

        public TranslateFunc(Function valueFunc, Function fromFunc, Function toFunc) {
            this.valueFunc = valueFunc;
            this.fromFunc = fromFunc;
            this.toFunc = toFunc;
        }

        @Override
        public Function getCenter() {
            return fromFunc;
        }

        @Override
        public Function getLeft() {
            return valueFunc;
        }

        @Override
        public String getName() {
            return "translate";
        }

        @Override
        public Function getRight() {
            return toFunc;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return translate(valueFunc.getStrA(rec), fromFunc.getStrA(rec), toFunc.getStrA(rec), sinkA);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return translate(valueFunc.getStrB(rec), fromFunc.getStrB(rec), toFunc.getStrB(rec), sinkB);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("translate(").val(valueFunc).val(',').val(fromFunc).val(',').val(toFunc).val(')');
        }

        private static int indexOf(CharSequence seq, char c) {
            for (int i = 0, n = seq.length(); i < n; i++) {
                if (seq.charAt(i) == c) {
                    return i;
                }
            }
            return -1;
        }

        @Nullable
        private static CharSequence translate(CharSequence value, CharSequence from, CharSequence to, StringSink sink) {
            if (value == null || from == null || to == null) {
                return null;
            }
            sink.clear();
            final int toLen = to.length();
            for (int i = 0, n = value.length(); i < n; i++) {
                final char c = value.charAt(i);
                final int idx = indexOf(from, c);
                if (idx < 0) {
                    sink.put(c);
                } else if (idx < toLen) {
                    sink.put(to.charAt(idx));
                }
                // idx >= toLen: character has no replacement, so it is removed
            }
            return sink;
        }
    }
}
