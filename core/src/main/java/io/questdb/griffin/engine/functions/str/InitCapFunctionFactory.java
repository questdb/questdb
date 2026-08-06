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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Postgres-compatible initcap(): converts the first letter of each word to upper
 * case and the rest to lower case. Words are sequences of alphanumeric characters
 * separated by non-alphanumeric characters.
 */
public class InitCapFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "initcap(S)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new InitCapFunc(args.get(0));
    }

    private static class InitCapFunc extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public InitCapFunc(final Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "initcap";
        }

        @Override
        public CharSequence getStrA(final Record rec) {
            return toInitCap(arg.getStrA(rec), sinkA);
        }

        @Override
        public CharSequence getStrB(final Record rec) {
            return toInitCap(arg.getStrA(rec), sinkB);
        }

        @Override
        public int getStrLen(final Record rec) {
            return arg.getStrLen(rec);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Nullable
        private static CharSequence toInitCap(final CharSequence str, final StringSink sink) {
            if (str == null) {
                return null;
            }
            sink.clear();
            boolean prevAlphanumeric = false;
            for (int i = 0, n = str.length(); i < n; i++) {
                final char c = str.charAt(i);
                final boolean alphanumeric = Character.isLetterOrDigit(c);
                if (alphanumeric) {
                    sink.put(prevAlphanumeric ? Character.toLowerCase(c) : Character.toUpperCase(c));
                } else {
                    sink.put(c);
                }
                prevAlphanumeric = alphanumeric;
            }
            return sink;
        }
    }
}
