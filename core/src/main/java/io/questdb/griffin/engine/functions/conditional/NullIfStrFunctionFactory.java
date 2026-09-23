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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class NullIfStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "nullif(SS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new Func(args.getQuick(0), args.getQuick(1));
    }

    private static class Func extends StrFunction implements BinaryFunction {
        private final StringSink aliasSink = new StringSink();
        private final Function strFunc1;
        private final Function strFunc2;

        public Func(Function strFunc1, Function strFunc2) {
            this.strFunc1 = strFunc1;
            this.strFunc2 = strFunc2;
        }

        @Override
        public Function getLeft() {
            return strFunc1;
        }

        @Override
        public String getName() {
            return "nullif";
        }

        @Override
        public Function getRight() {
            return strFunc2;
        }

        // Both arguments may resolve through one symbol table, e.g. lag(s) and s::string over a
        // NOCACHE column, and a non-static table hands out a single view per A/B slot. When the
        // second read lands on the object the first returned, it has overwritten the first
        // value: copy the second out and read the first again. Each path touches only its own
        // slot, so a caller holding the other slot's value is not disturbed.
        @Override
        public CharSequence getStrA(Record rec) {
            CharSequence cs1 = strFunc1.getStrA(rec);
            if (cs1 == null) {
                return null;
            }
            CharSequence cs2 = strFunc2.getStrA(rec);
            if (cs1 == cs2) {
                cs2 = copyAlias(cs2);
                cs1 = strFunc1.getStrA(rec);
            }
            if (cs2 == null || !Chars.equals(cs1, cs2)) {
                return cs1;
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            CharSequence cs1 = strFunc1.getStrB(rec);
            if (cs1 == null) {
                return null;
            }
            CharSequence cs2 = strFunc2.getStrB(rec);
            if (cs1 == cs2) {
                cs2 = copyAlias(cs2);
                cs1 = strFunc1.getStrB(rec);
            }
            if (cs2 == null || !Chars.equals(cs1, cs2)) {
                return cs1;
            }
            return null;
        }

        private CharSequence copyAlias(CharSequence cs) {
            aliasSink.clear();
            aliasSink.put(cs);
            return aliasSink;
        }
    }
}
