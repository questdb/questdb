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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class NullIfCharFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "nullif(AA)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function chrFunc1 = args.getQuick(0);
        Function chrFunc2 = args.getQuick(1);

        return new Func(chrFunc1, chrFunc2);
    }

    private static class Func extends CharFunction implements BinaryFunction {
        private final Function chrFunc1;
        private final Function chrFunc2;

        public Func(Function chrFunc1, Function chrFunc2) {
            this.chrFunc1 = chrFunc1;
            this.chrFunc2 = chrFunc2;
        }

        @Override
        public char getChar(Record rec) {
            return chrFunc1.getChar(rec) == chrFunc2.getChar(rec) ? Character.MIN_VALUE : chrFunc1.getChar(rec);
        }

        @Override
        public Function getLeft() {
            return chrFunc1;
        }

        @Override
        public String getName() {
            return "nullif";
        }

        @Override
        public Function getRight() {
            return chrFunc2;
        }
    }
}
