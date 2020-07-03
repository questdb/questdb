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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.AbstractBooleanFunctionFactory;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.ObjList;

public class EqCharCharFunctionFactory extends AbstractBooleanFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(AA)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        // there are optimisation opportunities
        // 1. when one of args is constant null comparison can boil down to checking
        //    length of non-constant (must be -1)
        // 2. when one of arguments is constant, save method call and use a field

        Function chrFunc1 = args.getQuick(0);
        Function chrFunc2 = args.getQuick(1);

        return new Func(position, chrFunc1, chrFunc2, isNegated);
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final boolean isNegated;
        private final Function chrFunc1;
        private final Function chrFunc2;

        public Func(int position, Function chrFunc1, Function chrFunc2, boolean isNegated) {
            super(position);
            this.chrFunc1 = chrFunc1;
            this.chrFunc2 = chrFunc2;
            this.isNegated = isNegated;
        }

        @Override
        public Function getLeft() {
            return chrFunc1;
        }

        @Override
        public Function getRight() {
            return chrFunc2;
        }

        @Override
        public boolean getBool(Record rec) {
            return isNegated != (chrFunc1.getChar(rec) == chrFunc2.getChar(rec));
        }
    }
}
