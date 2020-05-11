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
import io.questdb.std.Long256;
import io.questdb.std.ObjList;

public class EqLong256FunctionFactory extends AbstractBooleanFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "=(HH)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) {
        return new Func(position, args.getQuick(0), args.getQuick(1), isNegated);
    }

    private class Func extends BooleanFunction implements BinaryFunction {
        private final boolean isNegated;
        private final Function left;
        private final Function right;

        public Func(int position, Function left, Function right, boolean isNegated) {
            super(position);
            this.left = left;
            this.right = right;
            this.isNegated = isNegated;
        }

        @Override
        public boolean getBool(Record rec) {
            final Long256 lv = left.getLong256A(rec);
            final Long256 rv = right.getLong256A(rec);
            return isNegated != lv.equals(rv);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }
}
