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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class AddIntFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "+(II)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new AddIntFunc(args.getQuick(0), args.getQuick(1));
    }

    private static class AddIntFunc extends IntFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddIntFunc(Function left, Function right) {
            super();
            this.left = left;
            this.right = right;
        }

        @Override
        public int getInt(Record rec) {
            final int left = this.left.getInt(rec);
            final int right = this.right.getInt(rec);

            if (left == Numbers.INT_NaN || right == Numbers.INT_NaN) {
                return Numbers.INT_NaN;
            }

            return left + right;
        }

        @Override
        public boolean isConstant() {
            return left.isConstant() && right.isConstant()
                    || (left.isConstant() && left.getInt(null) == Numbers.INT_NaN)
                    || (right.isConstant() && right.getInt(null) == Numbers.INT_NaN);
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
