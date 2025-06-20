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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class DoubleArraySumFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "arraySum";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[])";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        return new Func(args.getQuick(0));
    }

    static class Func extends DoubleFunction implements UnaryFunction {

        protected final Function arrayArg;

        Func(Function arrayArg) {
            this.arrayArg = arrayArg;
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView view = arrayArg.getArray(rec);
            if (view.isNull() || view.isEmpty()) {
                return 0d;
            }
            if (view.isVanilla()) {
                double res = view.flatView().sumDouble(view.getFlatViewOffset(), view.getFlatViewLength());
                return Numbers.isNull(res) ? 0 : res;
            } else {
                return sumDouble(view, 0, 0, 0d);
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(FUNCTION_NAME).val('(').val(arrayArg).val(')');
        }

        private static double sumDouble(ArrayView view, int dim, int flatIndex, double value) {
            final int count = view.getDimLen(dim);
            final int stride = view.getStride(dim);
            final boolean atDeepestDim = dim == view.getDimCount() - 1;
            if (atDeepestDim) {
                double sum = 0;
                for (int i = 0; i < count; i++) {
                    double v = view.getDouble(flatIndex);
                    if (Numbers.isFinite(v)) {
                        sum += v;
                    }
                    flatIndex += stride;
                }
                return sum;
            } else {
                for (int i = 0; i < count; i++) {
                    value += sumDouble(view, dim + 1, flatIndex, value);
                    flatIndex += stride;
                }
                return value;
            }
        }
    }
}
