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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DoubleArrayMultiplyFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "*(D[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new MultiplyDoubleArrayFunction(configuration, args.getQuick(0), args.getQuick(1));
    }

    private static class MultiplyDoubleArrayFunction extends ArrayFunction implements BinaryFunction {

        private final DirectArrayView arrayOut;
        private final Function leftFn;
        private final Function rightFn;

        public MultiplyDoubleArrayFunction(CairoConfiguration configuration, Function leftFn, Function rightFn) {
            this.leftFn = leftFn;
            this.rightFn = rightFn;
            this.arrayOut = new DirectArrayView(configuration);
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            arrayOut.setType(type);
        }

        @Override
        public void close() {
            leftFn.close();
            rightFn.close();
            arrayOut.close();
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView left = leftFn.getArray(rec);
            ArrayView right = rightFn.getArray(rec);
            if (left.getDimCount() != 2 || right.getDimCount() != 2) {
                throw CairoException.nonCritical().put("arrays are not two-dimensional");
            }
            int commonDimLen = left.getDimLen(1);
            if (right.getDimLen(0) != commonDimLen) {
                throw CairoException.nonCritical().put("left array row length doesn't match right array column length");
            }
            int dimLen0 = left.getDimLen(0);
            int dimLen1 = right.getDimLen(1);
            int leftStride0 = left.getStride(0);
            int leftStride1 = left.getStride(1);
            int rightStride0 = right.getStride(0);
            int rightStride1 = right.getStride(1);
            arrayOut.clear();
            arrayOut.setDimLen(0, dimLen0);
            arrayOut.setDimLen(1, dimLen1);
            arrayOut.applyShape(0);
            int flatIndexOut = 0;
            for (int rowOut = 0; rowOut < dimLen0; rowOut++) {
                for (int colOut = 0; colOut < dimLen1; colOut++) {
                    double sum = 0;
                    for (int commonDim = 0; commonDim < commonDimLen; commonDim++) {
                        int flatIndexLeft = leftStride1 * commonDim + leftStride0 * rowOut;
                        int flatIndexRight = rightStride0 * commonDim + rightStride1 * colOut;
                        double elemLeft = left.flatView().getDouble(flatIndexLeft);
                        double elemRight = right.flatView().getDouble(flatIndexRight);
                        sum += elemLeft * elemRight;
                    }
                    arrayOut.putDouble(flatIndexOut++, sum);
                }
            }
            return arrayOut;
        }

        @Override
        public Function getLeft() {
            return leftFn;
        }

        @Override
        public String getName() {
            return "*";
        }

        @Override
        public Function getRight() {
            return rightFn;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
