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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleMatrixMultiplyFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "matmul(D[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(
                configuration,
                args.getQuick(0),
                args.getQuick(1),
                argPositions.getQuick(0),
                argPositions.getQuick(1)
        );
    }

    private static class Func extends ArrayFunction implements BinaryFunction {
        private final DirectArray arrayOut;
        private final Function leftArg;
        private final int leftArgPos;
        private final Function rightArg;
        private final int rightArgPos;
        private DerivedArrayView leftDerived;
        private DerivedArrayView rightDerived;

        public Func(
                CairoConfiguration configuration,
                Function leftArg,
                Function rightArg,
                int leftArgPos,
                int rightArgPos
        ) {
            try {
                this.leftArg = leftArg;
                this.rightArg = rightArg;
                this.arrayOut = new DirectArray(configuration);
                this.leftArgPos = leftArgPos;
                this.rightArgPos = rightArgPos;
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 2);
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            BinaryFunction.super.close();
            Misc.free(arrayOut);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView left = leftArg.getArray(rec);
            ArrayView right = rightArg.getArray(rec);
            if (left.isNull() || right.isNull()) {
                arrayOut.ofNull();
                return arrayOut;
            }
            if (leftDerived != null) {
                leftDerived.of(left);
                leftDerived.prependDimensions(1);
                left = leftDerived;
            }
            if (rightDerived != null) {
                rightDerived.of(right);
                rightDerived.appendDimensions(1);
                right = rightDerived;
            }
            int commonDimLen = left.getDimLen(1);
            if (right.getDimLen(0) != commonDimLen) {
                throw CairoException.nonCritical().position(leftArgPos)
                        .put("left array row length doesn't match right array column length ")
                        .put("[leftRowLen=").put(commonDimLen)
                        .put(", rightColLen=").put(right.getDimLen(0))
                        .put(']');
            }
            int outRowCount = left.getDimLen(0);
            int outColCount = right.getDimLen(1);
            int leftStride0 = left.getStride(0);
            int leftStride1 = left.getStride(1);
            int rightStride0 = right.getStride(0);
            int rightStride1 = right.getStride(1);

            arrayOut.setType(type);
            arrayOut.setDimLen(0, outRowCount);
            arrayOut.setDimLen(1, outColCount);
            arrayOut.applyShape(leftArgPos);
            MemoryA memOut = arrayOut.startMemoryA();
            for (int rowOut = 0; rowOut < outRowCount; rowOut++) {
                for (int colOut = 0; colOut < outColCount; colOut++) {
                    double sum = 0;
                    for (int commonDim = 0; commonDim < commonDimLen; commonDim++) {
                        int leftFlatIndex = leftStride0 * rowOut + leftStride1 * commonDim;
                        int rightFlatIndex = rightStride0 * commonDim + rightStride1 * colOut;
                        sum += left.getDouble(leftFlatIndex) * right.getDouble(rightFlatIndex);
                    }
                    memOut.putDouble(sum);
                }
            }
            return arrayOut;
        }

        @Override
        public Function getLeft() {
            return leftArg;
        }

        @Override
        public String getName() {
            return "matmul";
        }

        @Override
        public Function getRight() {
            return rightArg;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            // left/right argument may be a bind var, i.e. have weak dimensionality,
            // so that the number of dimensions is only available at init() time
            final int dimsLeft = ColumnType.decodeArrayDimensionality(leftArg.getType());
            final int dimsRight = ColumnType.decodeArrayDimensionality(rightArg.getType());
            if (dimsLeft == 1) {
                if (leftDerived == null) {
                    leftDerived = new DerivedArrayView();
                }
            } else if (dimsLeft == 2) {
                leftDerived = null;
            } else {
                throw SqlException.position(leftArgPos).put("left array is not one or two-dimensional");
            }

            if (dimsRight == 1) {
                if (rightDerived == null) {
                    rightDerived = new DerivedArrayView();
                }
            } else if (dimsRight == 2) {
                rightDerived = null;
            } else {
                throw SqlException.position(rightArgPos).put("right array is not one or two-dimensional");
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName()).val('(').val(leftArg).val(", ").val(rightArg).val(')');
        }
    }
}
