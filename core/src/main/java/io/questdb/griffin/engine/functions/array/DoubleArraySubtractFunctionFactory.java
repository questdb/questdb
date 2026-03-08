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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.WeakDimsArrayFunction;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArraySubtractFunctionFactory implements FunctionFactory {
    private static final String OPERATOR_NAME = "-";

    @Override
    public String getSignature() {
        return OPERATOR_NAME + "(D[]D[])";
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
                position
        );
    }

    private static class Func extends WeakDimsArrayFunction implements BinaryFunction {
        private final DirectArray arrayOut;
        private final Function leftArg;
        private final int leftArgPos;
        private final DerivedArrayView leftArgView = new DerivedArrayView();
        private final Function rightArg;
        private final DerivedArrayView rightArgView = new DerivedArrayView();

        private Func(
                CairoConfiguration configuration,
                Function leftArg,
                Function rightArg,
                int leftArgPos,
                int position
        ) {
            this.leftArg = leftArg;
            this.rightArg = rightArg;
            this.arrayOut = new DirectArray(configuration);
            this.leftArgPos = leftArgPos;
            final int dimsLeft = ColumnType.decodeWeakArrayDimensionality(leftArg.getType());
            final int dimsRight = ColumnType.decodeWeakArrayDimensionality(rightArg.getType());
            if (dimsLeft > 0 && dimsRight > 0) {
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, Math.max(dimsLeft, dimsRight));
                arrayOut.setType(type);
            } else {
                this.type = ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true);
            }
            this.position = position;
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

            if (left.shapeDiffers(right)) {
                DerivedArrayView.computeBroadcastShape(left, right, arrayOut.getShape(), leftArgPos);
                if (left.shapeDiffers(arrayOut)) {
                    leftArgView.of(left);
                    leftArgView.broadcast(arrayOut.getShape());
                    left = leftArgView;
                }
                if (right.shapeDiffers(arrayOut)) {
                    rightArgView.of(right);
                    rightArgView.broadcast(arrayOut.getShape());
                    right = rightArgView;
                }
            } else {
                arrayOut.copyShapeFrom(left);
            }

            arrayOut.applyShape();
            if (left.isVanilla() && right.isVanilla()) {
                for (int i = 0, n = left.getFlatViewLength(); i < n; i++) {
                    arrayOut.putDouble(i, left.getDouble(i) - right.getDouble(i));
                }
            } else {
                applyRecursive(0, left, 0, right, 0, arrayOut.startMemoryA());
            }
            return arrayOut;
        }

        @Override
        public Function getLeft() {
            return leftArg;
        }

        @Override
        public String getName() {
            return OPERATOR_NAME;
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
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, Math.max(dimsLeft, dimsRight));
            arrayOut.setType(type);

            validateAssignedType();
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private static void applyRecursive(
                int dim,
                ArrayView left,
                int flatIndexLeft,
                ArrayView right,
                int flatIndexRight,
                MemoryA memOut
        ) {
            final int count = left.getDimLen(dim);
            final int strideLeft = left.getStride(dim);
            final int strideRight = right.getStride(dim);
            final boolean atDeepestDim = dim == left.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    double leftVal = left.getDouble(flatIndexLeft);
                    double rightVal = right.getDouble(flatIndexRight);
                    memOut.putDouble(leftVal - rightVal);
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    applyRecursive(dim + 1, left, flatIndexLeft, right, flatIndexRight, memOut);
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            }
        }
    }
}
