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
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
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
        return new MultiplyDoubleArrayFunction(
                configuration,
                args.getQuick(0),
                args.getQuick(1),
                argPositions.getQuick(0),
                argPositions.getQuick(1)
        );
    }

    private static class MultiplyDoubleArrayFunction extends ArrayFunction implements BinaryFunction {

        private final int leftArgPos;
        private final int rightArgPos;
        private DirectArray arrayOut;
        private Function leftArg;
        private Function rightArg;

        public MultiplyDoubleArrayFunction(
                CairoConfiguration configuration,
                Function leftArg,
                Function rightArg,
                int leftArgPos,
                int rightArgPos
        ) throws SqlException {
            this.leftArg = leftArg;
            this.rightArg = rightArg;
            this.arrayOut = new DirectArray(configuration);
            this.leftArgPos = leftArgPos;
            this.rightArgPos = rightArgPos;
            int nDimsLeft = ColumnType.decodeArrayDimensionality(leftArg.getType());
            int nDimsRight = ColumnType.decodeArrayDimensionality(rightArg.getType());
            if (nDimsLeft != nDimsRight) {
                throw SqlException.position(leftArgPos)
                        .put("left and right arrays have different number of dimensions [nDimsLeft=").put(nDimsLeft)
                        .put(", nDimsRight=").put(nDimsRight).put(']');
            }
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDimsLeft);
            arrayOut.setType(type);
        }

        @Override
        public void close() {
            this.leftArg = Misc.free(this.leftArg);
            this.rightArg = Misc.free(this.rightArg);
            this.arrayOut = Misc.free(this.arrayOut);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView left = leftArg.getArray(rec);
            ArrayView right = rightArg.getArray(rec);
            if (left.isNull() || right.isNull()) {
                arrayOut.ofNull();
                return arrayOut;
            }
            if (left.getType() != type) {
                throw CairoException.nonCritical().position(leftArgPos)
                        .put("unexpected array type [expected=").put(type)
                        .put(", actual=").put(left.getType())
                        .put(']');
            }
            if (right.getType() != type) {
                throw CairoException.nonCritical().position(rightArgPos)
                        .put("unexpected array type [expected=").put(type)
                        .put(", actual=").put(right.getType())
                        .put(']');
            }
            if (!left.shapeEquals(right)) {
                throw CairoException.nonCritical().position(leftArgPos)
                        .put("arrays have different shapes [leftShape=").put(left.shapeToString())
                        .put(", rightShape=").put(right.shapeToString())
                        .put(']');
            }
            arrayOut.setType(type);
            arrayOut.copyShapeFrom(left);
            arrayOut.applyShape();
            if (left.isVanilla() && right.isVanilla()) {
                FlatArrayView flatViewLeft = left.flatView();
                FlatArrayView flatViewRight = right.flatView();
                int elemCount = flatViewLeft.length();
                for (int i = 0; i < elemCount; i++) {
                    double leftVal = flatViewLeft.getDoubleAtAbsIndex(i);
                    double rightVal = flatViewRight.getDoubleAtAbsIndex(i);
                    arrayOut.putDouble(i, leftVal * rightVal);
                }
            } else {
                multiplyRecursive(0, left, 0, right, 0, arrayOut.startMemoryA());
            }
            return arrayOut;
        }

        @Override
        public Function getLeft() {
            return leftArg;
        }

        @Override
        public String getName() {
            return "*";
        }

        @Override
        public Function getRight() {
            return rightArg;
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        private void multiplyRecursive(
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
                    memOut.putDouble(left.getDouble(flatIndexLeft) * right.getDouble(flatIndexLeft));
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    multiplyRecursive(dim + 1, left, flatIndexLeft, right, flatIndexRight, memOut);
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            }
        }
    }
}
