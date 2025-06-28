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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayDotProductFunctionFactory implements FunctionFactory {
    private static final String FUNCTION_NAME = "dot_product";

    @Override
    public String getSignature() {
        return FUNCTION_NAME + "(D[]D[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new Func(args.getQuick(0), args.getQuick(1), argPositions.getQuick(0));
    }

    private static class Func extends DoubleFunction implements BinaryFunction {
        private final Function leftArg;
        private final int leftArgPos;
        private final Function rightArg;
        private double value;

        public Func(
                Function leftArg,
                Function rightArg,
                int leftArgPos
        ) throws SqlException {
            this.leftArg = leftArg;
            this.rightArg = rightArg;
            this.leftArgPos = leftArgPos;
            int nDimsLeft = ColumnType.decodeArrayDimensionality(leftArg.getType());
            int nDimsRight = ColumnType.decodeArrayDimensionality(rightArg.getType());
            if (nDimsLeft != nDimsRight) {
                throw SqlException.position(leftArgPos)
                        .put("arrays have different number of dimensions [nDimsLeft=").put(nDimsLeft)
                        .put(", nDimsRight=").put(nDimsRight).put(']');
            }
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView left = leftArg.getArray(rec);
            ArrayView right = rightArg.getArray(rec);
            if (left.isNull() || right.isNull()) {
                return 0d;
            }

            if (!left.shapeEquals(right)) {
                throw CairoException.nonCritical().position(leftArgPos)
                        .put("arrays have different shapes [leftShape=").put(left.shapeToString())
                        .put(", rightShape=").put(right.shapeToString())
                        .put(']');
            }
            value = 0d;
            if (left.isVanilla() && right.isVanilla()) {
                for (int i = 0, n = left.getFlatViewLength(); i < n; i++) {
                    double leftVal = left.getDouble(i);
                    double rightVal = right.getDouble(i);
                    if (Double.isFinite(leftVal) && Double.isFinite(rightVal)) {
                        value += leftVal * rightVal;
                    }
                }
            } else {
                applyRecursive(0, left, 0, right, 0);
            }
            return value;
        }

        @Override
        public Function getLeft() {
            return leftArg;
        }

        @Override
        public String getName() {
            return FUNCTION_NAME;
        }

        @Override
        public Function getRight() {
            return rightArg;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void applyRecursive(
                int dim,
                ArrayView left,
                int flatIndexLeft,
                ArrayView right,
                int flatIndexRight
        ) {
            final int count = left.getDimLen(dim);
            final int strideLeft = left.getStride(dim);
            final int strideRight = right.getStride(dim);
            final boolean atDeepestDim = dim == left.getDimCount() - 1;
            if (atDeepestDim) {
                for (int i = 0; i < count; i++) {
                    double leftVal = left.getDouble(flatIndexLeft);
                    double rightVal = right.getDouble(flatIndexLeft);
                    if (Double.isFinite(leftVal) && Double.isFinite(rightVal)) {
                        value += leftVal * rightVal;
                    }
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            } else {
                for (int i = 0; i < count; i++) {
                    applyRecursive(dim + 1, left, flatIndexLeft, right, flatIndexRight);
                    flatIndexLeft += strideLeft;
                    flatIndexRight += strideRight;
                }
            }
        }
    }
}
