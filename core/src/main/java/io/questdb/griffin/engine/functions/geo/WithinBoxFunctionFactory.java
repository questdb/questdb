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

package io.questdb.griffin.engine.functions.geo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * Returns true if point (x, y) is within the bounding box defined by
 * (min_x, min_y) to (max_x, max_y), inclusive.
 * <p>
 * Signature: within_box(x, y, min_x, min_y, max_x, max_y)
 * <p>
 * Edge cases:
 * <ul>
 *   <li>Returns false if any input is NaN</li>
 *   <li>Returns true if point is exactly on the boundary (inclusive)</li>
 *   <li>Returns false if box is inverted (min > max)</li>
 *   <li>Returns false if point coordinate is -0.0 and box boundary is +0.0 (rare edge case)</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class WithinBoxFunctionFactory implements FunctionFactory {
    // Threshold for NaN detection: values with (bits & 0x7FFFFFFFFFFFFFFF) > this are NaN
    private static final long INF_BITS = 0x7FF0000000000000L;

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function xFunc = args.getQuick(0);
        final Function yFunc = args.getQuick(1);
        final Function minXFunc = args.getQuick(2);
        final Function minYFunc = args.getQuick(3);
        final Function maxXFunc = args.getQuick(4);
        final Function maxYFunc = args.getQuick(5);

        // Optimization: if bounding box is constant, pre-validate and use optimized function
        if (minXFunc.isConstant() && minYFunc.isConstant() && maxXFunc.isConstant() && maxYFunc.isConstant()) {
            final double minX = minXFunc.getDouble(null);
            final double minY = minYFunc.getDouble(null);
            final double maxX = maxXFunc.getDouble(null);
            final double maxY = maxYFunc.getDouble(null);

            // Check for NaN or inverted box - if invalid, always return false
            if (Numbers.isNull(minX) || Numbers.isNull(minY) || Numbers.isNull(maxX) || Numbers.isNull(maxY)
                    || minX > maxX || minY > maxY) {
                return BooleanConstant.FALSE;
            }

            return new ConstBoxGeoWithinBoxFunction(xFunc, yFunc, minX, minY, maxX, maxY);
        }

        return new GeoWithinBoxFunction(xFunc, yFunc, minXFunc, minYFunc, maxXFunc, maxYFunc);
    }

    @Override
    public String getSignature() {
        return "within_box(DDDDDD)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    /**
     * Branchless check if point (x, y) is within box [minX, maxX] x [minY, maxY].
     * Returns true if inside (inclusive), false if outside or any value is NaN.
     */
    private static boolean isWithinBox(double x, double y, double minX, double minY, double maxX, double maxY) {
        // Compute boundary differences
        final double dx1 = x - minX;
        final double dx2 = maxX - x;
        final double dy1 = y - minY;
        final double dy2 = maxY - y;

        // Convert to raw bits for branchless comparison
        final long b1 = Double.doubleToRawLongBits(dx1);
        final long b2 = Double.doubleToRawLongBits(dx2);
        final long b3 = Double.doubleToRawLongBits(dy1);
        final long b4 = Double.doubleToRawLongBits(dy2);

        // Sign check: OR propagates sign bit. Result < 0 if ANY difference is negative.
        final long signCheck = b1 | b2 | b3 | b4;

        // NaN check: A value is NaN iff (bits & 0x7FFFFFFFFFFFFFFF) > INF_BITS.
        // We compute INF_BITS - (bits & 0x7FFFFFFFFFFFFFFF), which is < 0 for NaN.
        // OR propagates negativity: result < 0 if ANY difference is NaN.
        final long n1 = INF_BITS - (b1 & 0x7FFFFFFFFFFFFFFFL);
        final long n2 = INF_BITS - (b2 & 0x7FFFFFFFFFFFFFFFL);
        final long n3 = INF_BITS - (b3 & 0x7FFFFFFFFFFFFFFFL);
        final long n4 = INF_BITS - (b4 & 0x7FFFFFFFFFFFFFFFL);
        final long nanCheck = n1 | n2 | n3 | n4;

        // Point is inside iff both checks pass (both >= 0).
        // OR propagates sign bit: (a | b) >= 0 iff both a >= 0 and b >= 0.
        return (signCheck | nanCheck) >= 0;
    }

    /**
     * Optimized function for the common case where bounding box is constant.
     * Pre-validates box at construction and stores bounds as primitives.
     */
    private static class ConstBoxGeoWithinBoxFunction extends BooleanFunction {
        private final double maxX;
        private final double maxY;
        private final double minX;
        private final double minY;
        private final Function xFunc;
        private final Function yFunc;

        public ConstBoxGeoWithinBoxFunction(
                Function xFunc,
                Function yFunc,
                double minX,
                double minY,
                double maxX,
                double maxY
        ) {
            this.xFunc = xFunc;
            this.yFunc = yFunc;
            this.minX = minX;
            this.minY = minY;
            this.maxX = maxX;
            this.maxY = maxY;
        }

        @Override
        public void close() {
            Misc.free(xFunc);
            Misc.free(yFunc);
        }

        @Override
        public void cursorClosed() {
            xFunc.cursorClosed();
            yFunc.cursorClosed();
        }

        @Override
        public boolean getBool(Record rec) {
            return isWithinBox(xFunc.getDouble(rec), yFunc.getDouble(rec), minX, minY, maxX, maxY);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            xFunc.init(symbolTableSource, executionContext);
            yFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return xFunc.isConstant() && yFunc.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof ConstBoxGeoWithinBoxFunction that) {
                return minX == that.minX
                        && minY == that.minY
                        && maxX == that.maxX
                        && maxY == that.maxY
                        && xFunc.isEquivalentTo(that.xFunc)
                        && yFunc.isEquivalentTo(that.yFunc);
            }
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return xFunc.isNonDeterministic() || yFunc.isNonDeterministic();
        }

        @Override
        public boolean isRandom() {
            return xFunc.isRandom() || yFunc.isRandom();
        }

        @Override
        public boolean isRuntimeConstant() {
            return (xFunc.isConstant() || xFunc.isRuntimeConstant())
                    && (yFunc.isConstant() || yFunc.isRuntimeConstant());
        }

        @Override
        public boolean isThreadSafe() {
            return xFunc.isThreadSafe() && yFunc.isThreadSafe();
        }

        @Override
        public boolean shouldMemoize() {
            return xFunc.shouldMemoize() || yFunc.shouldMemoize();
        }

        @Override
        public boolean supportsParallelism() {
            return xFunc.supportsParallelism() && yFunc.supportsParallelism();
        }

        @Override
        public boolean supportsRandomAccess() {
            return xFunc.supportsRandomAccess() && yFunc.supportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("within_box(")
                    .val(xFunc).val(',')
                    .val(yFunc).val(',')
                    .val(minX).val(',')
                    .val(minY).val(',')
                    .val(maxX).val(',')
                    .val(maxY).val(')');
        }

        @Override
        public void toTop() {
            xFunc.toTop();
            yFunc.toTop();
        }
    }

    private static class GeoWithinBoxFunction extends BooleanFunction {
        private final Function xFunc;
        private final Function yFunc;
        private final Function maxXFunc;
        private final Function maxYFunc;
        private final Function minXFunc;
        private final Function minYFunc;

        public GeoWithinBoxFunction(
                Function xFunc,
                Function yFunc,
                Function minXFunc,
                Function minYFunc,
                Function maxXFunc,
                Function maxYFunc
        ) {
            this.xFunc = xFunc;
            this.yFunc = yFunc;
            this.minXFunc = minXFunc;
            this.minYFunc = minYFunc;
            this.maxXFunc = maxXFunc;
            this.maxYFunc = maxYFunc;
        }

        @Override
        public void close() {
            Misc.free(xFunc);
            Misc.free(yFunc);
            Misc.free(minXFunc);
            Misc.free(minYFunc);
            Misc.free(maxXFunc);
            Misc.free(maxYFunc);
        }

        @Override
        public void cursorClosed() {
            xFunc.cursorClosed();
            yFunc.cursorClosed();
            minXFunc.cursorClosed();
            minYFunc.cursorClosed();
            maxXFunc.cursorClosed();
            maxYFunc.cursorClosed();
        }

        @Override
        public boolean getBool(Record rec) {
            return isWithinBox(
                    xFunc.getDouble(rec),
                    yFunc.getDouble(rec),
                    minXFunc.getDouble(rec),
                    minYFunc.getDouble(rec),
                    maxXFunc.getDouble(rec),
                    maxYFunc.getDouble(rec)
            );
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            xFunc.init(symbolTableSource, executionContext);
            yFunc.init(symbolTableSource, executionContext);
            minXFunc.init(symbolTableSource, executionContext);
            minYFunc.init(symbolTableSource, executionContext);
            maxXFunc.init(symbolTableSource, executionContext);
            maxYFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return xFunc.isConstant()
                    && yFunc.isConstant()
                    && minXFunc.isConstant()
                    && minYFunc.isConstant()
                    && maxXFunc.isConstant()
                    && maxYFunc.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof GeoWithinBoxFunction that) {
                return xFunc.isEquivalentTo(that.xFunc)
                        && yFunc.isEquivalentTo(that.yFunc)
                        && minXFunc.isEquivalentTo(that.minXFunc)
                        && minYFunc.isEquivalentTo(that.minYFunc)
                        && maxXFunc.isEquivalentTo(that.maxXFunc)
                        && maxYFunc.isEquivalentTo(that.maxYFunc);
            }
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return xFunc.isNonDeterministic()
                    || yFunc.isNonDeterministic()
                    || minXFunc.isNonDeterministic()
                    || minYFunc.isNonDeterministic()
                    || maxXFunc.isNonDeterministic()
                    || maxYFunc.isNonDeterministic();
        }

        @Override
        public boolean isRandom() {
            return xFunc.isRandom()
                    || yFunc.isRandom()
                    || minXFunc.isRandom()
                    || minYFunc.isRandom()
                    || maxXFunc.isRandom()
                    || maxYFunc.isRandom();
        }

        @Override
        public boolean isRuntimeConstant() {
            return (xFunc.isConstant() || xFunc.isRuntimeConstant())
                    && (yFunc.isConstant() || yFunc.isRuntimeConstant())
                    && (minXFunc.isConstant() || minXFunc.isRuntimeConstant())
                    && (minYFunc.isConstant() || minYFunc.isRuntimeConstant())
                    && (maxXFunc.isConstant() || maxXFunc.isRuntimeConstant())
                    && (maxYFunc.isConstant() || maxYFunc.isRuntimeConstant());
        }

        @Override
        public boolean isThreadSafe() {
            return xFunc.isThreadSafe()
                    && yFunc.isThreadSafe()
                    && minXFunc.isThreadSafe()
                    && minYFunc.isThreadSafe()
                    && maxXFunc.isThreadSafe()
                    && maxYFunc.isThreadSafe();
        }

        @Override
        public boolean shouldMemoize() {
            return xFunc.shouldMemoize()
                    || yFunc.shouldMemoize()
                    || minXFunc.shouldMemoize()
                    || minYFunc.shouldMemoize()
                    || maxXFunc.shouldMemoize()
                    || maxYFunc.shouldMemoize();
        }

        @Override
        public boolean supportsParallelism() {
            return xFunc.supportsParallelism()
                    && yFunc.supportsParallelism()
                    && minXFunc.supportsParallelism()
                    && minYFunc.supportsParallelism()
                    && maxXFunc.supportsParallelism()
                    && maxYFunc.supportsParallelism();
        }

        @Override
        public boolean supportsRandomAccess() {
            return xFunc.supportsRandomAccess()
                    && yFunc.supportsRandomAccess()
                    && minXFunc.supportsRandomAccess()
                    && minYFunc.supportsRandomAccess()
                    && maxXFunc.supportsRandomAccess()
                    && maxYFunc.supportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("within_box(")
                    .val(xFunc).val(',')
                    .val(yFunc).val(',')
                    .val(minXFunc).val(',')
                    .val(minYFunc).val(',')
                    .val(maxXFunc).val(',')
                    .val(maxYFunc).val(')');
        }

        @Override
        public void toTop() {
            xFunc.toTop();
            yFunc.toTop();
            minXFunc.toTop();
            minYFunc.toTop();
            maxXFunc.toTop();
            maxYFunc.toTop();
        }
    }
}
