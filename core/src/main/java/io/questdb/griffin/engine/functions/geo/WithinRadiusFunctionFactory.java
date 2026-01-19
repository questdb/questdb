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
 * Returns true if point (x, y) is within radius distance of (center_x, center_y), inclusive.
 * Uses Cartesian/Euclidean distance.
 * <p>
 * Signature: within_radius(x, y, center_x, center_y, radius)
 * <p>
 * Edge cases:
 * <ul>
 *   <li>Returns false if any input is NaN</li>
 *   <li>Returns true if point is exactly on the boundary (inclusive)</li>
 *   <li>Returns false if radius is negative</li>
 * </ul>
 */
@SuppressWarnings("unused")
public class WithinRadiusFunctionFactory implements FunctionFactory {
    // Threshold for NaN detection: values with (bits & 0x7FFFFFFFFFFFFFFF) > this are NaN
    private static final long INF_BITS = 0x7FF0000000000000L;

    @Override
    public String getSignature() {
        return "within_radius(DDDDD)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

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
        final Function centerXFunc = args.getQuick(2);
        final Function centerYFunc = args.getQuick(3);
        final Function radiusFunc = args.getQuick(4);

        // Optimization: if center and radius are constant, pre-validate and use optimized function
        if (centerXFunc.isConstant() && centerYFunc.isConstant() && radiusFunc.isConstant()) {
            final double centerX = centerXFunc.getDouble(null);
            final double centerY = centerYFunc.getDouble(null);
            final double radius = radiusFunc.getDouble(null);

            // Check for NaN or negative radius - if invalid, always return false
            if (Numbers.isNull(centerX) || Numbers.isNull(centerY) || Numbers.isNull(radius) || radius < 0) {
                return BooleanConstant.FALSE;
            }

            final double radiusSq = radius * radius;
            return new ConstRadiusGeoWithinRadiusFunction(xFunc, yFunc, centerX, centerY, radius, radiusSq);
        }

        return new GeoWithinRadiusFunction(xFunc, yFunc, centerXFunc, centerYFunc, radiusFunc);
    }

    /**
     * Branchless check if point (x, y) is within radius of (centerX, centerY).
     * Returns true if inside (inclusive), false if outside, NaN, or negative radius.
     */
    private static boolean isWithinRadius(double x, double y, double centerX, double centerY, double radiusSq) {
        final double dx = x - centerX;
        final double dy = y - centerY;
        final double diff = radiusSq - (dx * dx + dy * dy);

        // Convert to raw bits for branchless comparison
        final long bits = Double.doubleToRawLongBits(diff);

        // NaN check: A value is NaN iff (bits & 0x7FFFFFFFFFFFFFFF) > INF_BITS.
        // We compute INF_BITS - (bits & 0x7FFFFFFFFFFFFFFF), which is < 0 for NaN.
        final long nanCheck = INF_BITS - (bits & 0x7FFFFFFFFFFFFFFFL);

        // Point is inside iff diff >= 0 (bits >= 0) and not NaN (nanCheck >= 0).
        return (bits | nanCheck) >= 0;
    }

    /**
     * Optimized function for the common case where center and radius are constant.
     * Pre-validates inputs at construction and stores radiusSq.
     */
    private static class ConstRadiusGeoWithinRadiusFunction extends BooleanFunction {
        private final double centerX;
        private final double centerY;
        private final double radius;
        private final double radiusSq;
        private final Function xFunc;
        private final Function yFunc;

        public ConstRadiusGeoWithinRadiusFunction(
                Function xFunc,
                Function yFunc,
                double centerX,
                double centerY,
                double radius,
                double radiusSq
        ) {
            this.xFunc = xFunc;
            this.yFunc = yFunc;
            this.centerX = centerX;
            this.centerY = centerY;
            this.radius = radius;
            this.radiusSq = radiusSq;
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
            return isWithinRadius(xFunc.getDouble(rec), yFunc.getDouble(rec), centerX, centerY, radiusSq);
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
            if (other instanceof ConstRadiusGeoWithinRadiusFunction that) {
                return centerX == that.centerX
                        && centerY == that.centerY
                        && radius == that.radius
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
            sink.val("within_radius(")
                    .val(xFunc).val(',')
                    .val(yFunc).val(',')
                    .val(centerX).val(',')
                    .val(centerY).val(',')
                    .val(radius).val(')');
        }

        @Override
        public void toTop() {
            xFunc.toTop();
            yFunc.toTop();
        }
    }

    private static class GeoWithinRadiusFunction extends BooleanFunction {
        private final Function centerXFunc;
        private final Function centerYFunc;
        private final Function radiusFunc;
        private final Function xFunc;
        private final Function yFunc;

        public GeoWithinRadiusFunction(
                Function xFunc,
                Function yFunc,
                Function centerXFunc,
                Function centerYFunc,
                Function radiusFunc
        ) {
            this.xFunc = xFunc;
            this.yFunc = yFunc;
            this.centerXFunc = centerXFunc;
            this.centerYFunc = centerYFunc;
            this.radiusFunc = radiusFunc;
        }

        @Override
        public void close() {
            Misc.free(xFunc);
            Misc.free(yFunc);
            Misc.free(centerXFunc);
            Misc.free(centerYFunc);
            Misc.free(radiusFunc);
        }

        @Override
        public void cursorClosed() {
            xFunc.cursorClosed();
            yFunc.cursorClosed();
            centerXFunc.cursorClosed();
            centerYFunc.cursorClosed();
            radiusFunc.cursorClosed();
        }

        @Override
        public boolean getBool(Record rec) {
            final double radius = radiusFunc.getDouble(rec);
            // Negative radius check - must be done before squaring
            if (radius < 0) {
                return false;
            }
            return isWithinRadius(
                    xFunc.getDouble(rec),
                    yFunc.getDouble(rec),
                    centerXFunc.getDouble(rec),
                    centerYFunc.getDouble(rec),
                    radius * radius
            );
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            xFunc.init(symbolTableSource, executionContext);
            yFunc.init(symbolTableSource, executionContext);
            centerXFunc.init(symbolTableSource, executionContext);
            centerYFunc.init(symbolTableSource, executionContext);
            radiusFunc.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isConstant() {
            return xFunc.isConstant()
                    && yFunc.isConstant()
                    && centerXFunc.isConstant()
                    && centerYFunc.isConstant()
                    && radiusFunc.isConstant();
        }

        @Override
        public boolean isEquivalentTo(Function other) {
            if (this == other) {
                return true;
            }
            if (other instanceof GeoWithinRadiusFunction that) {
                return xFunc.isEquivalentTo(that.xFunc)
                        && yFunc.isEquivalentTo(that.yFunc)
                        && centerXFunc.isEquivalentTo(that.centerXFunc)
                        && centerYFunc.isEquivalentTo(that.centerYFunc)
                        && radiusFunc.isEquivalentTo(that.radiusFunc);
            }
            return false;
        }

        @Override
        public boolean isNonDeterministic() {
            return xFunc.isNonDeterministic()
                    || yFunc.isNonDeterministic()
                    || centerXFunc.isNonDeterministic()
                    || centerYFunc.isNonDeterministic()
                    || radiusFunc.isNonDeterministic();
        }

        @Override
        public boolean isRandom() {
            return xFunc.isRandom()
                    || yFunc.isRandom()
                    || centerXFunc.isRandom()
                    || centerYFunc.isRandom()
                    || radiusFunc.isRandom();
        }

        @Override
        public boolean isRuntimeConstant() {
            return (xFunc.isConstant() || xFunc.isRuntimeConstant())
                    && (yFunc.isConstant() || yFunc.isRuntimeConstant())
                    && (centerXFunc.isConstant() || centerXFunc.isRuntimeConstant())
                    && (centerYFunc.isConstant() || centerYFunc.isRuntimeConstant())
                    && (radiusFunc.isConstant() || radiusFunc.isRuntimeConstant());
        }

        @Override
        public boolean isThreadSafe() {
            return xFunc.isThreadSafe()
                    && yFunc.isThreadSafe()
                    && centerXFunc.isThreadSafe()
                    && centerYFunc.isThreadSafe()
                    && radiusFunc.isThreadSafe();
        }

        @Override
        public boolean shouldMemoize() {
            return xFunc.shouldMemoize()
                    || yFunc.shouldMemoize()
                    || centerXFunc.shouldMemoize()
                    || centerYFunc.shouldMemoize()
                    || radiusFunc.shouldMemoize();
        }

        @Override
        public boolean supportsParallelism() {
            return xFunc.supportsParallelism()
                    && yFunc.supportsParallelism()
                    && centerXFunc.supportsParallelism()
                    && centerYFunc.supportsParallelism()
                    && radiusFunc.supportsParallelism();
        }

        @Override
        public boolean supportsRandomAccess() {
            return xFunc.supportsRandomAccess()
                    && yFunc.supportsRandomAccess()
                    && centerXFunc.supportsRandomAccess()
                    && centerYFunc.supportsRandomAccess()
                    && radiusFunc.supportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("within_radius(")
                    .val(xFunc).val(',')
                    .val(yFunc).val(',')
                    .val(centerXFunc).val(',')
                    .val(centerYFunc).val(',')
                    .val(radiusFunc).val(')');
        }

        @Override
        public void toTop() {
            xFunc.toTop();
            yFunc.toTop();
            centerXFunc.toTop();
            centerYFunc.toTop();
            radiusFunc.toTop();
        }
    }
}
