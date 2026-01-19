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

package io.questdb.griffin.engine.functions.bool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Returns true if point (x, y) is within the bounding box defined by
 * (min_x, min_y) to (max_x, max_y), inclusive.
 * <p>
 * Signature: geo_within_box(x, y, min_x, min_y, max_x, max_y)
 * <p>
 * Edge cases:
 * - Returns false if any input is NaN
 * - Returns true if point is exactly on the boundary (inclusive)
 * - Returns false if box is inverted (min > max)
 */
public class GeoWithinBoxFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "geo_within_box(DDDDDD)";
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
        return new GeoWithinBoxFunction(
                args.getQuick(0),
                args.getQuick(1),
                args.getQuick(2),
                args.getQuick(3),
                args.getQuick(4),
                args.getQuick(5)
        );
    }

    private static class GeoWithinBoxFunction extends BooleanFunction implements MultiArgFunction {
        // Threshold for NaN detection: values with (bits & 0x7FFFFFFFFFFFFFFF) > this are NaN
        private static final long INF_BITS = 0x7FF0000000000000L;
        private final ObjList<Function> args;
        private final Function maxXFunc;
        private final Function maxYFunc;
        private final Function minXFunc;
        private final Function minYFunc;
        private final Function xFunc;
        private final Function yFunc;

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
            this.args = new ObjList<>(6);
            this.args.add(xFunc);
            this.args.add(yFunc);
            this.args.add(minXFunc);
            this.args.add(minYFunc);
            this.args.add(maxXFunc);
            this.args.add(maxYFunc);
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public boolean getBool(Record rec) {
            final double x = xFunc.getDouble(rec);
            final double y = yFunc.getDouble(rec);
            final double minX = minXFunc.getDouble(rec);
            final double minY = minYFunc.getDouble(rec);
            final double maxX = maxXFunc.getDouble(rec);
            final double maxY = maxYFunc.getDouble(rec);

            // Compute boundary differences. Adding 0.0 normalizes -0.0 to +0.0
            // which is needed because -0.0 has sign bit 1 but represents zero.
            final double dx1 = (x - minX) + 0.0;
            final double dx2 = (maxX - x) + 0.0;
            final double dy1 = (y - minY) + 0.0;
            final double dy2 = (maxY - y) + 0.0;

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

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("geo_within_box(")
                    .val(xFunc).val(',')
                    .val(yFunc).val(',')
                    .val(minXFunc).val(',')
                    .val(minYFunc).val(',')
                    .val(maxXFunc).val(',')
                    .val(maxYFunc).val(')');
        }
    }
}
