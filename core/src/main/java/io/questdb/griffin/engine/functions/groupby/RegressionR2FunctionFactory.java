/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class RegressionR2FunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_r2(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionR2Function(args.getQuick(0), args.getQuick(1));
    }

    private static class RegressionR2Function extends AbstractRegressionGroupByFunction {

        public RegressionR2Function(@NotNull Function arg0, @NotNull Function arg1) {
            super(arg0, arg1);
        }

        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 5);
            if (count <= 0) {
                return Double.NaN;
            }
            // SQL:2003 §10.9: when VAR_POP(X) = 0 (zero X variance) the result is NULL.
            // Covers count = 1 and all-identical-X cases.
            double sumX = rec.getDouble(valueIndex + 3);
            if (sumX == 0) {
                return Double.NaN;
            }
            // SQL:2003 §10.9: when VAR_POP(Y) = 0 and VAR_POP(X) != 0 the result is 1
            // (X varies, Y constant - a horizontal line fits perfectly).
            double sumY = rec.getDouble(valueIndex + 1);
            if (sumY == 0) {
                return 1.0;
            }
            double sumXY = rec.getDouble(valueIndex + 4);
            // Guard against intermediate overflow and underflow in the denominator product.
            // sumX * sumY can overflow to +Inf (inputs near ±1e153) or underflow to 0.0
            // (inputs near ±1e-150); both produce NaN from the subsequent division.
            // By Cauchy–Schwarz, sumXY² ≤ sumX·sumY, so when the product is finite the
            // numerator cannot overflow and the original formula is used unchanged.
            // When the product overflows or underflows we fall back to the split-sqrt form
            // sqrt(sumX) * sqrt(sumY), which keeps each factor well below overflow.
            // The result is clamped to ≤ 1.0 to absorb sub-ULP rounding drift from the
            // split-sqrt path (r² can be 1.0000000000000002 for perfect-correlation inputs).
            final double product = sumX * sumY;
            final double r2;
            if (product == 0.0 || Double.isInfinite(product)) {
                // Rare tail: fall back to split-sqrt to avoid Inf/NaN.
                final double r = sumXY / (Math.sqrt(sumX) * Math.sqrt(sumY));
                r2 = r * r;
            } else {
                r2 = (sumXY * sumXY) / product;
            }
            return r2 > 1.0 ? 1.0 : r2;
        }

        @Override
        public String getName() {
            return "regr_r2";
        }
    }
}
