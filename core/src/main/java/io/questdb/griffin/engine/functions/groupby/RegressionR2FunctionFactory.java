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

            // Protect against intermediate overflow/underflow in the denominator
            // sumX * sumY.  Mirror the fix applied to corr() in #7313.
            //
            // The Pearson denominator is sqrt(sumX * sumY).  When the product
            // would overflow to +Infinity or underflow to 0.0 while both factors
            // are non-zero, split into sqrt(sumX) * sqrt(sumY) to keep both
            // factors in the finite range.  The result is then squared to obtain
            // R² in [0, 1], and clamped to absorb the ~1 ULP rounding drift
            // possible in the split-sqrt path.
            double prod = sumX * sumY;
            boolean splitDenom = !Double.isFinite(prod) || (prod == 0.0 && sumX != 0.0 && sumY != 0.0);
            double denom = splitDenom ? Math.sqrt(sumX) * Math.sqrt(sumY) : Math.sqrt(prod);
            if (denom == 0.0) {
                return Double.NaN;
            }
            double r = sumXY / denom;
            if (r > 1.0) {
                r = 1.0;
            } else if (r < -1.0) {
                r = -1.0;
            }
            return r * r;
        }

        @Override
        public String getName() {
            return "regr_r2";
        }
    }
}
